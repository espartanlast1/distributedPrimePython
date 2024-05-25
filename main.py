import socket
import threading
import struct
import time
import psutil  # For getting RAM and CPU info
import multiprocessing
from prettytable import PrettyTable
import platform
import asyncio
from concurrent.futures import ProcessPoolExecutor

BROADCAST_PORT = 12345
TCP_PORT = 54321
INITIAL_BATCH_SIZE = 1000000  # Initial batch size
MAX_BATCH_SIZE = 10000000  # Maximum batch size
DISCOVERY_INTERVAL = 1  # Interval for discovering slaves
PRIMES_FILE = 'primes.txt'
HANDSHAKE_PORT = 54322

slaves = {}
lock = threading.Lock()
start_calculations = threading.Event()
server_calculations = False  # New flag to determine if server should perform calculations


def get_ip_address():
    system = platform.system()
    if system == 'Darwin':
        import netifaces as ni
        ni.ifaddresses('en0')
        ip = ni.ifaddresses('en0')[ni.AF_INET][0]['addr']
    elif system == 'Windows':
        hostname = socket.gethostname()
        ip = socket.gethostbyname(hostname)
    else:
        ip = socket.gethostbyname(socket.gethostname())
    return ip


def sieve_of_eratosthenes(start, end):
    sieve = [True] * (end + 1)
    sieve[0] = sieve[1] = False  # 0 and 1 are not primes
    for num in range(2, int(end ** 0.5) + 1):
        if sieve[num]:
            sieve[num * num:end + 1:num] = [False] * len(range(num * num, end + 1, num))
    return [num for num in range(start, end + 1) if sieve[num]]


def broadcast_presence():
    ip_address = get_ip_address()
    cores = multiprocessing.cpu_count()
    ram = psutil.virtual_memory().total
    message = f'SLAVE:{ip_address}:{cores}:{ram}'.encode('utf-8')
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        while True:
            s.sendto(message, ('<broadcast>', BROADCAST_PORT))
            time.sleep(DISCOVERY_INTERVAL)


def handle_master_connection(conn):
    try:
        data = conn.recv(1024)
        if data:
            start, end = struct.unpack('!QQ', data)
            print(f"Received range: {start} to {end}")

            # Process the range using multiprocessing
            start_time = time.time()
            with multiprocessing.Pool() as pool:
                step = (end - start) // multiprocessing.cpu_count() + 1
                tasks = [(max(start, i), min(end, i + step - 1)) for i in range(start, end + 1, step)]
                results = pool.starmap(sieve_of_eratosthenes, tasks)
                primes = [prime for sublist in results for prime in sublist]
            end_time = time.time()
            execution_time = end_time - start_time

            prime_count = len(primes)
            result = struct.pack('!QQ', prime_count, int(execution_time * 1000))  # Send execution time in milliseconds
            conn.sendall(result)
    finally:
        conn.close()


def start_slave_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', TCP_PORT))
        s.listen()
        print("Slave server listening for master connections...", end='\r')
        while True:
            conn, addr = s.accept()
            print(f"Accepted connection from {addr}")
            threading.Thread(target=handle_master_connection, args=(conn,)).start()


def respond_to_handshake():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(('', HANDSHAKE_PORT))
        while True:
            data, addr = s.recvfrom(1024)
            if data.decode('utf-8') == 'HANDSHAKE':
                ip_address = get_ip_address()
                cores = multiprocessing.cpu_count()
                ram = psutil.virtual_memory().total
                message = f'SLAVE:{ip_address}:{cores}:{ram}'
                s.sendto(message.encode('utf-8'), addr)
                print(f"Responded to handshake with {addr}")


def discover_slaves():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(2)  # Add timeout to avoid blocking indefinitely
        while not start_calculations.is_set():
            try:
                s.sendto(b'HANDSHAKE', ('<broadcast>', HANDSHAKE_PORT))
                print("Sent handshake request", end='\r')
                data, addr = s.recvfrom(1024)
                message = data.decode('utf-8')
                print(f"Received response from {addr}: {message}")
                if message.startswith('SLAVE:'):
                    parts = message.split(':')
                    slave_ip = parts[1]
                    cores = int(parts[2])
                    ram = int(parts[3])
                    with lock:
                        if slave_ip not in slaves:
                            slaves[slave_ip] = {
                                'name': f'Slave-{len(slaves) + 1}',
                                'cores': cores,
                                'ram': ram,
                                'execution_time': None,
                                'batch_size': INITIAL_BATCH_SIZE
                            }
                            print_cluster_info()  # Update the cluster info immediately when a new slave is discovered
            except socket.timeout:
                pass
            time.sleep(DISCOVERY_INTERVAL)


async def assign_ranges_to_slave(slave_ip, start, end):
    with open(PRIMES_FILE, 'a') as f:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((slave_ip, TCP_PORT))
                message = struct.pack('!QQ', start, end)
                s.sendall(message)
                print(f"Sent range {start} to {end} to {slaves[slave_ip]['name']}")

                data = s.recv(1024)
                prime_count, execution_time = struct.unpack('!QQ', data)
                slaves[slave_ip]['execution_time'] = execution_time
                print(
                    f"{slaves[slave_ip]['name']} found {prime_count} primes in range {start} to {end} in {execution_time / 1000.0} seconds")

                if execution_time < 1000 and slaves[slave_ip]['batch_size'] < MAX_BATCH_SIZE:
                    slaves[slave_ip]['batch_size'] = min(slaves[slave_ip]['batch_size'] * 2, MAX_BATCH_SIZE)
                elif execution_time > 2000:
                    slaves[slave_ip]['batch_size'] = max(slaves[slave_ip]['batch_size'] // 2, INITIAL_BATCH_SIZE)

                primes = sieve_of_eratosthenes(start, end)
                for prime in primes:
                    f.write(f"{prime}\n")
                return end + 1  # Return the next starting point
            except Exception as e:
                print(f"Failed to connect to {slaves[slave_ip]['name']} at {slave_ip}: {e}")
                return None


async def calculate_primes_master(start, end):
    loop = asyncio.get_event_loop()
    start_time = time.time()
    with ProcessPoolExecutor() as pool:
        step = (end - start) // multiprocessing.cpu_count() + 1
        tasks = [(max(start, i), min(end, i + step - 1)) for i in range(start, end + 1, step)]
        results = await asyncio.gather(*[loop.run_in_executor(pool, sieve_of_eratosthenes, t[0], t[1]) for t in tasks])
        primes = [prime for sublist in results for prime in sublist]
    end_time = time.time()
    execution_time = end_time - start_time

    prime_count = len(primes)
    with open(PRIMES_FILE, 'a') as f:
        for prime in primes:
            f.write(f"{prime}\n")
    print(f"Master found {prime_count} primes in range {start} to {end} in {execution_time} seconds")
    return end + 1  # Return the next starting point


def print_cluster_info():
    table = PrettyTable(['Slave Name', 'IP Address', 'Cores', 'RAM'])
    total_cores = multiprocessing.cpu_count()
    total_ram = psutil.virtual_memory().total
    for ip, info in slaves.items():
        table.add_row([info['name'], ip, info['cores'], f"{info['ram'] // (1024 ** 3)} GB"])
        total_cores += info['cores']
        total_ram += info['ram']
    table.add_row(
        ["Master", get_ip_address(), multiprocessing.cpu_count(), f"{psutil.virtual_memory().total // (1024 ** 3)} GB"])
    table.add_row(["Total", "", total_cores, f"{total_ram // (1024 ** 3)} GB"])
    print(table)
    print("Press 's' to start calculations...")


def handle_user_input():
    while not start_calculations.is_set():
        if input().strip().lower() == 's':
            start_calculations.set()


async def distribute_workload(current):
    pending_tasks = {}
    while not start_calculations.is_set():
        await asyncio.sleep(1)

    while True:
        with lock:
            for slave_ip, info in slaves.items():
                if not start_calculations.is_set():
                    break
                if pending_tasks.get(slave_ip, 0) < 4:  # Limit of 4 batches per slave
                    batch_size = info['batch_size']
                    start = current
                    end = start + batch_size - 1
                    pending_tasks.setdefault(slave_ip, 0)
                    pending_tasks[slave_ip] += 1
                    asyncio.create_task(assign_ranges_to_slave(slave_ip, start, end))
                    current = end + 1

        if server_calculations:
            future_master_batch = asyncio.create_task(calculate_primes_master(current, current + INITIAL_BATCH_SIZE - 1))
            result = await future_master_batch
            if result is not None:
                current = result

        # Update task count based on completed tasks
        for slave_ip in list(pending_tasks):
            if pending_tasks[slave_ip] == 0:
                del pending_tasks[slave_ip]

        await asyncio.sleep(1)  # Small sleep to prevent tight loop


def start_master():
    print("Starting master...")
    threading.Thread(target=discover_slaves).start()
    threading.Thread(target=handle_user_input).start()

    current = 2
    asyncio.run(distribute_workload(current))


def broadcast_and_serve_slave():
    threading.Thread(target=broadcast_presence).start()
    threading.Thread(target=respond_to_handshake).start()
    start_slave_server()


if __name__ == "__main__":
    mode = int(input("1. Master\n2. Slave\nChoose mode: "))
    if mode == 1:
        calculate_option = input("Should the master perform calculations too? (y/n): ").strip().lower()
        server_calculations = calculate_option == 'y'
        start_master()
    elif mode == 2:
        broadcast_and_serve_slave()
    else:
        print("Invalid choice.")
