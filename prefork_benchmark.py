from multiprocessing import Process

from client import Client, command2, command1
from worker import Worker

TENANT_COUNT = 3
WORKER_COUNT = 3
CLIENT_COUNT = 15
workers = []
clients = []


def client_routine(client_id: int, tenant_id: str):
    result = Client('registration', str(client_id)).process_single(command=command2, tenant_id=tenant_id)
    if not result['result']['before']:
        before = 0
    else:
        before = int(result['result']['before'].split('-')[-1])
    if not result['result']['after']:
        after = 0
    else:
        after = int(result['result']['after'].split('-')[-1])
    assert after - before == 1
    result = Client('registration', str(client_id)).process_single(command=command1, tenant_id=tenant_id)
    assert result['result']['after'] == result['result']['before']


for i in range(WORKER_COUNT):
    worker_process = Process(target=Worker('registration', str(i)).routine, daemon=True)
    worker_process.start()
    workers.append(worker_process)

for i in range(CLIENT_COUNT):
    tenant_id = f"tenant_{i % TENANT_COUNT}"
    client_process = Process(target=client_routine, args=(i, tenant_id), daemon=True)
    client_process.start()
    clients.append(client_process)


for c in clients:
    c.join()
