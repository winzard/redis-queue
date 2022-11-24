from multiprocessing import Process

from client import Client, command2
from worker import Worker

WORKER_COUNT = 3
CLIENT_COUNT = 10
workers = []
clients = []


def client_routine_tenant_1(client_id: int):
    result = Client('registration', str(client_id)).process_single(command=command2, tenant_id='tenant_1')
    print(result)


for i in range(WORKER_COUNT):
    worker_process = Process(target=Worker('registration', str(i)).routine, daemon=True)
    worker_process.start()
    workers.append(worker_process)

for i in range(CLIENT_COUNT):
    client_process = Process(target=client_routine_tenant_1, args=(i,), daemon=True)
    client_process.start()
    clients.append(client_process)


for c in clients:
    c.join()
