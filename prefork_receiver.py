import multiprocessing
import time
from multiprocessing.pool import Pool

from common.config import SLEEP
from common.infrastructure.receivers.abstract import AbstractReceiver
from common.messages import Command


from common.infrastructure.receivers.simple_receiver import SimpleReceiver

queue_pool = {}


def remove_tenant_from_pool(tenant_id):
    del queue_pool[tenant_id]


def process_queue(publisher, queue): # нужен отдельный метод, нельзя использовать self
    """Fetch new messages from the bus and dispatch them"""
    tenant_id = None
    while not queue.empty():
        command = queue.get()
        tenant_id = command.tenant_id
        receiver = SimpleReceiver()
        receiver.publisher = publisher
        receiver.process(command)
        time.sleep(SLEEP)
    return tenant_id


class PreforkReceiver:

    def __init__(self, bus=None, pool_size=4):
        super().__init__(bus)

        self.pool_size = pool_size

    def process(self, command: Command):
        pass

    @classmethod
    def process_queue(cls, queue):  # нужен отдельный метод, нельзя использовать self
        """Fetch new messages from the bus and dispatch them"""
        tenant_id = None
        while not queue.empty():
            command = queue.get()
            tenant_id = command.tenant_id
            receiver = SimpleReceiver()
            receiver.publisher = cls.publisher
            receiver.process(command)
            time.sleep(SLEEP)
        return tenant_id

    def run(self):
        manager = multiprocessing.Manager()
        with Pool(processes=self.pool_size) as pool:
            while True:
                command = self.bus.get_message()
                if command:
                    tenant_id = command.tenant_id

                    if tenant_id in queue_pool:
                        q = queue_pool.get(tenant_id)
                        q.put(command)
                    else:
                        queue_pool[tenant_id] = manager.Queue()
                        queue_pool[tenant_id].put(command)
                        pool.apply_async(self.__class__.process_queue, (queue_pool[tenant_id],),
                                         callback=remove_tenant_from_pool)
                time.sleep(sleep)
