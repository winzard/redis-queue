#! /usr/bin/env python3
'''
Клиент, посылающий случайные команды от случайных тенантов. Запускать можно сколько угодно штук в консоли с параметром <номер клиента> или <название>
$ python3 client.py A
в файлах tenant_1.txt tenant_2.txt tenant_3.txt будет лог обработки и диспетчеризации команд
'''

import json
import os
import random
from copy import copy
from datetime import datetime
from pprint import pprint
from time import sleep

import redis
import sys


REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')


command1 = {
    'id': None,
    'name': 'get',
    'type': 'query',
    'params': {
        'argument': 0
    }
}

command2 = {
    'id': None,
    'name': 'change',
    'type': 'command',
    'params': {
        'argument': 0
    }
}


def logger(filename, text):
    if filename:
        with open(filename, "a") as log:
            print(text, file=log)
    else:
        print(text)


class Client:
    # def __init__(self, subsystem: str, client_id: str):
    #     self.subsystem = subsystem
    #     self.client_id = client_id
    def __init__(self, name, group_name) -> None:
        super().__init__()
        self.transport = redis.Redis(host=REDIS_HOST, port=6379, db=0)
        self.name = name
        self.group_name = group_name
        self.last_seen = '$'
        # try:
        #     self.transport.xgroup_create(name, group_name, mkstream=True)
        # except:
        #     pass

    def send(self, recipient_channel, message):
        self.transport.xadd(recipient_channel, message)

    def got_expected_response(self, entries, command_id):
        message_id = None
        for message_id, entry in entries:
            self.transport.xdel(f"response-{self.group_name}", message_id)
            for event_id in entry:
                print(event_id.decode("utf-8"))
                print(command_id)
                print('-' * 10)
                print('Processing event:', event_id)
                event = json.loads(entry[event_id])
                pprint(event)
                if command_id == event_id.decode("utf-8"):
                    return event['result'], message_id
        return None, message_id

    def process_single(self, command: dict, tenant_id: str):
        # получить id последней записи из очереди events-tenant1
        # в принципе, он может как-нибудь по другому поддерживать
        # id последнего события, которое он видел




        if command['type'] == 'query':
            counter = self.transport.incr(f'query-{tenant_id}', 1)
            command['id'] = f'query-{tenant_id}-{counter}'
        else:
            counter = self.transport.incr(f'command-{tenant_id}', 1)
            command['id'] = f'command-{tenant_id}-{counter}'
        command['tenant_id'] = tenant_id
        command['response-to'] = f"response-{self.group_name}"
        command['params']['argument'] = counter
        start = datetime.now()
        for f in [None, f"{tenant_id}.txt", f"client-{self.group_name}.txt"]:
            logger(f, f"client-{self.group_name} S {start} {command['id']} {command['name']}")
        message_id = self.send(self.name, {command['id']: json.dumps(command)})

        # считывать ответы после last_id
        # когда мы видим событие, которое ждём, то можно выходить
        while True:  # TODO возможно, нужно выгребать старые сообщения тоже, плюс будет нужен таймаут
            print('wait for response')
            read = self.transport.xread({f"response-{self.group_name}": self.last_seen}, count=1, block=0)  # or timeout?
            if read:
                _, entries = read[0]
                # entries = r.xrange(f"response-{client_id}", '-', '+', count=1)
                result, last_seen = self.got_expected_response(entries, command['id'])
                if result is not None:
                    end = datetime.now()
                    for f in [None, f"{tenant_id}.txt", f"client-{self.group_name}.txt"]:
                        logger(f, f"client-{self.group_name} F {end} {command['id']} {command['name']} {result}")

                    return {
                        'client_id': f"client-{self.group_name}",
                        'command_id': command['id'],
                        'command_name': command['name'],
                        'result': result
                    }
                sleep(0.1)

    def routine(self):
        # получить id последней записи из очереди events-tenant1
        # в принципе, он может как-нибудь по другому поддерживать
        # id последнего события, которое он видел
        last_seen = '$'

        for x in range(0, 10):
            print(f'Отправляем команду #{x}')
            command = copy(random.choice([command1, command2]))
            tenant_id = random.choice(['tenant_1', 'tenant_2', 'tenant_3'])
            if command['type'] == 'query':
                counter = self.transport.incr(f'query-{tenant_id}', 1)
                command['id'] = f'query-{tenant_id}-{counter}'
            else:
                counter = self.transport.incr(f'command-{tenant_id}', 1)
                command['id'] = f'command-{tenant_id}-{counter}'
            command['tenant_id'] = tenant_id
            command['response-to'] = f"response-{self.group_name}"
            command['params']['argument'] = counter
            start = datetime.now()
            for f in [None, f"{tenant_id}.txt", f"client-{self.group_name}.txt"]:
                logger(f, f"client-{self.group_name} S {start} {command['id']} {command['name']}")
            message_id = self.send(self.name, {command['id']: json.dumps(command)})

            # считывать ответы после last_id
            # когда мы видим событие, которое ждём, то можно выходить
            while True:  # TODO возможно, нужно выгребать старые сообщения тоже, плюс будет нужен таймаут
                print(f'wait for response on {message_id}')
                read = self.transport.xread({f"response-{self.group_name}": last_seen}, count=1, block=0)  # or timeout?
                if read:
                    _, entries = read[0]
                    result, last_seen = self.got_expected_response(entries, command['id'])
                    if result is not None:
                        end = datetime.now()
                        for f in [None, f"{tenant_id}.txt", f"client-{self.group_name}.txt"]:
                            logger(f, f"client-{self.group_name} F {end} {command['id']} {command['name']} {result}")
                        break
                    sleep(0.1)



if __name__ == '__main__':
    Client(name=str(sys.argv[1]), group_name=str(sys.argv[2])).routine()
