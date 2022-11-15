#! /usr/bin/env python3
'''
Клиент, посылающий случайные команды от случайных тенантов. Запускать можно сколько угодно штук в консоли с параметром <номер клиента> или <название>
$ python3 client.py A
в файлах tenant_1.txt tenant_2.txt tenant_3.txt будет лог обработки и диспетчеризации команд
'''

import json
import os
import random
import uuid
from copy import copy
from datetime import datetime
from pprint import pprint
from time import sleep

import redis
import sys

subsystem = str(sys.argv[1])  # название подсистемы, оно же название стрима
client_id = str(sys.argv[2])  # идентификатор клиента


def got_expected_response(entries, command_id):
    for message_id, entry in entries:
        r.xdel(f"response-{client_id}", message_id)
        for event_id in entry:
            print(event_id.decode("utf-8"))
            print(command_id)
            print('-' * 10)
            print('Processing event:', event_id)
            event = json.loads(entry[event_id])
            pprint(event)
            if command_id == event_id.decode("utf-8"):
                return event['result']


REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
r = redis.Redis(host=REDIS_HOST, port=6379, db=0)

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
    'name': 'calculate-double',
    'type': 'command',
    'params': {
        'argument': 0
    }
}
command3 = {
    'id': None,
    'name': 'calculate-power',
    'type': 'command',
    'params': {
        'argument': 0
    }
}
command4 = {
    'id': None,
    'name': 'get_all',
    'type': 'query',
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


# получить id последней записи из очереди events-tenant1
# в принципе, он может как-нибудь по другому поддерживать
# id последнего события, которое он видел
try:
    r.xgroup_create('events', client_id, mkstream=True)  # будем подтверждать чтение?
except:
    pass

last_seen = '$'

for x in range(0, 20):
    command = copy(random.choice([command1, command2, command3, command4]))
    tenant_id = random.choice(['tenant_1', 'tenant_2', 'tenant_3'])
    if command['type'] == 'query':
        counter = r.incr(f'query-{tenant_id}', 1)
        command['id'] = f'query-{tenant_id}-{counter}'
    else:
        counter = r.incr(f'command-{tenant_id}', 1)
        command['id'] = f'command-{tenant_id}-{counter}'
    command['tenant_id'] = tenant_id
    command['response-to'] = f"response-{client_id}"
    command['params']['argument'] = counter
    start = datetime.now()
    for f in [None, f"{tenant_id}.txt", f"client-{client_id}.txt"]:
        logger(f, f"client-{client_id} S {start} {command['id']} {command['name']}")
    message_id = r.xadd(subsystem, {command['id']: json.dumps(command)})

    # считывать ответы после last_id
    # когда мы видим событие, которое ждём, то можно выходить
    while True:  # TODO возможно, нужно выгребать старые сообщения тоже, плюс будет нужен таймаут
        print('wait for response')
        read = r.xread({f"response-{client_id}": last_seen}, count=1, block=0)  # or timeout?
        if read:
            _, entries = read[0]
            # entries = r.xrange(f"response-{client_id}", '-', '+', count=1)
            result = got_expected_response(entries, command['id'])
            if result is not None:
                end = datetime.now()
                for f in [None, f"{tenant_id}.txt", f"client-{client_id}.txt"]:
                    logger(f, f"client-{client_id} F {end} {command['id']} {command['name']} {result}")
                break
            sleep(0.1)
r.xgroup_destroy('events', client_id)
