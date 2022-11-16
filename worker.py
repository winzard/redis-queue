#! /usr/bin/env python3
'''
Обработчик команд. Запускать можно сколько угодно штук в консоли с параметром <номер обработчика> или <название>
$ python3 worker.py 1
в файлах tenant_1.txt tenant_2.txt tenant_3.txt будет лог обработки и диспетчеризации команд
'''

import json
import os
import uuid
from datetime import datetime
from pprint import pprint
import random
from time import sleep
import sys
import redis


def fibonacci(n):
    a = 0
    b = 1
    if n < 0:
        print("Incorrect input")
    elif n == 0:
        return a
    elif n == 1:
        return b
    else:
        for i in range(2, n):
            c = a + b
            a = b
            b = c
            sleep(1)
        return b


def get(tenant_id):
    counter = r.get(f'result-command-{tenant_id}').decode("utf-8")
    fibonacci(1)  # надо чем-то занять
    result = r.get(f'result-command-{tenant_id}').decode("utf-8")
    return f'{counter} {result}'


def get_all(tenant_id):
    counter = r.get(f'result-command-{tenant_id}').decode("utf-8")
    fibonacci(4)  # надо чем-то занять
    result = r.get(f'result-command-{tenant_id}').decode("utf-8")
    return f'{counter} {result}'


def calculate_double(tenant_id):
    counter = str(r.incr(f'result-command-{tenant_id}', 1))  # увеличиваем счетчик
    fibonacci(random.randint(1, 15))  # длинная задача
    result = r.get(f'result-command-{tenant_id}').decode("utf-8")
    return f'{result} {result == counter}'


def calculate_power(tenant_id):
    return calculate_double(tenant_id)


REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
r = redis.Redis(host=REDIS_HOST, port=6379, db=0)
subsystem = str(sys.argv[1])  # название подсистемы, оно же название стрима
consumer_id = str(sys.argv[2])  # идентификатор воркера
group_name = 'workers'  # общее для всех воркеров системы


def logger(filename, text):
    if filename:
        with open(filename, "a") as log:
            print(text, file=log)
    else:
        print(text)


# todo происходит racing, когда один воркер еще не передал команду тенанта другому, а третий считает, что этот первый
#  обрабатывает команду и клеймит на него
def dispatched(message_id, tenant_id, command_id) -> bool:
    pending_entries = r.xpending_range(subsystem, group_name, '-', message_id, 20)
    if not pending_entries:
        return False
    print(f'Всего сообщений {len(pending_entries)}')
    print(f'{consumer_id} Обработка команды:', message_id, command_id)
    for message in pending_entries:  # todo time_since_delivered если застряло
        request_id = message['message_id']  # bytes
        other_worker = message['consumer']  # bytes
        print(f'{consumer_id} проверяет сообщение {request_id} для обработчика {other_worker}')
        if consumer_id != other_worker.decode('utf-8'):  # в строку преобразуем
            rng = r.xrange(subsystem, request_id, request_id, count=1)
            if not rng:
                print(f'непонятно, почему нет сообщения')
                r.xack(subsystem, group_name, request_id)
                continue
            _message_id, entry = rng[0]
            for _command_id in entry:
                _command = json.loads(entry[_command_id])
                _tenant_id = _command['tenant_id']
                if tenant_id == _tenant_id and \
                        _command[
                            'type'] != 'query':  # кто-то уже обрабатывает команду для этого тенанта и это не запрос
                    r.xclaim(subsystem, group_name, other_worker, 1, [message_id])
                    for f in [None, 'worker.txt', f"worker-{subsystem}-{consumer_id}.txt"]:
                        logger(f,
                               f'{consumer_id} обработчик передает обработку {command_id} для тенанта {tenant_id} обработчику {other_worker}, потому что он уже обрабатывает команду {_command_id}')

                    return True
                else:
                    with open(f'{_tenant_id}.txt', 'a') as log:
                        print(
                            f'{consumer_id} обработчик установил, что команду {_command_id} для тенанта {_tenant_id} обрабатывает {other_worker}.',
                            file=log)
    return False


def process_commands(entries, dispatch=True):
    for message_id, entry in entries:
        for command_id in entry:
            print('-' * 10)
            print('Обработка команды:', message_id, command_id)
            command = json.loads(entry[command_id])
            command_type = command['type']
            tenant_id = command['tenant_id']
            if not dispatch or command_type == 'query' or not dispatched(message_id, tenant_id, command_id):
                start_time = datetime.now()
                for f in [None, f"{tenant_id}.txt", "worker.txt", f"worker-{subsystem}-{consumer_id}.txt"]:
                    logger(f, f"worker-{subsystem}-{consumer_id} S {start_time} {command['id']} {command['name']}")
                pprint(command)
                if command['name'] == 'get':
                    argument = command['params']['argument']
                    result = get(tenant_id)
                    response = {
                        'id': command_id.decode("utf-8"),
                        'tenant_id': tenant_id,
                        'name': 'get-completed',
                        'result': result
                    }
                    end_command(command, command_id, message_id, response, tenant_id, result)
                elif command['name'] == 'get_all':
                    argument = command['params']['argument']
                    result = get_all(tenant_id)
                    response = {
                        'id': command_id.decode("utf-8"),
                        'tenant_id': tenant_id,
                        'name': 'get_all-completed',
                        'result': result
                    }
                    end_command(command, command_id, message_id, response, tenant_id, result)
                elif command['name'] == 'calculate-double':
                    argument = command['params']['argument']
                    result = calculate_double(tenant_id)
                    response = {
                        'id': command_id.decode("utf-8"),
                        'tenand_id': tenant_id,
                        'name': 'calculate-double-completed',
                        'result': result
                    }
                    end_command(command, command_id, message_id, response, tenant_id, result)
                elif command['name'] == 'calculate-power':
                    argument = command['params']['argument']
                    result = calculate_power(tenant_id)
                    response = {
                        'id': command_id.decode("utf-8"),
                        'tenand_id': tenant_id,
                        'name': 'calculate-power-completed',
                        'result': result
                    }
                    end_command(command, command_id, message_id, response, tenant_id, result)


def end_command(command, command_id, message_id, response, tenant_id, result):
    end_time = datetime.now()
    for f in [None, f"{tenant_id}.txt", "worker.txt", f"worker-{subsystem}-{consumer_id}.txt"]:
        logger(f, f"worker-{subsystem}-{consumer_id} F {end_time} {command['id']} {command['name']} {result}")
        r.xadd(
            command['response-to'],
            {command_id: json.dumps(response)}
        )
    r.xack(subsystem, group_name, message_id)


try:
    r.xgroup_create(subsystem, group_name, mkstream=True)
except:
    pass

last_seen = '>'
start_time = datetime.now()
for f in [None, 'worker.txt', f"worker-{subsystem}-{consumer_id}.txt"]:
    logger(f,
           f'{consumer_id} обработчик запущен {start_time} ....')

while True:
    print('обработка переданных')
    entries = r.xpending_range(subsystem, group_name, '-', '+', 20, consumername=consumer_id)

    for message in entries:  # todo time_since_delivered
        request_id = message['message_id'].decode("utf-8")
        entries = r.xrange(subsystem, request_id, request_id)
        if entries:
            process_commands(entries, dispatch=True)  # передиспатчиваем и тут
        else:
            for f in [None, "worker.txt", f"worker-{subsystem}-{consumer_id}.txt"]:
                logger(f, f'{consumer_id} обработчик удаляет сообщение {request_id}')
            r.xdel(subsystem, request_id)
    print('новый цикл')
    entries = r.xreadgroup(group_name, consumer_id, {subsystem: last_seen}, count=1, block=3000)  # or block None??
    # полученные сообщения попадают в PEL и другие консумеры считают, что этот воркер их обрабатывает
    print(entries)
    if entries:
        _, commands = entries[0]
        process_commands(commands)
    sleep(0.1)  # ждем, чтобы tsd было больше 1 мс
