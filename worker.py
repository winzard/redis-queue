#! /usr/bin/env python3
'''
Обработчик команд. Запускать можно сколько угодно штук в консоли с параметром <номер обработчика> или <название>
$ python3 worker.py 1
в файлах tenant_1.txt tenant_2.txt tenant_3.txt будет лог обработки и диспетчеризации команд
'''

import json
import os
from datetime import datetime
from pprint import pprint
import random
from time import sleep
import sys
import sqlite3
import transaction
from transaction.interfaces import IDataManager, DoomedTransaction
from zope.interface import implementer

from channel import Channel

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')


@implementer(IDataManager)
class DataManager(object):
    """ Sample data manager.

    Used by the 'datamanager' chapter in the Sphinx docs.
    """

    def __init__(self, worker):
        self.state = 0
        self.sp = 0
        self.transaction = None
        self.delta = 0
        self.txn_state = None
        self.begun = False
        self.worker = worker
        self.connection = sqlite3.connect("example.db")
        cur = self.connection.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS result(tenant_id TEXT PRIMARY KEY, command TEXT DEFAULT '');")
        cur.close()
        self.connection.commit()

    def _check_state(self, *ok_states):
        if self.txn_state not in ok_states:
            raise ValueError("txn in state %r but expected one of %r" % (self.txn_state, ok_states))

    def _check_transaction(self, transaction_instance):
        if transaction_instance is not self.transaction and self.transaction is not None:
            raise TypeError("Transaction missmatch", transaction_instance, self.transaction)

    def inc(self, tenant_id, value):
        cur = self.connection.cursor()
        sql = f"INSERT INTO result(tenant_id) VALUES('{tenant_id}') ON CONFLICT(tenant_id) DO UPDATE SET command = '{value}';"
        cur.execute(sql)
        cur.close()

    def get(self, tenant_id):
        cur = self.connection.cursor()
        res = cur.execute(f"SELECT command FROM result WHERE tenant_id = '{tenant_id}';")
        result = res.fetchone()
        cur.close()
        if result:
            return result[0]
        else:
            return None

    def tpc_begin(self, transaction_instance):
        print('tcp_begin')
        self._check_transaction(transaction_instance)
        self._check_state(None)
        self.transaction = transaction_instance
        self.txn_state = 'tpc_begin'
        self.begun = True

    def tpc_vote(self, transaction_instance):
        print('tcp_vote start')
        self._check_transaction(transaction_instance)
        self._check_state('tpc_begin')
        if not self.worker.is_job_exists(transaction_instance.data('message_id')):
            print('doom')
            raise DoomedTransaction()
        self.state += self.delta
        self.txn_state = 'tpc_vote'
        print('tcp_vote end')

    def tpc_finish(self, transaction_instance):
        print('tcp_finish')
        self._check_transaction(transaction_instance)
        self._check_state('tpc_vote')
        self.delta = 0
        self.transaction = None
        self.txn_state = None
        self.connection.commit()
        for f in [None, "worker.txt", f"worker-{self.worker.subsystem}-{self.worker.consumer_id}.txt"]:
            logger(f,
                   f"worker-{self.worker.subsystem}-{self.worker.consumer_id} КОММИТ {transaction_instance.data('command_id')}")

    def tpc_abort(self, transaction_instance):
        print('tcp_abort')
        self._check_transaction(transaction_instance)
        if self.transaction is not None:
            self.transaction = None

        if self.txn_state == 'tpc_vote':
            self.state -= self.delta

        self.txn_state = None
        self.delta = 0

    def abort(self, transaction_instance):
        print('abort')
        self._check_transaction(transaction_instance)
        if self.transaction is not None:
            self.transaction = None

        if self.begun:
            self.state -= self.delta
            self.begun = False

        self.delta = 0
        self.connection.rollback()
        for f in [None, "worker.txt", f"worker-{self.worker.subsystem}-{self.worker.consumer_id}.txt"]:
            logger(f,
                   f"worker-{self.worker.subsystem}-{self.worker.consumer_id} ОТМЕНА {transaction_instance.data('command_id')}")

    def commit(self, transaction_instance):
        print('commit')
        if not self.begun:
            raise TypeError('Not prepared to commit')
        self._check_transaction(transaction_instance)
        self.transaction = None


def fibonacci(n, worker=None):
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
            sleep(0.1)
            # if worker:
            #     worker.i_am_alive() # сообщаем, что живы и работаем. В этом случае задача не должна перехватываться
        return b


def get(data_manager, tenant_id):
    before = data_manager.get(tenant_id)
    fibonacci(1)  # надо чем-то занять
    after = data_manager.get(tenant_id)
    return {'before': before, 'after': after}


def change(data_manager, tenant_id, argument):
    before = data_manager.get(tenant_id)
    data_manager.inc(tenant_id, argument)  # увеличиваем счетчик
    fibonacci(random.randint(1, 15), data_manager.worker)  # длинная задача
    after = data_manager.get(tenant_id)
    return {'before': before, 'after': after}


group_name = 'workers'  # общее для всех воркеров системы


def logger(filename, text):
    if filename:
        with open(filename, "a") as log:
            print(text, file=log)
    else:
        print(text)


class Worker:

    def __init__(self, subsystem: str, consumer_id: str):
        self.subsystem = subsystem
        self.consumer_id = consumer_id
        self.redis_data_manager = DataManager(self)
        self.channel = Channel(subsystem, group_name)

    def process_message(self, message_id, command):
        print(command)
        command_id = command['id']
        print('-' * 10)
        print('Обработка команды:', message_id, command_id)

        command_type = command['type']
        tenant_id = command['tenant_id']
        start_time = datetime.now()
        for f in [None, f"{tenant_id}.txt", "worker.txt", f"worker-{self.subsystem}-{self.consumer_id}.txt"]:
            logger(f,
                   f"worker-{self.subsystem}-{self.consumer_id} S {start_time} {command['id']} {command['name']}")
        pprint(command)
        response = None
        if command_type == 'query':
            # no transaction
            if command['name'] == 'get':
                argument = command['params']['argument']
                result = get(self.redis_data_manager, tenant_id)
                response = {
                    'id': command_id,
                    'success': True,
                    'tenant_id': tenant_id,
                    'name': 'get-completed',
                    'result': result
                }
        else:
            tx = transaction.begin()
            tx.set_data('message_id', message_id)  # как-то неровно это
            tx.set_data('command_id', command_id)  # как-то неровно это
            try:
                tx.join(self.redis_data_manager)
                if command['name'] == 'change':
                    argument = command['params']['argument']
                    result = change(self.redis_data_manager, tenant_id, command_id)
                    if result:
                        response = {
                            'id': command_id,
                            'success': True,
                            'tenant_id': tenant_id,
                            'name': 'change-completed',
                            'result': result
                        }
                    else:
                        response = None
                tx.commit()
            except Exception as e:
                response = {
                    'id': command_id,
                    'success': False,
                    'tenant_id': tenant_id,
                    'name': 'change-completed',
                    'result': e
                }
                print('exception', e)
                tx.abort()
        self.end_command(command, response, message_id)

    def end_command(self, command, response, message_id):
        command_id = command['id']
        tenant_id = command['tenant_id']
        result = response['result'] if response else 'ОТМЕНА'
        end_time = datetime.now()
        for f in [None, f"{tenant_id}.txt", "worker.txt", f"worker-{self.subsystem}-{self.consumer_id}.txt"]:
            logger(f,
                   f"worker-{self.subsystem}-{self.consumer_id} F {end_time} {command_id} {command['name']} {result}")
        if response['success']:
            self.channel.send(
                command['response-to'],
                {command_id: json.dumps(response)}
            )
            self.channel.acknowledge(message_id)

    def is_job_exists(self, message_id):
        return self.channel.is_pending(message_id)

    def i_am_alive(self):
        return self.channel.ping(self.consumer_id)

    def routine(self):
        start_time = datetime.now()
        for f in [None, 'worker.txt', f"worker-{self.subsystem}-{self.consumer_id}.txt"]:
            logger(f,
                   f'{self.consumer_id} обработчик запущен {start_time} ....')
        while True:
            for message_id, message in self.channel.get_message_for(self.consumer_id):
                try:
                    self.process_message(message_id, message)
                except KeyboardInterrupt:
                    for f in [None, 'worker.txt', f"worker-{self.subsystem}-{self.consumer_id}.txt"]:
                        logger(f, f'{self.consumer_id} обработчик упал')
                    exit()
            sleep(0.1)  # ждем, чтобы tsd было больше 1 мс


if __name__ == '__main__':
    Worker(subsystem=str(sys.argv[1]), consumer_id=str(sys.argv[2])).routine()
