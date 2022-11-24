import json
import os

import redis

WORKER_TIMEOUT_MILLISECONDS = 3600000  # таймаут воркера, после которого он удаляется из consumers шины
MESSAGE_QUEUE_SIZE = 30  # как далеко мы смотрим в историю сообщений. Возможно, это зависит от числа воркеров
BLOCK_TIME_MILLISECONDS = 3000
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
COMMAND_TIMEOUT_MILLISECONDS = 5000  # таймаут команды


def logger(filename, text):
    if filename:
        with open(filename, "a") as log:
            print(text, file=log)
    else:
        print(text)


class Channel:

    def __init__(self, name, group_name) -> None:
        super().__init__()
        self.r = redis.Redis(host=REDIS_HOST, port=6379, db=0)
        self.name = name
        self.group_name = group_name
        self.last_seen = '>'
        try:
            self.r.xgroup_create(name, group_name, mkstream=True)
        except:
            pass

    def acknowledge(self, message_id):
        self.r.xack(self.name, self.group_name, message_id)

    def is_pending(self, message_id):
        pending_entries = self.r.xpending_range(self.name, self.group_name, message_id, message_id,
                                                1)  # а вот без консумера, consumername=consumername)
        if pending_entries:
            return True
        else:
            return False

    def ping(self, consumer_id):
        """
        Сообщаем, что consumer еще жив
        :param consumer_id: id worker
        :return:
        """
        self.r.xclaim(self.name, self.group_name, consumer_id, WORKER_TIMEOUT_MILLISECONDS, ['1'])

    def send(self, recipient_channel, message):
        self.r.xadd(recipient_channel, message)

    def _dispatch(self, consumer_id, message_id, command) -> bool:
        command_id = command['id']
        tenant_id = command['tenant_id']
        pending_entries = self.r.xpending_range(self.name, self.group_name, '-', message_id,
                                                MESSAGE_QUEUE_SIZE)
        if not pending_entries:
            return False
        print(f'Всего сообщений {len(pending_entries)}')
        print(f'{consumer_id} Обработка команды:', message_id, command_id)
        for message in pending_entries:  # todo time_since_delivered если застряло
            request_id = message['message_id']  # bytes
            other_worker = message['consumer']  # bytes
            for f in [None, f'{tenant_id}.txt', 'worker.txt', f"worker-{self.name}-{consumer_id}.txt"]:
                logger(f,
                       f'{consumer_id} проверяет сообщение {request_id} для обработчика {other_worker}')

            rng = self.r.xrange(self.name, request_id, request_id, count=1)
            if not rng:
                print(f'непонятно, почему нет сообщения')
                self.r.xack(self.name, self.group_name, request_id)
                continue
            _message_id, entry = rng[0]
            for _command_id in entry:
                _command = json.loads(entry[_command_id])
                _tenant_id = _command['tenant_id']
                if consumer_id != other_worker.decode('utf-8') and _command_id != command_id and tenant_id == _tenant_id and \
                        _command['type'] != 'query':  # кто-то уже обрабатывает команду для этого тенанта и это не запрос
                    self.r.xclaim(self.name, self.group_name, other_worker, 1, [message_id])
                    for f in [None, 'worker.txt', f"worker-{self.name}-{consumer_id}.txt"]:
                        logger(f,
                               f'{consumer_id} обработчик передает обработку {request_id} {command_id} для тенанта {tenant_id} обработчику {other_worker}, потому что он уже обрабатывает команду {_command_id}')

                    return True
                elif consumer_id == other_worker.decode('utf-8') and _command['type'] != 'query' and tenant_id == _tenant_id and message_id != _message_id:
                    # есть невыполненные задачи ДО текущей message_id, вываливаемся и ждем, когда другой обработает
                    for f in [None, f'{_tenant_id}.txt', 'worker.txt', f"worker-{self.name}-{consumer_id}.txt"]:
                        logger(f,
                               f'{consumer_id} обработчик должен пропустить команду {request_id} {command_id} для тенанта {_tenant_id} потому что зависла команда {_command_id}')
                    return True
                elif message_id != _message_id:
                    for f in [None, f'{_tenant_id}.txt', 'worker.txt', f"worker-{self.name}-{consumer_id}.txt"]:
                        logger(f,
                               f'{consumer_id} обработчик установил, что команду {request_id} {_command_id} для тенанта {_tenant_id} обрабатывает {other_worker}')
        return False

    def get_message_for(self, consumer_id):
        print('обработка зависших')
        consumers = self.r.xinfo_consumers(self.name, self.group_name)
        for info in consumers:
            idle_time = info['idle']
            pending_messages = info['pending']
            consumer = info['name']
            if pending_messages == 0 and idle_time > WORKER_TIMEOUT_MILLISECONDS:
                for f in [None, "worker.txt", f"worker-{self.name}-{consumer_id}.txt"]:
                    logger(f, f'{consumer_id} обработчик удаляет отвалившийся обработчик {consumer}')
                self.r.xgroup_delconsumer(self.name, self.group_name, consumer)
            if pending_messages and idle_time > COMMAND_TIMEOUT_MILLISECONDS:
                # забираем себе закисшие команды. TODO а что будет, если зависли команды разных тенантов?
                # self.r.xautoclaim(self.name, self.group_name, consumer_id, COMMAND_TIMEOUT_MILLISECONDS)
                # autoclaim не подходит, т.к. мы хотим забрать не все зависшие, а только для определенного воркера
                entries = self.r.xpending_range(self.name, self.group_name, '-', '+', MESSAGE_QUEUE_SIZE,
                                                consumername=consumer)
                message_ids = [message['message_id'] for message in entries]
                if message_ids:
                    self.r.xclaim(self.name, self.group_name, consumer_id, COMMAND_TIMEOUT_MILLISECONDS, message_ids)
                    for f in [None, 'worker.txt', f"worker-{self.name}-{consumer_id}.txt"]:
                        logger(f,
                               f'{consumer_id} обработчик забирает себе обработку сообщений {message_ids} обработчика {consumer}, потому что он упал')
                    break # забираем только у одного консумера, и хватит забирать лишнее, это бы успеть обработать
        print('обработка переданных')
        entries = self.r.xpending_range(self.name, self.group_name, '-', '+', 1,
                                        consumername=consumer_id)  # по одному, потому что может там отвис обработчик

        for message in entries:  # todo time_since_delivered
            request_id = message['message_id'].decode("utf-8")
            entries = self.r.xrange(self.name, request_id, request_id)
            if entries:
                for message_id, entry in entries:
                    for command_id in entry:
                        command = json.loads(entry[command_id])
                        yield message_id, command
            else:
                for f in [None, "worker.txt", f"worker-{self.name}-{consumer_id}.txt"]:
                    logger(f, f'{consumer_id} обработчик удаляет сообщение {request_id}')
                self.r.xdel(self.name, request_id)
        print(f'новый цикл {self.last_seen}')

        entries = self.r.xreadgroup(self.group_name, consumer_id, {self.name: self.last_seen}, count=1,
                                    block=BLOCK_TIME_MILLISECONDS)  # or block None??
        # полученные сообщения попадают в PEL и другие консумеры считают, что этот воркер их обрабатывает
        print(entries)
        if entries:
            _, commands = entries[0]
            for message_id, entry in commands:
                # self.last_seen = message_id # не работает
                for command_id in entry:
                    command = json.loads(entry[command_id])
                    command_type = command['type']
                    if command_type == 'query' or not self._dispatch(consumer_id, message_id, command):
                        yield message_id, command
