import os
import json
import uuid
from json import JSONEncoder

import redis

# TODO: Если вы в методе send руками пишете message.to_json()
# то зачем вам по жизни нестандартный encoder?


REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')


def _default(self, obj):
    return getattr(obj.__class__, "to_dict", _default.default)(obj)


_default.default = JSONEncoder().default
JSONEncoder.default = _default


class RedisStreamBus:

    def __init__(self, name):
        self.name = name
        self.worker_id = str(uuid.uuid4())
        pool = redis.ConnectionPool(host=REDIS_HOST, port=6379, db=0)
        self.redis = redis.Redis(connection_pool=pool, health_check_interval=600)
        try:
            self.redis.xgroup_create(self.name, self.worker_id, mkstream=True) # TODO потом нужно будет использовать название подсистемы для работы с группами
        except:
            pass

    def get_message(self):
        entries = self.redis.xreadgroup(self.worker_id, self.worker_id, {self.name: ">"}, count=1, block=0)  # or block None??
        if entries:
            _, commands = entries[0]
            for message_id, entry in commands:
                for command_id in entry:
                    message = json.loads(entry[command_id])
                    if message['type'] == 'command':
                        self.redis.xack(self.name, self.worker_id, message_id)
                        return message
                    elif message['type'] == 'event':
                        self.redis.xack(self.name, self.worker_id, message_id)
                        return message

    def send(self, message):
        self.redis.xadd(self.name, {str(uuid.uuid4()): json.dumps(message)})

