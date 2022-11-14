#! /usr/bin/env python3

# Читает команду из commands-in-tenant1, обрабатывает
# и посылает event в stream events-tenant1

import json
from pprint import pprint
import redis



r = redis.Redis(host='localhost', port=6379, db=0)

def get_queue_name(tenant_id):
    _, entries = r.xinfo_consumers('commands', 'workers')
    for _, entry in entries:
        for cid in entry:
            data = json.loads(entry[cid])
            consumer_name = data['name']
            r.xpending(consumer_name, consumer_name)
    return f'commands-1'

def dispatch(entries):
    for _, entry in entries:
        for command_id in entry:
            print('-' * 10)
            print('Dispatching command:',_, command_id)
            command = json.loads(entry[command_id])
            pprint(command)
            tenant_id = command['tenant_id']
            queue_name = get_queue_name(tenant_id)
            r.xadd(queue_name, {command_id.decode("utf-8"): json.dumps(command)})
r.xgroup_create('commands', 'workers')
while True:
    stream_name, entries = r.xread({'commands': '$'}, block=0)[0] # read from common commands bus
    dispatch(entries)
