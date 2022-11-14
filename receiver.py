import time

from redis_stream_bus import RedisStreamBus

command_bus = RedisStreamBus('command')
event_bus = RedisStreamBus('event')
while True:
    message = command_bus.get_message()
    result = None
    event_name = f"{message['publisher']}_{message['action']}_handled"
    event = {'name': event_name}
    event['request_id'] = message.get('request_id')
    event['type'] = 'event'
    time.sleep(3.001)
    event_bus.send(event)
