from redis_stream_bus import RedisStreamBus
from request_processor import BusRequestProcessor

cmd = {
    'publisher': 'aaa',
    'action': 'bbb',
    'type': 'command'
}
processor = BusRequestProcessor(RedisStreamBus('command'), RedisStreamBus('event'))
event = processor.process(cmd)
print(event)
event = processor.process(cmd)
print(event)
event = processor.process(cmd)
print(event)