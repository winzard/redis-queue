import time
import uuid


class BusRequestProcessor:
    """Посылает команду в шину и дожидается ответного события"""

    def __init__(self, command_bus, event_bus):

        self.command_bus = command_bus
        self.event_bus = event_bus

    def _get_response(self, request_id, timeout=15):
        """Ждёт пока по шине придёт событие о том,
        что команда обработана, и возвращает это событие."""

        starting_time = time.time()

        while True:
            message = self.event_bus.get_message()
            if message and message.get('request_id') == request_id:
                return message

            if time.time() - starting_time > timeout:
                msg = f'Ответ на команду не получен в течение {timeout} секунд'
                msg += '\nЗапущен ли обработчик команд (registration/run.py)?'
                raise TimeoutError(msg)

            time.sleep(0.0001)

    def process(self, cmd):
        """Посылать команду и дождаться события,
        которое рассказывает о том, что она обработана."""

        req_id = str(uuid.uuid4())
        cmd['request_id'] = req_id
        self.command_bus.send(cmd)
        timeout = 15
        if getattr(cmd, 'action', None) in ('post', 'unpost', 'process'):
            timeout = 600
        event = self._get_response(req_id, timeout)
        return event
