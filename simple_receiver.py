import time
from abc import ABCMeta


import sys

import redis

bus = redis.Redis(host='localhost', port=6379, db=0)

class SimpleReceiver():
    """Polls messages from bus, processes messages it is subscribed to"""

    def process_command(self, command: Command):  # Command
        """Process messages handlers are subscribed to"""
        try:
            publisher = getattr(self.publisher, command.publisher)
        except AttributeError:
            return

        try:
            handler = getattr(publisher, command.action)
        except AttributeError:
            return


        try:
            # обработать команду
            result = handler(command)
            return result

        except:
            raise

    def process_event(self, event: Event):  # Event
        """Process messages handlers are subscribed to"""
        handlers = self.matching_event_and_handlers.get(event.name)

        if not handlers:
            return

        # обработать событие
        for handler in handlers:
            trace_name = begin_transaction(
                trace_parent=event.trace_info,
                message=event
            )
            try:
                handler(event)
                end_transaction(name=trace_name, result="success")

            except:
                capture_exception(sys.exc_info())
                end_transaction(name=trace_name, result="fail")
                raise

    def run(self, sleep=SLEEP):
        """Fetch new messages from the bus and dispatch them"""

        while True:
            message = self.bus.get_message()

            if isinstance(message, Command):
                self.process_command(message)
            elif isinstance(message, Event):
                self.process_event(message)

            # time.sleep(sleep)
