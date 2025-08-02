from typing import Type

from events.base_event import AbstractEvent
from events.email_event import SendEmailEvent
from exceptions.events import EventNotFoundError


class EventContainer:

    def __init__(self, services):
        self.services = services
        self.events: dict[str, Type[AbstractEvent]] = {
            "email-event": SendEmailEvent
        }

    async def get_event_handler(self, event_name: str):
        if event_name not in self.events:
            raise EventNotFoundError

        event_cls = self.events[event_name]
        return event_cls(self.services)