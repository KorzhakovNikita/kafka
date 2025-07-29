from utils.event_container import EventContainer
from utils.service_container import ServiceContainer

_service_container = ServiceContainer()


async def get_event_manager() -> EventContainer:
    return EventContainer(_service_container)