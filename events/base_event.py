import logging
from abc import abstractmethod
from typing import Type

from pydantic import BaseModel, ValidationError

from exceptions.events import EventValidationError
from infrastructure.service_container import AbstractServiceContainer
from schemas.messages import KafkaMessage

logger = logging.getLogger(__name__)


class AbstractEvent:
    event_name: ""
    validation_model: Type[BaseModel]

    def __init__(self, services: AbstractServiceContainer):
        self._services = services

        if not self.event_name:
            raise AttributeError("Attribute event_name must be set")

    async def process_event(self, message: KafkaMessage):
        modeled = await self._validate_request(message.data)
        logger.info(
            f"{self.__class__.__name__}.{self.process_event.__name__}:"
            f" calling _on_event() with data: {modeled.model_dump_json()}",
        )
        await self._on_event(modeled)

    async def _validate_request(self, data: dict) -> BaseModel:
        try:
            return self.validation_model(**data)
        except ValidationError as e:
            raise EventValidationError(str(e))

    @abstractmethod
    async def _on_event(self, data: BaseModel): ...


