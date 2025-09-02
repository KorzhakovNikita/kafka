from functools import lru_cache
from typing import Annotated

from fastapi import Depends

from services.kafka_service import KafkaService
from utils.kafka.manager import kafka_manager


@lru_cache(maxsize=1)
def get_kafka_service() -> KafkaService:
    return KafkaService(kafka_manager)


KafkaService = Annotated[KafkaService, Depends(get_kafka_service)]

