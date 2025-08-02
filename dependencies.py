from typing import Annotated

from fastapi import Depends
from utils.kafka.manager import KafkaManager


async def get_kafka_manager_state() -> KafkaManager:
    from main import app
    return app.state.kafka_manager

KafkaDepState = Annotated[KafkaManager, Depends(get_kafka_manager_state)]

