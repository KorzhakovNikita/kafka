from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class BaseKafkaMessage(BaseModel):
    data: dict
    event: str


class MessageMetadata(BaseModel):
    topic: str
    partition: int
    offset: int


class KafkaMessage(BaseKafkaMessage):
    message_metadata: MessageMetadata


class ExceptionKafkaMessage(BaseModel):
    message: BaseKafkaMessage
    original_topic: str
    error: str
    traceback: Optional[str] = None
    timestamp: datetime = datetime.utcnow()