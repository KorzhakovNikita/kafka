from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class KafkaMessage(BaseModel):
    data: dict
    event: str


class ExceptionKafkaMessage(BaseModel):
    message: KafkaMessage
    original_topic: str
    error: str
    traceback: Optional[str] = None
    timestamp: datetime = datetime.utcnow()