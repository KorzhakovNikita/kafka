import logging
import traceback
from typing import Optional
from config import DLQConfig
from schemas.messages import KafkaMessage, ExceptionKafkaMessage
from utils.kafka.producer import KafkaProducer

logger = logging.getLogger(__name__)


class DLQManager:

    def __init__(self, producer: KafkaProducer, dlq_config: Optional[DLQConfig] = None):
        self.dlq_config = dlq_config or DLQConfig()
        self._producer = producer

    @staticmethod
    def _build_dlq_message(topic, message: KafkaMessage, error) -> ExceptionKafkaMessage:
        return ExceptionKafkaMessage(
            message=message,
            error=str(error),
            original_topic=topic,
            traceback=traceback.format_exc()
        )

    async def send_to_dlq(self, topic, message, error) -> None:
        try:
            logger.warning(
                f"Attempting to send failed message to DLQ topic. Message: %s",
                message)
            msg = self._build_dlq_message(topic, message, error)
            await self._producer.send(self.dlq_config.default_topic, msg)
        except Exception as dlq_error:
            logger.critical(
                "Failed to send message to DLQ topic. DLQ error: %s",
                str(dlq_error),
                extra={
                    "full_traceback": traceback.format_exc(),
                }
            )



