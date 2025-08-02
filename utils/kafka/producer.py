import logging
from typing import Optional

from aiokafka import AIOKafkaProducer

from config import KafkaProducerConfig
from utils.dlq_manager import DLQManager
from infrastructure.kafka_clients import BaseKafkaClient
from schemas.messages import KafkaMessage
from utils.serializer import SerializerRegister

logger = logging.getLogger(__name__)


class KafkaProducer(BaseKafkaClient):

    def __init__(self, producer_config: KafkaProducerConfig):
        self._producer_config = producer_config
        self.serializer_register = SerializerRegister()
        self._producer: Optional[AIOKafkaProducer] = None
        self._dlq_manager: Optional[DLQManager] = None
        self._is_running = False

    def _serialize_message(self, value):
        serializer = self.serializer_register.get_serializer(value)
        return serializer.serialize(value)

    async def start(self):
        if self._is_running:
            logger.warning("Producer is already running")
            return
        try:
            self._producer = AIOKafkaProducer(
                **self._producer_config.model_dump(),
                value_serializer=self._serialize_message
            )
            await self._producer.start()
            self._dlq_manager = DLQManager(self._producer)
            self._is_running = True
            logger.info("Starting Kafka producer")
        except Exception as e:
            logger.error("Failed to start producer: %s", str(e), exc_info=True)

    async def stop(self):
        if not self._is_running:
            logger.warning("Producer not running")

        try:
            await self._producer.stop()
            self._is_running = False
            logger.info("Producer stopped successfully")
        except Exception as e:
            logger.error("Failed to stop producer: %s", str(e), exc_info=True)
            raise
        finally:
            self._producer = None
            self._is_running = False

    async def health_check(self) -> dict:
        return {
            "status": "running" if self._is_running else "stopped"
        }

    async def send(self, topic: str, message: KafkaMessage):

        try:
            logger.info(f"Publish message: %s to topic %s", message, topic)
            await self._producer.send(topic, message)
        except Exception as e:
            logger.error(
                f"Failed to publish message to topic: %s to topic. Error: %s",
                message, str(e),
                exc_info=True
            )
            return await self._dlq_manager.send_to_dlq(
                topic,
                message,
                error=e,
            )


