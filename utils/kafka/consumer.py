import logging
from typing import Union, List, Optional, AsyncGenerator

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
import json
from config import KafkaConsumerConfig
from infrastructure.kafka_clients import BaseKafkaClient
from schemas.messages import MessageMetadata

logger = logging.getLogger(__name__)


class KafkaConsumer(BaseKafkaClient):

    def __init__(self, topic: str, consumer_config: KafkaConsumerConfig):
        self._topic = topic
        self._consumer_config = consumer_config
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._is_running = False

    async def start(self):
        if self._is_running:
            logger.warning("Consumer already running")
            return

        try:
            self._consumer = AIOKafkaConsumer(
                self._topic,
                **self._consumer_config.model_dump(),
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )

            await self._consumer.start()
            logger.info("Starting Kafka consumer for topics: %s", self._consumer.subscription())
            self._is_running = True
        except Exception as e:
            self._is_running = False
            logger.error("Failed to start consumer: %s", str(e), exc_info=True)
            raise

    async def stop(self):
        if not self._is_running:
            logger.warning("Consumer not running")
            return

        try:
            await self._consumer.stop()
            logger.info("Consumer stopped successfully")
        except Exception as e:
            logger.error("Failed to stop consumer: %s", str(e), exc_info=True)
            raise
        finally:
            self._is_running = False
            self._consumer = None

    async def get_message(self) -> AsyncGenerator[ConsumerRecord, None]:
        async for msg in self._consumer:
            yield msg

    async def commit(self, msg: MessageMetadata):
        tp = TopicPartition(msg.topic, msg.partition)
        await self._consumer.commit({tp: msg.offset + 1})

    async def subscribe(self, topics: Union[str, List[str]]):
        if not self._is_running:
            raise RuntimeError("Consumer is not running")

        if isinstance(topics, str):
            topics = [topics]

        self._consumer.subscribe(topics)
        logger.info(f"Subscribed to topics %s:", topics)

    async def health_check(self) -> dict:
        return {
            "status": "running" if self._is_running else "stopped",
            "subscription": self._consumer.subscription(),
            "assignment": [f"{t}-{p}" for t, p in self._consumer.assignment()],
        }