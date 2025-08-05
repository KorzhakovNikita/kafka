import asyncio
import logging
from typing import Union, List, Optional, AsyncGenerator

from aiokafka import AIOKafkaConsumer, ConsumerRecord, TopicPartition
import json
from config import KafkaConsumerConfig
from events.base_event import AbstractEvent
from infrastructure.kafka_clients import BaseKafkaClient
from schemas.messages import MessageMetadata, KafkaMessage

logger = logging.getLogger(__name__)


class KafkaConsumer(BaseKafkaClient):

    def __init__(self, topic: str, consumer_config: KafkaConsumerConfig):
        self._topic = topic
        self._consumer_config = consumer_config
        self._consumer_client: Optional[AIOKafkaConsumer] = None
        self._is_running = False
        self._max_retries = consumer_config.retry.max_retries
        self._base_delay = consumer_config.retry.backoff_ms
        self._retryable_errors = consumer_config.retry.retryable_errors

    async def start(self):
        if self._is_running:
            logger.warning("Consumer already running")
            return

        try:
            self._consumer_client = AIOKafkaConsumer(
                self._topic,
                **self._consumer_config.model_dump(exclude={"retry"}),
                value_deserializer=lambda v: json.loads(v.decode('utf-8'))
            )

            await self._consumer_client.start()
            logger.info("Starting Kafka consumer for topics: %s", self._consumer_client.subscription())
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
            await self._consumer_client.stop()
            logger.info("Consumer stopped successfully")
        except Exception as e:
            logger.error("Failed to stop consumer: %s", str(e), exc_info=True)
            raise
        finally:
            self._is_running = False
            self._consumer_client = None

    async def get_message(self) -> AsyncGenerator[ConsumerRecord, None]:
        async for msg in self._consumer_client:
            yield msg

    async def commit(self, msg: MessageMetadata):
        tp = TopicPartition(msg.topic, msg.partition)
        await self._consumer_client.commit({tp: msg.offset + 1})

    async def subscribe(self, topics: Union[str, List[str]]):
        if not self._is_running:
            raise RuntimeError("Consumer is not running")

        if isinstance(topics, str):
            topics = [topics]

        self._consumer_client.subscribe(topics)
        logger.info(f"Subscribed to topics %s:", topics)

    async def health_check(self) -> dict:
        return {
            "status": "running" if self._is_running else "stopped",
            "subscription": self._consumer_client.subscription(),
            "assignment": [f"{t}-{p}" for t, p in self._consumer_client.assignment()],
        }

    async def process_message(
        self,
        event_handler: AbstractEvent,
        message: ConsumerRecord
    ) -> tuple[Optional[KafkaMessage], Optional[Exception]]:
        parsed_msg = self._parse_message(message)
        for attempt in range(1, self._max_retries + 1):
            try:
                await event_handler.process_event(parsed_msg)
                await self.commit(parsed_msg.message_metadata)
                logger.info("The event %r was successful", parsed_msg.event.title())
                return parsed_msg, None
            except Exception as e:
                if not self._should_retry(attempt, e):
                    return parsed_msg, e
                await self._wait_before_retry(attempt, e)

    @staticmethod
    def _parse_message(message: ConsumerRecord) -> KafkaMessage:
        try:
            return KafkaMessage(
                data=message.value["data"],
                event=message.value["event"],
                message_metadata=MessageMetadata(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset
                )
            )
        except KeyError as e:
            raise ValueError(f"Invalid message format: missing {str(e)}") from e

    def _should_retry(self, attempt: int, error: Exception) -> bool:
        if type(error) in self._retryable_errors and attempt < self._max_retries:
            return True
        return False

    async def _wait_before_retry(self, attempt: int, error: Exception):
        delay = self._base_delay / 1000 * (2 ** attempt)
        logger.warning(
            "Waiting %.2f seconds before retry (attempt %d, error: %s)",
            delay,
            attempt,
            str(error)
        )
        await asyncio.sleep(delay)
