import asyncio
import logging
from typing import Optional

from config import ConsumerGroupConfig, KafkaConsumerConfig
from utils.containers.service_container import get_event_manager
from utils.dlq_manager import DLQManager
from utils.kafka.consumer import KafkaConsumer

logger = logging.getLogger(__name__)


class ConsumerGroup:

    def __init__(self, topic: str, config: ConsumerGroupConfig, dlq_manager: DLQManager):
        self._topic = topic
        self._config = config
        self._event_manager = None
        self._dlq_manager = dlq_manager
        self._consumers: list[KafkaConsumer] = []
        self._consumers_task: list[asyncio.Task] = []

    async def create_consumer(
            self, name: str, topic: str, config: Optional[KafkaConsumerConfig] = None
    ) -> KafkaConsumer:
        if config is None:
            config = self._config.consumer_config

        consumer = KafkaConsumer(
            config,
            self._event_manager,
            self._dlq_manager,
            name=name,
            topic=topic
        )

        return consumer

    async def start(self):
        self._event_manager = await get_event_manager()

        for i in range(self._config.consumer_count):
            consumer = await self.create_consumer(name=f"Consumer {i}", topic=self._topic)

            await consumer.start()
            self._consumers.append(consumer)

            if len(self._consumers) > len(consumer.partitions_for_topic()):
                logger.warning("The %s idle in the topic: %r", consumer.name, self._topic)

    async def start_consuming(self):

        for consumer in self._consumers:
            task = asyncio.create_task(self._consumer_processing_loop(consumer))
            self._consumers_task.append(task)
            logger.info("%s waiting for messages..", consumer.name)

        return self._consumers_task

    async def stop(self):

        for consumer in self._consumers:
            await consumer.stop()
        try:
            for task in self._consumers_task:
                task.cancel()
        except asyncio.CancelledError:
            logger.error("%s was canceled!", task)
            pass

    async def restart_consumer(self, task: asyncio.Task):
        try:
            task_index = self._consumers_task.index(task)
            old_consumer = self._consumers[task_index]
            await old_consumer.stop()

            new_consumer = await self.create_consumer(name=old_consumer.name, topic=self._topic)
            await new_consumer.start()

            self._consumers[task_index] = new_consumer
            new_task = asyncio.create_task(self._consumer_processing_loop(new_consumer))
            self._consumers_task[task_index] = new_task
        except Exception as e:
            logger.exception("Failed to restart consumer: %s", str(e))

    async def health_check(self) -> dict:
        health_check_consumers = {}

        for consumer in self._consumers:
            health_check_consumers[consumer.name] = await consumer.health_check()

        return health_check_consumers

    async def _consumer_processing_loop(self, consumer: KafkaConsumer):
        try:
            async for msg in consumer.get_message():
                await consumer.process_message(msg)
        except asyncio.CancelledError:
            logger.info(f"{consumer.name} processing cancelled")
