import asyncio
from typing import AsyncGenerator

from aiokafka import ConsumerRecord

from config import ConsumerGroupConfig
from utils.containers.event_container import EventContainer
from utils.kafka.consumer import KafkaConsumer
from schemas.types import ConsumerMessage

class ConsumerGroup:

    def __init__(self, topic: str, config: ConsumerGroupConfig, event_manager: EventContainer):
        self._topic = topic
        self._config = config
        self._event_manager = event_manager
        self._consumers: list[KafkaConsumer] = []
        self._queue: asyncio.Queue[ConsumerRecord] = asyncio.Queue()
        self._consumers_task: list[asyncio.Task] = []

    async def start(self):

        for _ in range(self._config.consumer_count):
            consumer = KafkaConsumer(self._topic, self._config.consumer_config)
            await consumer.start()
            self._consumers.append(consumer)

            task = asyncio.create_task(self._consume_message(consumer))
            self._consumers_task.append(task)
            #Todo: if len(consumer_count) > topic.partitions log.warning

    async def stop(self):

        for consumer in self._consumers:
            await consumer.stop()

        for task in self._consumers_task:
            task.cancel()

    async def get_message(self) -> AsyncGenerator[ConsumerMessage, None]:
        while True:
            msg = await self._queue.get()
            yield msg

    async def _consume_message(self, consumer: KafkaConsumer):
        async for message in consumer.get_message():
            await self._queue.put(ConsumerMessage(message, consumer))

