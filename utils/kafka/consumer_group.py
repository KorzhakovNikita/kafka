import asyncio
import logging
from typing import AsyncGenerator

from aiokafka import ConsumerRecord

from config import ConsumerGroupConfig
from utils.containers.event_container import EventContainer
from utils.containers.service_container import get_event_manager
from utils.dlq_manager import DLQManager
from utils.kafka.consumer import KafkaConsumer
from schemas.types import ConsumerMessage

logger = logging.getLogger(__name__)


class ConsumerGroup:

    def __init__(self, topic: str, config: ConsumerGroupConfig, dlq_manager: DLQManager):
        self._topic = topic
        self._config = config
        self._event_manager = None
        self._dlq_manager = dlq_manager
        self._consumers: list[KafkaConsumer] = []
        self._queue: asyncio.Queue[ConsumerMessage] = asyncio.Queue()
        self._consumers_task: list[asyncio.Task] = []

    async def start(self):
        self._event_manager = await get_event_manager()

        for i in range(self._config.consumer_count):
            consumer = KafkaConsumer(
                self._topic,
                self._config.consumer_config,
                self._event_manager,
                self._dlq_manager,
                name=f"Consumer {i}")
            await consumer.start()
            self._consumers.append(consumer)

            #Todo: if len(consumer_count) > topic.partitions log.warning

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
            #message about cancel task
            pass

    async def _consumer_processing_loop(self, consumer: KafkaConsumer):
        try:
            async for msg in consumer.get_message():
                await consumer.process_message(msg)
        except asyncio.CancelledError:
            logger.info(f"{consumer.name} processing cancelled")

    # async def get_message(self) -> AsyncGenerator[ConsumerMessage, None]:
    #     while True:
    #         logger.info("\n"
    #                     "QUEUE=%s"
    #                     "\n",self._queue
    #                     )
    #         msg = await self._queue.get()
    #         logger.info("\n %s gave partition=%s, offset=%s.\n", msg.consumer.name, msg.message.partition, msg.message.offset)
    #         yield msg
    #
    # async def _consume_message(self, consumer: KafkaConsumer):
    #     async for message in consumer.get_message():
    #         await self._queue.put(ConsumerMessage(message, consumer))

