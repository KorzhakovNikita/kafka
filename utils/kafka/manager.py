import asyncio
import logging

from typing import Optional
from config import KafkaConfig
from utils.dlq_manager import DLQManager
from utils.kafka.consumer import KafkaConsumer
from utils.kafka.consumer_group import ConsumerGroup
from utils.kafka.producer import KafkaProducer

logger = logging.getLogger(__name__)


class KafkaManager:

    def __init__(self, config: KafkaConfig):
        self._config = config
        self._dlq_manager: Optional[DLQManager] = None
        self._consumer_group: Optional[ConsumerGroup] = None
        self._consumer_tasks: list[asyncio.Task] = []
        self._producer: Optional[KafkaProducer] = None
        self._started = False

    @property
    def consumer(self) -> KafkaConsumer:
        if not self._consumer:
            raise RuntimeError("Consumer not initialized. Call start() first.")
        return self._consumer

    @property
    def producer(self) -> KafkaProducer:
        if not self._producer:
            raise RuntimeError("Producer not initialized. Call start() first.")
        return self._producer

    async def start(self):
        if self._started:
            logger.warning("KafkaManager is already started.")
            return

        if self._producer is None:
            self._producer = KafkaProducer(self._config.producer)
            self._dlq_manager = DLQManager(self._producer)

        if self._consumer_group is None:
            self._consumer_group = ConsumerGroup(self._config.topic, self._config.consumer_group, self._dlq_manager)

        await self._consumer_group.start()
        await self._producer.start()

        self._started = True
        logger.info("KafkaManager started successfully.")

    async def stop(self):
        if not self._started:
            logger.warning("KafkaManager is not started or already stopped.")
            return

        if self._consumer_group:
            await self._consumer_group.stop()
            self._consumer = None

        if self._producer:
            await self._producer.stop()
            self._producer = None
            self._dlq_manager = None

        self._started = False

        logger.info("KafkaManager stopped successfully.")

    async def restart(self):
        logger.info("Restarting KafkaManager...")
        await self.stop()
        await asyncio.sleep(1)
        await self.start()

    async def run(self):
        await self.start()

        try:
            consumer_tasks = await self._consumer_group.start_consuming()
            #await asyncio.gather(*consumer_tasks)

            while self._started:
                #await asyncio.wait(consumer_tasks, timeout=5.0)
                await asyncio.sleep(1)

            # async for message, consumer in self._consumer_group.get_message():
            #     logger.info("Received message: %s", message)
                # parsed_msg, error = await consumer.process_message(message)
                # if error:
                #     await self._dlq_manager.send_to_dlq(message.topic, parsed_msg, error)
        except asyncio.CancelledError:
            logger.info("Consumer shutdown requested")
        except Exception as e:
            logger.critical("Fatal consumer error: %s", e)
            raise
        finally:
            await self.stop()
