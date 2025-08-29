import asyncio
import logging

from typing import Optional
from config import KafkaConfig
from utils.dlq_manager import DLQManager
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
    def consumer_group(self) -> ConsumerGroup:
        if not self._consumer_group:
            raise RuntimeError("Consumer not initialized. Call start() first.")
        return self._consumer_group

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

            while self._started:
                done, pending = await asyncio.wait(
                    consumer_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=5.0
                )

                for task in done:
                    if task.exception():
                        logger.error(f"Consumer crashed: {task.exception()}")
                        await self._consumer_group.restart_consumer(task)
        except asyncio.CancelledError:
            logger.info("Consumer shutdown requested")
        except Exception as e:
            logger.critical("Fatal consumer error: %s", e)
            raise
        finally:
            await self.stop()
