import asyncio
import logging

from typing import Optional
from config import KafkaConfig
from utils.dlq_manager import DLQManager
from utils.kafka.consumer_group import ConsumerGroup
from utils.kafka.kafka_admin import KafkaAdmin
from utils.kafka.producer import KafkaProducer

logger = logging.getLogger(__name__)


class KafkaManager:

    def __init__(self, config: KafkaConfig):
        self._config = config
        self._dlq_manager: Optional[DLQManager] = None
        self._consumer_group: Optional[ConsumerGroup] = None
        self._producer: Optional[KafkaProducer] = None
        self._admin_client: Optional[KafkaAdmin] = None

    async def get_producer(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(self._config.producer)
            await self._producer.start()
        return self._producer

    async def get_admin_client(self) -> KafkaAdmin:
        if self._admin_client is None:
            self._admin_client = KafkaAdmin()
            await self._admin_client.start()
        return self._admin_client

    async def get_consumer_group(self) -> ConsumerGroup:
        if self._consumer_group is None:
            producer = await self.get_producer()
            self._consumer_group = ConsumerGroup(
                self._config.topic,
                self._config.consumer_group,
                DLQManager(producer)
            )
            await self._consumer_group.start()
        return self._consumer_group

    async def stop(self):
        stop_tasks = []

        if self._producer:
            stop_tasks.append(self._producer.stop())
        if self._admin_client:
            stop_tasks.append(self._admin_client.stop())
        if self._consumer_group:
            stop_tasks.append(self._consumer_group.stop())

        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        self._producer = None
        self._admin_client = None
        self._consumer_group = None

        logger.info("KafkaManager stopped successfully.")

    async def restart_full(self):
        logger.info("Full restarting KafkaManager...")

        await self.stop()
        await asyncio.sleep(1)

        try:
            await self.get_producer()
            await self.get_admin_client()
            await self.get_consumer_group()
            await self.run()
        except Exception as e:
            logger.error("Failed during restart: %s", e)
            raise

        logger.info("KafkaManager fully restarted")

    async def run(self):
        await self.get_consumer_group()

        try:
            consumer_tasks = await self._consumer_group.start_consuming()

            while consumer_tasks:
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
            logger.info("Consumer Group shutdown requested")
        except Exception as e:
            logger.critical("Fatal consumer error: %s", e)
            raise
        finally:
            await self._consumer_group.stop()

    async def health_check(self) -> dict:
        health = {
            "producer": "not_initialized",
            "consumer_group": "not_initialized",
            "admin": "not_initialized",
        }

        if self._producer:
            health["producer"] = await self._producer.health_check()

        if self._consumer_group:
            health["consumer_group"] = await self._consumer_group.health_check()

        if self._admin_client:
            health["admin"] = await self._admin_client.health_check()

        return health


config = KafkaConfig()
kafka_manager = KafkaManager(config)