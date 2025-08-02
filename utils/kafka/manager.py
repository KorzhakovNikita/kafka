import asyncio
import logging
from typing import Optional
from config import KafkaConfig
from utils.kafka.consumer import KafkaConsumer
from schemas.messages import KafkaMessage
from utils.containers.event_container import EventContainer
from events.base_event import AbstractEvent
from utils.kafka.producer import KafkaProducer

logger = logging.getLogger(__name__)


class KafkaManager:

    def __init__(self, config: KafkaConfig, event_manager: EventContainer):
        self._config = config
        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None
        self._started = False
        self._event_manager = event_manager

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

        if self._consumer is None:
            self._consumer = KafkaConsumer(self._config.topic, self._config.consumer)
        if self._producer is None:
            self._producer = KafkaProducer(self._config.producer)

        await self._consumer.start()
        await self._producer.start()

        self._started = True
        logger.info("KafkaManager started successfully.")

    async def stop(self):
        if not self._started:
            logger.warning("KafkaManager is not started or already stopped.")
            return

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None

        if self._producer:
            await self._producer.stop()
            self._producer = None
        self._started = False

        logger.info("KafkaManager stopped successfully.")

    async def restart(self):
        logger.info("Restarting KafkaManager...")
        await self.stop()
        await asyncio.sleep(1)
        await self.start()

    async def run(self):
        await self.start()
        logger.info("KafkaManager processing...")
        try:
            while True:
                async for message in self._consumer.get_message():
                    logger.info("Received message: %s", message)
                    event_handler: AbstractEvent = await self._event_manager.get_event_handler(message.value["event"])
                    msg = KafkaMessage(data=message.value["data"], event=message.value["event"])
                    _ = asyncio.create_task(self.handle_event(event_handler, msg, message.topic))
        except asyncio.CancelledError:
            logger.info("Consumer shutdown requested")
        except Exception as e:
            logger.critical("Fatal consumer error: %s", e)
            raise
        finally:
            await self.stop()

    async def handle_event(self, event_handler: AbstractEvent, message: KafkaMessage, original_topic: str):
        try:
            await event_handler.process_event(message)
        except Exception as e:
            logger.error(
                "Unexpected error processing %s: %s",
                message.event, str(e),
            )
            await self._producer.dlq_manager.send_to_dlq(original_topic, message, e)
        else:
            logger.info("The event '%s' was successful", message.event.title())

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        if self._started:
            await self.stop()
