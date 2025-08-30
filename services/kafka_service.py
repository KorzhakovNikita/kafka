import logging

from aiokafka.protocol.api import Response
from fastapi import HTTPException

from schemas.messages import BaseKafkaMessage
from schemas.topic import CreateNewTopic
from utils.kafka.manager import KafkaManager


logger = logging.getLogger(__name__)


class KafkaService:

    def __init__(self, kafka_manager: KafkaManager):
        self._kafka_manager = kafka_manager

    async def send(self, topic: str, msg: BaseKafkaMessage) -> dict:
        try:
            producer = await self._kafka_manager.get_producer()
            for _ in range(1, 10):
                await producer.send(topic, msg)

            response = {
                "topic": topic,
                "event": msg.event,
                "status": "success"
            }
            return response
        except Exception as e:
            error_msg = f"Failed to send message to topic '{topic}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise e

    async def restart_manager(self):
        try:
            await self._kafka_manager.restart_full()
            return {"message": "Kafka manager restarted successfully"}
        except Exception as e:
            logger.error("Error restarting Kafka manager: %s", str(e), exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to restart Kafka manager")

    async def health_check(self) -> dict:
        return await self._kafka_manager.health_check()

    async def create_topic(self, topic: CreateNewTopic) -> dict:
        admin_client = await self._kafka_manager.get_admin_client()

        async with admin_client:
            response = await admin_client.create_topic(topic)

        return response

    async def delete_topic(self, topic: str) -> Response:
        admin_client = await self._kafka_manager.get_admin_client()

        async with admin_client:
            response = await admin_client.delete_topic(topic)

        return response

    async def list_topics(self) -> list[str]:
        admin_client = await self._kafka_manager.get_admin_client()
        return await admin_client._list_topics()
