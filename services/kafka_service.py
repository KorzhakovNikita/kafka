import logging

from fastapi import HTTPException

from schemas.messages import BaseKafkaMessage
from utils.kafka.manager import KafkaManager


logger = logging.getLogger(__name__)


class KafkaService:

    def __init__(self, kafka_manager: KafkaManager):
        self._kafka_manager = kafka_manager

    async def send(self, topic: str, msg: BaseKafkaMessage):
        response = {
            "topic": topic,
            "event": msg.event,
        }

        try:
            async with self._kafka_manager.producer as p:
                for _ in range(1, 10):
                    await p.send(topic, msg)
            response["status"] = "success"
        except Exception as e:
            error_msg = f"Failed to send message to topic '{topic}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            response["status"] = "error"
            response["error"] = str(e)

    async def restart_manager(self):
        try:
            await self._kafka_manager.restart()
            return {"message": "Kafka manager restarted successfully"}
        except Exception as e:
            logger.error("Error restarting Kafka manager: %s", str(e), exc_info=True)
            raise HTTPException(status_code=500, detail="Failed to restart Kafka manager")

    async def health_check(self):
        return {
            "consumer": await self._kafka_manager.consumer_group.health_check(),
            "producer": await self._kafka_manager.producer.health_check()
        }