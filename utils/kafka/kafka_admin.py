import logging
from typing import Optional

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError
from aiokafka.protocol.api import Response

from infrastructure.kafka_clients import BaseKafkaClient
from schemas.topic import CreateNewTopic

logger = logging.getLogger(__name__)


class KafkaAdmin(BaseKafkaClient):

    def __init__(self):
        self._admin_client: Optional[AIOKafkaAdminClient] = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            logger.warning("Consumer already running")
            return

        try:
            # implemented admin config
            self._admin_client = AIOKafkaAdminClient(
                bootstrap_servers="broker:29092,localhost:9092",
                api_version="2.8.0"
            )
            await self._admin_client.start()
        except KafkaConnectionError as e:
            logger.error("Failed to connect to Kafka: %s", str(e), exc_info=True)
            raise

    async def stop(self) -> None:
        if not self._started:
            logger.warning("Consumer not running")
            return

        await self._admin_client.close()

    async def health_check(self) -> dict:
        return {
            "status": "running" if self._started else "stopped"
        }

    async def create_topic(self, topic: CreateNewTopic) -> dict:

        if topic not in await self._list_topics():
            new_topic = NewTopic(
                name=topic.name, num_partitions=topic.num_partitions, replication_factor=topic.replication_factor
            )
            await self._admin_client.create_topics([new_topic])
            return {"message": f"Topic {topic} created"}

        return {"message": f"Topic '{topic}' is already exist"}

    async def delete_topic(self, topic: str) -> Response:
        if topic in await self._list_topics():
            response = await self._admin_client.delete_topics([topic])
            logger.info("Topic %r success deleted", topic)
            return response

    async def _list_topics(self) -> list[str]:
        return await self._admin_client.list_topics()

    async def additional_partitions_for_topic(self): ...

    async def __aenter__(self):
        await self._admin_client.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._started:
            await self._admin_client.close()

