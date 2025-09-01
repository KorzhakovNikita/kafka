from abc import ABC, abstractmethod

from schemas.messages import BaseKafkaMessage
from schemas.topic import CreateNewTopic


class IKafkaService(ABC):
    """Interface for working with Kafka"""

    # Management state
    @abstractmethod
    async def restart_manager(self) -> dict:
        """Full restart manager"""

    @abstractmethod
    async def health_check(self) -> dict:
        """Get a health_check of producer, consumer_group and admin_client"""

    # Topics
    @abstractmethod
    async def send(self, topic: str, msg: BaseKafkaMessage) -> dict:
        """Sending a message to a Kafka topic"""

    @abstractmethod
    async def create_topic(self, topic: CreateNewTopic) -> dict:
        """Create new topics in the cluster metadata"""

    @abstractmethod
    async def delete_topic(self, topic: str) -> dict:
        """Delete new topics in the cluster metadata"""

    @abstractmethod
    async def list_topics(self) -> list[str]:
        """Get topics in the cluster metadata"""
