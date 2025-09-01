from fastapi import APIRouter

from dependencies import KafkaService
from schemas.messages import BaseKafkaMessage
from schemas.topic import CreateNewTopic

topic_router = APIRouter(prefix='/topics', tags=["Topics"])


@topic_router.post("",)
async def create_topic(topic: CreateNewTopic, kafka: KafkaService):
    return await kafka.create_topic(topic)


@topic_router.delete("/{topic_name}")
async def delete_topic(topic_name: str, kafka: KafkaService):
    return await kafka.delete_topic(topic_name)


@topic_router.post("/{topic_name}/send")
async def send_message(topic_name: str, msg: BaseKafkaMessage, kafka: KafkaService):
    return await kafka.send(topic_name, msg)


@topic_router.get("", response_model=list[str])
async def list_topics(kafka: KafkaService):
    return await kafka.list_topics()


@topic_router.get("/message_b y_topic")
async def get_messages(kafka: KafkaService):
    return await kafka.get_message_topic()
