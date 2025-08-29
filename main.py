import asyncio
import logging
import uvicorn
from fastapi import FastAPI
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from config import configure_logging
from dependencies import KafkaService
from utils.kafka.manager import kafka_manager
from schemas.messages import BaseKafkaMessage

app = FastAPI(title="Kafka")

logger = logging.getLogger(__name__)


@app.on_event("startup")
async def start_kafka():
    configure_logging()
    logger.info(f"Start app")
    asyncio.create_task(kafka_manager.run())


@app.on_event("shutdown")
async def shutdown_kafka():
    logger.info("Stopping Kafka manager...")
    await kafka_manager.stop()


@app.post("/create-topic/{topic}")
async def create_topic(topic: str):
    admin = AIOKafkaAdminClient(
        bootstrap_servers="broker:29092,localhost:9092",
        api_version="auto"
    )
    await admin.start()

    try:
        if topic not in await admin.list_topics():
            await admin.create_topics(
                [NewTopic(name=topic, num_partitions=1, replication_factor=1)]
            )
            return {"message": f"Topic {topic} created"}
        return {"message": f"Topic '{topic}' is already exist"}
    finally:
        await admin.close()


@app.post("/send-message")
async def send_message(topic: str, msg: BaseKafkaMessage, kafka: KafkaService):
    return await kafka.send(topic, msg)


@app.post("/restart")
async def restart_manager(kafka: KafkaService):
    return await kafka.restart_manager()


@app.get("/health-check")
async def health_check(kafka: KafkaService):
    return await kafka.health_check()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="localhost",
        port=8000,
        reload=True,
    )