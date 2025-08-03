import asyncio
import logging
import uvicorn
from fastapi import FastAPI, HTTPException
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from config import configure_logging, KafkaConfig
from utils.containers.service_container import get_event_manager
from dependencies import KafkaDepState
from utils.kafka.manager import KafkaManager
from schemas.messages import KafkaMessage, BaseKafkaMessage
from utils.kafka.producer import producer

app = FastAPI(title="Kafka")

logger = logging.getLogger(__name__)


@app.on_event("startup")
async def start_kafka():
    configure_logging()
    logger.info(f"Start app")
    event_manager = await get_event_manager()
    manager = KafkaManager(KafkaConfig(), event_manager)
    asyncio.create_task(manager.run())
    app.state.kafka_manager = manager


@app.on_event("shutdown")
async def shutdown_kafka():
    logger.info("Stopping Kafka manager...")
    await app.state.kafka_manager.stop()


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
async def send_message(topic: str, msg: BaseKafkaMessage):
    response = {
        "topic": topic,
        "event": msg.event,
    }

    try:
        async with producer as p:
            p.send(topic, msg)
        response["status"] = "success"
    except Exception as e:
        error_msg = f"Failed to send message to topic '{topic}': {str(e)}"
        logger.error(error_msg, exc_info=True)
        response["status"] = "error"
        response["error"] = str(e)

    return response


@app.post("/restart")
async def restart_manager(kafka: KafkaDepState):
    try:
        await kafka.restart()
        return {"message": "Kafka manager restarted successfully"}
    except Exception as e:
        logger.error("Error restarting Kafka manager: %s", str(e), exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to restart Kafka manager")


@app.get("/health-check")
async def health_check(kafka: KafkaDepState):
    return {
        "consumer": await kafka.consumer.health_check(),
        "producer": await kafka.producer.health_check()
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="localhost",
        port=8000,
        reload=True,
    )