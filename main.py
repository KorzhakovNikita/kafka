import asyncio
import logging
import uvicorn
from fastapi import FastAPI

from config import configure_logging
from routers import router
from utils.kafka.manager import kafka_manager


logger = logging.getLogger(__name__)


app = FastAPI(title="Kafka-FastAPI-demo")

app.include_router(router)


@app.on_event("startup")
async def start_kafka():
    configure_logging()
    logger.info(f"Start app")
    asyncio.create_task(kafka_manager.run())


@app.on_event("shutdown")
async def shutdown_kafka():
    logger.info("Stopping Kafka manager...")
    await kafka_manager.stop()


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="localhost",
        port=8000,
        reload=True,
    )