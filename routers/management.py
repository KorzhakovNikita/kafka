from fastapi import APIRouter

from dependencies import KafkaService

management_router = APIRouter(prefix='/management', tags=["Management"])


@management_router.post("/restart")
async def restart_manager(kafka: KafkaService):
    return await kafka.restart_manager()


@management_router.get("/health-check")
async def health_check(kafka: KafkaService):
    return await kafka.health_check()
