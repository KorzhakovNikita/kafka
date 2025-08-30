from fastapi import APIRouter
from routers.topics import topic_router
from routers.management import management_router

router = APIRouter()

router.include_router(topic_router)
router.include_router(management_router)
