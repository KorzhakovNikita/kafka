from infrastructure.analytics import AbstractAnalyticsService
from infrastructure.notifications import AbstractNotificationsService
from infrastructure.service_container import AbstractServiceContainer
from services.notification import NotificationsService


class ServiceContainer(AbstractServiceContainer):

    def __init__(self):
        self._notifications = NotificationsService()
        self._analytics = None # external service

    @property
    def notifications(self) -> AbstractNotificationsService:
        return self._notifications

    @property
    def analytics(self) -> AbstractAnalyticsService:
        return self._analytics
