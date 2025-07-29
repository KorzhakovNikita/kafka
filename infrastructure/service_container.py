from abc import ABC, abstractmethod

from infrastructure.analytics import AbstractAnalyticsService
from infrastructure.notifications import AbstractNotificationsService


class AbstractServiceContainer(ABC):

    @property
    @abstractmethod
    def notifications(self) -> AbstractNotificationsService: ...

    @property
    @abstractmethod
    def analytics(self) -> AbstractAnalyticsService: ...