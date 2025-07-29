from abc import ABC, abstractmethod


class AbstractAnalyticsService(ABC):

    @abstractmethod
    async def metrics(self): ...