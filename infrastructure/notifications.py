from abc import ABC, abstractmethod


class AbstractNotificationsService(ABC):

    @abstractmethod
    async def send(self, html, subject: str) -> None: ...