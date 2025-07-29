from abc import ABC, abstractmethod


class BaseKafkaClient(ABC):

    @abstractmethod
    async def start(self): ...

    @abstractmethod
    async def stop(self): ...

    @abstractmethod
    async def health_check(self): ...