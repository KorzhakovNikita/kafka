import logging
from typing import Tuple, Type, Union, Literal

from pydantic import BaseModel, Field


def configure_logging(level: str = logging.INFO):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S:",
        format="[%(asctime)s.%(msecs)03d] %(module)s:%(lineno)d %(levelname)s - %(message)s"
    )


class DLQConfig(BaseModel):
    default_topic: str = "dlq-topic"


class KafkaRetryConfig(BaseModel):
    backoff_ms: int = 1000
    max_retries: int = 3
    retryable_errors: Tuple[Type[BaseException], ...] = (TimeoutError, ConnectionError)


class KafkaProducerConfig(BaseModel):
    acks: Union[Literal["all", "0", "1", "-1"], int] = 1
    retry_backoff_ms: int = 100
    max_batch_size: int = 16384


class KafkaConsumerConfig(BaseModel):
    retry: KafkaRetryConfig = Field(default_factory=KafkaRetryConfig)
    group_id: str = "default-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False


class ConsumerGroupConfig(BaseModel):
    consumer_config: KafkaConsumerConfig = Field(default_factory=KafkaConsumerConfig)
    consumer_count: int = 2


class KafkaConfig(BaseModel):
    bootstrap_servers: list[str] = ["localhost:9092"]
    topic: str = "email"
    producer: KafkaProducerConfig = Field(default_factory=KafkaProducerConfig)
    consumer_group: ConsumerGroupConfig = Field(default_factory=ConsumerGroupConfig)
