import logging
from pydantic import BaseModel, Field


def configure_logging(level: str = logging.INFO):
    logging.basicConfig(
        level=level,
        datefmt="%Y-%m-%d %H:%M:%S:",
        format="[%(asctime)s.%(msecs)03d] %(module)s:%(lineno)d %(levelname)s - %(message)s"
    )


class DLQConfig(BaseModel):
    default_topic: str = "dlq-topic"
    max_retries: int = 3
    retry_backoff_ms: int = 100


class KafkaProducerConfig(BaseModel):
    retry_backoff_ms: int = 100
    max_batch_size: int = 16384


class KafkaConsumerConfig(BaseModel):
    group_id: str = "default-group"
    auto_offset_reset: str = "earliest"


class KafkaConfig(BaseModel):
    bootstrap_servers: list[str] = ["localhost:9092"]
    topic: str = "email"
    producer: KafkaProducerConfig = Field(default_factory=KafkaProducerConfig)
    consumer: KafkaConsumerConfig = Field(default_factory=KafkaConsumerConfig)