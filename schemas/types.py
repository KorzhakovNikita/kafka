from dataclasses import dataclass

from aiokafka import ConsumerRecord

from utils.kafka.consumer import KafkaConsumer


@dataclass
class ConsumerMessage:
    message: ConsumerRecord
    consumer: KafkaConsumer

    def __iter__(self):
        return iter((self.message, self.consumer))