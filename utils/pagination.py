from collections import defaultdict
from datetime import datetime
from math import ceil
from pprint import pprint
from typing import Optional

from aiokafka import TopicPartition, AIOKafkaConsumer

from settings import settings


class KafkaPagination:

    def __init__(self):
        self._consumer_ui: Optional[AIOKafkaConsumer] = None
        self._current_offsets = defaultdict(dict) # topic: {partition: [begin_offset, end_offset]}
        self.page_size = 50

    async def ensure_consumer_initialized(self):
        if self._consumer_ui is None:
            self._consumer_ui = AIOKafkaConsumer(
                bootstrap_servers=settings.KAFKA_BROKER,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                group_id=None
            )
            await self._consumer_ui.start()

    async def prepare_consumer_for_topic_partition(self, topic: str, partition: int):
        await self.ensure_consumer_initialized()

        tp = TopicPartition(topic, partition)

        if topic not in self._current_offsets or partition not in self._current_offsets[topic]:
            begin_offset = await self.get_begin_offset(tp)
            end_offset = await self.get_end_offset(tp)
            self._current_offsets[tp.topic][tp.partition] = [begin_offset, end_offset]

        self._consumer_ui.assign([tp])
        pprint(self._current_offsets)
        return tp

    async def get_page_messages(self, topic: str, partition: int, page: int):
        tp = await self.prepare_consumer_for_topic_partition(topic, partition)
        begin_offset, end_offset = self._current_offsets[topic][partition]
        total_msgs = end_offset - begin_offset

        messages = []

        total_pages = ceil(total_msgs / self.page_size)

        if page > total_pages:
            return {
                "current_page": page,
                "total_pages": total_pages,
                "data": []
            }

        start_offset = begin_offset + (page-1) * self.page_size
        end_offset = min(page * self.page_size + begin_offset, end_offset)

        self._consumer_ui.seek(tp, start_offset)
        max_messages = end_offset - start_offset

        async for msg in self._consumer_ui:
            messages.append({
                "offset": msg.offset,
                "key": msg.key.decode() if msg.key else None,
                "value": msg.value.decode(),
                "partition": msg.partition,
                "timestamp": datetime.fromtimestamp(msg.timestamp / 1000)
            })
            if len(messages) >= max_messages:
                break

        sorted_messages = sorted(messages, key=lambda mes: mes.get("timestamp"))
        return {
            "current_page": page,
            "total_pages": total_pages,
            "data": sorted_messages
        }

    async def get_begin_offset(self, tp: TopicPartition) -> int:
        begin_offset = await self._consumer_ui.beginning_offsets([tp])
        return begin_offset[tp]

    async def get_end_offset(self, tp: TopicPartition) -> int:
        end_offset = await self._consumer_ui.end_offsets([tp])
        return end_offset[tp]
