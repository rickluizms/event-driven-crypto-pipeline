import json
from typing import Callable, Awaitable
from aiokafka import AIOKafkaConsumer
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)


class KafkaConsumerClient:
    def __init__(self, topic: str, group_id: str):
        settings = get_settings()
        self._bootstrap_servers = settings.kafka.bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        self._running = True
        logger.info(f"Kafka consumer started: topic={self._topic}, group={self._group_id}")

    async def consume(self, handler: Callable[[dict], Awaitable[None]]) -> None:
        if not self._consumer:
            raise RuntimeError("Consumer not started. Call start() first.")

        try:
            async for message in self._consumer:
                if not self._running:
                    break

                try:
                    await handler(message.value)
                    await self._consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
            logger.info(f"Kafka consumer stopped: topic={self._topic}")
