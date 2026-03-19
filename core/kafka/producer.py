import json
from aiokafka import AIOKafkaProducer
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)


class KafkaProducerClient:
    def __init__(self):
        settings = get_settings()
        self._bootstrap_servers = settings.kafka.bootstrap_servers
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        await self._producer.start()
        logger.info("Kafka producer started")

    async def send(self, topic: str, key: str, value: dict) -> None:
        if not self._producer:
            raise RuntimeError("Producer not started. Call start() first.")

        try:
            await self._producer.send_and_wait(topic, key=key, value=value)
        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            raise

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()
            self._producer = None
            logger.info("Kafka producer stopped")
