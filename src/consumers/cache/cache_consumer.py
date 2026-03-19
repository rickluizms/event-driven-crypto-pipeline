from core.kafka.consumer import KafkaConsumerClient
from core.kafka.schemas import IndicatorEvent
from core.cache.redis_client import RedisClient
from core.utils.logger import get_logger
from infra.kafka.topics import TOPIC_INDICATORS_REALTIME

logger = get_logger(__name__)

CACHE_TTL_SECONDS = 60


class CacheConsumer:
    def __init__(self):
        self._consumer = KafkaConsumerClient(
            topic=TOPIC_INDICATORS_REALTIME,
            group_id="cache-consumer-group",
        )
        self._redis = RedisClient()

    async def _handle(self, message: dict) -> None:
        indicator = IndicatorEvent.from_dict(message)

        cache_key = f"indicator:{indicator.symbol}:{indicator.indicator_name}:{indicator.interval}"

        await self._redis.set(
            key=cache_key,
            value=indicator.to_dict(),
            ttl=CACHE_TTL_SECONDS,
        )

    async def start(self) -> None:
        await self._redis.connect()
        await self._consumer.start()
        logger.info("CacheConsumer started")
        await self._consumer.consume(self._handle)

    async def stop(self) -> None:
        await self._consumer.stop()
        await self._redis.close()
        logger.info("CacheConsumer stopped")
