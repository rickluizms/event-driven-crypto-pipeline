from core.kafka.consumer import KafkaConsumerClient
from core.kafka.producer import KafkaProducerClient
from core.kafka.schemas import TickEvent
from core.utils.logger import get_logger
from core.metrics.metrics import candles_emitted_total, message_processing_duration
from infra.kafka.topics import TOPIC_TICKS, TOPIC_CANDLES_1S
from src.services.candle_service import CandleAggregator

logger = get_logger(__name__)


class Candle1sConsumer:
    def __init__(self):
        self._consumer = KafkaConsumerClient(
            topic=TOPIC_TICKS,
            group_id="candle-1s-group",
        )
        self._producer = KafkaProducerClient()
        self._aggregator = CandleAggregator(interval_ms=1000, interval_label="1s")

    async def _handle(self, message: dict) -> None:
        with message_processing_duration.labels(component="candle_1s").time():
            tick = TickEvent.from_dict(message)
            candle = self._aggregator.process_tick(tick)

            if candle:
                await self._producer.send(
                    topic=TOPIC_CANDLES_1S,
                    key=candle.symbol,
                    value=candle.to_dict(),
                )
                candles_emitted_total.labels(symbol=candle.symbol, interval="1s").inc()
                logger.info(f"Candle 1s emitted: {candle.symbol} open={candle.open}")

    async def start(self) -> None:
        await self._producer.start()
        await self._consumer.start()
        logger.info("Candle1sConsumer started")
        await self._consumer.consume(self._handle)

    async def stop(self) -> None:
        await self._consumer.stop()
        await self._producer.stop()
        logger.info("Candle1sConsumer stopped")
