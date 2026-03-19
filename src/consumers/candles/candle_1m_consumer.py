from decimal import Decimal
from core.kafka.consumer import KafkaConsumerClient
from core.kafka.producer import KafkaProducerClient
from core.kafka.schemas import CandleEvent, TickEvent
from core.utils.logger import get_logger
from infra.kafka.topics import TOPIC_CANDLES_1S, TOPIC_CANDLES_1M
from src.services.candle_service import CandleAggregator

logger = get_logger(__name__)


class Candle1mConsumer:
    def __init__(self):
        self._consumer = KafkaConsumerClient(
            topic=TOPIC_CANDLES_1S,
            group_id="candle-1m-group",
        )
        self._producer = KafkaProducerClient()
        self._aggregator = CandleAggregator(interval_ms=60_000, interval_label="1m")

    async def _handle(self, message: dict) -> None:
        candle_1s = CandleEvent.from_dict(message)

        synthetic_tick = TickEvent(
            symbol=candle_1s.symbol,
            price=candle_1s.close,
            quantity=candle_1s.volume,
            timestamp=candle_1s.close_time,
            trade_id=0,
        )

        candle_1m = self._aggregator.process_tick(synthetic_tick)

        if candle_1m:
            await self._producer.send(
                topic=TOPIC_CANDLES_1M,
                key=candle_1m.symbol,
                value=candle_1m.to_dict(),
            )
            logger.info(f"Candle 1m emitted: {candle_1m.symbol} open={candle_1m.open}")

    async def start(self) -> None:
        await self._producer.start()
        await self._consumer.start()
        logger.info("Candle1mConsumer started")
        await self._consumer.consume(self._handle)

    async def stop(self) -> None:
        await self._consumer.stop()
        await self._producer.stop()
        logger.info("Candle1mConsumer stopped")
