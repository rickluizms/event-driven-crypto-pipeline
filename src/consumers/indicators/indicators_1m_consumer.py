from core.kafka.consumer import KafkaConsumerClient
from core.kafka.producer import KafkaProducerClient
from core.kafka.schemas import CandleEvent
from core.utils.logger import get_logger
from infra.kafka.topics import TOPIC_CANDLES_1M
from src.services.indicator_service import IndicatorService
from src.services.selector_service import SelectorService

logger = get_logger(__name__)


class Indicators1mConsumer:
    def __init__(self):
        self._consumer = KafkaConsumerClient(
            topic=TOPIC_CANDLES_1M,
            group_id="indicators-1m-group",
        )
        self._producer = KafkaProducerClient()
        self._indicator_service = IndicatorService()
        self._selector_service = SelectorService()

    async def _handle(self, message: dict) -> None:
        candle = CandleEvent.from_dict(message)
        indicators = self._indicator_service.process_candle(candle)

        for indicator in indicators:
            topics = self._selector_service.route(indicator)
            for topic in topics:
                await self._producer.send(
                    topic=topic,
                    key=indicator.symbol,
                    value=indicator.to_dict(),
                )

        if indicators:
            logger.info(
                f"Indicators 1m emitted: {candle.symbol} count={len(indicators)}"
            )

    async def start(self) -> None:
        await self._producer.start()
        await self._consumer.start()
        logger.info("Indicators1mConsumer started")
        await self._consumer.consume(self._handle)

    async def stop(self) -> None:
        await self._consumer.stop()
        await self._producer.stop()
        logger.info("Indicators1mConsumer stopped")
