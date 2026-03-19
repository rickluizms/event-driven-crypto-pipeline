import asyncio
import signal
from core.binance.ws_client import BinanceWebSocketClient
from core.kafka.producer import KafkaProducerClient
from core.utils.logger import get_logger
from infra.kafka.topics import TOPIC_TICKS
from src.producers.market_data.mapper import map_trade_to_tick

logger = get_logger(__name__)


class MarketDataProducer:
    def __init__(self):
        self._ws_client = BinanceWebSocketClient()
        self._producer = KafkaProducerClient()
        self._running = False

    async def _on_message(self, raw_msg: dict) -> None:
        try:
            tick = map_trade_to_tick(raw_msg)
            await self._producer.send(
                topic=TOPIC_TICKS,
                key=tick.symbol,
                value=tick.to_dict(),
            )
        except Exception as e:
            logger.error(f"Failed to process trade message: {e}")

    async def start(self) -> None:
        self._running = True
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                pass

        logger.info("MarketDataProducer starting...")

        await self._producer.start()

        try:
            await self._ws_client.connect(self._on_message)
        finally:
            await self.stop()

    async def stop(self) -> None:
        if not self._running:
            return

        self._running = False
        logger.info("MarketDataProducer shutting down...")

        self._ws_client.stop()
        await self._producer.stop()

        logger.info("MarketDataProducer stopped.")


async def main() -> None:
    producer = MarketDataProducer()
    await producer.start()


if __name__ == "__main__":
    asyncio.run(main())
