from core.kafka.consumer import KafkaConsumerClient
from core.kafka.schemas import CandleEvent
from core.db.postgres import PostgresClient
from core.utils.logger import get_logger
from infra.kafka.topics import TOPIC_CANDLES_1S

logger = get_logger(__name__)

INSERT_CANDLE_SQL = """
    INSERT INTO candles (symbol, interval, open, high, low, close, volume, open_time, close_time)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (symbol, interval, open_time) DO NOTHING
"""

BATCH_SIZE = 50


class CandleStorageConsumer:
    def __init__(self):
        self._consumer = KafkaConsumerClient(
            topic=TOPIC_CANDLES_1S,
            group_id="candle-storage-group",
        )
        self._db = PostgresClient()
        self._batch: list[tuple] = []

    async def _handle(self, message: dict) -> None:
        candle = CandleEvent.from_dict(message)

        self._batch.append((
            candle.symbol,
            candle.interval,
            float(candle.open),
            float(candle.high),
            float(candle.low),
            float(candle.close),
            float(candle.volume),
            candle.open_time,
            candle.close_time,
        ))

        if len(self._batch) >= BATCH_SIZE:
            await self._flush()

    async def _flush(self) -> None:
        if not self._batch:
            return

        await self._db.executemany(INSERT_CANDLE_SQL, self._batch)
        logger.info(f"Candles persisted: {len(self._batch)} records")
        self._batch.clear()

    async def start(self) -> None:
        await self._db.create_pool()
        await self._db.init_tables()
        await self._consumer.start()
        logger.info("CandleStorageConsumer started")
        await self._consumer.consume(self._handle)

    async def stop(self) -> None:
        await self._flush()
        await self._consumer.stop()
        await self._db.close()
        logger.info("CandleStorageConsumer stopped")
