from core.kafka.consumer import KafkaConsumerClient
from core.kafka.schemas import IndicatorEvent
from core.db.postgres import PostgresClient
from core.utils.logger import get_logger
from core.metrics.metrics import records_persisted_total, batch_flush_duration
from infra.kafka.topics import TOPIC_INDICATORS_PERSISTED

logger = get_logger(__name__)

INSERT_INDICATOR_SQL = """
    INSERT INTO indicators (symbol, interval, name, value, timestamp)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (symbol, interval, name, timestamp) DO NOTHING
"""

BATCH_SIZE = 50


class IndicatorStorageConsumer:
    def __init__(self):
        self._consumer = KafkaConsumerClient(
            topic=TOPIC_INDICATORS_PERSISTED,
            group_id="indicator-storage-group",
        )
        self._db = PostgresClient()
        self._batch: list[tuple] = []

    async def _handle(self, message: dict) -> None:
        indicator = IndicatorEvent.from_dict(message)

        self._batch.append((
            indicator.symbol,
            indicator.interval,
            indicator.indicator_name,
            indicator.value,
            indicator.timestamp,
        ))

        if len(self._batch) >= BATCH_SIZE:
            await self._flush()

    async def _flush(self) -> None:
        if not self._batch:
            return

        with batch_flush_duration.labels(table="indicators").time():
            await self._db.executemany(INSERT_INDICATOR_SQL, self._batch)
        records_persisted_total.labels(table="indicators").inc(len(self._batch))
        logger.info(f"Indicators persisted: {len(self._batch)} records")
        self._batch.clear()

    async def start(self) -> None:
        await self._db.create_pool()
        await self._db.init_tables()
        await self._consumer.start()
        logger.info("IndicatorStorageConsumer started")
        await self._consumer.consume(self._handle)

    async def stop(self) -> None:
        await self._flush()
        await self._consumer.stop()
        await self._db.close()
        logger.info("IndicatorStorageConsumer stopped")
