import asyncpg
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)

CANDLES_DDL = """
CREATE TABLE IF NOT EXISTS candles (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    interval VARCHAR(5) NOT NULL,
    open NUMERIC NOT NULL,
    high NUMERIC NOT NULL,
    low NUMERIC NOT NULL,
    close NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    UNIQUE(symbol, interval, open_time)
);
"""

INDICATORS_DDL = """
CREATE TABLE IF NOT EXISTS indicators (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    interval VARCHAR(5) NOT NULL,
    name VARCHAR(50) NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    timestamp BIGINT NOT NULL,
    UNIQUE(symbol, interval, name, timestamp)
);
"""


class PostgresClient:
    def __init__(self):
        self._pool: asyncpg.Pool | None = None

    async def create_pool(self) -> None:
        settings = get_settings()
        self._pool = await asyncpg.create_pool(
            dsn=settings.postgres.dsn,
            min_size=2,
            max_size=10,
        )
        logger.info("PostgreSQL connection pool created")

    def _get_pool(self) -> asyncpg.Pool:
        if not self._pool:
            raise RuntimeError("Pool not created. Call create_pool() first.")
        return self._pool

    async def execute(self, query: str, *args) -> str:
        pool = self._get_pool()
        return await pool.execute(query, *args)

    async def executemany(self, query: str, args_list: list) -> None:
        pool = self._get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(query, args_list)

    async def fetch(self, query: str, *args) -> list:
        pool = self._get_pool()
        return await pool.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        pool = self._get_pool()
        return await pool.fetchrow(query, *args)

    async def init_tables(self) -> None:
        await self.execute(CANDLES_DDL)
        await self.execute(INDICATORS_DDL)
        logger.info("Database tables initialized")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed")
