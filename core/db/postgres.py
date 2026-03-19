import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
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
        self._pool: AsyncConnectionPool | None = None

    async def create_pool(self) -> None:
        settings = get_settings()
        conninfo = (
            f"host={settings.postgres.host} "
            f"port={settings.postgres.port} "
            f"user={settings.postgres.user} "
            f"password={settings.postgres.password} "
            f"dbname={settings.postgres.db}"
        )
        self._pool = AsyncConnectionPool(
            conninfo=conninfo,
            min_size=2,
            max_size=10,
            open=False,
        )
        await self._pool.open(wait=True)
        logger.info("PostgreSQL connection pool created")

    def _get_pool(self) -> AsyncConnectionPool:
        if not self._pool:
            raise RuntimeError("Pool not created. Call create_pool() first.")
        return self._pool

    async def execute(self, query: str, *args) -> None:
        pool = self._get_pool()
        async with pool.connection() as conn:
            await conn.execute(query, args if args else None)

    async def executemany(self, query: str, args_list: list) -> None:
        pool = self._get_pool()
        # Convert $1, $2 style placeholders to %s for psycopg
        converted_query = _convert_placeholders(query)
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(converted_query, args_list)

    async def fetch(self, query: str, *args) -> list[dict]:
        pool = self._get_pool()
        converted_query = _convert_placeholders(query)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(converted_query, args if args else None)
                return await cur.fetchall()

    async def fetchrow(self, query: str, *args) -> dict | None:
        pool = self._get_pool()
        converted_query = _convert_placeholders(query)
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(converted_query, args if args else None)
                return await cur.fetchone()

    async def init_tables(self) -> None:
        await self.execute(CANDLES_DDL)
        await self.execute(INDICATORS_DDL)
        logger.info("Database tables initialized")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed")


def _convert_placeholders(query: str) -> str:
    """Convert asyncpg-style $1, $2 placeholders to psycopg %s style."""
    import re
    return re.sub(r'\$\d+', '%s', query)
