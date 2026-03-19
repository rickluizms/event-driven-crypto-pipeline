import json
import redis.asyncio as aioredis
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)


class RedisClient:
    def __init__(self):
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        settings = get_settings()
        self._client = aioredis.from_url(
            settings.redis.url,
            decode_responses=True,
        )
        logger.info("Redis client connected")

    def _get_client(self) -> aioredis.Redis:
        if not self._client:
            raise RuntimeError("Client not connected. Call connect() first.")
        return self._client

    async def get(self, key: str) -> dict | None:
        client = self._get_client()
        raw = await client.get(key)
        if raw is None:
            return None
        return json.loads(raw)

    async def set(self, key: str, value: dict, ttl: int | None = None) -> None:
        client = self._get_client()
        serialized = json.dumps(value)
        if ttl:
            await client.set(key, serialized, ex=ttl)
        else:
            await client.set(key, serialized)

    async def hset(self, name: str, key: str, value: dict) -> None:
        client = self._get_client()
        await client.hset(name, key, json.dumps(value))

    async def hget(self, name: str, key: str) -> dict | None:
        client = self._get_client()
        raw = await client.hget(name, key)
        if raw is None:
            return None
        return json.loads(raw)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            logger.info("Redis client closed")
