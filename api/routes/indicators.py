from fastapi import APIRouter, Query
from core.cache.redis_client import RedisClient
from core.db.postgres import PostgresClient

router = APIRouter()

redis_client = RedisClient()
postgres_client = PostgresClient()


@router.get("/{symbol}")
async def get_latest_indicators(
    symbol: str,
    interval: str = Query(default="1s", description="Candle interval (1s, 1m)"),
):
    indicators = {}
    for name in ["SMA", "EMA", "RSI", "VWAP"]:
        cache_key = f"indicator:{symbol}:{name}:{interval}"
        data = await redis_client.get(cache_key)
        if data:
            indicators[name] = data

    return {
        "symbol": symbol,
        "interval": interval,
        "indicators": indicators,
    }


@router.get("/{symbol}/history")
async def get_indicator_history(
    symbol: str,
    interval: str = Query(default="1s", description="Candle interval"),
    indicator_name: str = Query(default=None, description="Indicator name filter"),
    limit: int = Query(default=100, ge=1, le=1000, description="Max records"),
):
    query = """
        SELECT symbol, interval, name, value, timestamp
        FROM indicators
        WHERE symbol = $1 AND interval = $2
    """
    params: list = [symbol, interval]

    if indicator_name:
        query += " AND name = $3"
        params.append(indicator_name)

    query += " ORDER BY timestamp DESC LIMIT $" + str(len(params) + 1)
    params.append(limit)

    rows = await postgres_client.fetch(query, *params)

    return {
        "symbol": symbol,
        "interval": interval,
        "count": len(rows),
        "data": [
            {
                "name": row["name"],
                "value": row["value"],
                "timestamp": row["timestamp"],
            }
            for row in rows
        ],
    }
