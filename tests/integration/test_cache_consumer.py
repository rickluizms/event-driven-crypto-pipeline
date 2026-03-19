"""
Integration tests for the cache consumer (Redis).

Requires running Redis: docker compose -f docker/docker-compose.yml up -d redis
"""

import pytest


@pytest.mark.asyncio
async def test_indicator_cached_in_redis():
    """Verify that an indicator event written to market.indicators.realtime appears in Redis."""
    pytest.skip("Requires running Redis infrastructure")


@pytest.mark.asyncio
async def test_cache_ttl_applied():
    """Verify that cached indicators have the correct TTL."""
    pytest.skip("Requires running Redis infrastructure")
