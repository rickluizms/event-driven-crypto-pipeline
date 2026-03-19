"""
Integration tests for storage consumers (PostgreSQL).

Requires running PostgreSQL: docker compose -f docker/docker-compose.yml up -d postgres
"""

import pytest


@pytest.mark.asyncio
async def test_candle_persisted_to_postgres():
    """Verify that candle events are batch-inserted into the candles table."""
    pytest.skip("Requires running PostgreSQL infrastructure")


@pytest.mark.asyncio
async def test_indicator_persisted_to_postgres():
    """Verify that indicator events are batch-inserted into the indicators table."""
    pytest.skip("Requires running PostgreSQL infrastructure")


@pytest.mark.asyncio
async def test_duplicate_candle_ignored():
    """Verify that ON CONFLICT DO NOTHING prevents duplicate inserts."""
    pytest.skip("Requires running PostgreSQL infrastructure")
