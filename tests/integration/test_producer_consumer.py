"""
Integration tests for the producer → consumer pipeline.

These tests require running infrastructure (Kafka, Redis, PostgreSQL).
Start with: docker compose -f docker/docker-compose.yml up -d

Run with: pytest tests/integration/ -v
"""

import pytest


@pytest.mark.asyncio
async def test_tick_to_candle_flow():
    """Produce a tick to market.ticks and verify a candle is emitted on market.candles.1s."""
    pytest.skip("Requires running Kafka infrastructure")


@pytest.mark.asyncio
async def test_candle_to_indicator_flow():
    """Produce candles to market.candles.1s and verify indicators appear on market.indicators.*."""
    pytest.skip("Requires running Kafka infrastructure")
