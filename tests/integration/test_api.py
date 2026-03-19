"""
Integration tests for the REST API.

Requires running Redis and PostgreSQL.
"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client():
    pytest.skip("Requires running Redis and PostgreSQL infrastructure")


def test_health_check(client):
    """GET /health should return status healthy."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


def test_get_indicators(client):
    """GET /indicators/{symbol} should return indicator data from Redis."""
    response = client.get("/indicators/btcusdt?interval=1s")
    assert response.status_code == 200
    assert "indicators" in response.json()


def test_get_indicator_history(client):
    """GET /indicators/{symbol}/history should return historical data from Postgres."""
    response = client.get("/indicators/btcusdt/history?interval=1s&limit=10")
    assert response.status_code == 200
    assert "data" in response.json()
