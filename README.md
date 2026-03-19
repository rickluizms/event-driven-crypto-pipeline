# Real-Time Crypto Market Data Streaming Pipeline

Event-driven pipeline that ingests trade data from Binance WebSocket, aggregates candles, computes technical indicators (SMA, EMA, RSI, VWAP) via TA-Lib, and materializes state into Redis and PostgreSQL.

## Architecture

```
Binance WebSocket (@trade)
    ↓
MarketDataProducer → market.ticks
    ↓
Candle1sConsumer → market.candles.1s
    ├── Candle1mConsumer → market.candles.1m
    ├── Indicators1sConsumer → market.indicators.*
    │     ├── CacheConsumer → Redis (low-latency reads)
    │     └── IndicatorStorageConsumer → PostgreSQL (historical)
    └── CandleStorageConsumer → PostgreSQL
```

## Topics

| Topic | Description | Key |
|-------|-------------|-----|
| `market.ticks` | Raw trade events | symbol |
| `market.candles.1s` | 1-second OHLCV candles | symbol |
| `market.candles.1m` | 1-minute OHLCV candles | symbol |
| `market.indicators.realtime` | Indicators for Redis cache | symbol |
| `market.indicators.persisted` | Indicators for DB storage | symbol |

## Prerequisites

- Python 3.10+
- Docker & Docker Compose
- [TA-Lib C library](https://ta-lib.org/) installed on host

## Setup

```bash
# 1. Clone and install dependencies
git clone <repo-url> && cd event-driven-crypto-pipeline
pip install uv && uv sync

# 2. Start infrastructure
docker compose -f docker/docker-compose.yml up -d

# 3. Create Kafka topics
python -m scripts.create_topics

# 4. Configure environment
cp .env.example .env
# Edit .env with your settings (symbols, etc.)
```

## Running

```bash
# Run the full pipeline (producer + all consumers)
python main.py all

# Run specific components
python main.py run producer candle_1s indicators_1s cache

# Start the REST API
python main.py api --port 8000

# Replay a consumer group from beginning
python -m scripts.replay --topic market.ticks --group-id candle-1s-group
```

### Available Components

| Component | Description |
|-----------|-------------|
| `producer` | Binance WS → Kafka ticks |
| `candle_1s` | Ticks → 1s candles |
| `candle_1m` | 1s candles → 1m candles |
| `indicators_1s` | 1s candles → indicators |
| `indicators_1m` | 1m candles → indicators |
| `cache` | Indicators → Redis |
| `candle_storage` | Candles → PostgreSQL |
| `indicator_storage` | Indicators → PostgreSQL |

## API Endpoints

| Method | Path | Source | Description |
|--------|------|--------|-------------|
| `GET` | `/health` | — | Health check |
| `GET` | `/indicators/{symbol}` | Redis | Latest indicator values |
| `GET` | `/indicators/{symbol}/history` | PostgreSQL | Historical indicator data |

**Query Parameters** (history endpoint):
- `interval` — `1s` or `1m` (default: `1s`)
- `indicator_name` — Filter by name (SMA, EMA, RSI, VWAP)
- `limit` — Max records (1–1000, default: 100)

## Testing

```bash
# Run unit tests
python -m pytest tests/unit/ -v

# Run all tests (requires running infrastructure)
python -m pytest tests/ -v
```

## Project Structure

```
├── core/                  # Infrastructure layer (no business logic)
│   ├── binance/           # WebSocket client
│   ├── cache/             # Redis client
│   ├── db/                # PostgreSQL client
│   ├── kafka/             # Producer, consumer, admin, schemas
│   ├── settings/          # Centralized configuration
│   └── utils/             # Logger
├── src/                   # Application layer
│   ├── producers/         # Market data producer
│   ├── consumers/         # Stream consumers (candles, indicators, cache, storage)
│   └── services/          # Domain logic (candle aggregation, indicators, routing)
├── api/                   # REST API (FastAPI)
├── infra/                 # Infrastructure definitions (Kafka topics)
├── scripts/               # Operational utilities
├── tests/                 # Unit and integration tests
└── docker/                # Docker Compose
```

## Tech Stack

- **Streaming**: Apache Kafka (via aiokafka)
- **Ingestion**: Binance WebSocket API
- **Indicators**: TA-Lib (SMA, EMA, RSI) + VWAP
- **Cache**: Redis
- **Storage**: PostgreSQL (via asyncpg)
- **API**: FastAPI + Uvicorn
- **Config**: pydantic-settings
