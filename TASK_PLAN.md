# Task Plan — Real-Time Market Data Streaming Pipeline

> Detailed implementation plan based on the [PRD](./prd_kafka_streaming_pipeline.md).
> Each phase is ordered by dependency. Complete each phase before moving to the next.

---

## Phase 1 — Project Foundation & Infrastructure

### 1.1 Docker Compose Setup
- [x] Create `docker/docker-compose.yml` with services:
  - **Zookeeper** (required by Kafka)
  - **Kafka** (single broker, exposed on `localhost:9092`)
  - **Redis** (exposed on `localhost:6379`)
  - **PostgreSQL** (exposed on `localhost:5432`, with initial DB creation)
- [x] Add a `.env.example` with all required environment variables (Kafka broker, Redis URL, Postgres DSN, Binance WS URL, symbols list)

### 1.2 Settings & Configuration (`core/settings/settings.py`)
- [x] Implement a centralized config module using `pydantic-settings` (or `os.environ`)
- [x] Define settings groups: Kafka, Redis, Postgres, Binance, App
- [x] Load values from environment variables / `.env`

### 1.3 Logger (`core/utils/logger.py`)
- [x] Implement a structured logging setup (stdlib `logging` with JSON formatter or `structlog`)
- [x] Provide a `get_logger(name)` factory function used across the entire project

### 1.4 Dependencies
- [x] Add required packages to `pyproject.toml`:
  - `confluent-kafka` or `aiokafka`
  - `websockets` (already present)
  - `redis` / `aioredis`
  - `psycopg2-binary` or `asyncpg`
  - `pydantic` / `pydantic-settings`
  - `fastapi` + `uvicorn`
  - `ta-lib` or `pandas-ta` (for indicators)

---

## Phase 2 — Core Infrastructure Layer (`core/`)

### 2.1 Kafka Admin (`core/kafka/admin.py`)
- [x] Create a Kafka admin client wrapper
- [x] Expose helper to create topics programmatically (used by scripts)

### 2.2 Kafka Producer (`core/kafka/producer.py`)
- [x] Implement a reusable `KafkaProducer` class/wrapper
- [x] Support JSON serialization, key-based partitioning (by symbol), and delivery callbacks
- [x] Provide `send(topic, key, value)` async-safe method

### 2.3 Kafka Consumer (`core/kafka/consumer.py`)
- [x] Implement a reusable `KafkaConsumer` base class/wrapper
- [x] Support JSON deserialization, consumer group management, and manual commit
- [x] Provide a `consume(handler)` loop pattern

### 2.4 Kafka Schemas (`core/kafka/schemas.py`)
- [x] Define Pydantic models for all event schemas:
  - `TickEvent` (symbol, price, qty, timestamp, trade_id)
  - `CandleEvent` (symbol, interval, open, high, low, close, volume, open_time, close_time)
  - `IndicatorEvent` (symbol, interval, indicator_name, value, timestamp)

### 2.5 Binance WebSocket Client (`core/binance/ws_client.py`)
- [x] Implement a WebSocket client connecting to Binance `@trade` stream
- [x] Support multiple symbols via combined stream URL
- [x] Parse raw messages into `TickEvent` schema
- [x] Handle reconnection logic with exponential backoff

### 2.6 PostgreSQL Client (`core/db/postgres.py`)
- [x] Implement a DB connection pool / session manager
- [x] Provide helper methods for executing queries and batch inserts
- [x] Create table initialization (migrations or raw DDL):
  - `candles` table (symbol, interval, open, high, low, close, volume, open_time, close_time)
  - `indicators` table (symbol, interval, name, value, timestamp)

### 2.7 Redis Client (`core/cache/redis_client.py`)
- [x] Implement a Redis connection wrapper
- [x] Provide `get`, `set`, `hset`, `hget` helpers with serialization support
- [x] Support TTL-based expiration

---

## Phase 3 — Infrastructure Definitions (`infra/`)

### 3.1 Topic Definitions (`infra/kafka/topics.py`)
- [x] Define a `TOPICS` config dict/list with:
  - `market.ticks` — partitions by symbol
  - `market.candles.1s`
  - `market.candles.1m`
  - `market.indicators.realtime`
  - `market.indicators.persisted`
- [x] Include partition count and replication factor per topic

### 3.2 Topic Creation Script (`scripts/create_topics.py`)
- [x] Read topic definitions from `infra/kafka/topics.py`
- [x] Use `core/kafka/admin.py` to idempotently create all topics
- [x] Log results (created / already exists)

---

## Phase 4 — Market Data Producer (`src/producers/`)

### 4.1 Tick Mapper (`src/producers/market_data/mapper.py`)
- [x] Implement `map_trade_to_tick(raw_msg) -> TickEvent`
- [x] Normalize Binance raw trade fields to internal schema

### 4.2 Market Data Producer (`src/producers/market_data/producer.py`)
- [x] Connect to Binance WS via `core/binance/ws_client.py`
- [x] For each incoming trade, map it via `mapper.py`
- [x] Publish `TickEvent` to `market.ticks` topic, keyed by symbol
- [x] Include error handling, graceful shutdown, and metrics logging

---

## Phase 5 — Domain Services (`src/services/`)

### 5.1 Candle Service (`src/services/candle_service.py`)
- [x] Implement `CandleAggregator` that maintains in-memory OHLCV state per symbol
- [x] Accept ticks and aggregate into time-bucketed candles (1s window)
- [x] Emit a `CandleEvent` when the window closes
- [x] Support reset/roll-over for the next window

### 5.2 Indicator Service (`src/services/indicator_service.py`)
- [x] Implement indicator calculations on top of candle data:
  - **SMA** (Simple Moving Average) — via TA-Lib
  - **EMA** (Exponential Moving Average) — via TA-Lib
  - **RSI** (Relative Strength Index) — via TA-Lib
  - **VWAP** (Volume-Weighted Average Price) — cálculo manual (cumsum)
- [x] Accept a `CandleEvent` and return a list of `IndicatorEvent`

### 5.3 Selector Service (`src/services/selector_service.py`)
- [x] Implement routing logic to decide whether an indicator event should be:
  - Sent to the **realtime** path (Redis cache)
  - Sent to the **persisted** path (PostgreSQL storage)
  - Or both
- [x] Make routing configurable (e.g., all indicators go to both by default)

---

## Phase 6 — Stream Consumers (`src/consumers/`)

### 6.1 Candle 1s Consumer (`src/consumers/candles/candle_1s_consumer.py`)
- [x] Subscribe to `market.ticks`
- [x] Use `CandleService` to aggregate ticks into 1-second candles
- [x] Publish resulting `CandleEvent` to `market.candles.1s`

### 6.2 Candle 1m Consumer (`src/consumers/candles/candle_1m_consumer.py`)
- [x] Subscribe to `market.candles.1s`
- [x] Aggregate 1s candles into 1-minute candles
- [x] Publish resulting `CandleEvent` to `market.candles.1m`

### 6.3 Indicators 1s Consumer (`src/consumers/indicators/indicators_1s_consumer.py`)
- [x] Subscribe to `market.candles.1s`
- [x] Use `IndicatorService` to compute indicators on 1s candles
- [x] Use `SelectorService` to route output to `market.indicators.realtime` and/or `market.indicators.persisted`

### 6.4 Indicators 1m Consumer (`src/consumers/indicators/indicators_1m_consumer.py`)
- [x] Subscribe to `market.candles.1m`
- [x] Use `IndicatorService` to compute indicators on 1m candles
- [x] Use `SelectorService` to route output

### 6.5 Cache Consumer (`src/consumers/cache/cache_consumer.py`)
- [x] Subscribe to `market.indicators.realtime`
- [x] Write/update indicator values in Redis (keyed by `symbol:indicator:interval`)
- [x] Set appropriate TTLs

### 6.6 Candle Storage Consumer (`src/consumers/storage/candle_storage_consumer.py`)
- [x] Subscribe to `market.candles.1s` and/or `market.candles.1m`
- [x] Batch-insert candle records into PostgreSQL `candles` table

### 6.7 Indicator Storage Consumer (`src/consumers/storage/indicator_storage_consumer.py`)
- [x] Subscribe to `market.indicators.persisted`
- [x] Batch-insert indicator records into PostgreSQL `indicators` table

---

## Phase 7 — REST API (`api/`)

### 7.1 FastAPI App (`api/main.py`)
- [x] Initialize FastAPI application
- [x] Register routers
- [x] Configure CORS if needed
- [x] Add health-check endpoint (`GET /health`)

### 7.2 Indicators Route (`api/routes/indicators.py`)
- [x] `GET /indicators/{symbol}` — fetch latest indicators from Redis
- [x] `GET /indicators/{symbol}/history` — fetch historical indicators from PostgreSQL
- [x] Add query params for interval, indicator name, time range

---

## Phase 8 — Orchestration & Scripts

### 8.1 Main Entry Point (`main.py`)
- [x] Implement CLI or runner that starts:
  - Market Data Producer
  - All consumers (candle, indicator, cache, storage)
- [x] Support starting individual components via CLI args (e.g., `--producer`, `--consumer candle_1s`)
- [x] Handle graceful shutdown (SIGINT/SIGTERM)

### 8.2 Replay Script (`scripts/replay.py`)
- [x] Implement offset reset / replay utility for a given topic and consumer group
- [x] Support replay from beginning or from a specific timestamp

---

## Phase 9 — Testing (`tests/`)

### 9.1 Unit Tests
- [x] `tests/unit/services/test_candle_service.py` — test candle aggregation logic
- [x] `tests/unit/services/test_indicator_service.py` — test SMA, EMA, RSI, VWAP calculations
- [x] `tests/unit/services/test_selector_service.py` — test routing decisions
- [x] `tests/unit/producers/test_mapper.py` — test tick mapping from raw Binance data

### 9.2 Integration Tests
- [x] `tests/integration/test_producer_consumer.py` — end-to-end flow: produce tick → consume candle → verify
- [x] `tests/integration/test_cache_consumer.py` — verify Redis receives correct indicator data
- [x] `tests/integration/test_storage_consumer.py` — verify PostgreSQL receives correct records
- [x] `tests/integration/test_api.py` — test API endpoints return expected data from Redis/Postgres

---

## Phase 10 — Documentation & Polish

### 10.1 README
- [x] Write comprehensive `README.md` with:
  - Project overview and architecture diagram
  - Prerequisites (Docker, Python 3.10+)
  - Setup & running instructions
  - Topic and data flow description
  - API documentation summary

### 10.2 Code Quality
- [x] Add type hints across all modules
- [x] Ensure consistent naming conventions
- [x] Verify no business logic in `core/` layer
- [x] Verify no infrastructure code in `src/services/`
- [x] Remove all placeholder/stub code

---

## Dependency Graph

```
Phase 1 (Foundation)
  └── Phase 2 (Core Layer)
        ├── Phase 3 (Infra / Topics)
        │     └── Phase 8.1 (Scripts)
        ├── Phase 4 (Producer)
        ├── Phase 5 (Services)
        │     └── Phase 6 (Consumers)
        │           ├── Phase 7 (API) ← depends on cache/storage consumers running
        │           └── Phase 8 (Orchestration)
        └── Phase 9 (Testing) ← can start in parallel with Phase 6+
              └── Phase 10 (Docs & Polish)
```

---

## Quick Reference — File Mapping

| File | Phase | Description |
|------|-------|-------------|
| `docker/docker-compose.yml` | 1.1 | Infrastructure services |
| `.env.example` | 1.1 | Environment variables template |
| `core/settings/settings.py` | 1.2 | Centralized configuration |
| `core/utils/logger.py` | 1.3 | Structured logging |
| `core/kafka/admin.py` | 2.1 | Kafka admin client |
| `core/kafka/producer.py` | 2.2 | Reusable Kafka producer |
| `core/kafka/consumer.py` | 2.3 | Reusable Kafka consumer |
| `core/kafka/schemas.py` | 2.4 | Event Pydantic models |
| `core/binance/ws_client.py` | 2.5 | Binance WS client |
| `core/db/postgres.py` | 2.6 | PostgreSQL connection |
| `core/cache/redis_client.py` | 2.7 | Redis connection |
| `infra/kafka/topics.py` | 3.1 | Topic definitions |
| `scripts/create_topics.py` | 3.2 | Topic creation script |
| `src/producers/market_data/mapper.py` | 4.1 | Trade → Tick mapper |
| `src/producers/market_data/producer.py` | 4.2 | Market data producer |
| `src/services/candle_service.py` | 5.1 | Candle aggregation |
| `src/services/indicator_service.py` | 5.2 | Indicator calculations |
| `src/services/selector_service.py` | 5.3 | Routing logic |
| `src/consumers/candles/candle_1s_consumer.py` | 6.1 | 1s candle consumer |
| `src/consumers/candles/candle_1m_consumer.py` | 6.2 | 1m candle consumer |
| `src/consumers/indicators/indicators_1s_consumer.py` | 6.3 | 1s indicator consumer |
| `src/consumers/indicators/indicators_1m_consumer.py` | 6.4 | 1m indicator consumer |
| `src/consumers/cache/cache_consumer.py` | 6.5 | Redis cache consumer |
| `src/consumers/storage/candle_storage_consumer.py` | 6.6 | Candle DB consumer |
| `src/consumers/storage/indicator_storage_consumer.py` | 6.7 | Indicator DB consumer |
| `api/main.py` | 7.1 | FastAPI app |
| `api/routes/indicators.py` | 7.2 | Indicator endpoints |
| `main.py` | 8.1 | Orchestration entry point |
| `scripts/replay.py` | 8.2 | Replay utility |
