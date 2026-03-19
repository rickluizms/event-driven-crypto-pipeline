# PRD - Real-Time Market Data Streaming Pipeline

## Objective
Build a real-time event-driven data pipeline for crypto market data using Kafka, WebSocket ingestion and stream processing.

## Architecture Overview
The system ingests trade data from Binance WebSocket, publishes events to Kafka, processes candles and indicators, and materializes state into Redis and PostgreSQL.

## Project Structure

```
project/
├── core/
│   ├── config/
│   │   └── settings.py
│   ├── kafka/
│   │   ├── producer.py
│   │   ├── consumer.py
│   │   ├── admin.py
│   │   └── schemas.py
│   ├── binance/
│   │   └── ws_client.py
│   ├── db/
│   │   └── postgres.py
│   ├── cache/
│   │   └── redis_client.py
│   ├── utils/
│   │   └── logger.py
│   └── __init__.py
│
├── src/
│   ├── producers/
│   │   └── market_data/
│   │       ├── producer.py
│   │       └── mapper.py
│   ├── consumers/
│   │   ├── candles/
│   │   │   ├── candle_1s_consumer.py
│   │   │   └── candle_1m_consumer.py
│   │   ├── indicators/
│   │   │   ├── indicators_1s_consumer.py
│   │   │   └── indicators_1m_consumer.py
│   │   ├── storage/
│   │   │   ├── candle_storage_consumer.py
│   │   │   └── indicator_storage_consumer.py
│   │   └── cache/
│   │       └── cache_consumer.py
│   ├── services/
│   │   ├── candle_service.py
│   │   ├── indicator_service.py
│   │   └── selector_service.py
│   └── __init__.py
│
├── api/
│   ├── routes/
│   │   └── indicators.py
│   └── main.py
│
├── infra/
│   └── kafka/
│       └── topics.py
│
├── scripts/
│   ├── create_topics.py
│   └── replay.py
│
├── docker/
│   └── docker-compose.yml
│
├── tests/
└── README.md
```

## Topics Definition

```
market.ticks
market.candles.1s
market.candles.1m
market.indicators.realtime
market.indicators.persisted
```

## Data Flow

```
Binance WebSocket (@trade)
    ↓
Producer → market.ticks
    ↓
Candle Consumer → market.candles.1s
    ↓
Indicators Consumers
    ├── realtime → Redis → API
    └── persisted → Database
```

## Implementation Guidelines

### Clean Code
- Use clear and descriptive naming
- Keep functions small and focused
- Avoid side effects in services
- Prefer composition over inheritance

### DRY
- Centralize shared logic in services
- Avoid duplication in consumers and producers
- Reuse mappers and serializers

### KISS
- Avoid unnecessary abstractions
- Keep flows linear and easy to follow
- Prefer explicit logic over magic behavior

## Layer Responsibilities

### core/
Infrastructure layer. No business logic.

- WebSocket client
- Kafka clients
- Database and cache connections

### src/
Application layer. Contains business logic.

- Producers and consumers
- Data transformation
- Processing pipelines

### services/
Pure domain logic.

- Candle aggregation
- Indicator calculation
- Routing decisions

### infra/
Infrastructure definition.

- Kafka topics
- Configurable resources

### scripts/
Operational utilities.

- Topic creation
- Replay and offset management

## Coding Standards

- No comments in business logic
- Use type hints where applicable
- Follow consistent naming conventions
- Keep modules cohesive and decoupled

## Scalability Considerations

- Use Kafka partitions by symbol
- Scale consumers horizontally
- Separate realtime and persisted workloads
- Use Redis for low-latency reads

## Future Enhancements

- Schema Registry integration
- Kafka Streams for stateful processing
- Monitoring with Prometheus and Grafana
