<div align="center">
  <h1>Real-Time Crypto Market Data Streaming Pipeline</h1>
  
  <p>
    <img src="https://img.shields.io/badge/Python-3.10+-blue.svg?style=for-the-badge&logo=python&logoColor=white" alt="Python Version" />
    <img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi" alt="FastAPI" />
    <img src="https://img.shields.io/badge/Kafka-232F3E?style=for-the-badge&logo=apachekafka" alt="Kafka" />
    <img src="https://img.shields.io/badge/Redis-%23DD0031.svg?style=for-the-badge&logo=redis&logoColor=white" alt="Redis" />
    <img src="https://img.shields.io/badge/PostgreSQL-%23316192.svg?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL" />
    <img src="https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=Prometheus&logoColor=white" alt="Prometheus" />
    <img src="https://img.shields.io/badge/Grafana-%23F46800.svg?style=for-the-badge&logo=grafana&logoColor=white" alt="Grafana" />
    <img src="https://img.shields.io/badge/Docker-%232496ED.svg?style=for-the-badge&logo=docker&logoColor=white" alt="Docker" />
  </p>

  <p>
    <em>Event-driven pipeline that ingests trade data from Binance WebSocket, aggregates candles, computes technical indicators (SMA, EMA, RSI, VWAP) via TA-Lib, and materializes state into Redis and PostgreSQL. The system includes full observability with Prometheus and Grafana.</em>
  </p>
</div>

---

## 📖 Table of Contents

- [Architecture](#-architecture)
- [Project Structure](#-project-structure)
- [Prerequisites](#️-prerequisites)
- [Getting Started](#-getting-started)
- [API Endpoints](#-api-endpoints)
- [Kafka Topics](#-kafka-topics)
- [Observability](#-observability)

---

## 📊 Architecture

```mermaid
flowchart LR
    %% Data Sources
    Binance(("🌐 Binance\nWebSocket API")) 

    %% App Components
    subgraph Application Layer ["Application Layer (Python / uv)"]
        direction TB
        Producer["Market Data Producer"]
        Aggregator["Candle Aggregator"]
        TAEngine["Indicator Engine\n(TA-Lib)"]
        Consumers["Storage Consumers"]
    end

    %% Event Bus
    subgraph Event Backbone ["Event Backbone"]
        Kafka{"Apache Kafka"}
    end

    %% Storage
    subgraph Storage & Cache ["Storage & Cache"]
        Redis[("Redis\n(Real-time State)")]
        Postgres[("PostgreSQL\n(Historical Data)")]
    end

    %% API Layer
    API["🚀 FastAPI REST API"]

    %% Flow mapping
    Binance ==>|Raw Ticks| Producer
    Producer ==>|market.ticks| Kafka
    
    Kafka ==>|Subscribe| Aggregator
    Aggregator ==>|market.candles.*| Kafka
    
    Kafka ==>|Subscribe| TAEngine
    TAEngine ==>|market.indicators.*| Kafka
    
    Kafka ==>|Subscribe| Consumers
    Consumers -->|Update| Redis
    Consumers -->|Persist| Postgres
    
    API -.->|Query Real-time| Redis
    API -.->|Query History| Postgres

    %% Observability
    subgraph Observability Suite ["Observability Suite"]
        Prom["Prometheus"]
        Grafana["Grafana"]
    end
    
    Producer & Aggregator & TAEngine & Consumers -.->|Expose Metrics| Prom
    Prom -->|Visualize| Grafana

    %% Styling
    classDef source fill:#f9d0c4,stroke:#e06666,stroke-width:2px,color:#000
    classDef kafka fill:#232f3e,stroke:#ff9900,stroke-width:2px,color:#fff
    classDef app fill:#2b5b84,stroke:#1d3c58,stroke-width:2px,color:#fff
    classDef storage fill:#336791,stroke:#295071,stroke-width:2px,color:#fff
    classDef redis fill:#d82c20,stroke:#8e1d15,stroke-width:2px,color:#fff
    classDef api fill:#009688,stroke:#00796b,stroke-width:2px,color:#fff
    classDef obs fill:#f47a20,stroke:#d05e13,stroke-width:2px,color:#fff
    classDef prom fill:#e6522c,stroke:#b84223,stroke-width:2px,color:#fff

    class Binance source
    class Kafka kafka
    class Producer,Aggregator,TAEngine,Consumers app
    class Postgres storage
    class Redis redis
    class API api
    class Grafana obs
    class Prom prom
```

## 📁 Project Structure

```text
.
├── api/                   # REST API (FastAPI)
├── core/                  # Infrastructure layer (Metrics, DB, Cache, Kafka, Binance)
├── docker/                # Docker, Prometheus & Grafana config
├── infra/                 # Infrastructure definitions
├── scripts/               # Operational utilities
├── src/                   # Application layer (Producers, Consumers)
└── tests/                 # Unit and integration tests
```

## 🛠️ Prerequisites

- **Python 3.10+**
- **Docker & Docker Compose**
- **[TA-Lib C library](https://ta-lib.org/)** installed on your host machine

## 🚀 Getting Started

### 1. Clone and Install Dependencies

```bash
git clone <repo-url>
cd event-driven-crypto-pipeline

# Install and sync dependencies using uv
pip install uv
uv sync
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your specific configurations (e.g., BTCUSDT, ETHUSDT)
```

### 3. Start Infrastructure

Boot up Kafka, Redis, PostgreSQL, Prometheus, and Grafana via Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up -d
```

### 4. Create Kafka Topics

Ensure all required topics are initialized:

```bash
python -m scripts.create_topics
```

### 5. Run the Application

You can start the various components of the pipeline using the main CLI:

```bash
# Run the full pipeline (producer + all consumers + metrics server)
python main.py all

# Or run specific components individually
python main.py run producer candle_1s indicators_1s cache

# Start the REST API on port 8000
python main.py api --port 8000
```

## 📡 API Endpoints

The FastAPI REST service provides the following endpoints to query the pipeline state:

| Method | Path | Source | Description |
| :--- | :--- | :--- | :--- |
| `GET` | `/health` | — | Health check and API status |
| `GET` | `/indicators/{symbol}` | Redis | Retrieve the latest indicator values (low latency) |
| `GET` | `/indicators/{symbol}/history` | PostgreSQL | Query historical indicator data |

## 📋 Kafka Topics

The data flow is decoupled through the following heavily-partitioned Kafka topics:

| Topic | Key | Description |
| :--- | :--- | :--- |
| `market.ticks` | `symbol` | Raw trade events ingested directly from Binance |
| `market.candles.1s` | `symbol` | 1-second OHLCV aggregated candles |
| `market.candles.1m` | `symbol` | 1-minute OHLCV aggregated candles |
| `market.indicators.realtime` | `symbol` | Calculated technical indicators bound for Redis |
| `market.indicators.persisted` | `symbol` | Calculated technical indicators bound for PostgreSQL |

## 📈 Observability

The system exports real-time prometheus metrics exposing the internals of the pipeline.

- **Grafana**: [`http://localhost:3000`](http://localhost:3000) (Default credentials: `admin` / `admin`)
- **Prometheus**: [`http://localhost:9091`](http://localhost:9091)
- **Metrics Endpoint**: `http://localhost:9090/metrics` 

### Available Metrics
- `ticks_produced_total`: Number of trades ingested from Binance.
- `candles_produced_total`: Number of aggregated candles.
- `indicators_calculated_total`: Number of TA-Lib indicator calculations.
- `ws_connection_active`: Current WebSocket status gauge.

---
<div align="center">
  <i>Built with ☕ and data engineering best practices.</i>
</div>
