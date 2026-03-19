from prometheus_client import Counter, Histogram, Gauge

# ─── Counters ───

ticks_produced_total = Counter(
    "pipeline_ticks_produced_total",
    "Total ticks ingested from Binance and produced to Kafka",
    ["symbol"],
)

candles_emitted_total = Counter(
    "pipeline_candles_emitted_total",
    "Total candle events emitted",
    ["symbol", "interval"],
)

indicators_emitted_total = Counter(
    "pipeline_indicators_emitted_total",
    "Total indicator events emitted",
    ["symbol", "interval"],
)

records_persisted_total = Counter(
    "pipeline_records_persisted_total",
    "Total records persisted to PostgreSQL",
    ["table"],
)

cache_writes_total = Counter(
    "pipeline_cache_writes_total",
    "Total cache writes to Redis",
    ["symbol"],
)

# ─── Histograms ───

message_processing_duration = Histogram(
    "pipeline_message_processing_duration_seconds",
    "Time spent processing a single message",
    ["component"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

batch_flush_duration = Histogram(
    "pipeline_batch_flush_duration_seconds",
    "Time spent flushing a batch to storage",
    ["table"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# ─── Gauges ───

ws_connection_active = Gauge(
    "pipeline_ws_connection_active",
    "Whether the Binance WebSocket connection is active (1=connected, 0=disconnected)",
)

active_components = Gauge(
    "pipeline_active_components",
    "Number of currently running pipeline components",
)
