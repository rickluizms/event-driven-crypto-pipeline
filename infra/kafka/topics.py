from core.kafka.admin import TopicConfig

TOPIC_TICKS = "market.ticks"
TOPIC_CANDLES_1S = "market.candles.1s"
TOPIC_CANDLES_1M = "market.candles.1m"
TOPIC_INDICATORS_REALTIME = "market.indicators.realtime"
TOPIC_INDICATORS_PERSISTED = "market.indicators.persisted"

TOPICS: list[TopicConfig] = [
    TopicConfig(name=TOPIC_TICKS, num_partitions=3, replication_factor=1),
    TopicConfig(name=TOPIC_CANDLES_1S, num_partitions=3, replication_factor=1),
    TopicConfig(name=TOPIC_CANDLES_1M, num_partitions=3, replication_factor=1),
    TopicConfig(name=TOPIC_INDICATORS_REALTIME, num_partitions=3, replication_factor=1),
    TopicConfig(name=TOPIC_INDICATORS_PERSISTED, num_partitions=3, replication_factor=1),
]
