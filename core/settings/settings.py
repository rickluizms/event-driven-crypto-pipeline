from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import computed_field


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"

    model_config = {"env_prefix": "KAFKA_"}


class RedisSettings(BaseSettings):
    url: str = "redis://localhost:6379/0"

    model_config = {"env_prefix": "REDIS_"}


class PostgresSettings(BaseSettings):
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = "postgres"
    db: str = "crypto_pipeline"

    model_config = {"env_prefix": "POSTGRES_"}

    @computed_field
    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"


class BinanceSettings(BaseSettings):
    ws_url: str = "wss://stream.binance.com:9443"
    symbols: str = "ethusdt,btcusdt"

    model_config = {"env_prefix": "BINANCE_"}

    @property
    def symbol_list(self) -> list[str]:
        return [s.strip().lower() for s in self.symbols.split(",")]


class AppSettings(BaseSettings):
    log_level: str = "INFO"

    kafka: KafkaSettings = KafkaSettings()
    redis: RedisSettings = RedisSettings()
    postgres: PostgresSettings = PostgresSettings()
    binance: BinanceSettings = BinanceSettings()


@lru_cache
def get_settings() -> AppSettings:
    return AppSettings()
