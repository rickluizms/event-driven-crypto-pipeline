from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from dataclasses import dataclass
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)


@dataclass
class TopicConfig:
    name: str
    num_partitions: int = 3
    replication_factor: int = 1


class KafkaAdmin:
    def __init__(self):
        settings = get_settings()
        self._bootstrap_servers = settings.kafka.bootstrap_servers

    async def ensure_topics(self, topics: list[TopicConfig]) -> None:
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap_servers)
        await admin.start()

        try:
            new_topics = [
                NewTopic(
                    name=t.name,
                    num_partitions=t.num_partitions,
                    replication_factor=t.replication_factor,
                )
                for t in topics
            ]

            for topic in new_topics:
                try:
                    await admin.create_topics([topic])
                    logger.info(f"Topic created: {topic.name}")
                except TopicAlreadyExistsError:
                    logger.info(f"Topic already exists: {topic.name}")
        finally:
            await admin.close()
