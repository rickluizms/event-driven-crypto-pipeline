import asyncio
from core.kafka.admin import KafkaAdmin
from core.utils.logger import get_logger
from infra.kafka.topics import TOPICS

logger = get_logger(__name__)


async def main() -> None:
    logger.info("Starting topic creation...")
    admin = KafkaAdmin()
    await admin.ensure_topics(TOPICS)
    logger.info("Topic creation complete.")


if __name__ == "__main__":
    asyncio.run(main())
