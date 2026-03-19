import asyncio
import argparse
from aiokafka import AIOKafkaConsumer, TopicPartition
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)


async def replay(topic: str, group_id: str, from_beginning: bool = True, timestamp_ms: int | None = None) -> None:
    settings = get_settings()

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
    )

    await consumer.start()

    try:
        partitions = consumer.assignment()

        if not partitions:
            await asyncio.sleep(1)
            partitions = consumer.assignment()

        if from_beginning:
            await consumer.seek_to_beginning(*partitions)
            logger.info(f"Reset offsets to beginning for {topic} group={group_id} partitions={len(partitions)}")

        elif timestamp_ms is not None:
            timestamps = {tp: timestamp_ms for tp in partitions}
            offsets = await consumer.offsets_for_times(timestamps)

            for tp, offset_and_ts in offsets.items():
                if offset_and_ts is not None:
                    consumer.seek(tp, offset_and_ts.offset)
                    logger.info(f"Reset {tp} to offset {offset_and_ts.offset} (ts={timestamp_ms})")
                else:
                    logger.warning(f"No offset found for {tp} at timestamp {timestamp_ms}")

        await consumer.commit()
        logger.info("Offsets committed successfully")

    finally:
        await consumer.stop()


def main():
    parser = argparse.ArgumentParser(description="Kafka Offset Replay Utility")
    parser.add_argument("--topic", required=True, help="Topic to replay")
    parser.add_argument("--group-id", required=True, help="Consumer group ID")
    parser.add_argument("--from-beginning", action="store_true", default=True, help="Reset to beginning (default)")
    parser.add_argument("--timestamp", type=int, default=None, help="Reset to specific timestamp (ms)")

    args = parser.parse_args()

    from_beginning = args.timestamp is None

    asyncio.run(replay(
        topic=args.topic,
        group_id=args.group_id,
        from_beginning=from_beginning,
        timestamp_ms=args.timestamp,
    ))


if __name__ == "__main__":
    main()
