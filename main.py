import asyncio
import argparse
import signal
import sys
from core.utils.logger import get_logger

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = get_logger(__name__)

COMPONENTS = {
    "producer": "src.producers.market_data.producer.MarketDataProducer",
    "candle_1s": "src.consumers.candles.candle_1s_consumer.Candle1sConsumer",
    "candle_1m": "src.consumers.candles.candle_1m_consumer.Candle1mConsumer",
    "indicators_1s": "src.consumers.indicators.indicators_1s_consumer.Indicators1sConsumer",
    "indicators_1m": "src.consumers.indicators.indicators_1m_consumer.Indicators1mConsumer",
    "cache": "src.consumers.cache.cache_consumer.CacheConsumer",
    "candle_storage": "src.consumers.storage.candle_storage_consumer.CandleStorageConsumer",
    "indicator_storage": "src.consumers.storage.indicator_storage_consumer.IndicatorStorageConsumer",
}


def _import_component(dotted_path: str):
    module_path, class_name = dotted_path.rsplit(".", 1)
    module = __import__(module_path, fromlist=[class_name])
    return getattr(module, class_name)


async def _run_component(name: str, instance) -> None:
    try:
        await instance.start()
    except Exception as e:
        logger.error(f"Component '{name}' failed: {e}")


async def run_components(names: list[str]) -> None:
    instances = []
    tasks = []

    for name in names:
        if name not in COMPONENTS:
            logger.error(f"Unknown component: {name}. Available: {list(COMPONENTS.keys())}")
            return

        cls = _import_component(COMPONENTS[name])
        instance = cls()
        instances.append((name, instance))
        logger.info(f"Starting component: {name}")
        tasks.append(asyncio.create_task(_run_component(name, instance)))

    loop = asyncio.get_running_loop()

    def _shutdown():
        logger.info("Shutdown signal received")
        for _, inst in instances:
            if hasattr(inst, "stop"):
                asyncio.create_task(inst.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    logger.info("All components stopped")


def run_api(host: str = "0.0.0.0", port: int = 8000) -> None:
    import uvicorn
    uvicorn.run("api.main:app", host=host, port=port, reload=False)


def main():
    parser = argparse.ArgumentParser(description="Crypto Pipeline Runner")

    subparsers = parser.add_subparsers(dest="command")

    run_parser = subparsers.add_parser("run", help="Run pipeline components")
    run_parser.add_argument(
        "components",
        nargs="*",
        default=list(COMPONENTS.keys()),
        help=f"Components to run. Available: {list(COMPONENTS.keys())}. Default: all",
    )

    api_parser = subparsers.add_parser("api", help="Run the REST API server")
    api_parser.add_argument("--host", default="0.0.0.0", help="API host")
    api_parser.add_argument("--port", type=int, default=8000, help="API port")

    subparsers.add_parser("all", help="Run all pipeline components")

    args = parser.parse_args()

    if args.command == "run":
        asyncio.run(run_components(args.components))
    elif args.command == "api":
        run_api(host=args.host, port=args.port)
    elif args.command == "all":
        asyncio.run(run_components(list(COMPONENTS.keys())))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
