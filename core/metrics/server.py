import asyncio
from prometheus_client import start_http_server
from core.utils.logger import get_logger

logger = get_logger(__name__)

METRICS_PORT = 9090


async def start_metrics_server(port: int = METRICS_PORT) -> None:
    """Start a background Prometheus metrics HTTP server."""
    start_http_server(port)
    logger.info(f"Prometheus metrics server started on port {port}")
    # Keep running forever
    while True:
        await asyncio.sleep(3600)
