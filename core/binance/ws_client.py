import json
import asyncio
import websockets
from typing import Callable, Awaitable
from core.utils.logger import get_logger
from core.settings.settings import get_settings

logger = get_logger(__name__)

MAX_RECONNECT_DELAY = 60


class BinanceWebSocketClient:
    def __init__(self, streams: list[str] | None = None):
        settings = get_settings()
        self._base_url = settings.binance.ws_url
        self._streams = streams or [f"{s}@trade" for s in settings.binance.symbol_list]
        self._url = self._build_url()
        self._running = False

    def _build_url(self) -> str:
        return f"{self._base_url}/stream?streams={'/'.join(self._streams)}"

    async def connect(self, on_message: Callable[[dict], Awaitable[None]]) -> None:
        self._running = True
        attempt = 0

        while self._running:
            try:
                async with websockets.connect(self._url) as ws:
                    logger.info(f"Connected to Binance WebSocket: {len(self._streams)} streams")
                    attempt = 0

                    while self._running:
                        raw_msg = await ws.recv()
                        message = json.loads(raw_msg)

                        data = message.get("data", {})
                        if data:
                            await on_message(data)

            except Exception as e:
                if not self._running:
                    break

                attempt += 1
                delay = min(2 ** attempt, MAX_RECONNECT_DELAY)
                logger.error(f"Connection error: {e}. Reconnecting in {delay}s (attempt {attempt})")
                await asyncio.sleep(delay)

    def stop(self) -> None:
        self._running = False
        logger.info("Binance WebSocket client stopping")