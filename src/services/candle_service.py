from dataclasses import dataclass, field
from decimal import Decimal
from core.kafka.schemas import TickEvent, CandleEvent


@dataclass
class OHLCVBucket:
    symbol: str
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_time: int
    close_time: int

    def update(self, price: Decimal, quantity: Decimal) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += quantity

    def to_candle_event(self) -> CandleEvent:
        return CandleEvent(
            symbol=self.symbol,
            interval=self.interval,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            open_time=self.open_time,
            close_time=self.close_time,
        )


class CandleAggregator:
    def __init__(self, interval_ms: int = 1000, interval_label: str = "1s"):
        self._interval_ms = interval_ms
        self._interval_label = interval_label
        self._buckets: dict[str, OHLCVBucket] = {}

    def _truncate_time(self, timestamp_ms: int) -> int:
        return (timestamp_ms // self._interval_ms) * self._interval_ms

    def process_tick(self, tick: TickEvent) -> CandleEvent | None:
        window_start = self._truncate_time(tick.timestamp)
        window_end = window_start + self._interval_ms - 1
        symbol = tick.symbol

        current_bucket = self._buckets.get(symbol)

        if current_bucket is None:
            self._buckets[symbol] = OHLCVBucket(
                symbol=symbol,
                interval=self._interval_label,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=tick.quantity,
                open_time=window_start,
                close_time=window_end,
            )
            return None

        if current_bucket.open_time == window_start:
            current_bucket.update(tick.price, tick.quantity)
            return None

        completed_candle = current_bucket.to_candle_event()

        self._buckets[symbol] = OHLCVBucket(
            symbol=symbol,
            interval=self._interval_label,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.quantity,
            open_time=window_start,
            close_time=window_end,
        )

        return completed_candle

    def flush(self, symbol: str) -> CandleEvent | None:
        bucket = self._buckets.pop(symbol, None)
        if bucket is None:
            return None
        return bucket.to_candle_event()

    def flush_all(self) -> list[CandleEvent]:
        candles = []
        for symbol in list(self._buckets.keys()):
            candle = self.flush(symbol)
            if candle:
                candles.append(candle)
        return candles
