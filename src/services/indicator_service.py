from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal
import numpy as np
import talib
from core.kafka.schemas import CandleEvent, IndicatorEvent


@dataclass
class CandleBuffer:
    closes: deque = field(default_factory=lambda: deque(maxlen=200))
    highs: deque = field(default_factory=lambda: deque(maxlen=200))
    lows: deque = field(default_factory=lambda: deque(maxlen=200))
    volumes: deque = field(default_factory=lambda: deque(maxlen=200))
    timestamps: deque = field(default_factory=lambda: deque(maxlen=200))

    def append(self, candle: CandleEvent) -> None:
        self.closes.append(float(candle.close))
        self.highs.append(float(candle.high))
        self.lows.append(float(candle.low))
        self.volumes.append(float(candle.volume))
        self.timestamps.append(candle.close_time)


class IndicatorService:
    def __init__(
        self,
        sma_period: int = 14,
        ema_period: int = 14,
        rsi_period: int = 14,
        buffer_size: int = 200,
    ):
        self._sma_period = sma_period
        self._ema_period = ema_period
        self._rsi_period = rsi_period
        self._buffer_size = buffer_size
        self._buffers: dict[str, CandleBuffer] = {}

    def _get_buffer(self, symbol: str) -> CandleBuffer:
        if symbol not in self._buffers:
            self._buffers[symbol] = CandleBuffer(
                closes=deque(maxlen=self._buffer_size),
                highs=deque(maxlen=self._buffer_size),
                lows=deque(maxlen=self._buffer_size),
                volumes=deque(maxlen=self._buffer_size),
                timestamps=deque(maxlen=self._buffer_size),
            )
        return self._buffers[symbol]

    def _make_event(
        self, symbol: str, interval: str, name: str, value: float, timestamp: int
    ) -> IndicatorEvent:
        return IndicatorEvent(
            symbol=symbol,
            interval=interval,
            indicator_name=name,
            value=value,
            timestamp=timestamp,
        )

    def process_candle(self, candle: CandleEvent) -> list[IndicatorEvent]:
        buf = self._get_buffer(candle.symbol)
        buf.append(candle)

        closes = np.array(buf.closes, dtype=np.float64)
        volumes = np.array(buf.volumes, dtype=np.float64)
        highs = np.array(buf.highs, dtype=np.float64)
        lows = np.array(buf.lows, dtype=np.float64)
        timestamp = buf.timestamps[-1]

        events: list[IndicatorEvent] = []

        if len(closes) >= self._sma_period:
            sma = talib.SMA(closes, timeperiod=self._sma_period)
            if not np.isnan(sma[-1]):
                events.append(
                    self._make_event(
                        candle.symbol, candle.interval, "SMA", float(sma[-1]), timestamp
                    )
                )

        if len(closes) >= self._ema_period:
            ema = talib.EMA(closes, timeperiod=self._ema_period)
            if not np.isnan(ema[-1]):
                events.append(
                    self._make_event(
                        candle.symbol, candle.interval, "EMA", float(ema[-1]), timestamp
                    )
                )

        if len(closes) >= self._rsi_period + 1:
            rsi = talib.RSI(closes, timeperiod=self._rsi_period)
            if not np.isnan(rsi[-1]):
                events.append(
                    self._make_event(
                        candle.symbol, candle.interval, "RSI", float(rsi[-1]), timestamp
                    )
                )

        if len(closes) >= 1 and np.sum(volumes) > 0:
            typical_price = (highs + lows + closes) / 3.0
            cum_tp_vol = np.cumsum(typical_price * volumes)
            cum_vol = np.cumsum(volumes)
            vwap = cum_tp_vol[-1] / cum_vol[-1]
            if not np.isnan(vwap):
                events.append(
                    self._make_event(
                        candle.symbol, candle.interval, "VWAP", float(vwap), timestamp
                    )
                )

        return events
