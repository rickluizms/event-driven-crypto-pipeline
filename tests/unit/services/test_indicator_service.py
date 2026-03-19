from decimal import Decimal
from core.kafka.schemas import CandleEvent
from src.services.indicator_service import IndicatorService


def _make_candle(symbol: str, close: str, high: str, low: str, volume: str, close_time: int) -> CandleEvent:
    return CandleEvent(
        symbol=symbol,
        interval="1s",
        open=Decimal(close),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
        open_time=close_time - 999,
        close_time=close_time,
    )


class TestIndicatorService:
    def test_no_indicators_with_insufficient_data(self):
        service = IndicatorService(sma_period=14, ema_period=14, rsi_period=14)

        events = service.process_candle(
            _make_candle("btcusdt", "50000", "50100", "49900", "1.0", 1000)
        )

        indicator_names = {e.indicator_name for e in events}
        assert "SMA" not in indicator_names
        assert "EMA" not in indicator_names
        assert "RSI" not in indicator_names

    def test_vwap_available_immediately(self):
        service = IndicatorService(sma_period=14, ema_period=14, rsi_period=14)

        events = service.process_candle(
            _make_candle("btcusdt", "50000", "50100", "49900", "1.0", 1000)
        )

        vwap_events = [e for e in events if e.indicator_name == "VWAP"]
        assert len(vwap_events) == 1
        assert vwap_events[0].symbol == "btcusdt"
        assert vwap_events[0].value > 0

    def test_sma_after_enough_candles(self):
        service = IndicatorService(sma_period=5, ema_period=5, rsi_period=5)

        for i in range(6):
            price = str(50000 + i * 100)
            events = service.process_candle(
                _make_candle("btcusdt", price, price, price, "1.0", (i + 1) * 1000)
            )

        sma_events = [e for e in events if e.indicator_name == "SMA"]
        assert len(sma_events) == 1
        assert sma_events[0].value > 0

    def test_ema_after_enough_candles(self):
        service = IndicatorService(sma_period=5, ema_period=5, rsi_period=5)

        for i in range(6):
            price = str(50000 + i * 100)
            events = service.process_candle(
                _make_candle("btcusdt", price, price, price, "1.0", (i + 1) * 1000)
            )

        ema_events = [e for e in events if e.indicator_name == "EMA"]
        assert len(ema_events) == 1
        assert ema_events[0].value > 0

    def test_rsi_after_enough_candles(self):
        service = IndicatorService(sma_period=5, ema_period=5, rsi_period=5)

        prices = [100, 102, 101, 103, 105, 104, 106, 108, 107, 109]
        for i, price in enumerate(prices):
            events = service.process_candle(
                _make_candle("btcusdt", str(price), str(price), str(price), "1.0", (i + 1) * 1000)
            )

        rsi_events = [e for e in events if e.indicator_name == "RSI"]
        assert len(rsi_events) == 1
        assert 0 <= rsi_events[0].value <= 100

    def test_multiple_symbols_independent_buffers(self):
        service = IndicatorService(sma_period=3, ema_period=3, rsi_period=3)

        for i in range(5):
            service.process_candle(
                _make_candle("btcusdt", str(50000 + i * 100), str(50000 + i * 100), str(50000 + i * 100), "1.0", (i + 1) * 1000)
            )

        events = service.process_candle(
            _make_candle("ethusdt", "3000", "3000", "3000", "1.0", 1000)
        )

        sma_events = [e for e in events if e.indicator_name == "SMA"]
        assert len(sma_events) == 0

    def test_all_events_have_correct_symbol_and_interval(self):
        service = IndicatorService(sma_period=3, ema_period=3, rsi_period=3)

        for i in range(10):
            events = service.process_candle(
                _make_candle("ethusdt", str(3000 + i * 10), str(3000 + i * 10), str(3000 + i * 10), "1.0", (i + 1) * 1000)
            )

        for event in events:
            assert event.symbol == "ethusdt"
            assert event.interval == "1s"
