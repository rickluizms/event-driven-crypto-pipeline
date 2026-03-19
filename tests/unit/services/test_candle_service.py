from decimal import Decimal
from core.kafka.schemas import TickEvent
from src.services.candle_service import CandleAggregator


def _make_tick(symbol: str, price: str, qty: str, timestamp: int, trade_id: int = 1) -> TickEvent:
    return TickEvent(
        symbol=symbol,
        price=Decimal(price),
        quantity=Decimal(qty),
        timestamp=timestamp,
        trade_id=trade_id,
    )


class TestCandleAggregator:
    def test_first_tick_returns_none(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        result = agg.process_tick(_make_tick("btcusdt", "50000", "0.1", 1000))
        assert result is None

    def test_tick_in_same_window_returns_none(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        agg.process_tick(_make_tick("btcusdt", "50000", "0.1", 1000))
        result = agg.process_tick(_make_tick("btcusdt", "50100", "0.2", 1500))
        assert result is None

    def test_tick_in_new_window_emits_candle(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        agg.process_tick(_make_tick("btcusdt", "50000", "0.1", 1000))
        agg.process_tick(_make_tick("btcusdt", "50100", "0.2", 1500))

        candle = agg.process_tick(_make_tick("btcusdt", "50200", "0.3", 2000))

        assert candle is not None
        assert candle.symbol == "btcusdt"
        assert candle.interval == "1s"
        assert candle.open == Decimal("50000")
        assert candle.high == Decimal("50100")
        assert candle.low == Decimal("50000")
        assert candle.close == Decimal("50100")
        assert candle.volume == Decimal("0.3")
        assert candle.open_time == 1000
        assert candle.close_time == 1999

    def test_ohlcv_values_correct(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        agg.process_tick(_make_tick("ethusdt", "3000", "1.0", 5000))
        agg.process_tick(_make_tick("ethusdt", "3100", "0.5", 5200))
        agg.process_tick(_make_tick("ethusdt", "2900", "0.3", 5400))
        agg.process_tick(_make_tick("ethusdt", "3050", "0.7", 5800))

        candle = agg.process_tick(_make_tick("ethusdt", "3200", "0.1", 6000))

        assert candle.open == Decimal("3000")
        assert candle.high == Decimal("3100")
        assert candle.low == Decimal("2900")
        assert candle.close == Decimal("3050")
        assert candle.volume == Decimal("2.5")

    def test_multiple_symbols_independent(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        agg.process_tick(_make_tick("btcusdt", "50000", "0.1", 1000))
        agg.process_tick(_make_tick("ethusdt", "3000", "1.0", 1000))

        candle_btc = agg.process_tick(_make_tick("btcusdt", "50100", "0.2", 2000))
        candle_eth = agg.process_tick(_make_tick("ethusdt", "3100", "0.5", 2000))

        assert candle_btc is not None
        assert candle_btc.symbol == "btcusdt"
        assert candle_eth is not None
        assert candle_eth.symbol == "ethusdt"

    def test_flush_returns_candle(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        agg.process_tick(_make_tick("btcusdt", "50000", "0.1", 1000))

        candle = agg.flush("btcusdt")
        assert candle is not None
        assert candle.symbol == "btcusdt"

    def test_flush_empty_returns_none(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        assert agg.flush("btcusdt") is None

    def test_flush_all(self):
        agg = CandleAggregator(interval_ms=1000, interval_label="1s")
        agg.process_tick(_make_tick("btcusdt", "50000", "0.1", 1000))
        agg.process_tick(_make_tick("ethusdt", "3000", "1.0", 1000))

        candles = agg.flush_all()
        assert len(candles) == 2
        symbols = {c.symbol for c in candles}
        assert symbols == {"btcusdt", "ethusdt"}
