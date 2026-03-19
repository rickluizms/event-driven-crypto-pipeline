from decimal import Decimal
from core.kafka.schemas import TickEvent
from src.producers.market_data.mapper import map_trade_to_tick


class TestMapper:
    def test_map_trade_to_tick(self):
        raw_msg = {
            "s": "BTCUSDT",
            "p": "50000.50",
            "q": "0.001",
            "T": 1679000000000,
            "t": 123456789,
        }

        tick = map_trade_to_tick(raw_msg)

        assert isinstance(tick, TickEvent)
        assert tick.symbol == "btcusdt"
        assert tick.price == Decimal("50000.50")
        assert tick.quantity == Decimal("0.001")
        assert tick.timestamp == 1679000000000
        assert tick.trade_id == 123456789

    def test_symbol_lowercased(self):
        raw_msg = {
            "s": "ETHUSDT",
            "p": "3000",
            "q": "1.5",
            "T": 1679000000000,
            "t": 1,
        }

        tick = map_trade_to_tick(raw_msg)
        assert tick.symbol == "ethusdt"

    def test_decimal_precision(self):
        raw_msg = {
            "s": "BTCUSDT",
            "p": "50000.123456789",
            "q": "0.000000001",
            "T": 1679000000000,
            "t": 1,
        }

        tick = map_trade_to_tick(raw_msg)
        assert tick.price == Decimal("50000.123456789")
        assert tick.quantity == Decimal("0.000000001")

    def test_to_dict_roundtrip(self):
        raw_msg = {
            "s": "BTCUSDT",
            "p": "50000.50",
            "q": "0.001",
            "T": 1679000000000,
            "t": 123456789,
        }

        tick = map_trade_to_tick(raw_msg)
        tick_dict = tick.to_dict()
        restored = TickEvent.from_dict(tick_dict)

        assert restored.symbol == tick.symbol
        assert restored.price == tick.price
        assert restored.quantity == tick.quantity
        assert restored.timestamp == tick.timestamp
        assert restored.trade_id == tick.trade_id
