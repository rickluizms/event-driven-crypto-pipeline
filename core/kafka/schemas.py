from decimal import Decimal
from pydantic import BaseModel


class TickEvent(BaseModel):
    symbol: str
    price: Decimal
    quantity: Decimal
    timestamp: int
    trade_id: int

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "price": str(self.price),
            "quantity": str(self.quantity),
            "timestamp": self.timestamp,
            "trade_id": self.trade_id,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "TickEvent":
        return cls(
            symbol=data["symbol"],
            price=Decimal(data["price"]),
            quantity=Decimal(data["quantity"]),
            timestamp=data["timestamp"],
            trade_id=data["trade_id"],
        )


class CandleEvent(BaseModel):
    symbol: str
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    open_time: int
    close_time: int

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "open": str(self.open),
            "high": str(self.high),
            "low": str(self.low),
            "close": str(self.close),
            "volume": str(self.volume),
            "open_time": self.open_time,
            "close_time": self.close_time,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CandleEvent":
        return cls(
            symbol=data["symbol"],
            interval=data["interval"],
            open=Decimal(data["open"]),
            high=Decimal(data["high"]),
            low=Decimal(data["low"]),
            close=Decimal(data["close"]),
            volume=Decimal(data["volume"]),
            open_time=data["open_time"],
            close_time=data["close_time"],
        )


class IndicatorEvent(BaseModel):
    symbol: str
    interval: str
    indicator_name: str
    value: float
    timestamp: int

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "indicator_name": self.indicator_name,
            "value": self.value,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "IndicatorEvent":
        return cls(
            symbol=data["symbol"],
            interval=data["interval"],
            indicator_name=data["indicator_name"],
            value=data["value"],
            timestamp=data["timestamp"],
        )
