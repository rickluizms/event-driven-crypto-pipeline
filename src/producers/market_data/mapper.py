from decimal import Decimal
from core.kafka.schemas import TickEvent


def map_trade_to_tick(raw_msg: dict) -> TickEvent:
    return TickEvent(
        symbol=raw_msg["s"].lower(),
        price=Decimal(raw_msg["p"]),
        quantity=Decimal(raw_msg["q"]),
        timestamp=raw_msg["T"],
        trade_id=raw_msg["t"],
    )
