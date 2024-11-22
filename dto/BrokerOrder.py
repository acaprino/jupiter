from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from misc_utils.enums import FillingType, OrderType, OrderSource


@dataclass
class BrokerOrder:
    ticket: int
    order_type: OrderType
    symbol: str
    order_price: float
    price_current: float
    volume: float
    time_setup: datetime
    time_done: datetime
    position_id: int
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: Optional[str] = None
    filling_mode: Optional[FillingType] = None
    magic_number: Optional[int] = None
    order_source: Optional[OrderSource] = None
