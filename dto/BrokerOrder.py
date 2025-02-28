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

    def __str__(self) -> str:
        return (
            f"BrokerOrder(ticket={self.ticket}, order_type={self.order_type}, symbol='{self.symbol}', "
            f"order_price={self.order_price}, price_current={self.price_current}, volume={self.volume}, "
            f"time_setup={self.time_setup.isoformat()}, time_done={self.time_done.isoformat()}, "
            f"position_id={self.position_id}, sl={self.sl}, tp={self.tp}, comment={self.comment}, "
            f"filling_mode={self.filling_mode}, magic_number={self.magic_number}, order_source={self.order_source})"
        )

    def __repr__(self):
        return self.__str__()