from dataclasses import field, dataclass
from datetime import datetime
from typing import List, Optional

from dto.Deal import Deal
from misc_utils.enums import PositionType, OrderSource


@dataclass
class Position:
    position_id: int
    symbol: str
    open: bool
    ticket: Optional[int] = None
    time: Optional[datetime] = None
    last_update_timestamp: Optional[datetime] = None
    volume: Optional[float] = None
    price_open: Optional[float] = None
    price_current: Optional[float] = None
    swap: Optional[float] = None
    profit: Optional[float] = None
    commission: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    comment: Optional[str] = None
    position_type: PositionType = PositionType.OTHER
    order_source: Optional[OrderSource] = None
    deals: List[Deal] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"Position(position_id={self.position_id}, symbol='{self.symbol}', open={self.open}, "
            f"ticket={self.ticket}, time={self.time.isoformat() if self.time else None}, "
            f"last_update_timestamp={self.last_update_timestamp.isoformat() if self.last_update_timestamp else None}, volume={self.volume}, "
            f"price_open={self.price_open}, price_current={self.price_current}, swap={self.swap}, profit={self.profit}, "
            f"commission={self.commission}, sl={self.sl}, tp={self.tp}, comment={self.comment}, "
            f"position_type={self.position_type}, order_source={self.order_source}, deals_count={len(self.deals)})"
        )

    def __repr__(self):
        return self.__str__()
