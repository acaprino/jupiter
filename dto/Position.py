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
