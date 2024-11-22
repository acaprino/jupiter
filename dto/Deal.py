from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dto.BrokerOrder import BrokerOrder
from misc_utils.enums import DealType, OrderSource


@dataclass
class Deal:
    ticket: int
    order_id: int
    time: datetime
    magic_number: int
    position_id: int
    volume: float
    execution_price: float
    commission: float
    swap: float
    profit: float
    fee: float
    symbol: str
    comment: str
    external_id: str
    deal_type: DealType = DealType.OTHER
    order_source: Optional[OrderSource] = None
    order: Optional[BrokerOrder] = None
