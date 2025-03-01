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

    def __str__(self) -> str:
        return (
            f"Deal(ticket={self.ticket}, order_id={self.order_id}, time={self.time.isoformat()}, "
            f"magic_number={self.magic_number}, position_id={self.position_id}, volume={self.volume}, "
            f"execution_price={self.execution_price}, commission={self.commission}, swap={self.swap}, "
            f"profit={self.profit}, fee={self.fee}, symbol='{self.symbol}', comment='{self.comment}', "
            f"external_id='{self.external_id}', deal_type={self.deal_type}, order_source={self.order_source})"
        )

    def __repr__(self):
        return self.__str__()