from dataclasses import dataclass
from typing import Optional

from misc_utils.enums import OpType, FillingType


@dataclass
class OrderRequest:
    order_type: OpType
    symbol: str
    order_price: float
    volume: float
    sl: float
    tp: float
    comment: str
    filling_mode: Optional[FillingType] = None
    magic_number: Optional[int] = None

    def __str__(self) -> str:
        return (
            f"OrderRequest(order_type='{self.order_type.name}', symbol='{self.symbol}', "
            f"order_price={self.order_price:.4f}, volume={self.volume:.2f}, sl={self.sl:.4f}, "
            f"tp={self.tp:.4f}, comment='{self.comment}', "
            f"filling_mode='{self.filling_mode.value if self.filling_mode else 'N/A'}', "
            f"magic_number={self.magic_number if self.magic_number is not None else 'N/A'})"
        )

    def __repr__(self):
        return self.__str__()
