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

    def __str__(self):
        return (f"Trade Order:\n"
                f"  Type: {self.order_type.name}\n"
                f"  Symbol: {self.symbol}\n"
                f"  Price: {self.order_price}\n"
                f"  Volume: {self.volume}\n"
                f"  Stop Loss: {self.sl}\n"
                f"  Take Profit: {self.tp}\n"
                f"  Comment: {self.comment}\n"
                f"  Filling Mode: {self.filling_mode.value if self.filling_mode else 'N/A'}\n"
                f"  Magic Number: {self.magic_number if self.magic_number else 'N/A'}")
