from dataclasses import dataclass
from typing import Optional, Dict, Any

from misc_utils.enums import Timeframe, TradingDirection
from misc_utils.utils_functions import string_to_enum


@dataclass
class Signal:
    bot_name: str
    signal_id: str
    symbol: str
    timeframe: Timeframe
    direction: TradingDirection
    candle: dict
    routine_id: str
    creation_tms: float
    update_tms: Optional[float]
    confirmed: Optional[bool]
    agent: Optional[str]
    user: Optional[str]

    def __str__(self) -> str:
        # Handle candle keys display safely
        candle_keys = None
        if self.candle is not None:
            if hasattr(self.candle, 'empty'):
                # Handle Pandas Series/DataFrame
                if not self.candle.empty:
                    candle_keys = list(self.candle.keys())
            else:
                # Handle dict or other objects
                try:
                    candle_keys = list(self.candle.keys())
                except (AttributeError, TypeError):
                    candle_keys = str(type(self.candle))

        return (
            f"Signal(bot_name='{self.bot_name}', signal_id='{self.signal_id}', symbol='{self.symbol}', "
            f"timeframe={self.timeframe}, direction={self.direction}, routine_id='{self.routine_id}', "
            f"creation_tms={self.creation_tms}, update_tms={self.update_tms}, confirmed={self.confirmed}, "
            f"agent={self.agent}, user={self.user}, candle_keys={candle_keys})"
        )

    def __repr__(self):
        return self.__str__()

    def to_json(self) -> Dict[str, Any]:
        return {
            "bot_name": self.bot_name,
            "signal_id": self.signal_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe.name,
            "direction": self.direction.name,
            "candle": self.candle,
            "routine_id": self.routine_id,
            "creation_tms": self.creation_tms,
            "update_tms": self.update_tms,
            "confirmed": self.confirmed,
            "agent": self.agent,
            "user": self.user
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Signal":
        return Signal(
            bot_name=data["bot_name"],
            signal_id=data["signal_id"],
            symbol=data["symbol"],
            timeframe=string_to_enum(Timeframe, data["timeframe"]),
            direction=string_to_enum(TradingDirection, data["direction"]),
            candle=data["candle"],
            routine_id=data["routine_id"],
            creation_tms=data["creation_tms"],
            update_tms=data["update_tms"],
            confirmed=data["confirmed"],
            agent=data.get("agent"),
            user=data.get("user")
        )
