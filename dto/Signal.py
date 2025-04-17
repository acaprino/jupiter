from dataclasses import dataclass
from typing import Optional, Dict, Any

from misc_utils.enums import Timeframe, TradingDirection
from misc_utils.utils_functions import string_to_enum

@dataclass
class Signal:
    bot_name: str
    instance_name: str
    signal_id: str
    symbol: str
    timeframe: Timeframe
    direction: TradingDirection
    cur_candle: dict
    routine_id: str
    creation_tms: float
    update_tms: Optional[float]
    confirmed: Optional[bool]
    agent: Optional[str]
    user: Optional[str]
    prev_candle: Optional[dict] = None

    def __str__(self) -> str:
        # Handle candle keys display safely
        cur_keys = None
        if self.cur_candle is not None:
            if hasattr(self.cur_candle, 'empty'):
                if not self.cur_candle.empty:
                    cur_keys = list(self.cur_candle.keys())
            else:
                try:
                    cur_keys = list(self.cur_candle.keys())
                except (AttributeError, TypeError):
                    cur_keys = str(type(self.cur_candle))
        prev_keys = None
        if self.prev_candle is not None:
            try:
                prev_keys = list(self.prev_candle.keys())
            except (AttributeError, TypeError):
                prev_keys = str(type(self.prev_candle))
        return (
            f"Signal(bot='{self.bot_name}', id='{self.signal_id}', symbol='{self.symbol}', "
            f"tf={self.timeframe.name}, dir={self.direction.name}, "
            f"routine='{self.routine_id}', created={self.creation_tms}, updated={self.update_tms}, "
            f"confirmed={self.confirmed}, agent={self.agent}, user={self.user}, "
            f"cur_keys={cur_keys}, prev_keys={prev_keys})"
        )

    def __repr__(self):
        return self.__str__()

    def to_json(self) -> Dict[str, Any]:
        return {
            "bot_name": self.bot_name,
            "instance_name": self.instance_name,
            "signal_id": self.signal_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe.name,
            "direction": self.direction.name,
            "cur_candle": self.cur_candle,
            "routine_id": self.routine_id,
            "creation_tms": self.creation_tms,
            "update_tms": self.update_tms,
            "confirmed": self.confirmed,
            "agent": self.agent,
            "user": self.user,
            "prev_candle": self.prev_candle
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Signal":
        return Signal(
            bot_name=data["bot_name"],
            instance_name=data["instance_name"],
            signal_id=data["signal_id"],
            symbol=data["symbol"],
            timeframe=string_to_enum(Timeframe, data["timeframe"]),
            direction=string_to_enum(TradingDirection, data["direction"]),
            cur_candle=data.get("cur_candle", {}),
            routine_id=data["routine_id"],
            creation_tms=data["creation_tms"],
            update_tms=data.get("update_tms"),
            confirmed=data.get("confirmed"),
            agent=data.get("agent"),
            user=data.get("user"),
            prev_candle=data.get("prev_candle")
        )
