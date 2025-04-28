# dto/Signal.py

from dataclasses import dataclass
from typing import Optional, Dict, Any
from enum import Enum, auto

from misc_utils.enums import Timeframe, TradingDirection
from misc_utils.utils_functions import string_to_enum, to_serializable


class SignalStatus(Enum):
    GENERATED = auto()  # Signal just generated (opportunity)
    CONFIRMED = auto()  # Signal confirmed by the user
    BLOCKED = auto()  # Signal blocked by the user
    CONSUMED_ENTRY = auto()  # Signal consumed because it generated an entry
    FIRED = auto()  # Signal consumed because it expired without entry
    UNKNOWN = auto()  # Unknown or undefined state


@dataclass
class Signal:
    bot_name: str
    instance_name: str
    signal_id: str
    symbol: str
    timeframe: Timeframe
    direction: TradingDirection
    creation_tms: float
    update_tms: Optional[float]
    confirmed: Optional[bool]
    agent: Optional[str]
    user: Optional[str]
    opportunity_candle: dict
    signal_candle: Optional[dict] = None
    status: SignalStatus = SignalStatus.UNKNOWN

    # ... (dentro la classe Signal) ...

    def __str__(self) -> str:
        json_dict = self.to_json()
        return str(json_dict)

    def __repr__(self):
        return self.__str__()

    def to_json(self) -> Dict[str, Any]:
        # Serialize status using its name
        return {
            "bot_name": self.bot_name,
            "instance_name": self.instance_name,
            "signal_id": self.signal_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe.name,
            "direction": self.direction.name,
            "signal_candle": to_serializable(self.signal_candle),
            "creation_tms": self.creation_tms,
            "update_tms": self.update_tms,
            "confirmed": self.confirmed,
            "agent": self.agent,
            "user": self.user,
            "opportunity_candle": to_serializable(self.opportunity_candle),
            "status": self.status.name
        }

    @staticmethod
    def from_json(data: Dict[str, Any]) -> "Signal":
        status_str = data.get("status", SignalStatus.UNKNOWN.name)
        try:
            status_enum = SignalStatus[status_str]
        except KeyError:
            print(f"Warning: Invalid status '{status_str}' found for signal_id {data.get('signal_id')}. Defaulting to UNKNOWN.")
            status_enum = SignalStatus.UNKNOWN

        return Signal(
            bot_name=data["bot_name"],
            instance_name=data["instance_name"],
            signal_id=data["signal_id"],
            symbol=data["symbol"],
            timeframe=string_to_enum(Timeframe, data["timeframe"]),
            direction=string_to_enum(TradingDirection, data["direction"]),
            signal_candle=data.get("signal_candle", {}),
            creation_tms=data["creation_tms"],
            update_tms=data.get("update_tms"),
            confirmed=data.get("confirmed"),
            agent=data.get("agent"),
            user=data.get("user"),
            opportunity_candle=data.get("opportunity_candle"),
            status=status_enum
        )
