import json
import uuid
from dataclasses import dataclass, field
from typing import Optional, Dict

from misc_utils.enums import Timeframe, TradingDirection
from misc_utils.utils_functions import to_serializable, dt_to_unix, now_utc, string_to_enum


@dataclass
class QueueMessage:
    sender: str
    recipient: str
    trading_configuration: dict[str, any]
    payload: Dict
    timestamp: Optional[int] = field(default_factory=lambda: dt_to_unix(now_utc()))
    message_id: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))

    def __str__(self):
        return f"QueueMessage(sender={self.sender}, message_id={self.message_id}, payload={self.payload})"

    def serialize(self):
        return to_serializable(self)

    def to_json(self):
        return json.dumps(self.serialize(), default=lambda obj: to_serializable(obj))

    def get(self, key, alt: Optional[any] = None) -> any:
        return self.payload.get(key, alt)

    @classmethod
    def from_json(cls, json_data: str):
        data = json.loads(json_data)
        return cls(
            sender=data["sender"],
            payload=data["payload"],
            recipient=data["recipient"],
            timestamp=data["timestamp"],
            message_id=data["message_id"],
            trading_configuration=data["trading_configuration"]
        )

    def get_bot_name(self) -> str:
        return self.trading_configuration["bot_name"]

    def get_timeframe(self) -> Timeframe | None:
        timeframe_str = self.trading_configuration.get("timeframe", None)
        if timeframe_str is None:
            return None
        return string_to_enum(Timeframe, timeframe_str)

    def get_symbol(self) -> str | None:
        return self.trading_configuration.get("symbol", None)

    def get_direction(self) -> TradingDirection | None:
        direction_str = self.trading_configuration.get("trading_direction", None)
        if direction_str is None:
            return None
        return string_to_enum(TradingDirection, direction_str)
