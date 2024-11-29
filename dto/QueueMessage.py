import json
import uuid
from dataclasses import dataclass, field
from typing import Optional

from misc_utils.config import TradingConfiguration
from misc_utils.enums import Timeframe
from misc_utils.utils_functions import to_serializable, dt_to_unix, now_utc


@dataclass
class QueueMessage:
    sender: str
    recipient: str
    trading_configuration: dict[str, any]
    payload: dict
    timestamp: Optional[int] = field(default_factory=lambda: dt_to_unix(now_utc()))
    message_id: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))

    def __str__(self):
        return f"QueueMessage(sender={self.sender}, message_id={self.message_id}, payload={self.payload})"

    def serialize(self):
        return to_serializable(self)

    def to_json(self):
        return json.dumps(self.serialize())

    def get(self, key, alt: Optional[any] = None) -> any:
        return self.payload.get(key, alt)

    @classmethod
    def from_json(cls, json_data: str):
        data = json.loads(json_data)
        return cls(
            sender=data["sender"],
            payload=data["payload"],
            recipient=data["sender"],
            timestamp=data["timestamp"],
            message_id=data["message_id"],
            trading_configuration=data["trading_configuration"]
        )

    def get_bot_name(self) -> str:
        return self.trading_configuration["bot_name"]

    def get_timeframe(self) -> str:
        return self.trading_configuration["timeframe"]

    def get_symbol(self) -> str:
        return self.trading_configuration["symbol"]

    def get_direction(self) -> str:
        return self.trading_configuration["trading_direction"]
