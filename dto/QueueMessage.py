import json
import uuid
from dataclasses import dataclass, field
from typing import Optional

from misc_utils.utils_functions import to_serializable


@dataclass
class QueueMessage:
    sender: str
    payload: dict
    recipient: Optional[str] = None
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
            message_id=data["message_id"]
        )
