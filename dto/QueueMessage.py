import json
from dataclasses import dataclass, field
from typing import Optional, Dict

from nanoid.generate import generate

from misc_utils.message_metainf import MessageMetaInf
from misc_utils.utils_functions import to_serializable, dt_to_unix, now_utc


@dataclass
class QueueMessage:
    sender: str
    recipient: str
    meta_inf: MessageMetaInf
    payload: Dict
    timestamp: Optional[int] = field(default_factory=lambda: dt_to_unix(now_utc()))
    message_id: Optional[str] = field(default_factory=lambda: str(generate(size=10)))

    def __str__(self):
        return f"QueueMessage(sender={self.sender}, message_id={self.message_id}, payload={self.payload})"

    def __repr__(self):
        return self.__str__()

    def serialize(self):
        return to_serializable(self)

    def to_json(self):
        return json.dumps(self.serialize(), default=lambda obj: to_serializable(obj))

    def get(self, key, alt: Optional[any] = None) -> any:
        return self.payload.get(key, alt)

    def get_meta_inf(self) -> MessageMetaInf:
        return self.meta_inf

    @classmethod
    def from_json(cls, json_data: str):
        data = json.loads(json_data)
        return cls(
            sender=data["sender"],
            payload=data["payload"],
            recipient=data["recipient"],
            timestamp=data["timestamp"],
            message_id=data["message_id"],
            meta_inf=MessageMetaInf.from_json(data["meta_inf"])
        )
