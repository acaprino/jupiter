from datetime import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Optional

class EventImportance(Enum):
    NONE = 0
    LOW = 1
    MODERATE = 2
    HIGH = 3

@dataclass
class EconomicEvent:
    event_id: str
    name: str
    country: str
    description: Optional[str]
    time: datetime
    importance: EventImportance
    source_url: Optional[str]
    url: Optional[str]
    is_holiday: bool