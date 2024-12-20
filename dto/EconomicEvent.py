import json
import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List

from misc_utils.error_handler import exception_handler
from misc_utils.utils_functions import string_to_enum, dt_to_unix, unix_to_datetime


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
    is_holiday: bool

    def to_json(self) -> dict:
        """
        Serializes the EconomicEvent instance to a JSON-compatible dictionary.
        """
        return {
            "event_id": self.event_id,
            "name": self.name,
            "country": self.country,
            "description": self.description,
            "time": dt_to_unix(self.time),
            "importance": self.importance.name,  # Serialize enum as name (e.g., "HIGH")
            "source_url": self.source_url,
            "is_holiday": self.is_holiday
        }

    @staticmethod
    def from_json(data: dict) -> "EconomicEvent":
        """
        Deserializes a JSON-compatible dictionary into an EconomicEvent instance.
        """
        return EconomicEvent(
            event_id=data["event_id"],
            name=data["name"],
            country=data["country"],
            description=data.get("description"),
            time=unix_to_datetime(data["time"]),
            importance=string_to_enum(EventImportance, data["importance"]),  # Map name to enum
            source_url=data.get("source_url"),
            is_holiday=data["is_holiday"]
        )


def map_from_metatrader(json_obj: dict, timezone_offset: int) -> EconomicEvent:
    # Determine if the event is a holiday
    event_type = json_obj.get("event_type", 0)
    is_holiday = event_type == 2  # CALENDAR_TYPE_HOLIDAY corresponds to 2

    # Map importance to EventImportance enum
    importance = EventImportance(json_obj["event_importance"])

    return EconomicEvent(
        event_id=str(json_obj["event_id"]),
        country=json_obj["country_code"],
        name=json_obj["event_name"],
        description=json_obj.get("event_code"),
        time=datetime.strptime(json_obj["event_time"], "%Y.%m.%d %H:%M") - timedelta(hours=timezone_offset),
        importance=importance,
        source_url=json_obj.get("event_source_url"),
        is_holiday=is_holiday
    )


@exception_handler
async def get_pairs() -> List[Dict]:
    """Loads pairs and their associated countries from pairs.json file."""
    try:
        cur_script_directory = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(os.path.dirname(cur_script_directory), 'pairs.json')

        with open(file_path, 'r') as file:
            data = json.load(file)
        return data
    except Exception:
        return []


@exception_handler
async def get_pair(symbol: str) -> Optional[Dict]:
    """Fetches the pair data for the specified symbol."""
    pairs = await get_pairs()
    for pair in pairs:
        if pair["symbol"] == symbol:
            return pair
    return None


@exception_handler
async def get_symbol_countries_of_interest(symbol: str) -> List[str]:
    try:
        pair = await get_pair(symbol)
        countries = pair.get("countries", []) if pair else []
        return countries
    except Exception as e:
        return []
