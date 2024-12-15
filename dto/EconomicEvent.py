import json
import os
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, List

from misc_utils.error_handler import exception_handler


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
    except Exception as e:
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
