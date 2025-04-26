# File: dynamic_message_generator_with_coords.py

import time
import json
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

# Ensure import paths are correct based on your structure
from misc_utils.enums import Timeframe, TradingDirection, Mode, RabbitExchange
from misc_utils.utils_functions import (
    get_recent_past_multiple_of_timeframe,
    dt_to_unix,
    to_serializable,
    new_id
)

# --- Simulation Parameters ---
TARGET_TIMEFRAME = Timeframe.M30 # <<< Change the desired timeframe here (M1, M5, M15, M30, H1, H4, D1)
TARGET_SYMBOL = "EURUSD"
TARGET_DIRECTION = TradingDirection.LONG
# You can use fictitious IDs or generate them
GENERATOR_ROUTINE_ID = new_id(length=5, lower=False, upper=True)
SENTINEL_RECIPIENT_NAME = f"SEN_{TARGET_SYMBOL}.{TARGET_TIMEFRAME.name}.{TARGET_DIRECTION.name}_sen_dyn_xyz789"
BOT_NAME_PREFIX = "prod_dynamic" # Prefix used for exchanges (from config)

# --- Generation Functions ---

def get_candle_times(timeframe: Timeframe) -> tuple[datetime, datetime]:
    """
    Calculates the start and end times (naive UTC) of the last completed candle.
    """
    last_completed_candle_end_utc: datetime = get_recent_past_multiple_of_timeframe(timeframe)
    last_completed_candle_start_utc: datetime = last_completed_candle_end_utc - timedelta(seconds=timeframe.to_seconds())

    print(f"Current Time (UTC): {datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)}")
    print(f"Last Completed Candle Start (UTC): {last_completed_candle_start_utc}")
    print(f"Last Completed Candle End (UTC):   {last_completed_candle_end_utc}")

    return last_completed_candle_start_utc, last_completed_candle_end_utc

def generate_opportunity_message(
    symbol: str,
    timeframe: Timeframe,
    direction: TradingDirection,
    generator_routine_id: str,
    opportunity_timestamp: float # Unix timestamp
) -> tuple[str, Dict[str, Any]]:
    """Generates the JSON message for the opportunity."""
    signal_id = f"sig_{symbol}_{timeframe.name}_{direction.name}_{int(opportunity_timestamp)}_{new_id(4)}"
    agent_name = f"GEN_{symbol}.{timeframe.name}.{direction.name}_{generator_routine_id}"

    message = {
        "sender": agent_name,
        "recipient": "middleware",
        "meta_inf": {
            "agent_name": agent_name,
            "routine_id": generator_routine_id,
            "mode": Mode.GENERATOR.name,
            "bot_name": BOT_NAME_PREFIX,
            "instance_name": "DYNAMIC_GENERATOR", # Example
            "symbol": symbol,
            "timeframe": timeframe.name,
            "direction": direction.name,
            "ui_token": None,
            "ui_users": None
        },
        "payload": {
            "signal_id": signal_id
        },
        "timestamp": opportunity_timestamp, # Timestamp when opportunity is detected (candle end)
        "message_id": new_id(20)
    }

    # --- Print Coordinates ---
    exchange = RabbitExchange.jupiter_events
    exchange_name = f"{BOT_NAME_PREFIX}_{exchange.name}"
    routing_key = f"event.signal.opportunity.{symbol}.{timeframe.name}.{direction.name}"
    print(f" Target Exchange: {exchange_name} (Type: {exchange.exchange_type.value})")
    print(f" Target Routing Key: {routing_key}")
    # --- End Print Coordinates ---

    return signal_id, message

def generate_entry_message(
    symbol: str,
    timeframe: Timeframe,
    direction: TradingDirection,
    signal_id: str,
    generator_routine_id: str,
    sentinel_recipient_name: str,
    entry_timestamp: float # Unix timestamp
) -> Dict[str, Any]:
    """Generates the JSON message for the entry."""
    agent_name = f"GEN_{symbol}.{timeframe.name}.{direction.name}_{generator_routine_id}"

    message = {
        "sender": agent_name,
        "recipient": sentinel_recipient_name,
        "meta_inf": {
            "agent_name": agent_name,
            "routine_id": generator_routine_id,
            "mode": Mode.GENERATOR.name,
            "bot_name": BOT_NAME_PREFIX,
            "instance_name": "DYNAMIC_GENERATOR",
            "symbol": symbol,
            "timeframe": timeframe.name,
            "direction": direction.name,
            "ui_token": None,
            "ui_users": None
        },
        "payload": {
            "signal_id": signal_id # ID of the original opportunity
        },
        "timestamp": entry_timestamp, # Timestamp when entry is requested
        "message_id": new_id(20)
    }

    # --- Print Coordinates ---
    exchange = RabbitExchange.jupiter_events
    exchange_name = f"{BOT_NAME_PREFIX}_{exchange.name}"
    routing_key = f"event.signal.enter.{symbol}.{timeframe.name}.{direction.name}"
    print(f" Target Exchange: {exchange_name} (Type: {exchange.exchange_type.value})")
    print(f" Target Routing Key: {routing_key}")
    # --- End Print Coordinates ---

    return message

def generate_dummy_candle_data(start_time_unix: float, end_time_unix: float) -> Dict[str, Any]:
    """Generates fictitious data for a candle."""
    # You can make this data more realistic if needed
    base_price = 1.0850
    spread = 0.0010
    return {
        "time_open": start_time_unix,
        "time_close": end_time_unix,
        "time_open_broker": start_time_unix, # Simplified, could differ
        "time_close_broker": end_time_unix, # Simplified
        "open": base_price + (spread * 0.1),
        "high": base_price + (spread * 0.6),
        "low": base_price - (spread * 0.4),
        "close": base_price + (spread * 0.3),
        "HA_close": base_price + (spread * 0.25),
        "HA_open": base_price + (spread * 0.15),
        "HA_high": base_price + (spread * 0.6),
        "HA_low": base_price - (spread * 0.4),
        "SUPERTREND_10_1.0": base_price - (spread * 0.2),
        "SUPERTREND_40_3.0": base_price - (spread * 0.5),
        "STOCHk_24_5_3": random.uniform(30, 70),
        "STOCHd_24_5_3": random.uniform(30, 70),
        "ATR_5": spread * 0.8,
        "ATR_2": spread * 0.6
    }

def generate_signal_bson(
    symbol: str,
    timeframe: Timeframe,
    direction: TradingDirection,
    signal_id: str,
    generator_routine_id: str,
    creation_timestamp: float, # Unix timestamp (end of current candle)
    confirmation_timestamp: float, # Unix timestamp (when confirmed)
    current_candle_start_unix: float,
    current_candle_end_unix: float
) -> Dict[str, Any]:
    """Generates the BSON object (as Python dict) for the signal."""

    # Calculate previous candle times
    timeframe_seconds = timeframe.to_seconds()
    prev_candle_start_unix = current_candle_start_unix - timeframe_seconds
    prev_candle_end_unix = current_candle_start_unix # End of previous is start of current

    bson_object = {
        # "_id": "MongoDB will generate this", # Don't include here
        "bot_name": BOT_NAME_PREFIX,
        "instance_name": "DYNAMIC_GENERATOR",
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe.name,
        "direction": direction.name,
        "cur_candle": generate_dummy_candle_data(current_candle_start_unix, current_candle_end_unix),
        "prev_candle": generate_dummy_candle_data(prev_candle_start_unix, prev_candle_end_unix),
        "routine_id": generator_routine_id,
        "creation_tms": creation_timestamp, # When the opportunity was created
        "update_tms": confirmation_timestamp, # When it was confirmed/updated
        "confirmed": True, # Assume confirmed for entry
        "agent": f"GEN_{symbol}.{timeframe.name}.{direction.name}_{generator_routine_id}",
        "user": "SimulatedUser" # Fictitious user
    }
    # Use to_serializable to ensure all types are JSON/BSON compatible
    # even though we are already using primitive types or dicts here.
    return to_serializable(bson_object)

# --- Script Execution ---
if __name__ == "__main__":
    last_candle_start, last_candle_end = get_candle_times(TARGET_TIMEFRAME)

    last_candle_start_unix = dt_to_unix(last_candle_start)
    last_candle_end_unix = dt_to_unix(last_candle_end)

    # Define timestamps based on the last completed candle
    opportunity_ts = last_candle_end_unix # Opportunity detected at candle close
    confirmation_ts = opportunity_ts + 3 # Simulate confirmation 3 seconds later
    entry_ts = opportunity_ts + 5        # Simulate entry request 5 seconds after candle close

    print("\n--- Generating Opportunity Message ---")
    sig_id, opp_msg = generate_opportunity_message(
        TARGET_SYMBOL, TARGET_TIMEFRAME, TARGET_DIRECTION, GENERATOR_ROUTINE_ID, opportunity_ts
    )
    print(json.dumps(opp_msg, indent=2))

    print("\n--- Generating Entry Message ---")
    entry_msg = generate_entry_message(
        TARGET_SYMBOL, TARGET_TIMEFRAME, TARGET_DIRECTION, sig_id, GENERATOR_ROUTINE_ID, SENTINEL_RECIPIENT_NAME, entry_ts
    )
    print(json.dumps(entry_msg, indent=2))

    print("\n--- Generating Signal BSON Object (as dict) ---")
    bson_dict = generate_signal_bson(
        TARGET_SYMBOL, TARGET_TIMEFRAME, TARGET_DIRECTION, sig_id, GENERATOR_ROUTINE_ID,
        opportunity_ts, confirmation_ts, last_candle_start_unix, last_candle_end_unix
    )
    print(json.dumps(bson_dict, indent=2))

    print("\n--- Generation Complete ---")