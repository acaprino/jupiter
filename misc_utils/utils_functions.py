import calendar
import decimal
import math
import time
import uuid
from enum import Enum

import numpy
import numpy as np
import pandas as pd
import pytz
import os

from typing import Union, Dict, List
from datetime import timedelta, datetime, timezone
from tzlocal import get_localzone
from misc_utils.enums import Timeframe


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def dt_to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("The datetime object must be timezone-aware.")
    return dt.astimezone(pytz.UTC)


def dt_to_unix(dt: datetime) -> float:
    if dt is None:
        return -1
    elif dt.tzinfo is None:
        # Datetime naive: interpretato come UTC
        return calendar.timegm(dt.timetuple())
    else:
        # Datetime aware: converti a UTC e ottieni il timestamp
        dt_utc = dt.astimezone(timezone.utc)
        return int(dt_utc.timestamp())


def unix_to_datetime(unix_timestamp: Union[int, float]) -> datetime:
    try:
        return datetime.fromtimestamp(unix_timestamp, tz=timezone.utc).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError) as e:
        raise ValueError(f"Timestamp UNIX non valido: {unix_timestamp}") from e


def get_recent_past_multiple_of_timeframe(timeframe):
    timeframe_seconds = timeframe.to_seconds()
    current_timestamp = int(time.time())
    past_timestamp = current_timestamp - (current_timestamp % timeframe_seconds)
    return datetime.fromtimestamp(past_timestamp)


def get_frames_count_in_period(start_datetime, end_datetime, timeframe: Timeframe):
    seconds_per_frame = timeframe.to_seconds()
    duration = end_datetime - start_datetime
    duration_in_seconds = duration.total_seconds()
    frames_count = duration_in_seconds / seconds_per_frame
    return int(frames_count)


def get_start_datetime(end_datetime, timeframe: Timeframe, frames_count):
    seconds_per_frame = timeframe.to_seconds()
    tot_seconds = seconds_per_frame * frames_count
    end_datetime = end_datetime - timedelta(seconds=tot_seconds)
    return end_datetime


def get_end_datetime(backtesting_start, timeframe: Timeframe, candle_index):
    seconds_per_frame = timeframe.to_seconds()
    tot_seconds = seconds_per_frame * candle_index
    end_datetime = backtesting_start + timedelta(seconds=tot_seconds)
    return end_datetime


def string_to_enum(enum_class, value):
    try:
        enum_member = value.upper()
        return enum_class[enum_member]
    except KeyError:
        valid_values = ", ".join([e.name for e in enum_class])
        raise ValueError(f"Invalid value '{value}' for {enum_class.__name__}. Valid values are: {valid_values}")


def utc_to_local(utc_dt):
    utc_dt = utc_dt.replace(tzinfo=pytz.utc)
    local_dt = utc_dt.astimezone(get_localzone())
    return local_dt


def sanitize_filename(filename):
    invalid_chars = r'<>:"/\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')  # Sostituisce i caratteri non validi con underscore
    filename = filename.rstrip('. ')  # Rimuove punti o spazi alla fine del nome file
    return filename


def delete_file(file):
    try:
        os.remove(file)
        return True
    except Exception:
        return False


def is_future_utc(timestamp):
    current_timestamp = now_utc()
    return timestamp > current_timestamp


def round_to_step(volume, volume_step):
    num_steps = volume / volume_step
    rounded_steps = round(num_steps)
    rounded_volume = rounded_steps * volume_step
    return rounded_volume


def create_directories(path):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass


def describe_candle(candle):
    # Early return if candle is None
    if candle is None:
        return "-"

    # Helper function to format values and avoid repetitive access
    def format_value(key, precision=5):
        return f"{candle.get(key, 0):.{precision}f}"

    # Using a list to accumulate parts of the message to reduce concatenation overhead
    log_parts = [
        f"Time Open UTC: {candle.get('time_open', '-')}",
        f"Time Close UTC: {candle.get('time_close', '-')}",
        f"Time Open Broker: {candle.get('time_open_broker', '-')}",
        f"Time Close Broker: {candle.get('time_close_broker', '-')}",
        f"Open: {format_value('open')}",
        f"High: {format_value('high')}",
        f"Low: {format_value('low')}",
        f"Close: {format_value('close')}"
    ]

    # Check for HA values and add them if present
    if all(key in candle for key in ['HA_open', 'HA_close', 'HA_high', 'HA_low']):
        log_parts.extend([
            f"HA Open: {format_value('HA_open')}",
            f"HA Close: {format_value('HA_close')}",
            f"HA High: {format_value('HA_high')}",
            f"HA Low: {format_value('HA_low')}"
        ])

    # Join all parts into the final log message
    return ", ".join(log_parts)


def next_timeframe_occurrence(timeframe: Timeframe):
    timeframe_seconds = timeframe.to_seconds()
    now = datetime.now()
    # Calculate the remainder of seconds until the next timeframe boundary
    remainder = timeframe_seconds - (now.timestamp() % timeframe_seconds)

    # Calculate the datetime for the next occurrence
    next_occurrence = now + timedelta(seconds=remainder)

    # Calculate the minutes until the next occurrence
    minutes_until_next = remainder // 60

    return next_occurrence, int(minutes_until_next)


def calc_timestamp_diff_hours(tms1, tms2):
    unix_tms1 = int(tms1.timestamp())
    unix_tms2 = int(tms2.timestamp())

    # Calculate the difference in seconds
    time_diff_seconds = abs(unix_tms2 - unix_tms1)

    # Convert the difference to hours, rounding up to the nearest hour
    offset_hours = math.ceil(time_diff_seconds / 3600)

    return offset_hours


def round_to_point(value, point) -> np.ndarray or float:
    # Calculate the number of decimal places to round based on point
    num_decimal_places = abs(int(math.log10(point)))

    # If the input is a Pandas Series or numpy array, apply rounding to each element
    if isinstance(value, pd.Series) or isinstance(value, np.ndarray):
        return value.round(decimals=num_decimal_places)
    else:  # Otherwise, assume it's a single number and round it directly
        return round(value, num_decimal_places)


def to_serializable(element):
    def convert(obj):
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert(v) for v in obj]
        elif isinstance(obj, tuple):
            return tuple(convert(v) for v in obj)
        elif isinstance(obj, pd.Timestamp):
            return dt_to_unix(obj)
        elif isinstance(obj, datetime):
            return dt_to_unix(obj)
        elif isinstance(obj, pd.Series):
            return convert(obj.to_dict())
        elif isinstance(obj, Enum):
            return obj.name
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        else:
            return obj

    if isinstance(element, dict):
        return convert(element)
    elif hasattr(element, '__dict__'):
        return convert(vars(element))
    else:
        return convert(element)

def extract_properties(instance, properties: List[str]) -> Dict[str, any]:
    """
    Extracts specified properties from a class instance and returns them as a dictionary.

    :param instance: The class instance to extract properties from.
    :param properties: A list of property names to extract.
    :return: A dictionary with property names as keys and their corresponding values.
    """
    result = {}
    for prop in properties:
        if hasattr(instance, prop):
            result[prop] = getattr(instance, prop)
        else:
            raise AttributeError(f"The property '{prop}' does not exist in the instance.")
    return result
