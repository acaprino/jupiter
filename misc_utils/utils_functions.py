import calendar
import decimal
import math
import os
import time
import uuid
from datetime import timedelta, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Union, Dict, List, Optional

import numpy
import numpy as np
import pandas as pd
import pytz
from nanoid.generate import generate
from tzlocal import get_localzone

from misc_utils.enums import Timeframe


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def dt_to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("The datetime object must be timezone-aware.")
    return dt.astimezone(pytz.UTC)


def dt_to_unix(dt: Optional[datetime]) -> float:
    """
    Convert a datetime object to a Unix timestamp.

    If the datetime is None, returns -1.
    If the datetime is naive (without timezone information), it is interpreted as UTC.
    If the datetime is timezone-aware, it is first converted to UTC before calculating the timestamp.

    Args:
        dt (datetime): The datetime object to convert.

    Returns:
        float: The corresponding Unix timestamp.
    """
    if dt is None:
        return -1
    elif dt.tzinfo is None:
        # Naive datetime: interpreted as UTC.
        return calendar.timegm(dt.timetuple())
    else:
        # Timezone-aware datetime: convert to UTC and obtain the timestamp.
        dt_utc = dt.astimezone(timezone.utc)
        return int(dt_utc.timestamp())


def unix_to_datetime(unix_timestamp: Optional[Union[int, float]]) -> Optional[datetime]:
    """
    Converts a UNIX timestamp (int or float) into a naive UTC datetime object.
    Returns None if the input is None.

    Args:
        unix_timestamp: The UNIX timestamp to convert (can be None).

    Returns:
        A datetime object corresponding to the timestamp, in UTC but without tzinfo (naive),
        or None if the input was None.

    Raises:
        ValueError: If the timestamp is not None and is invalid (e.g., OverflowError, OSError).
    """
    if unix_timestamp is None:
        return None
    try:
        # Convert to UTC aware datetime
        dt_utc_aware = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        # Remove timezone information to make it naive
        return dt_utc_aware.replace(tzinfo=None)
    except (OverflowError, OSError, ValueError) as e:
        raise ValueError(f"Invalid UNIX timestamp: {unix_timestamp}") from e


def get_recent_past_multiple_of_timeframe(timeframe: Timeframe) -> datetime:
    """
    Calculates the most recent past timestamp that is an exact multiple
    of the specified timeframe, relative to the Unix epoch (UTC), and
    returns it as a NAIVE datetime object (tzinfo=None) whose numerical
    values correspond to UTC time.

    Args:
        timeframe: A Timeframe object with a to_seconds() method.

    Returns:
        A naive datetime object (tzinfo=None) representing the start time
        of the most recent timeframe interval, with values corresponding to UTC.

    Raises:
        ValueError: If timeframe.to_seconds() returns a non-positive value.
    """
    timeframe_seconds = timeframe.to_seconds()
    if timeframe_seconds <= 0:
        # It's important to handle this case to avoid division by zero
        raise ValueError("Timeframe duration must be positive.")

    # time.time() returns float seconds since the epoch (UTC)
    current_timestamp_utc = time.time()

    # Floor the timestamp to the nearest lower multiple of timeframe_seconds
    # Convert to int *before* the modulo operation for safety
    current_timestamp_int_utc = int(current_timestamp_utc)
    past_timestamp_utc = current_timestamp_int_utc - (current_timestamp_int_utc % timeframe_seconds)

    # Convert the Unix timestamp (UTC) into a NAIVE datetime object
    # whose values correspond to UTC time.
    # WARNING: This object does not "know" it's UTC.
    return datetime.utcfromtimestamp(past_timestamp_utc)


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
        filename = filename.replace(char, '_')  # Replace invalid characters with underscore
    filename = filename.rstrip('. ')  # Remove dots or spaces from the end of the filename
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
    dec_volume = Decimal(str(volume))
    dec_step = Decimal(str(volume_step))

    # Calculate the number of steps (as a Decimal) and round to the nearest integer.
    steps = dec_volume / dec_step
    steps_rounded = steps.quantize(Decimal('1'), rounding=ROUND_HALF_UP)

    # Multiply by the step again to obtain the rounded volume.
    rounded_volume = steps_rounded * dec_step

    # Return as float (this may introduce a slight precision issue when printing,
    # but the underlying Decimal value is correct).
    return float(rounded_volume)


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

    # Using a list to accumulate parts of the message to reduce string concatenation overhead
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

    # Check for Heikin-Ashi values and add them if present
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
    # Calculate the remaining seconds until the next timeframe boundary
    remainder = timeframe_seconds - (now.timestamp() % timeframe_seconds)

    # Calculate the datetime for the next occurrence
    next_occurrence = now + timedelta(seconds=remainder)

    # Calculate the number of minutes until the next occurrence
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
    # Calculate the number of decimal places to round based on the point value
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
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif hasattr(obj, '__dict__'):
            return convert(vars(obj))
        else:
            return obj

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


def log_config_str(c):
    return f"{c.get_symbol()}.{c.get_timeframe().name}.{c.get_trading_direction().name}"


def new_id(length: int = 8, lower: bool = True, upper: bool = True) -> str:
    """
    Generates a random ID of a specified length.

    Args:
        length: The desired length for the ID (default: 8).
        lower: If True, includes lowercase letters in the alphabet (default: True).
        upper: If True, includes uppercase letters in the alphabet (default: True).

    Returns:
        A randomly generated ID string.

    Raises:
        ValueError: If both 'lower' and 'upper' are False and there are
                    no other characters (like digits) in the base alphabet,
                    resulting in an empty alphabet.
    """
    base_alphabet = "0123456789"
    final_alphabet = base_alphabet

    if upper:
        final_alphabet += "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if lower:
        final_alphabet += "abcdefghijklmnopqrstuvwxyz"

    if not final_alphabet:
        raise ValueError("The alphabet cannot be empty. Enable at least one character set (digits, lower, upper).")

    return str(generate(size=length, alphabet=final_alphabet))
