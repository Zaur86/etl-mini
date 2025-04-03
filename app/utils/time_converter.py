from datetime import datetime, timedelta
from typing import Dict
from app.errors import EmptyValueError, InvalidFormatError, TimezoneMismatchError, InvalidParameterValueError
from app.warnings import TimezoneWarning


def normalize_iso_time(iso_time: str, max_fraction_length: int = 9) -> str:
    """
    Normalizes an ISO 8601 time string to ensure 6 digits after the decimal point
    with rounding when necessary.

    Args:
        iso_time (str): The ISO 8601 formatted time string to normalize.
        max_fraction_length (int): Maximum allowed length for the fractional part of a second.

    Returns:
        str: A normalized ISO 8601 time string with a fractional part rounded to 6 digits.

    Raises:
        InvalidFormatError: If the fractional part exceeds the maximum allowed length.
    """
    date_part, time_part = iso_time.split('T')
    if '.' in time_part:
        main_time, fraction_and_tz = time_part.split('.', maxsplit=1)
        fraction = fraction_and_tz.rstrip('Z').split('+')[0].split('-')[0]

        # Validate fractional length
        if len(fraction) > max_fraction_length:
            raise InvalidFormatError(
                f"The fractional part is too long: {len(fraction)} digits (max {max_fraction_length})."
            )

        # Normalize to 6 digits
        fraction_normalized = str(
            round(int(fraction.ljust(max_fraction_length, '0')) / 10 ** (max_fraction_length - 6))).zfill(6)
        if fraction_normalized == "1000000":
            fraction_normalized = "999999"
        normalized_time = f"{date_part}T{main_time}.{fraction_normalized}{fraction_and_tz[len(fraction):]}"
    elif int(time_part.split(":")[-1]) == 0:
        normalized_time = iso_time
    else:
        normalized_time = f"{iso_time[:26]}000000{iso_time[26:]}"  # Add ".000000" if missing fractional part
    return normalized_time


def iso_to_dict(
        iso_time: str,
        expected_timezone: int = 0,
        handle_timezone: str = 'error',  # 'error', 'warning', or 'ignore'
        allow_empty: bool = False,
        datetime_key: str = "datetime_key",  # Customizable key for datetime
        microseconds_key: str = "microseconds_key",  # Customizable key for microseconds
        month_key: str = "months_key",  # Customizable key for month
        max_fraction_length: int = 9  # Maximum length for the fractional part of a second
) -> Dict[str, str]:
    """
    Converts an ISO 8601 formatted time string into a dictionary with two customizable components:
    1. The datetime in "YYYY-MM-DD HH:MM:SS" format (default key: `date_time_`).
    2. The microseconds, adjusted to 6 digits (default key: `time_mcs_`).

    Args:
        iso_time (str): The ISO 8601 formatted time string to be converted.
        expected_timezone (int): The expected timezone offset in hours (e.g., 0, -3, 2).
        handle_timezone (str): How to handle timezone mismatches.
                               Options: 'error', 'warning', 'ignore'.
        allow_empty (bool): Whether empty strings are allowed. If False, raises an EmptyValueError.
        datetime_key (str): Key name for the datetime component in the result dictionary.
        microseconds_key (str): Key name for the microseconds component in the result dictionary.
        month_key (str): Key name for the month component in the result dictionary.
        max_fraction_length (int): Maximum length for the fractional part of a second.

    Returns:
        Dict[str, str]: A dictionary containing customizable keys for `datetime_` and `time_mcs_`.

    Raises:
        EmptyValueError: If `iso_time` is empty and `allow_empty` is False.
        InvalidFormatError: If `iso_time` is not in a valid ISO 8601 format or fractional part exceeds the limit.
        TimezoneMismatchError: If timezone mismatch is found and `handle_timezone` is 'error'.
    """
    if not iso_time:
        if not allow_empty:
            raise EmptyValueError("Empty values are not allowed.")
        return {datetime_key: None, microseconds_key: None}

    # Validate handle_timezone parameter
    if handle_timezone not in {'error', 'warning', 'ignore'}:
        raise InvalidParameterValueError(
            f"Invalid value for handle_timezone: '{handle_timezone}'. "
            "Expected one of: 'error', 'warning', 'ignore'."
        )

    try:
        # Check minimum length
        if len(iso_time) < 20:
            raise InvalidFormatError("The ISO time string is too short to be valid.")

        # Check if normalization is needed
        if len(iso_time) < 26 or iso_time[19] != '.' or (iso_time[26] != '+' and iso_time[26] != 'Z'):
            normalized_iso_time = normalize_iso_time(iso_time, max_fraction_length=max_fraction_length)
        else:
            normalized_iso_time = iso_time

        # Parse the normalized ISO time
        dt = datetime.fromisoformat(normalized_iso_time)
        actual_timezone = dt.utcoffset().total_seconds() // 3600 if dt.utcoffset() else 0

        # Handle timezone mismatches
        if actual_timezone != expected_timezone:
            if handle_timezone == 'error':
                raise TimezoneMismatchError(
                    f"Timezone mismatch: expected {expected_timezone}, got {int(actual_timezone)}."
                )
            elif handle_timezone == 'warning':
                import warnings
                warnings.warn(
                    f"Timezone mismatch: expected {expected_timezone}, got {int(actual_timezone)}.",
                    TimezoneWarning
                )
            # Adjust to expected timezone
            dt += timedelta(hours=(expected_timezone - actual_timezone))

        # Extract microseconds
        time_mcs_ = dt.microsecond

        return {
            datetime_key: dt.strftime("%Y-%m-%d %H:%M:%S"),
            microseconds_key: time_mcs_,
            month_key: dt.strftime("%Y-%m-01")
        }
    except ValueError:
        raise InvalidFormatError(f"Invalid time format: {iso_time}")
