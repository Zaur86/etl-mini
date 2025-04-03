import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import List, Dict


def get_nested_value(data: Dict, keys: List[str], default=None):
    """Retrieves a nested value from a dictionary given a list of keys."""
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data


def parse_json_lines(
    file_path: str,
    type_path: List[str],
    timestamp_path: List[str],
    date_format: str = "%Y-%m-%d",
    allow_missing_timestamp: bool = False
):
    """
    Reads a JSON Lines (NDJSON) file, groups data by a nested 'type' and 'timestamp' field,
    and returns a structure: {TYPE: {DATE: [records]}}

    :param file_path: Path to the JSON Lines file.
    :param type_path: List of keys defining the path to the 'type' field.
    :param timestamp_path: List of keys defining the path to the 'timestamp' field.
    :param date_format: Expected date format for grouping (default: "%Y-%m-%d").
    :param allow_missing_timestamp: If True, log a warning and skip missing timestamps; if False, raise an error.
    :return: Dictionary structured as {TYPE: {DATE: [records]}}.
    """
    grouped_data = defaultdict(lambda: defaultdict(list))

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line)
                event_type = get_nested_value(event, type_path, "unknown")
                timestamp = get_nested_value(event, timestamp_path)

                if not timestamp:
                    if allow_missing_timestamp:
                        logging.warning(f"Missing timestamp, skipping record: {event}")
                        continue
                    else:
                        raise ValueError(f"Missing timestamp in record: {event}")

                event_date = datetime.fromisoformat(timestamp).strftime(date_format)
                grouped_data[event_type][event_date].append(event)

            except json.JSONDecodeError as e:
                logging.error(f"JSON decoding error: {e}")
            except ValueError as e:
                logging.error(e)

    return grouped_data


def validate_json_structure(json_obj, required_fields):
    """Checks if a JSON object contains the required fields."""
    return all(field in json_obj for field in required_fields)


def save_json_to_file(data, file_path):
    """Saves a JSON object to a file."""
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


def load_json_from_file(file_path):
    """Loads a JSON object from a file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)
