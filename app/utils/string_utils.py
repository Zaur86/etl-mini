import re
from typing import List, Union


def extract_placeholders(data: Union[dict, list, str]) -> List[str]:
    """
    Recursively searches for placeholders in the format {SOME_KEY}
    in all strings within a dictionary or list.
    :param data: Data structure (dictionary, list, or string)
    :return: List of found placeholders
    """
    placeholders = []

    if isinstance(data, dict):
        for key, value in data.items():
            placeholders.extend(extract_placeholders(key))   # Search in dictionary keys
            placeholders.extend(extract_placeholders(value))  # Search in dictionary values

    elif isinstance(data, list):
        for item in data:
            placeholders.extend(extract_placeholders(item))  # Search in list elements

    elif isinstance(data, str):
        placeholders.extend(re.findall(r"{(.*?)}", data))  # Extract placeholders from strings

    return placeholders
