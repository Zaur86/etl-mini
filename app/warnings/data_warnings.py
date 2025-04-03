"""
Contains custom warnings related to data size or structure.
"""

from app.warnings import CustomWarning


class JsonLengthWarning(CustomWarning):
    """Warning for excessively long JSON strings."""

    def __init__(self, length: int, max_length: int):
        super().__init__(f"JSON string length {length} exceeds the maximum allowed length of {max_length} characters.")
