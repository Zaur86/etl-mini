"""
Contains custom errors related to data validation and structure.
"""

from app.errors import CustomError


class MissingFieldError(CustomError):
    """Raised when a required field is missing."""
    pass


class EmptyValueError(CustomError):
    """Raised when an empty value is not allowed."""
    pass


class NestedKeyError(CustomError):
    """Raised when a specified nested key does not exist."""
    pass
