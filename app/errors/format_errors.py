"""
Contains custom errors related to data formatting issues.
"""

from app.errors import CustomError


class InvalidFormatError(CustomError):
    """Raised when the provided data has an invalid format."""
    pass


class TimezoneMismatchError(CustomError):
    """Raised when a provided time has a different timezone, causing a mismatch."""
    pass
