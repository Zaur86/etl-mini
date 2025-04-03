"""
Contains custom errors related to parameter validation.
"""

from app.errors import CustomError


class InvalidParameterValueError(CustomError):
    """Raised when a parameter has an invalid value."""
    pass


class MethodNotSetError(CustomError):
    """Raised when no valid method is set for an operation."""
    pass
