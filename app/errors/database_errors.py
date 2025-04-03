"""
Contains custom errors related to database operations.
"""

from app.errors import CustomError


class DatabaseError(CustomError):
    """Base class for custom database errors."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when a connection to the database fails."""
    pass
