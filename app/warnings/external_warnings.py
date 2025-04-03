"""
Contains custom warnings related to interactions with external services
"""

from app.warnings import CustomWarning


class ScrollClearWarning(CustomWarning):
    """Warning for issues with clearing external service scroll ID."""
    pass


class ConnectionCloseWarning(CustomWarning):
    """Warning for issues with closing connection."""
    pass
