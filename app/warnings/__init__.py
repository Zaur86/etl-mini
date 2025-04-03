"""
This package contains custom warning classes for the application.

It provides a structured way to issue warnings in different contexts, such as:
- Performance-related warnings
- Data structure and size warnings
- Time and timezone-related warnings

All warning classes can be imported from this module for ease of use.

Example:
    from app.warnings import ExcessiveProcessesWarning, TimezoneWarning
"""
from .base_warning import CustomWarning
from .performance_warnings import ExcessiveProcessesWarning
from .data_warnings import JsonLengthWarning
from .time_warnings import TimezoneWarning
from .external_warnings import ScrollClearWarning, ConnectionCloseWarning
