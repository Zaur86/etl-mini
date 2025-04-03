"""
This package contains custom error classes for the application.

It provides a structured way to handle various types of errors, such as:
- Data validation errors
- Database operation errors
- External service interaction errors
- Formatting and parameter validation errors

All error classes can be imported from this module for ease of use.

Example:
    from app.errors import MissingFieldError, DatabaseError
"""
from .base_error import CustomError
from .data_errors import MissingFieldError, EmptyValueError, NestedKeyError
from .format_errors import InvalidFormatError, TimezoneMismatchError
from .validation_errors import InvalidParameterValueError, MethodNotSetError
from .external_errors import ElasticSearchError
from .database_errors import DatabaseError, DatabaseConnectionError
from .io_errors import TemplateNotFoundError
