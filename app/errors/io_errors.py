"""
Contains custom errors related to Input/Output operations.
It includes errors for missing files, invalid paths, and other I/O-related issues.
"""

from app.errors import CustomError


class TemplateNotFoundError(FileNotFoundError, CustomError):
    """Custom error for missing template files."""
    def __init__(self, template_path):
        self.template_path = template_path
        super().__init__(f"Template not found: {self.template_path}")
