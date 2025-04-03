"""
Contains custom warnings related to performance issues.
"""

from app.warnings import CustomWarning


class ExcessiveProcessesWarning(CustomWarning):
    """Warning for excessive number of processes."""

    def __init__(self, num_processes: int, available_cores: int):
        super().__init__(f"Requested {num_processes} processes, but only {available_cores} cores are available.")
