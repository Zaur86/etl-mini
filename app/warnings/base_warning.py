class CustomWarning(UserWarning):
    """Base class for all custom warnings."""
    def __init__(self, message=None):
        self.message = message or "A warning occurred"
        super().__init__(self.message)
