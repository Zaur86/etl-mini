class Transformer:
    """
    Base class for transformers.
    """

    def prepare_transformation(self, *args, **kwargs):
        """
        Prepares for data transformation.
        Must be implemented in subclasses.
        """
        raise NotImplementedError("The 'prepare_transformation' method must be implemented in a subclass")

    def transform(self, *args, **kwargs):
        """Abstract method for transforming data."""
        raise NotImplementedError("Subclasses must implement this method.")
