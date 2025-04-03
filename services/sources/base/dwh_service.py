from abc import ABC, abstractmethod


class DWHService(ABC):
    """
    Base class for working with DWH.
    """

    @abstractmethod
    def load_data(self, *args, **kwargs):
        """
        Load data.
        """
        pass
