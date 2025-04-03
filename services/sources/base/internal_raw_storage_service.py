from abc import ABC, abstractmethod


class InternalRawStorageService(ABC):
    """
    Base class for working with internal raw data storage.
    """

    @abstractmethod
    def prepare_extraction(self, *args, **kwargs):
        """
        Method for preparing data extraction.
        Must be implemented in subclasses.
        """
        pass

    @abstractmethod
    def extract_data(self, *args, **kwargs):
        """
        Method for extracting data.
        Must be implemented in subclasses.
        """
        pass

    @abstractmethod
    def check_source_exists(self, *args, **kwargs):
        """
        Method for checking the existence of a source.
        Must be implemented in subclasses.
        """
        pass
