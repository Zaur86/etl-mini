from dataclasses import dataclass, field
import logging


@dataclass
class ExternalSourceToRawStandardPipeline:
    """
    Standard pipeline for extracting data from an external source and loading it into raw storage.

    This class orchestrates the extraction and loading (EL) of data.
    It relies on user-provided classes for the extractor and loader,
    allowing configuration through keyword arguments for each stage.

    Attributes:
        extractor_class (type): The class responsible for data extraction.
        loader_class (type): The class responsible for loading data into the target system.
        extractor_kwargs (dict): Configuration for initializing the extractor.
        loader_kwargs (dict): Configuration for initializing the loader.
    """
    extractor_class: type
    loader_class: type

    extractor_kwargs: dict = field(init=False, default_factory=dict)
    loader_kwargs: dict = field(init=False, default_factory=dict)

    def __post_init__(self):
        self.logger = logging.getLogger(__name__)

    def set_extractor_kwargs(self, section: str, kwargs: dict):
        """Sets keyword arguments for the extractor under a specific section."""
        self.logger.info(f"Setting extractor kwargs for section '{section}'.")
        self.extractor_kwargs[section] = kwargs

    def set_loader_kwargs(self, section: str, kwargs: dict):
        """Sets keyword arguments for the loader under a specific section."""
        self.logger.info(f"Setting loader kwargs for section '{section}'.")
        self.loader_kwargs[section] = kwargs

    def run(self):
        """Executes the pipeline by extracting and loading data."""
        self.logger.info("Initializing extractor and loader.")
        extractor = self.extractor_class(**self.extractor_kwargs.get("init", {}))
        loader = self.loader_class(**self.loader_kwargs.get("init", {}))

        self.logger.info("Starting data extraction.")
        data = extractor.extract(**self.extractor_kwargs.get("run", {}))

        self.logger.info("Starting data loading.")
        loader.load(data=data, **self.loader_kwargs.get("run", {}))

        self.logger.info("Pipeline execution completed successfully.")
