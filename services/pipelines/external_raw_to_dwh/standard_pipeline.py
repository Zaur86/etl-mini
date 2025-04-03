from dataclasses import dataclass, field
import logging


@dataclass
class ExternalRawToDWHStandardPipeline:
    """
    A standard pipeline for processing data from external raw source to a data warehouse (DWH).

    This class orchestrates the extraction, transformation, and loading (ETL) of data
    in batches. It relies on user-provided classes for the extractor, transformer,
    and loader, and allows configuration through keyword arguments for each stage.

    Attributes:
        extractor_class (type): The class responsible for data extraction.
        transformer_class (type): The class responsible for data transformation.
        loader_class (type): The class responsible for loading data into the target system.
        fail_on_missing (bool):
            - If True, raises an error when the required entity (e.g., index, table) is missing in the source.
            - If False, the process stops with a warning instead of an error, and metadata is **not** updated
                (to prevent moving the checkpoint forward).
        extractor_kwargs (dict): Configuration for the extractor.
        transformer_kwargs (dict): Configuration for the transformer.
        loader_kwargs (dict): Configuration for the loader.
    """
    extractor_class: type
    transformer_class: type
    loader_class: type
    fail_on_missing: bool

    extractor_kwargs: dict = field(init=False, default_factory=dict)
    transformer_kwargs: dict = field(init=False, default_factory=dict)
    loader_kwargs: dict = field(init=False, default_factory=dict)

    def __post_init__(self):
        self.logger = logging.getLogger(__name__)

    def set_extractor_kwargs(self, section: str, kwargs: dict):
        """"Set extractor configuration parameters for a specific section."""
        self.logger.info(f"Setting extractor kwargs for section '{section}'.")
        self.extractor_kwargs[section] = kwargs

    def set_transformer_kwargs(self, section: str, kwargs: dict):
        """"Set transformer configuration parameters for a specific section."""
        self.logger.info(f"Setting transformer kwargs for section '{section}'.")
        self.transformer_kwargs[section] = kwargs

    def set_loader_kwargs(self, section: str, kwargs: dict):
        """"Set loader configuration parameters for a specific section."""
        self.logger.info(f"Setting loader kwargs for section '{section}'.")
        self.loader_kwargs[section] = kwargs

    def run(self):
        """Execute the pipeline: extract, transform, and load data in batches."""
        self.logger.info("Starting the pipeline execution.")
        try:
            # Initialize extractor, transformer, and loader instances
            self.logger.info("Initializing extractor, transformer, and loader instances.")
            extractor = self.extractor_class(**self.extractor_kwargs.get('init', {}))
            transformer = self.transformer_class(**self.transformer_kwargs.get('init', {}))
            loader = self.loader_class(**self.loader_kwargs.get('init', {}))

            # Verify source entity existence
            if not extractor.check_source_exists(**self.extractor_kwargs.get('check_exists', {})):
                if self.fail_on_missing:
                    self.logger.error("Source entity does not exist! Process aborted.")
                    raise ValueError()
                else:
                    self.logger.warning("Source entity does not exist! Process halted gracefully.")
                    return

            # Initialize extractor chunks generator
            ext_chunks = extractor.extract(**self.extractor_kwargs.get('extract', {}))

            with loader as load_service:
                self.logger.info("Loader service initialized.")

                rows_loaded = 0

                try:
                    # Process data in chunks
                    for chunk in ext_chunks:
                        self.logger.debug("Processing a new chunk of data.")

                        # Transform data
                        self.logger.debug("Transform data")
                        chunk_transformed = transformer.transform(
                            data=chunk,
                            **self.transformer_kwargs.get('transform', {})
                        )

                        # Prepare loader
                        self.logger.debug("Prepare loader")
                        load_service.prepare_loading(**self.loader_kwargs.get('preparation', {}))
                        self.loader_kwargs.get('load', {})['data'] = chunk_transformed

                        # Load data into the target system
                        self.logger.debug("Loading transformed data into the target system.")
                        load_service.load_data(args=self.loader_kwargs.get('load'))

                        rows_loaded += len(chunk)

                    self.logger.info(f"{rows_loaded} rows successfully loaded.")

                except RuntimeError as e:
                    self.logger.error(f"Pipeline failed: {e}")
                    raise
                except Exception as e:
                    self.logger.error(f"Unexpected error in pipeline execution: {e}")
                    raise

        except Exception as pipeline_error:
            # Handle errors during extraction
            raise Exception(f"Pipeline failed: {pipeline_error}")
