import logging
from services.pipelines.external_raw_to_dwh import ExternalRawToDWHStandardPipeline
from services.sources.implementations.external_raw_storage import S3Service
from services.transformers import PandasSelectAndEnrichTransformer
from services.sources.implementations.dwh import PostgreSQLService
from app.settings import get_settings

logger = logging.getLogger(__name__)


def aws_s3_to_postgresql(
    env: str,
    s3_bucket: str,
    source_type: str,
    data_type: str,
    path_suffix: str,
    file_format: str,
    extracting_type: str,
    table_name: str,
    file_name: str = "latest",
    constants: dict = None,
    columns: list = None,
    require_all_columns: bool = False,
    dedup_by: list = None,
    order_by: list = None,
    conflict_action: str = None,
    conflict_columns: list = None,
    update_columns: list = None,
    extracting_args: dict = None,
    fail_on_missing: bool = False,
):
    """
    Run the ETL pipeline from AWS S3 to PostgreSQL.

    Args:
        env (str): Environment name (e.g. "DEV", "TEST", "PROD").
        s3_bucket (str): S3 bucket where the data will be stored.
        source_type (str): Top-level folder in the S3 bucket.
        data_type (str): Second-level folder inside the S3 path.
        path_suffix (str): Additional path after source_type/data_type, such as date or file partition.
        file_format (str): Format of the file in S3 (e.g. "csv", "json", "parquet").
        extracting_type (str): Type of extraction.
        table_name (str): Target PostgreSQL table name.
        file_name (str, optional): Specific file name in S3 (default is "latest").
        constants (dict, optional): Dictionary of constant values to inject into the dataset.
        columns (list, optional): List of columns to load. If None, all columns are loaded.
        require_all_columns (bool, optional):
            Whether to raise an error if not all specified columns exist in the DataFrame.
        dedup_by (list, optional): Columns to use for deduplication.
        order_by (list, optional): Columns to use for ordering when deduplicating.
        conflict_action (str, optional): Action on conflict ("update", "nothing" or None.).
        conflict_columns (list, optional): List of columns to check for conflicts (used in ON CONFLICT).
        update_columns (list, optional): List of columns to update in case of conflict (used in DO UPDATE).
        extracting_args (dict, optional): Additional arguments to pass to the extractor.
        fail_on_missing (bool, optional): If True, raises error when source file is missing.

    Returns:
        None
    """
    # Validate environment
    if env not in ["DEV", "TEST", "PROD"]:
        raise ValueError("Invalid environment. Choose from: DEV, TEST, PROD.")

    # Set default mutable arguments
    constants = constants or {}

    # Load settings based on the environment
    settings = get_settings(env=env)

    # Configure pipeline
    pipeline = ExternalRawToDWHStandardPipeline(
        extractor_class=S3Service,
        transformer_class=PandasSelectAndEnrichTransformer,
        loader_class=PostgreSQLService,
        fail_on_missing=fail_on_missing,
    )

    # Set extractor kwargs
    pipeline.set_extractor_kwargs(
        section="init",
        kwargs={
            "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
            "aws_region": settings.AWS_DEFAULT_REGION,
            "bucket": s3_bucket,
        },
    )

    pipeline.set_extractor_kwargs(
        section="check_exists",
        kwargs={
            "source_type": source_type,
            "data_type": data_type,
            "path_suffix": path_suffix,
            "file_name": file_name,
            "file_format": file_format,
        },
    )

    pipeline.set_extractor_kwargs(
        section="extract",
        kwargs={
            "source_type": source_type,
            "data_type": data_type,
            "path_suffix": path_suffix,
            "file_name": file_name,
            "file_format": file_format,
            "extracting_type": extracting_type,
            **(extracting_args or {})
        },
    )

    # Set transformer kwargs
    pipeline.set_transformer_kwargs(
        section="init",
        kwargs={
            "constants": constants,
            "columns": columns,
            "dedup_by": dedup_by,
            "order_by": order_by,
            "require_all_columns": require_all_columns,
        },
    )

    # Set loader kwargs
    pipeline.set_loader_kwargs(
        section="init",
        kwargs={"db_url": settings.DB_URL},
    )

    pipeline.set_loader_kwargs(
        section="preparation",
        kwargs={"method_name": "_load_from_pandas_df"},
    )

    pipeline.set_loader_kwargs(
        section="load",
        kwargs={
            "table_name": table_name,
            "conflict_action": conflict_action,
            "conflict_columns": conflict_columns,
            "update_columns": update_columns,
        },
    )

    # Run pipeline
    logger.info("Starting pipeline execution.")
    pipeline.run()
    logger.info("Pipeline execution completed.")
