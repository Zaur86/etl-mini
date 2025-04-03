import logging
from datetime import datetime
from services.pipelines.internal_raw_to_dwh import InternalRawToDWHStandardPipeline
from services.sources.implementations.internal_raw_storage import ElasticSearchService
from services.transformers import TSVConverter
from services.sources.implementations.dwh import PostgreSQLService
from models.queries import ElasticQueryModel
from models.helpers import AdditionalFields
import app.utils as utils
from app.settings import get_settings

logger = logging.getLogger(__name__)


def elasticsearch_to_postgresql(
    env: str,
    index: str,
    start_time: str,
    end_time: str,
    source_fields: list,
    fields_mapping: dict,
    table_name: str,
    nested_key: list,
    filters: dict = None,
    not_null_fields: list = None,
    additional_fields: list = None,
    metadata: dict = None,
    time_format: str = "%Y-%m-%d %H:%M:%S",
    batch_size: int = None,
    scroll: str = None,
    num_processes: int = None,
    debug: bool = False,
    fail_on_missing: bool = False,
):
    """
    Run the ETL pipeline from Elasticsearch to PostgreSQL.

    Args:
        env (str): The environment to use (DEV, TEST, PROD).
        index (str): Elasticsearch index to extract data from.
        start_time (str): Start timestamp for data extraction.
        end_time (str): End timestamp for data extraction.
        source_fields (list): Fields to extract from Elasticsearch.
        fields_mapping (dict): Mapping of source fields to target fields.
        table_name (str): Target table name in PostgreSQL.
        nested_key (list): List of keys to extract nested data.
        filters (dict, optional): Elastic query filters. Defaults to None.
        not_null_fields (list, optional): Fields that must not be null. Defaults to None.
        additional_fields (list, optional): Additional fields configuration. Defaults to None.
        metadata (dict, optional): Metadata configuration for pipeline. Defaults to None.
        time_format (str, optional): Format of the timestamps. Defaults to "%Y-%m-%d %H:%M:%S".
        batch_size (int, optional): Batch size for extraction. Defaults to None (from settings).
        scroll (str, optional): Scroll parameter for Elasticsearch. Defaults to None (from settings).
        num_processes (int, optional): Number of processes for transformation.
            Defaults to None (from settings).
        debug (bool, optional): Debug mode. Defaults to False.
        fail_on_missing (bool, optional): Fail on missing source entity. Defaults to False.

    Returns:
        None
    """
    # Validate environment
    if env not in ["DEV", "TEST", "PROD"]:
        raise ValueError("Invalid environment. Choose from: DEV, TEST, PROD.")

    # Set default mutable arguments
    not_null_fields = not_null_fields or []
    additional_fields = additional_fields or []
    metadata = metadata or {}

    # Validate timestamps
    try:
        datetime.strptime(start_time, time_format)
        datetime.strptime(end_time, time_format)
    except ValueError:
        raise ValueError(f"start_time and end_time must be in format: {time_format}")

    # Validate additional fields
    additional_fields_objects = []
    if additional_fields:
        if not isinstance(additional_fields, list):
            raise ValueError("additional_fields must be a list")
        for field in additional_fields:
            if not isinstance(field, dict):
                raise ValueError("Each entry in additional_fields must be a dictionary")
            if "value" not in field or not isinstance(field["value"], str):
                raise ValueError(
                    "Each entry in additional_fields must have a 'value' key of type string"
                )
            if "output_fields" in field:
                if (not isinstance(field["output_fields"], list) or
                        not all(isinstance(i, str) for i in field["output_fields"])):
                    raise ValueError("output_fields must be a list of strings")
                additional_fields_objects.append(AdditionalFields(
                    value=field["value"],
                    output_fields=field["output_fields"]
                ))
            elif {"input_mapping", "output_mapping"}.issubset(field):
                static_args = field.get("static_args", {})
                if field["value"] in utils.__all__:
                    function_value = getattr(utils, field["value"])
                    additional_fields_objects.append(AdditionalFields(
                        value=function_value,
                        input_mapping=field["input_mapping"],
                        static_args=static_args,
                        output_mapping=field["output_mapping"]
                    ))
                else:
                    raise ValueError(
                        f"Function {field['value']} does not exist in app.utils.__all__"
                    )
            else:
                raise ValueError(
                    "Each entry in additional_fields must have either 'output_fields' or "
                    "all of 'input_mapping', 'output_mapping', and optionally 'static_args'"
                )

    # Load settings based on the environment
    settings = get_settings(env=env)
    batch_size = batch_size or settings.EL_DEFAULT_BATCH_SIZE
    scroll = scroll or settings.EL_DEFAULT_SCROLL
    num_processes = num_processes or settings.NUM_THREADS

    load_metadata = True if metadata else False

    # Configure pipeline
    pipeline = InternalRawToDWHStandardPipeline(
        extractor_class=ElasticSearchService,
        transformer_class=TSVConverter,
        loader_class=PostgreSQLService,
        load_metadata=load_metadata,
        fail_on_missing=fail_on_missing,
    )

    # Set extractor kwargs
    pipeline.set_extractor_kwargs(
        section="init",
        kwargs={
            "host": settings.EL_HOST,
            "port": settings.EL_PORT,
            "use_iam": settings.EL_USE_IAM,
            "aws_profile": settings.EL_AWS_PROFILE,
            "aws_region": settings.EL_AWS_REGION,
            "aws_service": settings.EL_AWS_SERVICE,
        },
    )

    pipeline.set_extractor_kwargs(
        section="check_exists",
        kwargs={
            "index": index,
        },
    )

    pipeline.set_extractor_kwargs(
        section="preparation",
        kwargs={
            "index": index,
            "query_model": ElasticQueryModel(
                start_time=start_time,
                end_time=end_time,
                source_fields=source_fields,
                filters=filters,
            ),
        },
    )

    pipeline.set_extractor_kwargs(
        section="extract",
        kwargs={
            "batch_size": batch_size,
            "scroll": scroll,
        },
    )

    # Set transformer kwargs
    pipeline.set_transformer_kwargs(
        section="init",
        kwargs={
            "fields_mapping": fields_mapping,
            "not_null_fields": not_null_fields,
            "num_processes": num_processes,
            "nested_key": nested_key,
            "debug": debug
        },
    )

    pipeline.set_transformer_kwargs(
        section="preparation",
        kwargs={"additional_fields": additional_fields_objects},
    )

    # Set loader kwargs
    pipeline.set_loader_kwargs(
        section="init",
        kwargs={"db_url": settings.DB_URL},
    )

    pipeline.set_loader_kwargs(
        section="preparation",
        kwargs={"method_name": "_load_from_tsv"},
    )

    pipeline.set_loader_kwargs(
        section="load",
        kwargs={
            "table_name": table_name,
            "source_type": "buffer",
            "source": pipeline.buffer,
        },
    )

    if metadata:
        meta_table_name = metadata.get("table_name")
        if not isinstance(meta_table_name, str):
            raise ValueError("metadata['table_name'] must be a string")

        values = metadata.get("values", [])
        if not isinstance(values, list) or not all(isinstance(v, dict) for v in values):
            raise ValueError("metadata['values'] must be a list of dictionaries")

        current_time_field = metadata.get("current_time_field", None)

        conflict_action = metadata.get("conflict_action", None)
        if conflict_action not in [None, "update", "nothing"]:
            raise ValueError("metadata['conflict_action'] must be 'update', 'nothing', or None")

        conflict_columns = metadata.get("conflict_columns", None)
        if conflict_columns and (not isinstance(conflict_columns, list) or
                                 not all(isinstance(c, str) for c in conflict_columns)):
            raise ValueError("metadata['conflict_columns'] must be a list of strings")

        update_columns = metadata.get("update_columns", None)
        if update_columns and (not isinstance(update_columns, list) or
                               not all(isinstance(c, str) for c in update_columns)):
            raise ValueError("metadata['update_columns'] must be a list of strings")

        dynamic_values = []
        for value_dict in values:
            dynamic_dict = utils.DynamicTimeDict(dynamic_key=current_time_field) if current_time_field else {}
            dynamic_dict.update(value_dict)
            dynamic_values.append(dynamic_dict)

        pipeline.set_loader_kwargs(
            section="preparation_meta",
            kwargs={"method_name": "_load_with_values"},
        )

        pipeline.set_loader_kwargs(
            section="load_meta",
            kwargs={
                "table_name": meta_table_name,
                "values": dynamic_values,
                "conflict_action": conflict_action,
                "conflict_columns": conflict_columns,
                "update_columns": update_columns,
            },
        )

    # Run pipeline
    logger.info("Starting pipeline execution.")
    pipeline.run()
    logger.info("Pipeline execution completed.")
