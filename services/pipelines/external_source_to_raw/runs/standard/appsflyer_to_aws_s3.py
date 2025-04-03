import logging
from datetime import datetime
from services.pipelines.external_source_to_raw.standard_pipeline import ExternalSourceToRawStandardPipeline
from services.sources.implementations.external_source.simple_api_service import SimpleAPIService
from services.sources.implementations.external_raw_storage.s3_service import S3Service
from app.settings import get_settings
import yaml

logger = logging.getLogger(__name__)


def appsflyer_to_s3(
    env: str,
    date: str,
    template_key: str,
    template_params: dict,
    s3_bucket: str,
    s3_source_type: str,
    s3_data_type: str,
    processing_type: str
):
    """
    Runs the pipeline to extract data from Appsflyer and load it into S3.

    Args:
        env (str): Environment (DEV, TEST, PROD).
        date (str): The date for which data is extracted in YYYY-MM-DD format.
        template_key (str): Key to retrieve the API request template from api_templates.yaml.
        template_params (dict): Dictionary with variable values to construct the request.
        s3_bucket (str): S3 bucket where the data will be stored.
        s3_source_type (str): Source defining the top-level directory in the S3 bucket.
        s3_data_type (str): Data type sub-directory in S3 storage.
        processing_type (str): Processing method applied before storing data in S3.
    """
    # Validate environment
    if env not in ["DEV", "TEST", "PROD"]:
        raise ValueError("Invalid environment. Choose from: DEV, TEST, PROD.")

    # Validate date format
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise ValueError("date must be in YYYY-MM-DD format")

    # Load settings based on environment
    settings = get_settings(env=env)

    # Load api token
    with open("config/secrets/api_tokens.yaml", "r") as f:
        template_params["AF_TOKEN"] = yaml.load(f, Loader=yaml.FullLoader)['appsflyer']['token']

    # Construct API request parameters
    template_params["API_VERSION"] = settings.AF_API_VERSION
    template_params["DATE"] = date

    # Create and configure the standard pipeline
    pipeline = ExternalSourceToRawStandardPipeline(
        extractor_class=SimpleAPIService,
        loader_class=S3Service,
    )

    pipeline.set_extractor_kwargs(
        section="init",
        kwargs={
            "template_key": template_key,
            "params": template_params
        }
    )
    pipeline.set_loader_kwargs(
        section="init",
        kwargs={
            "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
            "aws_region": settings.AWS_DEFAULT_REGION,
            "bucket": s3_bucket
        }
    )

    pipeline.set_loader_kwargs(
        section="run",
        kwargs={
            "source_type": s3_source_type,
            "data_type": s3_data_type,
            "path_suffix": date,
            "processing_type": processing_type
        }
    )

    # Run the pipeline
    logger.info("Starting Appsflyer to S3 pipeline.")
    pipeline.run()
    logger.info("Pipeline execution completed.")
