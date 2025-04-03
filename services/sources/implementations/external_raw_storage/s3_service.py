from services.sources.base.external_raw_storage_service import ExternalRawStorageService
from models.mappings import type_maps, rename_maps
from dataclasses import dataclass, field
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError
import pandas as pd
import boto3
import time
import csv
import json
import io


@dataclass
class S3Service(ExternalRawStorageService):
    """
    Service for interacting with AWS S3 as a raw data storage.
    Supports structured storage per source type and data type.
    """
    s3_client: boto3.client = field(init=False)
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_region: str
    bucket: str

    def __post_init__(self):
        """
        Initializes the S3 client using provided credentials.
        Checks if the connection to the bucket is successful.
        """
        try:
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            # Check if the bucket exists
            self.s3_client.head_bucket(Bucket=self.bucket)
        except (BotoCoreError, NoCredentialsError, ClientError) as e:
            raise ConnectionError(f"Failed to connect to S3 bucket '{self.bucket}': {str(e)}")
        except Exception as e:
            raise ConnectionError(f"Unexpected error while connecting to S3 bucket '{self.bucket}': {str(e)}")

    def load(self, source_type, data_type, path_suffix, data, processing_type):
        """
        Uploads data to an S3 bucket with a structured key format.
        If a processing type is provided, the data is processed accordingly before uploading.
        Checks if source_type and data_type directories exist before uploading.
        """
        source_type_prefix = f"{source_type}/"
        data_type_prefix = f"{source_type}/{data_type}/"
        if not self._folder_exists(source_type_prefix):
            raise FileNotFoundError(f"The folder '{source_type}' does not exist in the S3 bucket.")
        if not self._folder_exists(data_type_prefix):
            raise FileNotFoundError(f"The folder '{source_type}/{data_type}' does not exist in the S3 bucket.")
        data, file_format = self.process_data(data, processing_type)
        timestamp = int(time.time())
        key = f"{source_type}/{data_type}/{path_suffix}/{timestamp}.{file_format}"
        self.s3_client.put_object(Bucket=self.bucket, Key=key, Body=data)
        latest_key = f"{source_type}/{data_type}/{path_suffix}/latest.{file_format}"
        self.s3_client.copy_object(Bucket=self.bucket, CopySource={"Bucket": self.bucket, "Key": key}, Key=latest_key)

    def extract(
        self, source_type, data_type, path_suffix, extracting_type, file_format, file_name="latest", **kwargs
    ):
        """
        Entry point to extract structured data from S3 based on extracting_type.
        Uses internal extract_data method.
        """
        source_type_prefix = f"{source_type}/"
        data_type_prefix = f"{source_type}/{data_type}/"
        if not self._folder_exists(source_type_prefix):
            raise FileNotFoundError(f"The folder '{source_type}' does not exist in the S3 bucket.")
        if not self._folder_exists(data_type_prefix):
            raise FileNotFoundError(f"The folder '{source_type}/{data_type}' does not exist in the S3 bucket.")
        key = f"{source_type}/{data_type}/{path_suffix}/{file_name}.{file_format}"
        return self.extract_data(key, extracting_type, **kwargs)

    def check_source_exists(
        self, source_type: str, data_type: str, path_suffix: str, file_format: str, file_name: str = "latest"
    ) -> bool:
        """
        Checks if a specific object (key) exists in the S3 bucket.
        """
        key = f"{source_type}/{data_type}/{path_suffix}/{file_name}.{file_format}"
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise  # Propagate other errors (e.g. permissions, connectivity)

    def _folder_exists(self, prefix):
        """
        Checks if a folder (prefix) exists in the S3 bucket.
        """
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
        return 'Contents' in response

    def close(self):
        """
        Closes the S3 client session.
        """
        try:
            self.s3_client.close()
        except Exception as e:
            print(f"Error while closing S3 client: {str(e)}")

    def __del__(self):
        self.close()

    def process_data(self, data, processing_type):
        """
        Processes data based on the specified template.
        """
        if processing_type == "csv_binary":
            return self._process_csv_binary(data), "csv"
        elif processing_type == "NDJSON":
            return self._process_ndjson(data), "ndjson"
        else:
            raise ValueError(f"Unsupported processing type: {processing_type}")

    def extract_data(self, s3_key: str, extracting_type: str, **kwargs):
        """
        Dispatch method that calls appropriate extract method based on extracting_type.
        """
        if extracting_type == "csv_stream":
            return self._extract_csv_in_chunks(s3_key, **kwargs)
        else:
            raise ValueError(f"Unsupported extracting type: {extracting_type}")

    @staticmethod
    def _process_csv_binary(raw_data):
        """
        Processes CSV data in binary format by removing BOM and ensuring correct encoding.
        """
        decoded_data = raw_data.decode("utf-8-sig")  # Remove BOM if present
        reader = csv.reader(io.StringIO(decoded_data))
        processed_rows = [row for row in reader]
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerows(processed_rows)
        return output.getvalue().encode("utf-8")

    @staticmethod
    def _process_ndjson(data_list):
        """
        Processes a list of dictionaries into NDJSON format (JSON Lines).
        """
        if not isinstance(data_list, list) or not all(isinstance(item, dict) for item in data_list):
            raise ValueError("NDJSON processing requires a list of dictionaries.")
        return "\n".join(json.dumps(item) for item in data_list).encode("utf-8")

    def _extract_csv_in_chunks(self, s3_key: str, rename_map_key=None, type_map_key=None, chunk_size=10000):
        """
        Streams CSV from S3 in chunks and yields transformed DataFrames.
        - rename_map: dict[old_column] = new_column
        - type_map: dict[column] = type (e.g. str, float, "datetime64[ns]")
        """
        obj = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
        body = obj["Body"]

        for chunk in pd.read_csv(body, chunksize=chunk_size):
            if rename_map_key:
                if rename_map_key not in rename_maps:
                    raise ValueError(f"Rename map for key '{rename_map_key}' not found in rename_maps")
                rename_map = rename_maps[rename_map_key]
                chunk.rename(columns=rename_map, inplace=True)

            if type_map_key:
                if type_map_key not in type_maps:
                    raise ValueError(f"Type map for key '{type_map_key}' not found in type_maps")
                type_map = type_maps[type_map_key]
                for col, dtype in type_map.items():
                    if col in chunk.columns:
                        if "datetime" in str(dtype):
                            chunk[col] = pd.to_datetime(chunk[col], errors="coerce")
                        elif dtype == str:
                            chunk[col] = chunk[col].where(pd.notnull(chunk[col]), None)
                        else:
                            chunk[col] = chunk[col].astype(dtype, errors="ignore")

            yield chunk
