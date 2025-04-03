import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
from dotenv import load_dotenv
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)


@dataclass
class S3ConnectionManager:
    """
    Manages connection to AWS S3 using credentials stored in environment variables.
    """
    access_key: str
    secret_key: str
    region: str
    s3_client: boto3.client = None

    def __post_init__(self):
        load_dotenv()
        if not self.access_key or not self.secret_key or not self.region:
            raise ValueError("AWS credentials or region are not properly set in the environment.")

        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region
            )
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise ConnectionError(f"Failed to connect to S3: {e}")

    def list_buckets(self):
        """List all S3 buckets."""
        try:
            response = self.s3_client.list_buckets()
            return [bucket['Name'] for bucket in response['Buckets']]
        except Exception as e:
            raise IOError(f"Failed to list buckets: {e}")


@dataclass
class S3BucketManager:
    """
    Manages operations within a specific S3 bucket.
    """
    connection_manager: S3ConnectionManager
    bucket_name: str

    def __post_init__(self):
        self.s3_client = self.connection_manager.s3_client
        self.validate_bucket()

    def validate_bucket(self):
        """Check if the bucket exists and raise an error if it does not."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                raise ValueError(f"Bucket '{self.bucket_name}' does not exist.")
            else:
                raise ConnectionError(f"Error checking bucket '{self.bucket_name}': {e}")

    def get_bucket_name(self):
        """Returns the name of the connected bucket."""
        return self.bucket_name

    def list_directories(self, directory):
        """List directories in a specific S3 directory."""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{directory}/", Delimiter='/')
        return [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]

    def list_files(self, directory):
        """List files in a specific S3 directory."""
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{directory}/")
        return [obj['Key'] for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]

    def create_directory(self, directory):
        """Create a new directory in the specified bucket."""
        self.s3_client.put_object(Bucket=self.bucket_name, Key=f"{directory}/")
        logging.info(f"Directory '{directory}' created successfully.")

    def delete_directory(self, directory):
        """Delete a directory and its contents from the specified bucket."""
        objects_to_delete = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=f"{directory}/")
        if 'Contents' in objects_to_delete:
            delete_objects = [{'Key': obj['Key']} for obj in objects_to_delete['Contents']]
            self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={'Objects': delete_objects})
            logging.info(f"Directory '{directory}' and its contents deleted successfully.")
        else:
            logging.info(f"Directory '{directory}' does not exist or is already empty.")

    def upload_file(self, file_path, s3_key):
        """Upload a file to a specific S3 location."""
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logging.info(f"File '{file_path}' uploaded to '{s3_key}'.")
        except Exception as e:
            raise IOError(f"Failed to upload file: {e}")

    def delete_file(self, s3_key):
        """Delete a file from a specific S3 location."""
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            logging.info(f"File '{s3_key}' deleted successfully.")
        except Exception as e:
            raise IOError(f"Failed to delete file: {e}")

    def download_file(self, s3_key, download_path):
        """Download a file from a specific S3 location."""
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, download_path)
            logging.info(f"File '{s3_key}' downloaded to '{download_path}'.")
        except Exception as e:
            raise IOError(f"Failed to download file: {e}")

# Example usage:
# connection_manager = S3ConnectionManager(
#   access_key=my_access_key,
#    secret_key=my_secret_key,
#    region=my_region
# )
# print(connection_manager.list_buckets())
# bucket_manager = S3BucketManager(connection_manager, 'my-bucket')
# logging.info(bucket_manager.get_bucket_name())
# bucket_manager.create_directory('my-directory/my-subdirectory')
# print(bucket_manager.list_directories('my-directory'))
# print(bucket_manager.list_files('my-directory/my-subdirectory'))
# bucket_manager.download_file('my-directory/my-subdirectory/my-file.txt', 'local-path/my-file.txt')
