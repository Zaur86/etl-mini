from dataclasses import dataclass, field
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch.exceptions import ConnectionError, TransportError, ConnectionTimeout
from requests_aws4auth import AWS4Auth
from services.sources.base import InternalRawStorageService
from models.queries import ElasticQueryModel
from app.errors import ElasticSearchError
from app.warnings import ScrollClearWarning, ConnectionCloseWarning
import boto3
import logging
import warnings


@dataclass
class ElasticSearchService(InternalRawStorageService):
    """
    Elasticsearch implementation for internal raw data storage.
    Provides methods for retrieving data in batches using the scroll API.
    """
    host: str
    port: int
    use_iam: bool = False
    aws_profile: str = None
    aws_region: str = None
    aws_service: str = 'es'
    client: Elasticsearch = field(init=False, default=None)
    index: str = field(init=False, default=None)
    query: dict = field(init=False, default=None)
    scroll_id: str = field(init=False, default=None)

    def connect(self):
        """
        Establishes a connection to Elasticsearch.
        """
        if self.use_iam:
            session = boto3.Session(profile_name=self.aws_profile, region_name=self.aws_region)
            credentials = session.get_credentials()
            awsauth = AWS4Auth(
                credentials.access_key,
                credentials.secret_key,
                self.aws_region,
                self.aws_service
            )
            self.client = Elasticsearch(
                hosts=[{'host': self.host, 'port': self.port}],
                http_auth=awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )
        else:
            self.client = Elasticsearch(hosts=[{'host': self.host, 'port': self.port}], use_ssl=True)

        logging.info("Elasticsearch connection established.")

    def prepare_extraction(self, index: str, query_model: ElasticQueryModel):
        """
        Prepares an Elasticsearch index and query based on the provided query model.
        """
        self.index = index
        self.query = query_model.build_query()

    def extract_data(self, batch_size: int, scroll: str):
        """
        Extracts data from Elasticsearch in batches using the scroll API.
        """
        if not self.client:
            raise ElasticSearchError("Elasticsearch client is not connected. Call `connect` first.")

        logging.debug(f"Extracting data from index '{self.index}' with batch size {batch_size}")

        try:
            response = self.client.search(index=self.index, body=self.query, scroll=scroll, size=batch_size)
            self.scroll_id = response.get('_scroll_id')
            hits = response.get('hits', {}).get('hits', [])
            if hits:
                logging.debug(f"First row from one hit: {hits[0]}")

            while hits:
                yield hits
                response = self.client.scroll(scroll_id=self.scroll_id, scroll=scroll)
                self.scroll_id = response.get('_scroll_id')
                hits = response.get('hits', {}).get('hits', [])
        except (ConnectionError, ConnectionTimeout, TransportError) as e:
            raise ElasticSearchError(f"Elasticsearch connection failed: {e}")
        except Exception as e:
            raise ElasticSearchError(f"Failed to scroll Elasticsearch data: {str(e)}")

    def check_source_exists(self, index: str):
        """
        Checks if the source entity exists.
        """
        if self.client.indices.exists(index=index):
            return True
        else:
            return False

    def clear_scroll(self):
        """
        Clears the scroll context to free server resources.
        """
        if self.scroll_id:
            try:
                self.client.clear_scroll(scroll_id=self.scroll_id)
                logging.debug(f"Cleared scroll ID: {self.scroll_id}")
            except Exception as e:
                warnings.warn(
                    ScrollClearWarning(f"Failed to clear scroll ID: {self.scroll_id}. Error: {str(e)}")
                )

    def close_connection(self):
        """
        Closes the Elasticsearch connection.
        """
        if self.client:
            try:
                self.client.close()
                logging.info("Elasticsearch connection closed.")
            except Exception as e:
                warnings.warn(
                    ConnectionCloseWarning(f"Failed to close Elasticsearch connection: {str(e)}")
                )

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context and clean up resources.
        """
        self.clear_scroll()
        self.close_connection()
