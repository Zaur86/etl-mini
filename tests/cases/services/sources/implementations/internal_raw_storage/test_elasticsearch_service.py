import pytest
import yaml
from services.sources.implementations.internal_raw_storage import ElasticSearchService
from models.queries import ElasticQueryModel
from app.settings import get_settings

# Load test configuration from YAML file
conf_path = 'tests/cases/services/sources/implementations/internal_raw_storage/test_elasticsearch_service_config.yaml'
with open(conf_path, 'r') as f:
    CONFIG = yaml.safe_load(f)


@pytest.fixture(scope="function")
def service(mocker, use_mocker):
    # Determine whether to use a mock or real service based on test markers
    env = CONFIG.get('environment', 'TEST')
    settings = get_settings(env=env)

    if use_mocker:
        # Mock service for non-integration tests
        mocked_client = mocker.MagicMock()
        mocker.patch(
            'elasticsearch.Elasticsearch',
            return_value=mocked_client
        )
        result = ElasticSearchService(
            host=settings.EL_HOST,
            port=settings.EL_PORT,
            use_iam=settings.EL_USE_IAM,
            aws_profile=settings.EL_AWS_PROFILE,
            aws_region=settings.EL_AWS_REGION,
            aws_service=settings.EL_AWS_SERVICE
        )
        result.client = mocked_client
    else:
        # Real service for integration tests
        result = ElasticSearchService(
            host=settings.EL_HOST,
            port=settings.EL_PORT,
            use_iam=settings.EL_USE_IAM,
            aws_profile=settings.EL_AWS_PROFILE,
            aws_region=settings.EL_AWS_REGION,
            aws_service=settings.EL_AWS_SERVICE
        )
    result.connect()
    return result


@pytest.mark.parametrize("test_case", CONFIG["tests"]["prepare_extraction"])
def test_prepare_extraction(service, test_case):
    query_model = ElasticQueryModel(
        start_time=test_case['query_model']['start_time'],
        end_time=test_case['query_model']['end_time'],
        source_fields=test_case['query_model'].get('source_fields'),
        filters=test_case['query_model'].get('filters')
    )
    service.prepare_extraction(index=test_case['index'], query_model=query_model)

    # Verify the index and query
    assert service.index == test_case['index']
    assert 'query' in service.query


@pytest.mark.parametrize("test_case", CONFIG["tests"]["extract_data"])
def test_extract_data(service, test_case, mocker, use_mocker):
    env = CONFIG.get('environment', 'TEST')
    mock_config = CONFIG.get('mocks')
    settings = get_settings(env=env)
    batch_size = test_case['batch_size'] if test_case['batch_size'] is not None \
        else settings.EL_DEFAULT_BATCH_SIZE
    scroll = test_case['scroll'] if test_case['scroll'] is not None else settings.EL_DEFAULT_SCROLL

    if use_mocker:
        # Mock search and scroll methods for non-integration tests
        mocker.patch.object(
            service.client,
            'search',
            return_value={
                '_scroll_id': mock_config['search']['_scroll_id'],
                'hits': {
                    'hits': mock_config['search']['hits']
                }
            }
        )
        mocker.patch.object(
            service.client,
            'scroll',
            side_effect=[
                {
                    '_scroll_id': mock_config['scroll']['_scroll_id'],
                    'hits': {
                        'hits': mock_config['scroll']['hits']
                    }
                },
                {
                    '_scroll_id': None,
                    'hits': {'hits': []}
                }  # End of scroll
            ]
        )
        mocker.patch.object(
            service.client,
            "clear_scroll",
            return_value=None
        )

    service.prepare_extraction(
        index=test_case['index'],
        query_model=ElasticQueryModel(
            start_time=test_case['query_model']['start_time'],
            end_time=test_case['query_model']['end_time'],
            source_fields=test_case['query_model'].get('source_fields'),
            filters=test_case['query_model'].get('filters')
        )
    )

    results = []
    results_num = 0

    # Iterate over batches of data extracted by the service
    for batch in service.extract_data(batch_size=batch_size, scroll=scroll):
        results.append(batch)
        results_num += len(batch)

    # Cleaar scroll
    service.clear_scroll()

    # Close connection
    service.close_connection()

    # Validate results
    if use_mocker:
        assert results_num == 2
    else:
        assert results_num == test_case['expected_results']
