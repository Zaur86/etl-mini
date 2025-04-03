import pytest
import yaml
from models.queries import ElasticQueryModel
from tests.utils import print_query

# Load test configuration from YAML file
with open("tests/cases/models/queries/test_elastic_query_model_config.yaml") as f:
    CONFIG = yaml.safe_load(f)


@pytest.mark.parametrize("test_data", CONFIG["range_tests"])
def test_match_all_and_range_query(test_data, print_results):
    """
    Test range query behavior based on start_time and end_time.
    If both start_time and end_time are None, match_all is expected.
    Otherwise, a range query should be applied.
    """
    query_model = ElasticQueryModel(
        start_time=test_data["start_time"],
        end_time=test_data["end_time"]
    )
    query = query_model.build_query()
    print_query(
        {"start_time": test_data["start_time"], "end_time": test_data["end_time"]},
        query,
        print_results
    )
    if test_data["expected_match_all"]:
        assert "match_all" in query["query"]["bool"]["must"][0]
    else:
        assert "match_all" not in query["query"]["bool"]["must"]
        assert "range" in query["query"]["bool"]["must"][0]


@pytest.mark.parametrize("test_data", CONFIG["filter_tests"])
def test_filter_query(test_data, print_results):
    """
    Test filter application in the query.
    If filters are provided, they should appear in the 'filter' section of the query.
    """
    query_model = ElasticQueryModel(filters=test_data["filters"])
    query = query_model.build_query()
    print_query({"filters": test_data["filters"]}, query, print_results)
    if test_data["expected_filter"]:
        assert "term" in query["query"]["bool"]["filter"][0]
        assert query["query"]["bool"]["filter"][0]["term"]["status"] == "active"
    else:
        assert "filter" not in query["query"]["bool"]


@pytest.mark.parametrize("test_data", CONFIG["source_field_tests"])
def test_source_fields(test_data, print_results):
    """
    Test selection of specific source fields in the query.
    If source_fields are provided, the '_source' section should be present in the query.
    """
    query_model = ElasticQueryModel(
        source_fields=test_data["source_fields"],
        start_time=test_data["start_time"],
        end_time=test_data["end_time"]
    )
    query = query_model.build_query()
    print_query(
        {
            "source_fields": test_data["source_fields"],
            "start_time": test_data["start_time"],
            "end_time": test_data["end_time"],
        },
        query,
        print_results
    )
    if test_data["expected_source"]:
        assert "_source" in query
        assert query["_source"] == test_data["source_fields"]
    else:
        assert "_source" not in query
