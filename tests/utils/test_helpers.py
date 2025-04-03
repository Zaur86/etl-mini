"""
tests/utils/test_helpers.py

This file contains helper functions that are used across multiple tests.
These functions are meant to simplify common testing tasks, such as:

1. Printing input and output for easier debugging (`print_query`).
2. Any additional test utilities that don't belong to specific test logic.

Expected contents:
- Functions for debugging and logging test data.
- General-purpose utilities used in test scenarios.
"""
import json


def print_query(input_data, query, print_results):
    """
    Print the input data and the generated query if the --print-results flag is set.

    Args:
        input_data (dict): Input data for the query.
        query (dict): Generated Elasticsearch query.
        print_results (bool): Flag indicating whether to print results.
    """
    if print_results:
        print("\nInput Data:")
        print(json.dumps(input_data, indent=4, sort_keys=True))
        print("Generated Query:")
        print(json.dumps(query, indent=4, sort_keys=True))
