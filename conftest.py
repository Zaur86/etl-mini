import pytest


def pytest_addoption(parser):
    """
    Adds custom command-line options:
    - `--print-results` to control result printing.
    - `--use-mocker` to control whether to use mocks or real services.
    """
    parser.addoption(
        "--print-results", action="store_true", default=False,
        help="Print test results to stdout"
    )
    parser.addoption(
        "--use-mocker", action="store_true", default=False,
        help="Use mocked services instead of real ones"
    )


@pytest.fixture
def print_results(request):
    """
    Fixture to check if the `--print-results` flag is set.
    """
    return request.config.getoption("--print-results")


@pytest.fixture
def use_mocker(request):
    """
    Fixture to check if the `--use-mocker` flag is set.
    """
    return request.config.getoption("--use-mocker")
