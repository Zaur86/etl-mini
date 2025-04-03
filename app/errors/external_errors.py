"""
Contains custom errors related to interactions with external services
"""


from app.errors import CustomError


class ElasticSearchError(CustomError):
    """Raised for errors related to Elasticsearch operations."""
    pass
