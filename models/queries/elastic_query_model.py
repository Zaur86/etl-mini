from typing import List, Optional, Dict
from pydantic import BaseModel


class ElasticQueryModel(BaseModel):
    """
    Model for constructing an Elasticsearch query.
    """
    source_fields: Optional[List[str]] = None  # Fields to retrieve
    range_field: str = "timestamp"             # Field for range filtering (default: "timestamp")
    start_time: Optional[str] = None          # Start time for range filter
    end_time: Optional[str] = None            # End time for range filter
    range_format: Optional[str] = "yyyy-MM-dd HH:mm:ss"  # Format for range filter (default: standard)
    filters: Optional[Dict[str, str]] = None  # Additional filters as key-value pairs
    sort_field: str = "timestamp"             # Field for sorting (default: "timestamp")
    sort_order: str = "asc"                   # Sort order (default: ascending)

    def build_query(self) -> dict:
        """
        Constructs an Elasticsearch query based on the model fields.
        """
        must_conditions = []

        # Add range filter if start and end times are provided
        if self.start_time and self.end_time:
            range_filter = {
                "range": {
                    self.range_field: {
                        "gte": self.start_time,
                        "lt": self.end_time
                    }
                }
            }
            if self.range_format:
                range_filter["range"][self.range_field]["format"] = self.range_format
            must_conditions.append(range_filter)

        # Add match_all only if no other must conditions exist
        if not must_conditions:
            must_conditions.append({"match_all": {}})

        query = {
            "query": {
                "bool": {
                    "must": must_conditions
                }
            },
            "sort": [
                {self.sort_field: {"order": self.sort_order}}
            ]
        }

        # Add additional filters if provided
        if self.filters:
            query["query"]["bool"]["filter"] = [{"term": {key: value}} for key, value in self.filters.items()]

        # Specify source fields if provided
        if self.source_fields:
            query["_source"] = self.source_fields

        return query
