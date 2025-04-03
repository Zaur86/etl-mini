from services.transformers.base_transformer import Transformer
from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
import pandas as pd
import logging


@dataclass
class PandasSelectAndEnrichTransformer(Transformer):
    """
    A transformer that:
      - adds constant columns,
      - optionally selects only specified columns,
      - optionally removes duplicates based on keys and order.

    Attributes:
    ----------
    constants : Dict[str, Any]
        Dictionary of constant values to add as new columns.
    columns : Optional[List[str]]
        List of columns to select after transformation.
    require_all_columns : bool
        Whether to raise an error if not all specified columns exist in the DataFrame.
    dedup_by : Optional[List[str]]
        Keys to drop duplicates by (keeping first based on order_by).
    order_by : Optional[List[str]]
        Columns to sort by before deduplication.
    """

    constants: Dict[str, Any] = field(default_factory=dict)
    columns: Optional[List[str]] = None
    require_all_columns: bool = False
    dedup_by: Optional[List[str]] = None
    order_by: Optional[List[str]] = None

    def __post_init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"Initialized PandasSelectAndEnrichTransformer with constants={self.constants}, "
                          f"columns={self.columns}, require_all_columns={self.require_all_columns}, "
                          f"dedup_by={self.dedup_by}, order_by={self.order_by}")

    def prepare_transformation(self, *args, **kwargs):
        pass

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        # Add constant columns
        for col_name, value in self.constants.items():
            data[col_name] = value
            self.logger.debug(f"Added constant column '{col_name}' with value '{value}'")

        # Sort before deduplication if needed
        if self.dedup_by:
            if self.order_by:
                data = data.sort_values(by=self.order_by)
                self.logger.debug(f"Sorted DataFrame by {self.order_by}")
            data = data.drop_duplicates(subset=self.dedup_by, keep="first")
            self.logger.debug(f"Dropped duplicates by {self.dedup_by}")

        # Select specific columns if specified
        if self.columns:
            missing_cols = [col for col in self.columns if col not in data.columns]
            if missing_cols:
                if self.require_all_columns:
                    raise ValueError(f"The following required columns are missing from DataFrame: {missing_cols}")
                else:
                    self.logger.warning(f"The following columns are missing and will be ignored: {missing_cols}")
            selected_columns = [col for col in self.columns if col in data.columns]
            data = data[selected_columns]
            self.logger.debug(f"Selected columns: {selected_columns}")

        return data
