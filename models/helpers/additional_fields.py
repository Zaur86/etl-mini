from dataclasses import dataclass, field
from typing import Callable, Dict, Any, List, Union


@dataclass
class AdditionalFields:
    """
    Represents additional fields to be added to the TSV output.

    Attributes:
    ----------
    value : Union[Any, Callable]
        A constant value or a function used to calculate the additional fields.
    input_mapping : Dict[str, str], optional
        A mapping from the function argument names to the input data keys.
    static_args : Dict[str, Any], optional
        Static arguments to be passed to the function in addition to mapped inputs.
    output_mapping : Dict[str, str], optional
        A mapping from the function's output keys to the desired output field names in TSV.
    output_fields : List[str], optional
        Names of the additional fields to be added when a constant value is provided.
    """
    value: Union[Any, Callable]  # Constant value or a function
    input_mapping: Dict[str, Dict] = field(default_factory=dict)  # Mapping from function args to input keys
    static_args: Dict[str, Any] = field(default_factory=dict)  # Static arguments for the function
    output_mapping: Dict[str, str] = field(default_factory=dict)  # Mapping from function output to TSV fields
    output_fields: List[str] = field(default_factory=list)  # Fields for constant value
