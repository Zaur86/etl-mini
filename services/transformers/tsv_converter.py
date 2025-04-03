from services.transformers.base_transformer import Transformer
from dataclasses import dataclass, field
from multiprocessing import Pool
from io import StringIO
import json
import os
import warnings
from typing import Dict, List, Any, Optional
from models.helpers import AdditionalFields
from app.errors import MissingFieldError, NestedKeyError
from app.warnings import ExcessiveProcessesWarning, JsonLengthWarning
import logging


@dataclass
class TSVConverter(Transformer):
    """
    A class for converting a list of dictionaries to a TSV (Tab-Separated Values) format.

    Attributes:
    ----------
    fields_mapping : Dict[str, str]
        A mapping of input dictionary keys to column names in the resulting TSV file.
    not_null_fields : List[str], optional
        A list of fields that must not be empty in the input data.
    missing_value_placeholder : str, optional
        A placeholder for missing values (default is "NULL").
    num_processes : int, optional
        Number of parallel processes to use for conversion (default is 4).
    additional_fields : List[AdditionalFields], optional
        A list of additional fields to be added to the output TSV, either as constant values
        or calculated using functions.
    max_json_length : int, optional
        Maximum allowed length for JSON strings (default: 100000).
    nested_key : Optional[List[str]], optional
        List of keys representing the path to the nested data.

    Methods:
    -------
    prepare_transformation(additional_fields: AdditionalFields):
        Adds additional fields to the converter.
    transform(data: List[Dict[str, Any]]) -> str:
        Converts the input data to a TSV string.
    """

    fields_mapping: Dict[str, Dict]
    not_null_fields: List[str] = field(default_factory=list)
    missing_value_placeholder: str = "NULL"
    num_processes: int = 4
    additional_fields: List[AdditionalFields] = field(default_factory=list)
    max_json_length: int = 100000
    nested_key: Optional[List[str]] = field(default_factory=list)
    debug: bool = False

    def __post_init__(self):
        """Check available CPU cores and issue a warning if num_processes exceeds them."""
        available_cores = os.cpu_count() or 1
        self.logger = logging.getLogger(__name__)
        if self.num_processes > available_cores:
            warnings.warn(
                ExcessiveProcessesWarning(self.num_processes, available_cores),
                stacklevel=2
            )
        self.logger.debug(f"Initialized TSVConverter with {self.num_processes} processes.")

    def _extract_nested_data(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Extract data from a nested dictionary."""
        current_level = row
        for key in self.nested_key:
            if not isinstance(current_level, dict) or key not in current_level:
                raise NestedKeyError(
                    f"Failed to extract data at nested key path {self.nested_key}. "
                    f"Current level: {current_level}"
                )
            current_level = current_level[key]
        self.logger.debug(f"Extracted nested data: {current_level}")
        return current_level

    def prepare_transformation(self, additional_fields: List[AdditionalFields]):
        """Add additional fields to the converter."""
        self.additional_fields.extend(additional_fields)

    def _sanitize_value(self, value: Any) -> str:
        """Sanitize the value by replacing tabs, newlines, and converting JSON to string if needed."""
        if isinstance(value, (dict, list)):
            value = json.dumps(value)  # Convert JSON to string
            if len(value) > self.max_json_length:
                warnings.warn(
                    JsonLengthWarning(self.max_json_length, len(value)),
                    stacklevel=2
                )
        sanitized = str(value).replace("\t", " ").replace("\n", " ")
        self.logger.debug(f"Sanitized value: {sanitized}")
        return sanitized

    def _process_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single row, extracting nested data and applying additional fields if necessary."""
        if self.nested_key:
            row = self._extract_nested_data(row)
        if self.additional_fields:
            self._apply_additional_fields(row)
        self.logger.debug(f"Processed row: {row}")
        return row

    def _apply_additional_fields(self, row: Dict[str, Any]):
        """Apply all additional fields to the given row."""
        for additional_field in self.additional_fields:
            if callable(additional_field.value):  # If it's a function
                # Build function arguments from input mapping
                self.logger.debug(f"callable additional_field.value: {additional_field.value}")
                function_args = {}
                for func_arg in additional_field.input_mapping.keys():
                    source_dict = additional_field.input_mapping[func_arg]
                    current_level = row
                    nested_key = source_dict.get('nested_key', [])
                    for key in nested_key:
                        if not isinstance(current_level, dict) or key not in current_level:
                            raise NestedKeyError(
                                "Apply additional fields input mapping error"
                                f"Failed to extract data at nested key path {nested_key}. "
                                f"Current level: {current_level}"
                            )
                        current_level = current_level.get(key)
                    # Check that all required arguments are provided
                    if source_dict['key'] not in current_level.keys():
                        raise MissingFieldError(
                            f"Required field for function is missing: {source_dict['key']}"
                        )
                    function_args[func_arg] = current_level[source_dict['key']]

                self.logger.debug(f"Function args: {function_args}")
                self.logger.debug(f"Static args: {additional_field.static_args}")

                # Add static arguments
                function_args.update(additional_field.static_args)

                # Call the function and store the result
                result = additional_field.value(**function_args)

                self.logger.debug(f"Function result: {result}")

                # Apply output mapping
                if isinstance(result, dict):
                    for out_key, out_value in result.items():
                        mapped_field = additional_field.output_mapping.get(out_key, out_key)
                        row[mapped_field] = out_value
                else:
                    raise ValueError("Function result must be a dictionary with keys matching output_mapping.")
            else:  # If it's a constant value
                # Directly add constant value to all output fields
                for output_field in additional_field.output_fields:
                    row[output_field] = additional_field.value
        self.logger.debug(f"Applied additional fields: {row}")
        pass

    def _process_chunk(self, chunk: List[Dict[str, Any]]) -> str:
        """Process a chunk of data and convert it to TSV lines."""
        buffer = StringIO()
        for row in chunk:
            processed_row = self._process_row(row)
            self.logger.debug(f"Processed row: {processed_row}")
            line = []

            # Process only fields from fields_mapping
            for key in self.fields_mapping:
                source_dict = self.fields_mapping[key]
                current_level = processed_row
                self.logger.debug(f"Current level : {current_level}")
                nested_key = source_dict.get("nested_key", [])
                for key_key in nested_key:
                    if not isinstance(current_level, dict) or key_key not in current_level:
                        raise NestedKeyError(
                            "Proccess chunk field mapping error"
                            f"Failed to extract data at nested key path {nested_key}. "
                            f"Current level: {current_level}"
                        )
                    current_level = current_level.get(key_key)
                if key in self.not_null_fields and source_dict['key'] not in current_level:
                    raise MissingFieldError(f"Field '{source_dict['key']}' is missing in row: {current_level}")

                value = current_level.get(source_dict['key'], self.missing_value_placeholder)
                self.logger.debug(f"value: {value}")
                sanitized_value = self._sanitize_value(value)
                line.append(sanitized_value)

                self.logger.debug(f"line by fields_mapping with key {key}: {line}")

            # Append additional fields
            for additional_field in self.additional_fields:

                # constant values
                for output_field in additional_field.output_fields:
                    value = processed_row.get(output_field, self.missing_value_placeholder)
                    sanitized_value = self._sanitize_value(value)
                    line.append(sanitized_value)

                    self.logger.debug(f"line with constant additional field {output_field}: {line}")

                # dynamic values from function
                for output_field in additional_field.output_mapping.values():
                    if output_field in processed_row:
                        value = processed_row.get(output_field, self.missing_value_placeholder)
                        self.logger.debug(f"value: {value}")
                        sanitized_value = self._sanitize_value(value)
                        line.append(sanitized_value)
                    else:
                        raise MissingFieldError(f"Field '{output_field}' is missing in row: {processed_row}")

                    self.logger.debug(f"line with additional field from function {output_field}: {line}")

            buffer.write("\t".join(line) + "\n")
        return buffer.getvalue()

    def _split_data(self, data: List[Dict[str, Any]], num_chunks: int) -> List[List[Dict[str, Any]]]:
        """Split data into chunks for parallel processing."""

        chunk_size = max(1, len(data) // num_chunks)
        remainder = len(data) % num_chunks
        split_data = []
        start = 0
        for i in range(num_chunks):
            end = start + chunk_size + (1 if i < remainder else 0)
            split_data.append(data[start:end])
            start = end
        self.logger.debug(f"Split data into {len(split_data)} chunks.")
        return split_data

    def transform(self, data: List[Dict[str, Any]], reset_buffer=True) -> StringIO:
        """Main method to convert data to TSV."""

        # Make header
        header = [key for key in self.fields_mapping]
        for additional_field in self.additional_fields:
            if callable(additional_field.value):
                header.extend(additional_field.output_mapping.values())
            else:
                header.extend(additional_field.output_fields)
        self.logger.debug(f"Header: {header}")

        final_buffer = StringIO()

        # Make chunks
        chunks = self._split_data(data, self.num_processes)
        if chunks:
            self.logger.debug(f"First row from one chunk: {chunks[0][0]}")
        if self.additional_fields:
            self.logger.debug(f"Additional fields: {self.additional_fields}")
        results = []
        if self.debug:
            for chunk in chunks:
                results.append(self._process_chunk(chunk))
        else:
            with Pool(self.num_processes) as pool:
                results = pool.map(self._process_chunk, chunks)

        # Add header
        final_buffer.write("\t".join(header) + "\n")

        # Combine all chunks
        for result in results:
            final_buffer.write(result)

        self.logger.debug(f"Final TSV size: {final_buffer.tell()} bytes.")

        if reset_buffer:
            final_buffer.seek(0)  # Reset the buffer pointer to the start

        return final_buffer
