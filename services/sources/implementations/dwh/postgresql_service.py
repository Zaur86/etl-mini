from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from dataclasses import dataclass, field
from services.sources.base import DWHService
from app.errors import MethodNotSetError
from typing import Any
from io import StringIO
import pandas as pd
import logging


@dataclass
class PostgreSQLService(DWHService):
    """
    PostgreSQL implementation of the DWHService with context manager support.
    """
    db_url: str
    engine: object = field(init=False, default=None)
    session_factory: sessionmaker = field(init=False, default=None)
    session: Session = field(init=False, default=None)
    _load_method: callable = field(init=False, default=None)

    def connect(self):
        """
        Establish a connection using SQLAlchemy and initialize the session factory.
        """
        try:
            self.engine = create_engine(self.db_url)
            self.session_factory = sessionmaker(bind=self.engine)
        except SQLAlchemyError as e:
            raise ConnectionError(f"Failed to connect to the database: {e}")
        logging.info("PostgreSQL connection established.")

    def begin_session(self):
        """
        Start a new session for transactional operations.
        """
        if not self.session_factory:
            raise ConnectionError("Session factory is not initialized. Call 'connect' first.")
        self.session = self.session_factory()
        logging.info("PostgreSQL session started.")

    def close_session(self, commit: bool = True):
        """
        Close the current session with optional commit or rollback.
        :param commit: Whether to commit the transaction. Rollback if False.
        """
        if not self.session:
            raise ConnectionError("No active session to close.")
        try:
            if commit:
                self.session.commit()
                logging.info("PostgreSQL session commited.")
            else:
                self.session.rollback()
                logging.info("PostgreSQL session rolled back.")
        finally:
            self.session.close()
            self.session = None

    def disconnect(self):
        """
        Dispose of the database connection.
        """
        if self.session:
            self.close_session(commit=False)  # Rollback any pending transactions
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logging.info("PostgreSQL disconnected.")

    def __enter__(self):
        """
        Context manager entry point. Connects to the database and starts a session.
        """
        self.connect()
        self.begin_session()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Context manager exit point. Closes the session and disconnects from the database.
        """
        if exc_type is not None:
            self.close_session(commit=False)  # Rollback on exception
        else:
            self.close_session(commit=True)  # Commit if no exception
        self.disconnect()

    def prepare_loading(self, method_name):
        """
        Prepare for data loading by setting the method to be used.
        :param method_name: Name of the internal loading method (must start with '_load_').
        """
        if not method_name.startswith("_load_"):
            raise ValueError(
                f"Method '{method_name}' is not a valid loading method. It must start with '_load_'."
            )
        if not hasattr(self, method_name):
            raise ValueError(f"Method '{method_name}' does not exist.")
        self._load_method = getattr(self, method_name)

    def load_data(self, args: dict[str, Any]):
        """
        Execute the previously prepared loading method with given arguments.
        :param args: Arguments for the selected loading method.
        """
        if not self._load_method:
            raise MethodNotSetError(
                "No valid loading method has been set. Call 'prepare_loading' first."
            )
        self._load_method(**args)
        self._load_method = None  # Reset after execution

    def _load_from_tsv(
        self, table_name, source, source_type,
            columns=None, reset_buffer=True, truncate_buffer=True
    ):
        """
        General method to load data from a TSV source with column mapping support.
        :param table_name: Target table name.
        :param source: The data source (file path, string, or buffer).
        :param source_type: The type of the source ('file', 'str', or 'buffer').
        :param columns: List of column names to map the data (if not provided then extracted from header).
        :param reset_buffer: Whether to reset the buffer pointer to the start (for 'buffer' source type).
        """
        logging.debug(f"Loading data to table '{table_name}' using _load_from_tsv")
        if not self.session:
            raise ConnectionError("No active session for loading data.")

        file = None  # Initialize the file variable to avoid reference errors in finally
        try:
            # Determine the source based on the source type and read header and data_stream
            if source_type == 'file':
                file = open(source, 'r')
                header = file.readline().strip().split("\t")
                data_stream = file
            elif source_type == 'str':
                lines = source.splitlines()
                header = lines[0].strip().split("\t")
                data_stream = StringIO('\n'.join(lines[1:]))
            elif source_type == 'buffer':
                if reset_buffer:
                    source.seek(0)  # Reset the buffer pointer to the start
                header = next(source).strip().split("\t")
                data_stream = source
            else:
                raise ValueError(
                    f"Invalid source_type '{source_type}'. Must be 'file', 'str', or 'buffer'."
                )

            # If columns are not provided, use the header
            if not columns:
                columns = header

            # Validate selected columns
            if not all(col in header for col in columns):
                missing = [col for col in columns if col not in header]
                raise ValueError(f"The following columns are missing in the file: {missing}")

            # Format the column list for COPY command
            column_list = f"({', '.join(columns)})"

            # Use the COPY command to load data
            copy_command = text(
                f"COPY {table_name} {column_list} FROM STDIN WITH (FORMAT text, DELIMITER '\t', NULL '')"
            )
            self.session.connection().connection.cursor().copy_expert(copy_command.text, data_stream)
            logging.debug("SUCCESS")
        except Exception as e:
            raise Exception(f"Error during TSV loading from {source_type}: {e}")
        finally:
            if source_type == 'buffer' and truncate_buffer:
                source.seek(0)
                source.truncate(0)
            if file is not None:
                file.close()  # Close file if it was opened

    def _load_with_values(
            self,
            table_name: str,
            values: list[dict],
            conflict_action: str = None,
            conflict_columns: list[str] = None,
            update_columns: list[str] = None
    ):
        """
        Insert data into a table with explicit values, handling conflicts if specified.
        :param table_name: Name of the target table.
        :param values: List of dictionaries representing rows to insert.
        :param conflict_action: Action to take on conflict.
            Options: 'update', 'nothing', or None (no conflict handling).
        :param conflict_columns: List of columns to handle conflicts on. Required if conflict_action is not None.
        :param update_columns: List of columns to update on conflict. Required if conflict_action='update'.
        """
        logging.debug(f"Loading data to table '{table_name}' using _load_with_values")
        if not self.session:
            raise ConnectionError("No active session for loading data.")

        if not values:
            raise ValueError("The 'values' list is empty. Provide data to insert.")

        # Prepare the column names and values
        columns = values[0].keys()
        placeholders = ", ".join(f":{col}" for col in columns)
        column_names = ", ".join(columns)

        # Base insert query
        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        # Add conflict handling logic if specified
        if conflict_action:
            if not conflict_columns:
                raise ValueError("conflict_columns must be provided when conflict_action is specified.")
            conflict_clause = ", ".join(conflict_columns)

            if conflict_action == "update":
                if not update_columns:
                    raise ValueError("update_columns must be provided when conflict_action='update'.")
                update_clause = ", ".join(
                    f"{col} = excluded.{col}" for col in update_columns
                )
                insert_query += f" ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
            elif conflict_action == "nothing":
                insert_query += f" ON CONFLICT ({conflict_clause}) DO NOTHING"
            else:
                raise ValueError(f"Invalid conflict_action '{conflict_action}'. Use 'update', 'nothing', or None.")

        # Convert the query to a SQLAlchemy text object
        insert_query = text(insert_query)

        try:
            # Execute the query with the provided values
            self.session.execute(insert_query, values)
            logging.debug("SUCCESS")
        except Exception as e:
            raise Exception(f"Error while executing insert: {e}")

    def _load_from_pandas_df(
        self,
        table_name: str,
        data,
        conflict_action: str = None,
        conflict_columns: list[str] = None,
        update_columns: list[str] = None
    ):
        """
        Load data from a pandas DataFrame into a PostgreSQL table with optional conflict handling.

        :param table_name: Target table name.
        :param data: pandas DataFrame containing the data.
        :param conflict_action: 'update', 'nothing', or None.
        :param conflict_columns: Columns to check for conflict (required if conflict_action is used).
        :param update_columns: Columns to update on conflict (required if conflict_action='update').
        """
        if not self.session:
            raise ConnectionError("No active session for loading data.")
        if data.empty:
            logging.warning("DataFrame is empty. Skipping load.")
            return

        columns = data.columns.tolist()
        placeholders = ", ".join(f":{col}" for col in columns)
        column_names = ", ".join(columns)

        insert_query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"

        if conflict_action:
            if not conflict_columns:
                raise ValueError("conflict_columns must be provided when conflict_action is specified.")
            conflict_clause = ", ".join(conflict_columns)

            if conflict_action == "update":
                if not update_columns:
                    raise ValueError("update_columns must be provided when conflict_action='update'.")
                update_clause = ", ".join(
                    f"{col} = excluded.{col}" for col in update_columns
                )
                insert_query += f" ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
            elif conflict_action == "nothing":
                insert_query += f" ON CONFLICT ({conflict_clause}) DO NOTHING"
            else:
                raise ValueError(f"Invalid conflict_action '{conflict_action}'.")

        insert_stmt = text(insert_query)

        try:
            data = data.replace({pd.NA: None, float("nan"): None, "nan": None, "NaN": None})
            data = data.where(pd.notnull(data), None)
            values = data.to_dict(orient="records")
            self.session.execute(insert_stmt, values)
            logging.info(f"Loaded {len(data)} rows into '{table_name}'")
        except Exception as e:
            raise Exception(f"Error while loading DataFrame: {e}")
