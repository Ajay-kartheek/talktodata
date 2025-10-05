"""DuckDB database manager for Text-to-SQL system."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb


logger = logging.getLogger(__name__)


class DuckDBManager:
    """Manages DuckDB connection and operations."""

    def __init__(self, db_path: str = ":memory:"):
        """Initialize DuckDB connection.

        Args:
            db_path: Path to database file or ":memory:" for in-memory database
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._connect()

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")

    def load_table_from_csv(
        self,
        table_name: str,
        csv_path: str,
        **kwargs
    ) -> None:
        """Load data from CSV file into a table.

        Args:
            table_name: Name of the table to create
            csv_path: Path to CSV file
            **kwargs: Additional arguments for read_csv
        """
        try:
            if not Path(csv_path).exists():
                raise FileNotFoundError(f"CSV file not found: {csv_path}")

            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
            )
            logger.info(f"Loaded table '{table_name}' from {csv_path}")
        except Exception as e:
            logger.error(f"Failed to load CSV into table '{table_name}': {e}")
            raise

    def load_table_from_parquet(
        self,
        table_name: str,
        parquet_path: str
    ) -> None:
        """Load data from Parquet file into a table.

        Args:
            table_name: Name of the table to create
            parquet_path: Path to Parquet file
        """
        try:
            if not Path(parquet_path).exists():
                raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
            )
            logger.info(f"Loaded table '{table_name}' from {parquet_path}")
        except Exception as e:
            logger.error(f"Failed to load Parquet into table '{table_name}': {e}")
            raise

    def load_table_from_json(
        self,
        table_name: str,
        json_path: str
    ) -> None:
        """Load data from JSON file into a table.

        Args:
            table_name: Name of the table to create
            json_path: Path to JSON file
        """
        try:
            if not Path(json_path).exists():
                raise FileNotFoundError(f"JSON file not found: {json_path}")

            self.conn.execute(
                f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{json_path}')"
            )
            logger.info(f"Loaded table '{table_name}' from {json_path}")
        except Exception as e:
            logger.error(f"Failed to load JSON into table '{table_name}': {e}")
            raise

    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SQL query and return results.

        Args:
            query: SQL query to execute

        Returns:
            List of dictionaries representing query results
        """
        try:
            logger.info(f"Executing query: {query}")
            result = self.conn.execute(query).fetchdf()

            # Convert DataFrame to list of dictionaries
            records = result.to_dict('records')
            logger.info(f"Query returned {len(records)} rows")
            return records
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema information for a table.

        Args:
            table_name: Name of the table

        Returns:
            List of column definitions with name and type
        """
        try:
            query = f"DESCRIBE {table_name}"
            result = self.conn.execute(query).fetchall()

            schema = []
            for row in result:
                schema.append({
                    "column_name": row[0],
                    "column_type": row[1],
                    "null": row[2],
                    "key": row[3] if len(row) > 3 else None,
                    "default": row[4] if len(row) > 4 else None,
                })

            logger.info(f"Retrieved schema for table '{table_name}': {len(schema)} columns")
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for table '{table_name}': {e}")
            raise

    def get_all_tables(self) -> List[str]:
        """Get list of all tables in the database.

        Returns:
            List of table names
        """
        try:
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            result = self.conn.execute(query).fetchall()
            tables = [row[0] for row in result]
            logger.info(f"Found {len(tables)} tables in database")
            return tables
        except Exception as e:
            logger.error(f"Failed to retrieve table list: {e}")
            raise

    def validate_query(self, query: str) -> bool:
        """Validate SQL query without executing it.

        Args:
            query: SQL query to validate

        Returns:
            True if query is valid, False otherwise
        """
        try:
            self.conn.execute(f"EXPLAIN {query}")
            return True
        except Exception as e:
            logger.warning(f"Query validation failed: {e}")
            return False

    def get_sample_data(self, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get sample rows from a table.

        Args:
            table_name: Name of the table
            limit: Number of rows to return

        Returns:
            List of sample records
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Failed to get sample data from '{table_name}': {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
