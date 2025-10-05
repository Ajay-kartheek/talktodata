"""Main Text-to-SQL application."""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .bedrock_client import BedrockClient
from .config import Settings
from .duckdb_manager import DuckDBManager
from .query_validator import QueryValidator
from .schema_loader import SchemaLoader
from .sql_generator import SQLGenerator


logger = logging.getLogger(__name__)


class Text2SQL:
    """Main application class for Text-to-SQL system."""

    def __init__(
        self,
        schema_path: str,
        settings: Optional[Settings] = None
    ):
        """Initialize Text-to-SQL application.

        Args:
            schema_path: Path to schema JSON file
            settings: Application settings (uses defaults if not provided)
        """
        self.settings = settings or Settings()
        self.schema_path = schema_path

        # Initialize components
        logger.info("Initializing Text-to-SQL system")

        self.db_manager = DuckDBManager(db_path=self.settings.duckdb_path)
        self.schema_loader = SchemaLoader(schema_path=schema_path)

        self.bedrock_client = BedrockClient(
            model_id=self.settings.bedrock_model_id,
            region=self.settings.aws_region,
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
            temperature=self.settings.bedrock_temperature,
            max_tokens=self.settings.bedrock_max_tokens
        )

        self.sql_generator = SQLGenerator(
            bedrock_client=self.bedrock_client,
            schema_loader=self.schema_loader
        )

        self.validator = QueryValidator(
            db_manager=self.db_manager,
            schema_loader=self.schema_loader
        )

        logger.info("Text-to-SQL system initialized successfully")

    def load_data(self, table_name: str, file_path: str, file_type: str = "csv") -> None:
        """Load data into a table.

        Args:
            table_name: Name of the table
            file_path: Path to data file
            file_type: Type of file (csv, parquet, json)
        """
        logger.info(f"Loading data into table '{table_name}' from {file_path}")

        if file_type.lower() == "csv":
            self.db_manager.load_table_from_csv(table_name, file_path)
        elif file_type.lower() == "parquet":
            self.db_manager.load_table_from_parquet(table_name, file_path)
        elif file_type.lower() == "json":
            self.db_manager.load_table_from_json(table_name, file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def query(
        self,
        question: str,
        validate: bool = True,
        retry_on_error: bool = True
    ) -> Dict[str, Any]:
        """Execute natural language query.

        Args:
            question: Natural language question
            validate: Whether to validate SQL before execution
            retry_on_error: Whether to retry on validation errors

        Returns:
            Dictionary with query results and metadata
        """
        logger.info(f"Processing query: {question}")

        try:
            # Generate SQL
            sql_query = self.sql_generator.generate(question)

            # Validate if requested
            if validate:
                is_valid, error_msg = self.validator.validate(sql_query)

                if not is_valid:
                    logger.warning(f"Generated SQL failed validation: {error_msg}")

                    if retry_on_error:
                        logger.info("Attempting to fix SQL with retry")
                        sql_query = self.sql_generator.generate_with_retry(
                            question=question,
                            error_message=error_msg,
                            original_sql=sql_query
                        )

                        # Validate again
                        is_valid, error_msg = self.validator.validate(sql_query)
                        if not is_valid:
                            raise ValueError(f"SQL validation failed after retry: {error_msg}")
                    else:
                        raise ValueError(f"SQL validation failed: {error_msg}")

            # Execute query
            results = self.db_manager.execute_query(sql_query)

            # Get query metrics
            metrics = self.validator.estimate_query_cost(sql_query)

            return {
                "success": True,
                "question": question,
                "sql": sql_query,
                "results": results,
                "row_count": len(results),
                "metrics": metrics
            }

        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {
                "success": False,
                "question": question,
                "error": str(e),
                "sql": sql_query if 'sql_query' in locals() else None
            }

    def explain_sql(self, sql: str) -> str:
        """Get explanation of SQL query.

        Args:
            sql: SQL query to explain

        Returns:
            Natural language explanation
        """
        return self.sql_generator.explain_query(sql)

    def get_suggestions(self, question: str, num_suggestions: int = 3) -> List[str]:
        """Get suggested related questions.

        Args:
            question: Original question
            num_suggestions: Number of suggestions

        Returns:
            List of suggested questions
        """
        return self.sql_generator.suggest_similar_questions(question, num_suggestions)

    def get_schema_info(self) -> str:
        """Get formatted schema information.

        Returns:
            Formatted schema string
        """
        return self.schema_loader.format_schema_for_llm()

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a specific table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information
        """
        table_def = self.schema_loader.get_table(table_name)
        if not table_def:
            raise ValueError(f"Table '{table_name}' not found in schema")

        schema = self.db_manager.get_table_schema(table_name)
        sample_data = self.db_manager.get_sample_data(table_name, limit=5)

        return {
            "name": table_name,
            "description": table_def.description,
            "columns": [{"name": col.name, "type": col.type, "description": col.description}
                       for col in table_def.columns],
            "schema": schema,
            "sample_data": sample_data
        }

    def list_tables(self) -> List[str]:
        """Get list of all tables.

        Returns:
            List of table names
        """
        return self.schema_loader.get_all_tables()

    def close(self) -> None:
        """Close database connection and cleanup resources."""
        logger.info("Closing Text-to-SQL system")
        self.db_manager.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
