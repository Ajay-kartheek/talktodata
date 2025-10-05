"""Schema loader for parsing and managing database schemas."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class ColumnDefinition(BaseModel):
    """Definition of a database column."""

    name: str
    type: str
    description: Optional[str] = None
    nullable: bool = True
    primary_key: bool = False
    foreign_key: Optional[str] = None  # Format: "table_name.column_name"


class TableDefinition(BaseModel):
    """Definition of a database table."""

    name: str
    description: Optional[str] = None
    columns: List[ColumnDefinition]


class DatabaseSchema(BaseModel):
    """Complete database schema definition."""

    database_name: str = "database"
    description: Optional[str] = None
    tables: List[TableDefinition]


class SchemaLoader:
    """Loads and manages database schema information."""

    def __init__(self, schema_path: Optional[str] = None):
        """Initialize schema loader.

        Args:
            schema_path: Path to schema JSON file
        """
        self.schema_path = schema_path
        self.schema: Optional[DatabaseSchema] = None

        if schema_path:
            self.load_from_file(schema_path)

    def load_from_file(self, schema_path: str) -> DatabaseSchema:
        """Load schema from JSON file.

        Args:
            schema_path: Path to schema JSON file

        Returns:
            Loaded database schema
        """
        try:
            path = Path(schema_path)
            if not path.exists():
                raise FileNotFoundError(f"Schema file not found: {schema_path}")

            with open(path, 'r') as f:
                schema_data = json.load(f)

            self.schema = DatabaseSchema(**schema_data)
            logger.info(f"Loaded schema with {len(self.schema.tables)} tables from {schema_path}")
            return self.schema
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            raise

    def load_from_dict(self, schema_data: Dict[str, Any]) -> DatabaseSchema:
        """Load schema from dictionary.

        Args:
            schema_data: Schema definition as dictionary

        Returns:
            Loaded database schema
        """
        try:
            self.schema = DatabaseSchema(**schema_data)
            logger.info(f"Loaded schema with {len(self.schema.tables)} tables from dictionary")
            return self.schema
        except Exception as e:
            logger.error(f"Failed to load schema from dict: {e}")
            raise

    def get_table(self, table_name: str) -> Optional[TableDefinition]:
        """Get table definition by name.

        Args:
            table_name: Name of the table

        Returns:
            Table definition or None if not found
        """
        if not self.schema:
            return None

        for table in self.schema.tables:
            if table.name.lower() == table_name.lower():
                return table
        return None

    def get_all_tables(self) -> List[str]:
        """Get list of all table names.

        Returns:
            List of table names
        """
        if not self.schema:
            return []
        return [table.name for table in self.schema.tables]

    def get_relationships(self) -> List[Dict[str, str]]:
        """Get all foreign key relationships.

        Returns:
            List of relationship definitions
        """
        relationships = []

        if not self.schema:
            return relationships

        for table in self.schema.tables:
            for column in table.columns:
                if column.foreign_key:
                    try:
                        ref_table, ref_column = column.foreign_key.split('.')
                        relationships.append({
                            "from_table": table.name,
                            "from_column": column.name,
                            "to_table": ref_table,
                            "to_column": ref_column
                        })
                    except ValueError:
                        logger.warning(f"Invalid foreign key format: {column.foreign_key}")

        return relationships

    def format_schema_for_llm(self, include_sample_data: bool = False) -> str:
        """Format schema for LLM prompt.

        Args:
            include_sample_data: Whether to include sample data note

        Returns:
            Formatted schema string
        """
        if not self.schema:
            return "No schema loaded."

        lines = []
        lines.append("# Database Schema\n")

        if self.schema.description:
            lines.append(f"{self.schema.description}\n")

        lines.append(f"\nThe database contains {len(self.schema.tables)} tables:\n")

        for table in self.schema.tables:
            lines.append(f"\n## Table: {table.name}")
            if table.description:
                lines.append(f"Description: {table.description}")

            lines.append("\nColumns:")
            for col in table.columns:
                col_def = f"- {col.name} ({col.type})"

                attributes = []
                if col.primary_key:
                    attributes.append("PRIMARY KEY")
                if col.foreign_key:
                    attributes.append(f"FOREIGN KEY -> {col.foreign_key}")
                if not col.nullable:
                    attributes.append("NOT NULL")

                if attributes:
                    col_def += f" [{', '.join(attributes)}]"

                if col.description:
                    col_def += f" - {col.description}"

                lines.append(col_def)

        # Add relationships section
        relationships = self.get_relationships()
        if relationships:
            lines.append("\n## Relationships")
            for rel in relationships:
                lines.append(
                    f"- {rel['from_table']}.{rel['from_column']} -> "
                    f"{rel['to_table']}.{rel['to_column']}"
                )

        return "\n".join(lines)

    def get_table_summary(self) -> str:
        """Get brief summary of all tables.

        Returns:
            Brief table summary
        """
        if not self.schema:
            return "No schema loaded."

        summaries = []
        for table in self.schema.tables:
            cols = ", ".join([col.name for col in table.columns])
            summary = f"{table.name} ({cols})"
            if table.description:
                summary += f" - {table.description}"
            summaries.append(summary)

        return "\n".join(summaries)

    def validate_table_exists(self, table_name: str) -> bool:
        """Check if table exists in schema.

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise
        """
        return self.get_table(table_name) is not None

    def validate_column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if column exists in table.

        Args:
            table_name: Name of the table
            column_name: Name of the column

        Returns:
            True if column exists, False otherwise
        """
        table = self.get_table(table_name)
        if not table:
            return False

        for col in table.columns:
            if col.name.lower() == column_name.lower():
                return True
        return False
