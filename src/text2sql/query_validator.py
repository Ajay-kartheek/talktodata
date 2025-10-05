"""Query validator for SQL safety and correctness checks."""

import logging
import re
from typing import List, Optional, Tuple

from .duckdb_manager import DuckDBManager
from .schema_loader import SchemaLoader


logger = logging.getLogger(__name__)


class QueryValidator:
    """Validates SQL queries for safety and correctness."""

    def __init__(
        self,
        db_manager: DuckDBManager,
        schema_loader: SchemaLoader
    ):
        """Initialize query validator.

        Args:
            db_manager: DuckDB manager for syntax validation
            schema_loader: Schema loader for schema validation
        """
        self.db_manager = db_manager
        self.schema_loader = schema_loader

        # Dangerous SQL patterns to block
        self.dangerous_patterns = [
            r'\bDROP\b',
            r'\bDELETE\b.*\bWHERE\s+1\s*=\s*1\b',
            r'\bTRUNCATE\b',
            r'\bALTER\b',
            r'\bCREATE\b',
            r'\bGRANT\b',
            r'\bREVOKE\b',
            r';.*;\s*',  # Multiple statements
        ]

    def validate(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate SQL query comprehensively.

        Args:
            sql: SQL query to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Check for dangerous patterns
            is_safe, safety_error = self._check_safety(sql)
            if not is_safe:
                return False, safety_error

            # Validate syntax
            is_valid_syntax, syntax_error = self._validate_syntax(sql)
            if not is_valid_syntax:
                return False, syntax_error

            # Validate schema references
            is_valid_schema, schema_error = self._validate_schema_references(sql)
            if not is_valid_schema:
                return False, schema_error

            logger.info("Query validation passed")
            return True, None

        except Exception as e:
            error_msg = f"Validation error: {str(e)}"
            logger.error(error_msg)
            return False, error_msg

    def _check_safety(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Check for dangerous SQL patterns.

        Args:
            sql: SQL query to check

        Returns:
            Tuple of (is_safe, error_message)
        """
        sql_upper = sql.upper()

        for pattern in self.dangerous_patterns:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                error = f"Query contains potentially dangerous pattern: {pattern}"
                logger.warning(error)
                return False, error

        return True, None

    def _validate_syntax(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate SQL syntax using DuckDB EXPLAIN.

        Args:
            sql: SQL query to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Use DuckDB's EXPLAIN to validate without executing
            is_valid = self.db_manager.validate_query(sql)

            if not is_valid:
                return False, "Invalid SQL syntax"

            return True, None

        except Exception as e:
            error = f"Syntax validation failed: {str(e)}"
            logger.error(error)
            return False, error

    def _validate_schema_references(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Validate that referenced tables and columns exist in schema.

        Args:
            sql: SQL query to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Extract table names from SQL
            referenced_tables = self._extract_table_names(sql)

            # Check if tables exist in schema
            schema_tables = self.schema_loader.get_all_tables()
            schema_tables_lower = [t.lower() for t in schema_tables]

            for table in referenced_tables:
                if table.lower() not in schema_tables_lower:
                    error = f"Table '{table}' not found in schema. Available tables: {', '.join(schema_tables)}"
                    logger.warning(error)
                    return False, error

            return True, None

        except Exception as e:
            # If extraction fails, we'll let syntax validation catch real errors
            logger.debug(f"Schema reference validation skipped: {e}")
            return True, None

    def _extract_table_names(self, sql: str) -> List[str]:
        """Extract table names from SQL query.

        Args:
            sql: SQL query

        Returns:
            List of table names
        """
        tables = []

        # Pattern for FROM clause
        from_pattern = r'\bFROM\s+([a-zA-Z0-9_]+)'
        from_matches = re.findall(from_pattern, sql, re.IGNORECASE)
        tables.extend(from_matches)

        # Pattern for JOIN clause
        join_pattern = r'\bJOIN\s+([a-zA-Z0-9_]+)'
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
        tables.extend(join_matches)

        # Remove duplicates and common SQL keywords
        sql_keywords = {'select', 'where', 'group', 'order', 'having', 'limit', 'offset'}
        tables = list(set([t for t in tables if t.lower() not in sql_keywords]))

        return tables

    def validate_query_type(self, sql: str, allowed_types: List[str]) -> Tuple[bool, Optional[str]]:
        """Validate that query is of allowed type.

        Args:
            sql: SQL query
            allowed_types: List of allowed query types (SELECT, INSERT, UPDATE, DELETE)

        Returns:
            Tuple of (is_valid, error_message)
        """
        sql_stripped = sql.strip().upper()

        query_type = None
        for qtype in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']:
            if sql_stripped.startswith(qtype):
                query_type = qtype
                break

        if query_type == 'WITH':
            # WITH clause is typically followed by SELECT
            query_type = 'SELECT'

        if not query_type:
            return False, "Unable to determine query type"

        allowed_upper = [t.upper() for t in allowed_types]
        if query_type not in allowed_upper:
            error = f"Query type '{query_type}' not allowed. Allowed types: {', '.join(allowed_types)}"
            logger.warning(error)
            return False, error

        return True, None

    def check_for_injection(self, sql: str) -> Tuple[bool, Optional[str]]:
        """Check for potential SQL injection patterns.

        Args:
            sql: SQL query

        Returns:
            Tuple of (is_safe, warning_message)
        """
        # Common SQL injection patterns
        injection_patterns = [
            r"'\s*OR\s+'1'\s*=\s*'1",
            r"'\s*OR\s+1\s*=\s*1",
            r"--",  # SQL comments
            r"/\*.*\*/",  # Block comments
            r"\bEXEC\b",
            r"\bEXECUTE\b",
            r";\s*DROP",
            r"UNION\s+SELECT",  # Could be legitimate, but suspicious
        ]

        for pattern in injection_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                warning = f"Potential SQL injection pattern detected: {pattern}"
                logger.warning(warning)
                return False, warning

        return True, None

    def estimate_query_cost(self, sql: str) -> dict:
        """Estimate query complexity/cost.

        Args:
            sql: SQL query

        Returns:
            Dictionary with cost metrics
        """
        metrics = {
            "num_joins": len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE)),
            "num_subqueries": sql.count('(SELECT'),
            "has_aggregation": bool(re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\b', sql, re.IGNORECASE)),
            "has_group_by": bool(re.search(r'\bGROUP BY\b', sql, re.IGNORECASE)),
            "has_order_by": bool(re.search(r'\bORDER BY\b', sql, re.IGNORECASE)),
            "has_limit": bool(re.search(r'\bLIMIT\b', sql, re.IGNORECASE)),
            "num_tables": len(self._extract_table_names(sql))
        }

        # Estimate complexity score (simple heuristic)
        score = (
            metrics["num_joins"] * 2 +
            metrics["num_subqueries"] * 3 +
            metrics["has_aggregation"] * 1 +
            metrics["has_group_by"] * 1 +
            metrics["num_tables"]
        )

        metrics["complexity_score"] = score
        metrics["complexity_level"] = (
            "low" if score < 3 else
            "medium" if score < 7 else
            "high"
        )

        return metrics
