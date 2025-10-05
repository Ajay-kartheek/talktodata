"""Prompt builder for Text-to-SQL system."""

import logging
from typing import List, Optional

from .schema_loader import SchemaLoader


logger = logging.getLogger(__name__)


class PromptBuilder:
    """Builds prompts for SQL generation using Claude."""

    def __init__(self, schema_loader: SchemaLoader):
        """Initialize prompt builder.

        Args:
            schema_loader: Schema loader instance
        """
        self.schema_loader = schema_loader

    def build_system_prompt(
        self,
        include_examples: bool = True,
        custom_instructions: Optional[str] = None
    ) -> str:
        """Build system prompt with schema and instructions.

        Args:
            include_examples: Whether to include example queries
            custom_instructions: Additional custom instructions

        Returns:
            Complete system prompt
        """
        sections = []

        # Introduction
        sections.append(
            "You are an expert SQL query generator. Your task is to convert natural language "
            "questions into valid DuckDB SQL queries based on the provided database schema."
        )

        # Schema information
        sections.append("\n" + self.schema_loader.format_schema_for_llm())

        # DuckDB-specific instructions
        sections.append(self._get_duckdb_instructions())

        # Query generation rules
        sections.append(self._get_query_rules())

        # Examples
        if include_examples:
            sections.append(self._get_examples())

        # Custom instructions
        if custom_instructions:
            sections.append(f"\n## Additional Instructions\n{custom_instructions}")

        # Output format
        sections.append(self._get_output_format())

        return "\n\n".join(sections)

    def build_user_prompt(self, question: str) -> str:
        """Build user prompt with the question.

        Args:
            question: Natural language question

        Returns:
            User prompt
        """
        return f"Generate a SQL query to answer this question:\n\n{question}"

    def _get_duckdb_instructions(self) -> str:
        """Get DuckDB-specific syntax instructions."""
        return """## DuckDB Syntax Guidelines

DuckDB is a SQL database engine with some specific syntax features:

- Use LIMIT for row limiting, not TOP
- String concatenation: Use || operator or CONCAT() function
- Date functions: DATE_TRUNC(), DATE_DIFF(), CURRENT_DATE
- String matching: Use LIKE, ILIKE (case-insensitive), or SIMILAR TO for patterns
- Aggregations: Standard functions (COUNT, SUM, AVG, MIN, MAX)
- Window functions: ROW_NUMBER(), RANK(), DENSE_RANK(), etc.
- CTEs: Supported with WITH clause
- Subqueries: Fully supported
- CASE expressions: Standard SQL syntax
- Type casting: Use CAST() or :: operator (e.g., column::INTEGER)
"""

    def _get_query_rules(self) -> str:
        """Get query generation rules."""
        return """## Query Generation Rules

1. **Always return valid SQL**: The query must be executable in DuckDB
2. **Use proper table/column names**: Exactly match the schema (case-sensitive)
3. **Handle JOINs intelligently**:
   - Use foreign key relationships from the schema
   - Choose appropriate join types (INNER, LEFT, RIGHT, FULL)
   - Always specify join conditions
4. **Apply filters appropriately**: Use WHERE clauses for row filtering
5. **Use aggregations when needed**: GROUP BY for aggregated queries
6. **Sort results meaningfully**: Add ORDER BY when ordering makes sense
7. **Limit results reasonably**: Use LIMIT for queries that might return many rows
8. **Handle NULL values**: Consider NULL handling in comparisons
9. **Use table aliases**: For readability in multi-table queries
10. **Avoid SELECT ***: Specify columns when possible, unless all columns are needed
11. **Be case-sensitive**: Column and table names should match schema exactly
12. **Use single quotes for strings**: DuckDB uses single quotes for string literals
"""

    def _get_examples(self) -> str:
        """Get few-shot examples."""
        return """## Examples

Example 1:
Question: "How many customers are there?"
SQL: SELECT COUNT(*) as customer_count FROM customers;

Example 2:
Question: "Show me the top 5 most expensive products"
SQL: SELECT name, price FROM products ORDER BY price DESC LIMIT 5;

Example 3:
Question: "What is the total revenue per customer?"
SQL:
SELECT
    c.customer_id,
    c.name,
    SUM(o.total_amount) as total_revenue
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_revenue DESC;

Example 4:
Question: "Which products have never been ordered?"
SQL:
SELECT p.product_id, p.name
FROM products p
LEFT JOIN order_items oi ON p.product_id = oi.product_id
WHERE oi.product_id IS NULL;

Example 5:
Question: "Show monthly sales for the last 6 months"
SQL:
SELECT
    DATE_TRUNC('month', order_date) as month,
    SUM(total_amount) as monthly_sales
FROM orders
WHERE order_date >= CURRENT_DATE - INTERVAL '6 months'
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month DESC;
"""

    def _get_output_format(self) -> str:
        """Get output format instructions."""
        return """## Output Format

Return ONLY the SQL query without any additional explanation or markdown formatting.
The query should be production-ready and executable as-is.

If the question cannot be answered with the available schema, respond with:
ERROR: [Brief explanation of why the query cannot be generated]
"""

    def build_clarification_prompt(self, question: str, issue: str) -> str:
        """Build prompt for clarifying ambiguous questions.

        Args:
            question: Original question
            issue: Description of the issue/ambiguity

        Returns:
            Clarification prompt
        """
        return f"""The following question is ambiguous or unclear:

Question: {question}

Issue: {issue}

Available tables: {', '.join(self.schema_loader.get_all_tables())}

Please suggest:
1. What information is needed to clarify this question
2. Possible interpretations of the question
3. Example rephrased questions that would be clearer
"""

    def build_validation_prompt(self, question: str, sql: str, error: str) -> str:
        """Build prompt for fixing invalid SQL.

        Args:
            question: Original question
            sql: Generated SQL that failed
            error: Error message from validation

        Returns:
            Validation fix prompt
        """
        return f"""The following SQL query has an error:

Question: {question}
SQL: {sql}
Error: {error}

Please provide a corrected SQL query that:
1. Fixes the error
2. Still answers the original question
3. Uses only tables and columns from the schema
4. Is valid DuckDB syntax

Return only the corrected SQL query.
"""
