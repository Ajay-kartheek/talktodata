"""SQL generator orchestrating the NL-to-SQL pipeline."""

import logging
from typing import Optional

from .bedrock_client import BedrockClient
from .prompt_builder import PromptBuilder
from .schema_loader import SchemaLoader


logger = logging.getLogger(__name__)


class SQLGenerator:
    """Orchestrates the natural language to SQL conversion pipeline."""

    def __init__(
        self,
        bedrock_client: BedrockClient,
        schema_loader: SchemaLoader
    ):
        """Initialize SQL generator.

        Args:
            bedrock_client: Bedrock client for LLM calls
            schema_loader: Schema loader with database schema
        """
        self.bedrock_client = bedrock_client
        self.schema_loader = schema_loader
        self.prompt_builder = PromptBuilder(schema_loader)

    def generate(
        self,
        question: str,
        include_examples: bool = True,
        custom_instructions: Optional[str] = None
    ) -> str:
        """Generate SQL query from natural language question.

        Args:
            question: Natural language question
            include_examples: Whether to include examples in prompt
            custom_instructions: Additional instructions for the LLM

        Returns:
            Generated SQL query
        """
        try:
            logger.info(f"Generating SQL for question: {question}")

            # Build prompts
            system_prompt = self.prompt_builder.build_system_prompt(
                include_examples=include_examples,
                custom_instructions=custom_instructions
            )
            user_prompt = self.prompt_builder.build_user_prompt(question)

            # Log prompts for debugging
            logger.debug(f"System prompt length: {len(system_prompt)} chars")
            logger.debug(f"User prompt: {user_prompt}")

            # Generate SQL
            sql_query = self.bedrock_client.generate_sql(
                system_prompt=system_prompt,
                user_prompt=user_prompt
            )

            # Check for error responses
            if sql_query.startswith("ERROR:"):
                logger.warning(f"LLM returned error: {sql_query}")
                raise ValueError(sql_query)

            # Clean up the query
            sql_query = self._clean_sql(sql_query)

            logger.info(f"Successfully generated SQL: {sql_query[:100]}...")
            return sql_query

        except Exception as e:
            logger.error(f"SQL generation failed: {e}")
            raise

    def generate_with_retry(
        self,
        question: str,
        error_message: str,
        original_sql: str,
        max_retries: int = 2
    ) -> str:
        """Retry SQL generation after validation failure.

        Args:
            question: Original question
            error_message: Error from validation
            original_sql: SQL that failed validation
            max_retries: Maximum retry attempts

        Returns:
            Corrected SQL query
        """
        logger.info(f"Retrying SQL generation due to error: {error_message}")

        try:
            # Build validation fix prompt
            system_prompt = self.prompt_builder.build_system_prompt(include_examples=True)
            user_prompt = self.prompt_builder.build_validation_prompt(
                question=question,
                sql=original_sql,
                error=error_message
            )

            # Generate corrected SQL
            sql_query = self.bedrock_client.generate_sql(
                system_prompt=system_prompt,
                user_prompt=user_prompt
            )

            sql_query = self._clean_sql(sql_query)
            logger.info(f"Retry successful: {sql_query[:100]}...")
            return sql_query

        except Exception as e:
            logger.error(f"Retry failed: {e}")
            raise

    def explain_query(self, sql_query: str) -> str:
        """Get natural language explanation of SQL query.

        Args:
            sql_query: SQL query to explain

        Returns:
            Natural language explanation
        """
        try:
            logger.info("Generating query explanation")

            system_prompt = (
                "You are a SQL expert. Explain the following SQL query in simple, "
                "natural language. Break down what data it retrieves, how it filters "
                "and transforms the data, and what the result represents."
            )

            user_prompt = f"Explain this SQL query:\n\n{sql_query}"

            explanation = self.bedrock_client.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt
            )

            return explanation

        except Exception as e:
            logger.error(f"Query explanation failed: {e}")
            raise

    def suggest_similar_questions(self, question: str, num_suggestions: int = 3) -> list:
        """Suggest similar questions based on schema.

        Args:
            question: Original question
            num_suggestions: Number of suggestions to generate

        Returns:
            List of suggested questions
        """
        try:
            logger.info("Generating question suggestions")

            schema_summary = self.schema_loader.get_table_summary()

            system_prompt = (
                f"You are a data analyst. Based on the following database schema, "
                f"suggest {num_suggestions} similar or related questions that could be "
                f"answered with this data.\n\n{schema_summary}"
            )

            user_prompt = f"Original question: {question}\n\nSuggest related questions:"

            response = self.bedrock_client.chat(
                system_prompt=system_prompt,
                user_prompt=user_prompt
            )

            # Parse suggestions (assuming they're returned as numbered list)
            suggestions = []
            for line in response.split('\n'):
                line = line.strip()
                if line and (line[0].isdigit() or line.startswith('-')):
                    # Remove numbering/bullets
                    cleaned = line.lstrip('0123456789.-) ')
                    if cleaned:
                        suggestions.append(cleaned)

            return suggestions[:num_suggestions]

        except Exception as e:
            logger.error(f"Question suggestion failed: {e}")
            return []

    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL query.

        Args:
            sql: Raw SQL query

        Returns:
            Cleaned SQL query
        """
        # Remove leading/trailing whitespace
        sql = sql.strip()

        # Remove semicolon at end if present
        if sql.endswith(';'):
            sql = sql[:-1]

        # Remove any markdown artifacts that might have slipped through
        sql = sql.replace('```sql', '').replace('```', '')

        # Normalize whitespace
        sql = ' '.join(sql.split())

        return sql
