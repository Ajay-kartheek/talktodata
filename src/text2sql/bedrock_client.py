"""AWS Bedrock client for Claude LLM interactions."""

import json
import logging
import re
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)


class BedrockClient:
    """Client for AWS Bedrock Claude API."""

    def __init__(
        self,
        model_id: str,
        region: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 4000
    ):
        """Initialize Bedrock client.

        Args:
            model_id: Bedrock model identifier
            region: AWS region
            aws_access_key_id: AWS access key (optional, uses default credentials if not provided)
            aws_secret_access_key: AWS secret key (optional)
            temperature: Model temperature (0.0 for deterministic)
            max_tokens: Maximum tokens to generate
        """
        self.model_id = model_id
        self.temperature = temperature
        self.max_tokens = max_tokens

        # Initialize Bedrock runtime client
        try:
            session_kwargs = {"region_name": region}
            if aws_access_key_id and aws_secret_access_key:
                session_kwargs["aws_access_key_id"] = aws_access_key_id
                session_kwargs["aws_secret_access_key"] = aws_secret_access_key

            session = boto3.Session(**session_kwargs)
            self.client = session.client("bedrock-runtime")
            logger.info(f"Initialized Bedrock client with model {model_id} in {region}")
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise

    def generate_sql(
        self,
        system_prompt: str,
        user_prompt: str,
        retry_count: int = 3
    ) -> str:
        """Generate SQL query using Claude.

        Args:
            system_prompt: System prompt with schema and instructions
            user_prompt: User's natural language question
            retry_count: Number of retries for transient failures

        Returns:
            Generated SQL query
        """
        for attempt in range(retry_count):
            try:
                logger.info(f"Generating SQL (attempt {attempt + 1}/{retry_count})")

                response = self._call_bedrock_converse(system_prompt, user_prompt)
                sql_query = self._extract_sql_from_response(response)

                logger.info(f"Successfully generated SQL: {sql_query}")
                return sql_query

            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')

                if error_code == 'ThrottlingException':
                    logger.warning(f"Throttled by Bedrock API, retry {attempt + 1}/{retry_count}")
                    if attempt < retry_count - 1:
                        import time
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                else:
                    logger.error(f"Bedrock API error: {e}")
                    raise

            except Exception as e:
                logger.error(f"Error generating SQL: {e}")
                raise

        raise Exception("Failed to generate SQL after all retries")

    def _call_bedrock_converse(self, system_prompt: str, user_prompt: str) -> str:
        """Call Bedrock Converse API.

        Args:
            system_prompt: System prompt
            user_prompt: User prompt

        Returns:
            Model response text
        """
        try:
            response = self.client.converse(
                modelId=self.model_id,
                messages=[
                    {
                        "role": "user",
                        "content": [{"text": user_prompt}]
                    }
                ],
                system=[{"text": system_prompt}],
                inferenceConfig={
                    "temperature": self.temperature,
                    "maxTokens": self.max_tokens
                }
            )

            # Extract text from response
            output_message = response.get('output', {}).get('message', {})
            content = output_message.get('content', [])

            if content and len(content) > 0:
                return content[0].get('text', '')

            raise Exception("No content in Bedrock response")

        except Exception as e:
            logger.error(f"Bedrock API call failed: {e}")
            raise

    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL query from Claude's response.

        Args:
            response: Raw response from Claude

        Returns:
            Extracted SQL query
        """
        # Try to extract SQL from markdown code block
        sql_pattern = r"```sql\s*(.*?)\s*```"
        match = re.search(sql_pattern, response, re.DOTALL | re.IGNORECASE)

        if match:
            sql = match.group(1).strip()
            logger.debug(f"Extracted SQL from markdown code block")
            return sql

        # Try generic code block
        code_pattern = r"```\s*(.*?)\s*```"
        match = re.search(code_pattern, response, re.DOTALL)

        if match:
            sql = match.group(1).strip()
            logger.debug(f"Extracted SQL from generic code block")
            return sql

        # If no code block, try to find SELECT/WITH/INSERT/UPDATE/DELETE statements
        sql_keywords = r"^\s*(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE)"
        lines = response.split('\n')

        for i, line in enumerate(lines):
            if re.match(sql_keywords, line.strip(), re.IGNORECASE):
                # Found start of SQL, extract until end or next non-SQL line
                sql_lines = []
                for j in range(i, len(lines)):
                    stripped = lines[j].strip()
                    if stripped:
                        sql_lines.append(stripped)
                    elif sql_lines:  # Empty line after we've collected some SQL
                        break

                sql = ' '.join(sql_lines)
                logger.debug(f"Extracted SQL by keyword detection")
                return sql

        # Fallback: return cleaned response
        sql = response.strip()
        logger.warning(f"Could not extract SQL with patterns, returning cleaned response")
        return sql

    def chat(
        self,
        system_prompt: str,
        user_prompt: str
    ) -> str:
        """General chat with Claude (non-SQL generation).

        Args:
            system_prompt: System prompt
            user_prompt: User prompt

        Returns:
            Model response
        """
        try:
            logger.info("Calling Bedrock for general chat")
            response = self._call_bedrock_converse(system_prompt, user_prompt)
            return response
        except Exception as e:
            logger.error(f"Chat failed: {e}")
            raise
