"""Configuration management for Text-to-SQL system."""

import logging
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # AWS Bedrock Configuration
    aws_region: str = Field(default="us-east-1", validation_alias="AWS_REGION")
    aws_access_key_id: Optional[str] = Field(default=None, validation_alias="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(default=None, validation_alias="AWS_SECRET_ACCESS_KEY")

    bedrock_model_id: str = Field(
        default="us.anthropic.claude-3-5-sonnet-20240620-v1:0",
        validation_alias="BEDROCK_MODEL_ID"
    )
    bedrock_temperature: float = Field(default=0.0, validation_alias="BEDROCK_TEMPERATURE")
    bedrock_max_tokens: int = Field(default=4000, validation_alias="BEDROCK_MAX_TOKENS")

    # DuckDB Configuration
    duckdb_path: str = Field(default=":memory:", validation_alias="DUCKDB_PATH")

    # Logging Configuration
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


def get_settings() -> Settings:
    """Get application settings singleton."""
    return Settings()


def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
