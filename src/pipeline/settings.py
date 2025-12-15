from __future__ import annotations

from pathlib import Path

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the data pipeline."""

    alpha_vantage_api_key: str = Field(..., env="ALPHAVANTAGE_API_KEY")
    alpha_vantage_timeout: float = 10.0
    alpha_vantage_min_interval_seconds: float = 15.0  # free tier: 5 calls/min
    max_retries: int = 3
    backoff_seconds: float = 20.0

    output_dir: Path = Path("data")
    docs_data_dir: Path = Path("docs/data")
    raw_data_dir: Path = Path("data/raw")
    ticker_config_path: Path = Path("config/tickers.yaml")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def load_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as exc:  # pragma: no cover - configuration guard
        raise RuntimeError(f"Configuration error: {exc}") from exc

