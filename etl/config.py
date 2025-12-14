from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # This will read from env var ALPHA_VANTAGE_API_KEY
    # ... simply means it's required
    alpha_vantage_api_key: str = Field(..., env="ALPHA_VANTAGE_API_KEY")

    # Optional settings with defaults
    alpha_vantage_timeout: float = 10.0  # seconds

    # Global config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # ignore unknown env vars
    )


try:
    settings = Settings()
except ValidationError as e:
    raise RuntimeError(f"Configuration error: {e}")
