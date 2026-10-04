"""Pipeline settings, read from environment variables (prefix EMAPS_) and the repository's .env."""

from functools import lru_cache
from pathlib import Path

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EMAPS_", env_file=REPO_ROOT / ".env", extra="ignore"
    )

    zone: str = "FR"
    # Local folder of the data lake (Bronze, Silver, Gold).
    data_dir: Path = REPO_ROOT / "data"
    api_base_url: str = "https://api.electricitymaps.com/v4"
    # Electricity Maps API key; needed to fetch data (Bronze), not to read the tables.
    api_key: SecretStr | None = None
    zones_file: Path = REPO_ROOT / "zones.json"


@lru_cache
def get_settings() -> Settings:
    return Settings()


def get_api_key(settings: Settings) -> str:
    if not settings.api_key:
        raise ValueError("EMAPS_API_KEY is not set: add it to .env")
    return settings.api_key.get_secret_value()
