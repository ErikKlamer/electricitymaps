"""Pipeline settings, read from environment variables (prefix EMAPS_) and a local .env file."""

from functools import lru_cache
from pathlib import Path

import boto3
from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="EMAPS_", env_file=".env", extra="ignore")

    zone: str = "FR"
    # S3 location of the data lake, e.g. "s3://my-bucket". Required.
    storage_uri: str
    api_base_url: str = "https://api.electricitymaps.com/v4"
    # Set locally via .env; when empty the key is read from SSM Parameter Store.
    api_key: SecretStr | None = None
    api_key_parameter: str = "/emaps-etl/electricitymaps/api-key"
    aws_region: str = "eu-central-1"
    # AWS profile for S3 and SSM, e.g. one that assumes the writer role. None = default chain.
    aws_profile: str | None = None
    zones_file: Path = REPO_ROOT / "zones.json"

    @field_validator("storage_uri")
    @classmethod
    def must_be_s3(cls, value: str) -> str:
        if not value.startswith("s3://"):
            raise ValueError(f"EMAPS_STORAGE_URI must be an S3 location (s3://...), got {value!r}")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


def aws_session(settings: Settings) -> boto3.Session:
    return boto3.Session(profile_name=settings.aws_profile, region_name=settings.aws_region)


def get_api_key(settings: Settings) -> str:
    if settings.api_key:
        return settings.api_key.get_secret_value()
    ssm = aws_session(settings).client("ssm")
    response = ssm.get_parameter(Name=settings.api_key_parameter, WithDecryption=True)
    return response["Parameter"]["Value"]
