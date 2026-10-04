"""Read and write files and Delta tables, on the local filesystem or on S3."""

import json
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from emaps_etl.config import Settings, aws_session


def is_s3(uri: str) -> bool:
    return uri.startswith("s3://")


def path(settings: Settings, *parts: str) -> str:
    """Path inside the storage location, e.g. path(settings, "silver", "electricity_mix")."""
    return "/".join([settings.storage_uri.rstrip("/"), *parts])


def _split_s3(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def write_json(uri: str, data: dict, settings: Settings) -> None:
    body = json.dumps(data, indent=2)
    if is_s3(uri):
        bucket, key = _split_s3(uri)
        s3 = aws_session(settings).client("s3")
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
    else:
        Path(uri).parent.mkdir(parents=True, exist_ok=True)
        Path(uri).write_text(body)


def delta_options(settings: Settings) -> dict[str, str]:
    """Storage options for delta-rs. On S3, credentials come from the configured AWS profile
    (e.g. one that assumes the writer role) or the standard AWS chain."""
    if not is_s3(settings.storage_uri):
        return {}
    credentials = aws_session(settings).get_credentials().get_frozen_credentials()
    options = {
        "AWS_REGION": settings.aws_region,
        "AWS_ACCESS_KEY_ID": credentials.access_key,
        "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
        # The pipeline is the only writer (one Dagster instance), so no locking provider is needed.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if credentials.token:
        options["AWS_SESSION_TOKEN"] = credentials.token
    return options


def table_exists(uri: str, settings: Settings) -> bool:
    return DeltaTable.is_deltatable(uri, storage_options=delta_options(settings))


def read_table(uri: str, settings: Settings) -> pl.DataFrame:
    return pl.read_delta(uri, storage_options=delta_options(settings))
