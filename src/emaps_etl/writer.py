"""Write to the S3 data lake: raw JSON files (Bronze) and Delta tables (Silver, Gold).

Writes use the credentials of the configured AWS profile (EMAPS_AWS_PROFILE), e.g. one that
assumes the writer role.
"""

import json

from deltalake import DeltaTable

from emaps_etl.config import Settings, aws_session


def path(settings: Settings, *parts: str) -> str:
    """S3 location inside the data lake, e.g. path(settings, "silver", "electricity_mix")."""
    return "/".join([settings.storage_uri.rstrip("/"), *parts])


def write_json(uri: str, data: dict, settings: Settings) -> None:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    s3 = aws_session(settings).client("s3")
    s3.put_object(
        Bucket=bucket, Key=key, Body=json.dumps(data, indent=2), ContentType="application/json"
    )


def delta_options(settings: Settings) -> dict[str, str]:
    """Storage options for delta-rs, with the credentials of the configured AWS profile."""
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
