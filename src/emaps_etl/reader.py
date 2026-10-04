"""Read from the S3 data lake or from a local copy (e.g. sample_data): Delta tables and JSON files.

S3 is read without credentials (the bucket is publicly readable), so only the AWS region is
needed, not the pipeline settings.
"""

import json
from pathlib import Path

import boto3
import polars as pl
from botocore import UNSIGNED
from botocore.config import Config

DEFAULT_REGION = "eu-central-1"


def table_path(root: str, layer: str, table: str) -> str:
    return "/".join([root.rstrip("/"), layer, table])


def read_table(uri: str, region: str = DEFAULT_REGION) -> pl.DataFrame:
    options = {}
    if uri.startswith("s3://"):
        options = {"aws_region": region, "aws_skip_signature": "true"}
    return pl.read_delta(uri, storage_options=options)


def _s3(region: str):
    return boto3.client("s3", region_name=region, config=Config(signature_version=UNSIGNED))


def _split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def list_files(prefix: str, region: str = DEFAULT_REGION) -> list[str]:
    """All files under a folder (S3 prefix or local directory), sorted by path."""
    if not prefix.startswith("s3://"):
        return sorted(str(p) for p in Path(prefix).rglob("*") if p.is_file())
    bucket, key = _split(prefix)
    paginator = _s3(region).get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=key.rstrip("/") + "/")
    return sorted(
        f"s3://{bucket}/{obj['Key']}" for page in pages for obj in page.get("Contents", [])
    )


def read_json(uri: str, region: str = DEFAULT_REGION) -> dict | None:
    """Read a JSON file; None when it does not exist."""
    if not uri.startswith("s3://"):
        path = Path(uri)
        return json.loads(path.read_text()) if path.exists() else None
    bucket, key = _split(uri)
    s3 = _s3(region)
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None
