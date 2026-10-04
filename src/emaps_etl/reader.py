"""Read from the local data lake (or a copy such as sample_output): Delta tables and JSON files."""

import json
from pathlib import Path

import polars as pl


def table_path(root: str, layer: str, table: str) -> str:
    return str(Path(root, layer, table))


def read_table(uri: str) -> pl.DataFrame:
    return pl.read_delta(uri)


def list_files(folder: str) -> list[str]:
    """All files under a folder, sorted by path; empty when the folder does not exist."""
    return sorted(str(p) for p in Path(folder).rglob("*") if p.is_file())


def read_json(uri: str) -> dict | None:
    """Read a JSON file; None when it does not exist."""
    path = Path(uri)
    return json.loads(path.read_text()) if path.exists() else None
