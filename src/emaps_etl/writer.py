"""Write to the local data lake: raw JSON files (Bronze) and Delta tables (Silver, Gold)."""

import json
from pathlib import Path

from deltalake import DeltaTable

from emaps_etl.config import Settings


def path(settings: Settings, *parts: str) -> str:
    """Location inside the data lake, e.g. path(settings, "silver", "electricity_mix")."""
    return str(Path(settings.data_dir, *parts))


def write_json(uri: str, data: dict) -> None:
    Path(uri).parent.mkdir(parents=True, exist_ok=True)
    Path(uri).write_text(json.dumps(data, indent=2))


def table_exists(uri: str) -> bool:
    return DeltaTable.is_deltatable(uri)
