"""High-water mark per Silver table: the ingested_at of the last Bronze file loaded into it.

Stored as a small JSON file in the data lake, one per table (the Silver tables are loaded in
parallel): _state/watermark_silver_<table>.json
"""

from datetime import datetime

from emaps_etl import reader, writer
from emaps_etl.config import Settings


def _uri(table: str, settings: Settings) -> str:
    return writer.path(settings, "_state", f"watermark_silver_{table}.json")


def read(table: str, settings: Settings) -> datetime | None:
    """The watermark of a Silver table, or None when nothing has been loaded yet.

    Without a watermark file (first run, or after a rebuild), it is derived once from the
    latest ingested_at in the Silver table itself.
    """
    data = reader.read_json(_uri(table, settings), settings.aws_region)
    if data:
        return datetime.fromisoformat(data["ingested_at"])
    uri = writer.path(settings, "silver", table)
    if writer.table_exists(uri, settings):
        return reader.read_table(uri, settings.aws_region).get_column("ingested_at").max()
    return None


def write(table: str, ingested_at: datetime, settings: Settings) -> None:
    writer.write_json(_uri(table, settings), {"ingested_at": ingested_at.isoformat()}, settings)
