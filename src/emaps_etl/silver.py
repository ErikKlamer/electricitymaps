"""Silver layer: flattened, typed and deduplicated Delta tables.

Partitioned by the data timestamp (UTC): year=YYYY/month=MM/day=DD.
Rows are upserted (Delta MERGE) on the natural key, so overlapping API windows and
re-runs never create duplicates; the most recently ingested value wins.
"""

import logging
from datetime import datetime

import polars as pl
from deltalake import DeltaTable

from emaps_etl import bronze, watermark, writer
from emaps_etl.config import Settings

log = logging.getLogger(__name__)

UTC_DATETIME = pl.Datetime("us", "UTC")
PARTITION_COLUMNS = ["year", "month", "day"]

MIX_TABLE = "electricity_mix"
MIX_KEY = ["zone", "datetime_utc", "source"]
MIX_SCHEMA = pl.Schema(
    {
        "zone": pl.String,
        "datetime_utc": UTC_DATETIME,
        "source": pl.String,
        "power_mw": pl.Float64,
        "estimation_method": pl.String,
        "is_estimated": pl.Boolean,
        "updated_at": UTC_DATETIME,
        "ingested_at": UTC_DATETIME,
    }
)
MIX_CONSTRAINTS = {
    "key_not_null": "zone IS NOT NULL AND datetime_utc IS NOT NULL AND source IS NOT NULL",
    "power_non_negative": "power_mw IS NULL OR power_mw >= 0",
}

FLOWS_TABLE = "electricity_flows"
FLOWS_KEY = ["zone", "datetime_utc", "counterpart_zone"]
FLOWS_SCHEMA = pl.Schema(
    {
        "zone": pl.String,
        "datetime_utc": UTC_DATETIME,
        "counterpart_zone": pl.String,
        "import_mw": pl.Float64,
        "export_mw": pl.Float64,
        "updated_at": UTC_DATETIME,
        "ingested_at": UTC_DATETIME,
    }
)
FLOWS_CONSTRAINTS = {
    "key_not_null": (
        "zone IS NOT NULL AND datetime_utc IS NOT NULL AND counterpart_zone IS NOT NULL"
    ),
    "flows_non_negative": "import_mw >= 0 AND export_mw >= 0",
}


def _ts(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def flatten_mix(record: dict) -> pl.DataFrame:
    """One row per zone, hour and source.

    Storage is split into charge and discharge (e.g. `hydro_storage_charge`). The mix's
    import/export totals are left out: the electricity_flows table has them per zone.
    """
    response = record["response"]
    rows = []
    for entry in response["history"]:
        estimation_method = entry.get("estimationMethod")
        base = {
            "zone": response["zone"],
            "datetime_utc": _ts(entry["datetime"]),
            "estimation_method": estimation_method,
            "is_estimated": estimation_method not in (None, "MEASURED"),
            "updated_at": _ts(entry.get("updatedAt")),
            "ingested_at": _ts(record["ingested_at"]),
        }
        for source, value in entry["mix"].items():
            if source == "flows":
                continue
            if isinstance(value, dict):  # storage: {"charge": ..., "discharge": ...}
                for direction, power in value.items():
                    rows.append({**base, "source": f"{source}_{direction}", "power_mw": power})
            else:
                rows.append({**base, "source": source, "power_mw": value})

    df = pl.DataFrame(rows, schema=MIX_SCHEMA)
    return df.with_columns(pl.col("source").str.replace_all(" ", "_"))


def flatten_flows(record: dict) -> pl.DataFrame:
    """One row per zone, hour and neighbouring zone, with import and export in MW."""
    response = record["response"]
    rows = []
    for entry in response["history"]:
        imports, exports = entry.get("import") or {}, entry.get("export") or {}
        for counterpart in sorted(imports.keys() | exports.keys()):
            rows.append(
                {
                    "zone": response["zone"],
                    "datetime_utc": _ts(entry["datetime"]),
                    "counterpart_zone": counterpart,
                    "import_mw": imports.get(counterpart, 0.0),
                    "export_mw": exports.get(counterpart, 0.0),
                    "updated_at": _ts(entry.get("updatedAt")),
                    "ingested_at": _ts(record["ingested_at"]),
                }
            )
    return pl.DataFrame(rows, schema=FLOWS_SCHEMA)


def deduplicate(df: pl.DataFrame, key: list[str]) -> pl.DataFrame:
    """Keep the most recently ingested row per key."""
    return df.sort("ingested_at").unique(subset=key, keep="last", maintain_order=True)


def add_partition_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        year=pl.col("datetime_utc").dt.strftime("%Y"),
        month=pl.col("datetime_utc").dt.strftime("%m"),
        day=pl.col("datetime_utc").dt.strftime("%d"),
    )


def upsert(
    df: pl.DataFrame, table: str, key: list[str], constraints: dict[str, str], settings: Settings
) -> None:
    """Create the Delta table on first write, otherwise MERGE on the key."""
    uri = writer.path(settings, "silver", table)

    if not writer.table_exists(uri):
        df.write_delta(
            uri,
            mode="error",
            delta_write_options={"partition_by": PARTITION_COLUMNS, "name": table},
        )
        DeltaTable(uri).alter.add_constraint(constraints)
    else:
        (
            df.write_delta(
                uri,
                mode="merge",
                delta_merge_options={
                    "predicate": " AND ".join(f"t.{column} = s.{column}" for column in key),
                    "source_alias": "s",
                    "target_alias": "t",
                },
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
    log.info("Upserted %d rows into silver/%s", len(df), table)


def update_mix(records: list[dict], settings: Settings) -> None:
    df = pl.concat([flatten_mix(record) for record in records])
    df = add_partition_columns(deduplicate(df, MIX_KEY))
    upsert(df, MIX_TABLE, MIX_KEY, MIX_CONSTRAINTS, settings)


def update_flows(records: list[dict], settings: Settings) -> None:
    df = pl.concat([flatten_flows(record) for record in records])
    df = add_partition_columns(deduplicate(df, FLOWS_KEY))
    upsert(df, FLOWS_TABLE, FLOWS_KEY, FLOWS_CONSTRAINTS, settings)


def load_new_bronze(table: str, settings: Settings) -> int:
    """Load the Bronze files ingested since the table's watermark. Returns the number of files.

    The watermark is moved only after a successful MERGE. If that update fails, the next run
    loads the same files again, which is harmless because the MERGE is idempotent.
    """
    after = watermark.read(table, settings)
    records = bronze.records_since(table, after, settings)
    if not records:
        log.info("silver/%s: no new Bronze files since %s", table, after)
        return 0

    update = update_mix if table == MIX_TABLE else update_flows
    update(records, settings)
    latest = max(datetime.fromisoformat(record["ingested_at"]) for record in records)
    watermark.write(table, latest, settings)
    log.info("silver/%s: loaded %d Bronze files, watermark now %s", table, len(records), latest)
    return len(records)
