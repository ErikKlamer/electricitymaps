"""Gold layer: daily, analytics-ready data products built from Silver.

Days are UTC days. Hourly values are average power in MW over one hour, so the sum of the
hourly values of a day is the energy in MWh.

The tables are small (one row per day and source/zone), so they are fully rebuilt from Silver
on every run and partitioned by year.
"""

import json
import logging

import polars as pl
from deltalake import DeltaTable

from emaps_etl import storage
from emaps_etl.config import Settings

log = logging.getLogger(__name__)

DAILY_MIX_TABLE = "daily_relative_mix"
IMPORTS_TABLE = "fr_daily_imports"
EXPORTS_TABLE = "fr_daily_exports"


def load_zones(settings: Settings) -> pl.DataFrame:
    """Zone metadata (name, country) from zones.json, a snapshot of the API's /v4/zones."""
    zones = json.loads(settings.zones_file.read_text())
    return pl.DataFrame(
        [
            {"zone": key, "zone_name": zone["zoneName"], "country_code": zone["countryCode"]}
            for key, zone in zones.items()
        ]
    )


def _add_day_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Reference timestamps of the UTC day the row belongs to, plus the partition column."""
    period_start = pl.col("date_utc").cast(pl.Datetime("us", "UTC"))
    return df.with_columns(
        period_start_utc=period_start,
        period_end_utc=period_start + pl.duration(days=1),
        year=pl.col("date_utc").dt.strftime("%Y"),
    )


def daily_relative_mix(mix: pl.DataFrame, zones: pl.DataFrame) -> pl.DataFrame:
    """Percentage contribution of each energy source to the daily production.

    Storage charging is consumption, not production, so `*_charge` sources are excluded.
    Missing (null) values count as 0 MWh.
    """
    production = mix.filter(~pl.col("source").str.ends_with("_charge")).with_columns(
        date_utc=pl.col("datetime_utc").dt.date(),
        power_mw=pl.col("power_mw").fill_null(0.0),
    )
    daily = production.group_by("zone", "date_utc", "source").agg(
        energy_mwh=pl.col("power_mw").sum(),
        hours_covered=pl.col("datetime_utc").n_unique(),
        surrogate_hours=pl.col("is_surrogate").sum().cast(pl.Int32),
    )
    total = pl.col("energy_mwh").sum().over("zone", "date_utc")
    daily = daily.with_columns(
        percentage=pl.when(total > 0).then(pl.col("energy_mwh") / total * 100),
    )
    return (
        _add_day_columns(daily)
        .join(zones, on="zone", how="left")
        .select(
            "date_utc",
            "period_start_utc",
            "period_end_utc",
            "hours_covered",
            "surrogate_hours",
            "zone",
            "zone_name",
            "country_code",
            "source",
            "energy_mwh",
            "percentage",
            "year",
        )
        .sort("date_utc", "zone", "source")
    )


def daily_net_flows(flows: pl.DataFrame) -> pl.DataFrame:
    """Daily import, export and net import (import - export) in MWh per neighbouring zone."""
    return (
        flows.with_columns(date_utc=pl.col("datetime_utc").dt.date())
        .group_by("zone", "counterpart_zone", "date_utc")
        .agg(
            import_mwh=pl.col("import_mw").sum(),
            export_mwh=pl.col("export_mw").sum(),
            hours_covered=pl.col("datetime_utc").n_unique(),
            surrogate_hours=pl.col("is_surrogate").sum().cast(pl.Int32),
        )
        .with_columns(net_import_mwh=pl.col("import_mwh") - pl.col("export_mwh"))
    )


def _flow_table(net_flows: pl.DataFrame, zones: pl.DataFrame, direction: str) -> pl.DataFrame:
    """Rows with a net flow in one direction, as from_zone -> to_zone with a positive net_mwh."""
    if direction == "import":
        df = net_flows.filter(pl.col("net_import_mwh") > 0).select(
            "date_utc",
            "hours_covered",
            "surrogate_hours",
            from_zone=pl.col("counterpart_zone"),
            to_zone=pl.col("zone"),
            net_mwh=pl.col("net_import_mwh"),
        )
    else:
        df = net_flows.filter(pl.col("net_import_mwh") < 0).select(
            "date_utc",
            "hours_covered",
            "surrogate_hours",
            from_zone=pl.col("zone"),
            to_zone=pl.col("counterpart_zone"),
            net_mwh=-pl.col("net_import_mwh"),
        )

    zone_names = zones.select("zone", "zone_name", "country_code")
    return (
        _add_day_columns(df)
        .join(zone_names.rename(lambda c: f"from_{c}"), on="from_zone", how="left")
        .join(zone_names.rename(lambda c: f"to_{c}"), on="to_zone", how="left")
        .select(
            "date_utc",
            "period_start_utc",
            "period_end_utc",
            "hours_covered",
            "surrogate_hours",
            "from_zone",
            "from_zone_name",
            "from_country_code",
            "to_zone",
            "to_zone_name",
            "to_country_code",
            "net_mwh",
            "year",
        )
        .sort("date_utc", "from_zone", "to_zone")
    )


def daily_imports(flows: pl.DataFrame, zones: pl.DataFrame) -> pl.DataFrame:
    """Net imports into France per source zone and day."""
    return _flow_table(daily_net_flows(flows), zones, "import")


def daily_exports(flows: pl.DataFrame, zones: pl.DataFrame) -> pl.DataFrame:
    """Net exports from France per destination zone and day."""
    return _flow_table(daily_net_flows(flows), zones, "export")


def write(df: pl.DataFrame, table: str, constraints: dict[str, str], settings: Settings) -> None:
    """Overwrite the Gold table; constraints are added when the table is created."""
    uri = storage.path(settings, "gold", table)
    options = storage.delta_options(settings)
    is_new = not storage.table_exists(uri, settings)

    df.write_delta(
        uri,
        mode="overwrite",
        storage_options=options,
        delta_write_options={"partition_by": ["year"], "name": table},
    )
    if is_new:
        DeltaTable(uri, storage_options=options).alter.add_constraint(constraints)
    log.info("Wrote %d rows to gold/%s", len(df), table)


def build_daily_relative_mix(settings: Settings) -> None:
    mix = storage.read_table(storage.path(settings, "silver", "electricity_mix"), settings)
    write(
        daily_relative_mix(mix, load_zones(settings)),
        DAILY_MIX_TABLE,
        {"percentage_range": "percentage IS NULL OR (percentage >= 0 AND percentage <= 100)"},
        settings,
    )


def build_daily_imports(settings: Settings) -> None:
    flows = storage.read_table(storage.path(settings, "silver", "electricity_flows"), settings)
    write(
        daily_imports(flows, load_zones(settings)),
        IMPORTS_TABLE,
        {"net_positive": "net_mwh > 0"},
        settings,
    )


def build_daily_exports(settings: Settings) -> None:
    flows = storage.read_table(storage.path(settings, "silver", "electricity_flows"), settings)
    write(
        daily_exports(flows, load_zones(settings)),
        EXPORTS_TABLE,
        {"net_positive": "net_mwh > 0"},
        settings,
    )
