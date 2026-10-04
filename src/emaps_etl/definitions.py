"""Dagster assets, checks and schedule. Start the UI with: dagster dev -m emaps_etl.definitions"""

import dagster as dg

from emaps_etl import bronze, checks, gold, silver, storage
from emaps_etl.config import get_settings


def _read(layer: str, table: str):
    settings = get_settings()
    return storage.read_table(storage.path(settings, layer, table), settings)


# --- Bronze: raw API responses ---


@dg.asset(group_name="bronze")
def bronze_electricity_mix() -> dict:
    return bronze.ingest("electricity_mix", get_settings())


@dg.asset(group_name="bronze")
def bronze_electricity_flows() -> dict:
    return bronze.ingest("electricity_flows", get_settings())


# --- Silver: clean Delta tables ---


@dg.asset(group_name="silver")
def silver_electricity_mix(bronze_electricity_mix: dict) -> None:
    silver.update_mix([bronze_electricity_mix], get_settings())


@dg.asset(group_name="silver")
def silver_electricity_flows(bronze_electricity_flows: dict) -> None:
    silver.update_flows([bronze_electricity_flows], get_settings())


# --- Gold: data products ---


@dg.asset(group_name="gold", deps=[silver_electricity_mix])
def gold_daily_relative_mix() -> None:
    gold.build_daily_relative_mix(get_settings())


@dg.asset(group_name="gold", deps=[silver_electricity_flows])
def gold_fr_daily_imports() -> None:
    gold.build_daily_imports(get_settings())


@dg.asset(group_name="gold", deps=[silver_electricity_flows])
def gold_fr_daily_exports() -> None:
    gold.build_daily_exports(get_settings())


# --- Data quality checks ---
# Blocking checks stop the downstream Gold assets when they fail.


@dg.asset_check(asset=silver_electricity_mix, blocking=True)
def silver_mix_unique_key() -> dg.AssetCheckResult:
    passed, description = checks.unique_key(_read("silver", silver.MIX_TABLE), silver.MIX_KEY)
    return dg.AssetCheckResult(passed=passed, description=description)


@dg.asset_check(asset=silver_electricity_flows, blocking=True)
def silver_flows_unique_key() -> dg.AssetCheckResult:
    passed, description = checks.unique_key(_read("silver", silver.FLOWS_TABLE), silver.FLOWS_KEY)
    return dg.AssetCheckResult(passed=passed, description=description)


@dg.asset_check(asset=silver_electricity_mix)
def silver_mix_no_missing_hours() -> dg.AssetCheckResult:
    # A warning only: gaps are expected when the pipeline did not run for more than 24 hours.
    passed, description = checks.no_missing_hours(_read("silver", silver.MIX_TABLE))
    return dg.AssetCheckResult(
        passed=passed, description=description, severity=dg.AssetCheckSeverity.WARN
    )


@dg.asset_check(asset=gold_daily_relative_mix)
def gold_mix_percentages_sum_to_100() -> dg.AssetCheckResult:
    passed, description = checks.percentages_sum_to_100(_read("gold", gold.DAILY_MIX_TABLE))
    return dg.AssetCheckResult(passed=passed, description=description)


# --- Job and schedule ---

etl_job = dg.define_asset_job("etl", selection=dg.AssetSelection.all())

# Every 12 hours: the API only returns the last 24 hours, so consecutive runs overlap and a
# single missed run does not leave a gap.
etl_schedule = dg.ScheduleDefinition(
    job=etl_job, cron_schedule="0 */12 * * *", execution_timezone="UTC"
)

defs = dg.Definitions(
    assets=[
        bronze_electricity_mix,
        bronze_electricity_flows,
        silver_electricity_mix,
        silver_electricity_flows,
        gold_daily_relative_mix,
        gold_fr_daily_imports,
        gold_fr_daily_exports,
    ],
    asset_checks=[
        silver_mix_unique_key,
        silver_flows_unique_key,
        silver_mix_no_missing_hours,
        gold_mix_percentages_sum_to_100,
    ],
    jobs=[etl_job],
    schedules=[etl_schedule],
)
