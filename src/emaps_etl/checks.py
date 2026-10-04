"""Data quality checks that Delta constraints cannot express (they span several rows).

Each check returns (passed, description). Dagster runs them as asset checks.
"""

import polars as pl


def unique_key(df: pl.DataFrame, key: list[str]) -> tuple[bool, str]:
    duplicates = len(df) - df.select(key).n_unique()
    return duplicates == 0, f"{duplicates} duplicate rows on key {key}"


def no_missing_hours(df: pl.DataFrame) -> tuple[bool, str]:
    """Every hour between the first and the last hour in the table is present."""
    hours = df.get_column("datetime_utc").unique().sort()
    if hours.is_empty():
        return True, "table is empty"
    expected = pl.datetime_range(hours.min(), hours.max(), interval="1h", eager=True)
    missing = expected.filter(~expected.is_in(hours.implode()))
    return missing.is_empty(), f"{len(missing)} missing hours: {missing.head(5).to_list()}"


def percentages_sum_to_100(daily_mix: pl.DataFrame) -> tuple[bool, str]:
    """The source percentages of each zone and day add up to 100%."""
    totals = (
        daily_mix.group_by("zone", "date_utc")
        .agg(total=pl.col("percentage").sum())
        .filter((pl.col("total") - 100).abs() > 0.01)
    )
    return totals.is_empty(), f"{len(totals)} days where percentages do not sum to 100"
