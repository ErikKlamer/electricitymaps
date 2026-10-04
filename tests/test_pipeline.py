import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest
import requests
from tenacity import wait_none

from emaps_etl import api, checks, gold, silver, storage
from emaps_etl.config import Settings

FIXTURES = Path(__file__).parent / "fixtures"


def load_record(stream: str) -> dict:
    return json.loads((FIXTURES / f"bronze_{stream}.json").read_text())


@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(storage_uri=str(tmp_path), api_key="test-key")


# --- Silver ---


def test_flatten_mix_has_one_row_per_hour_and_source():
    df = silver.flatten_mix(load_record("electricity_mix"))

    assert df.schema == silver.MIX_SCHEMA
    assert df.select(silver.MIX_KEY).n_unique() == len(df)
    assert df["datetime_utc"].n_unique() == 24
    assert "hydro_storage_charge" in df["source"].to_list()
    assert "flows" not in df["source"].to_list()


def test_flatten_flows_has_import_and_export_per_neighbour():
    df = silver.flatten_flows(load_record("electricity_flows"))

    assert df.schema == silver.FLOWS_SCHEMA
    assert df.select(silver.FLOWS_KEY).n_unique() == len(df)
    assert {"ES", "IT-NO"} <= set(df["counterpart_zone"])
    assert (df["import_mw"] >= 0).all() and (df["export_mw"] >= 0).all()


def test_deduplicate_keeps_latest_ingestion():
    df = pl.DataFrame(
        {
            "key": ["a", "a"],
            "value": [1, 2],
            "ingested_at": [datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)],
        }
    )
    assert silver.deduplicate(df, ["key"])["value"].to_list() == [2]


def test_silver_upsert_is_idempotent_and_updates_values(settings):
    record = load_record("electricity_mix")
    silver.update_mix([record], settings)
    silver.update_mix([record], settings)

    uri = storage.path(settings, "silver", silver.MIX_TABLE)
    first = storage.read_table(uri, settings)
    assert len(first) == len(silver.flatten_mix(record))

    # A newer ingestion of the same hour overwrites the value instead of adding a row.
    newer = json.loads(json.dumps(record))
    newer["ingested_at"] = "2026-10-03T21:30:00+00:00"
    newer["response"]["history"][0]["mix"]["nuclear"] = 12345.0
    silver.update_mix([newer], settings)

    updated = storage.read_table(uri, settings)
    assert len(updated) == len(first)
    hour = datetime.fromisoformat(record["response"]["history"][0]["datetime"])
    nuclear = updated.filter(pl.col("datetime_utc") == hour, pl.col("source") == "nuclear")
    assert nuclear["power_mw"].to_list() == [12345.0]


def test_silver_rejects_negative_power(settings):
    record = load_record("electricity_mix")
    silver.update_mix([record], settings)

    bad = json.loads(json.dumps(record))
    bad["response"]["history"][0]["mix"]["nuclear"] = -1.0
    with pytest.raises(Exception, match="failed validation check"):
        silver.update_mix([bad], settings)


# --- Gold ---


def hourly(hour: int) -> datetime:
    return datetime(2026, 10, 2, hour, tzinfo=UTC)


def test_daily_relative_mix_percentages():
    mix = pl.DataFrame(
        {
            "zone": ["FR"] * 6,
            "datetime_utc": [hourly(0), hourly(0), hourly(0), hourly(1), hourly(1), hourly(1)],
            "source": ["nuclear", "wind", "battery_storage_charge"] * 2,
            "power_mw": [300.0, 100.0, 50.0, 300.0, None, 50.0],
            "is_surrogate": [True] * 3 + [False] * 3,
        }
    )
    zones = pl.DataFrame({"zone": ["FR"], "zone_name": ["France"], "country_code": ["FR"]})

    df = gold.daily_relative_mix(mix, zones)

    assert df["source"].to_list() == ["nuclear", "wind"]  # charging is not production
    assert df["energy_mwh"].to_list() == [600.0, 100.0]
    assert df["percentage"].round(2).to_list() == [85.71, 14.29]
    assert df["hours_covered"].to_list() == [2, 2]
    assert df["surrogate_hours"].to_list() == [1, 1]
    assert df["date_utc"].to_list() == [date(2026, 10, 2)] * 2
    assert df["zone_name"].to_list() == ["France"] * 2
    assert checks.percentages_sum_to_100(df)[0]


def test_daily_imports_and_exports_are_netted():
    flows = pl.DataFrame(
        {
            "zone": ["FR"] * 4,
            "datetime_utc": [hourly(0), hourly(1), hourly(0), hourly(1)],
            "counterpart_zone": ["ES", "ES", "DE", "DE"],
            "import_mw": [100.0, 100.0, 10.0, 0.0],
            "export_mw": [30.0, 0.0, 50.0, 60.0],
            "is_surrogate": [False] * 4,
        }
    )
    zones = pl.DataFrame(
        {
            "zone": ["FR", "ES", "DE"],
            "zone_name": ["France", "Spain", "Germany"],
            "country_code": ["FR", "ES", "DE"],
        }
    )

    imports = gold.daily_imports(flows, zones)
    exports = gold.daily_exports(flows, zones)

    assert imports.select("from_zone", "to_zone", "net_mwh").rows() == [("ES", "FR", 170.0)]
    assert exports.select("from_zone", "to_zone", "net_mwh").rows() == [("FR", "DE", 100.0)]
    assert exports["to_zone_name"].to_list() == ["Germany"]


def test_full_pipeline_from_fixtures(settings):
    silver.update_mix([load_record("electricity_mix")], settings)
    silver.update_flows([load_record("electricity_flows")], settings)
    gold.build_daily_relative_mix(settings)
    gold.build_daily_imports(settings)
    gold.build_daily_exports(settings)

    daily_mix = storage.read_table(storage.path(settings, "gold", gold.DAILY_MIX_TABLE), settings)
    assert checks.percentages_sum_to_100(daily_mix)[0]
    for table in (gold.IMPORTS_TABLE, gold.EXPORTS_TABLE):
        df = storage.read_table(storage.path(settings, "gold", table), settings)
        assert (df["net_mwh"] > 0).all()


# --- Checks ---


def test_no_missing_hours_detects_gap():
    complete = pl.DataFrame({"datetime_utc": [hourly(0), hourly(1), hourly(2)]})
    gap = pl.DataFrame({"datetime_utc": [hourly(0), hourly(2)]})

    assert checks.no_missing_hours(complete)[0]
    assert not checks.no_missing_hours(gap)[0]


# --- API ---


def test_api_retries_on_rate_limit(monkeypatch):
    def response(status: int) -> requests.Response:
        r = requests.Response()
        r.status_code, r.url, r._content = status, "https://example.test", b'{"ok": true}'
        return r

    responses = iter([response(429), response(503), response(200)])
    monkeypatch.setattr(api.requests, "get", lambda *args, **kwargs: next(responses))
    monkeypatch.setattr(api.get_json.retry, "wait", wait_none())

    assert api.get_json("https://example.test", {}, "key") == ("https://example.test", {"ok": True})


def test_api_does_not_retry_unauthorized(monkeypatch):
    calls = []

    def get(*args, **kwargs):
        calls.append(1)
        r = requests.Response()
        r.status_code, r.url = 401, "https://example.test"
        return r

    monkeypatch.setattr(api.requests, "get", get)

    with pytest.raises(requests.HTTPError):
        api.get_json("https://example.test", {}, "key")
    assert len(calls) == 1
