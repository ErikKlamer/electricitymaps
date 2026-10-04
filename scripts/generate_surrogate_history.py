"""Generate 5 years of surrogate (fake) history and rebuild Silver and Gold.

The API key only gives access to the last 24 hours, so history cannot be loaded. This script
fills everything before the most recent real API call with generated data:

    trend (2021 -> 2026 level) x seasonality x daily cycle (solar) x random noise

The data is written as Bronze files in the API's JSON format, marked with a source_url that
starts with "surrogate://", and then processed by the normal pipeline. In Silver the mix rows
have estimation_method = "SURROGATE". Real hours before the last API call are replaced by
surrogate data.

Writes to the local data lake (EMAPS_DATA_DIR, default data/) and replaces its Silver and Gold
tables:
    poetry run python scripts/generate_surrogate_history.py
"""

import json
import math
import random
import shutil
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from emaps_etl import bronze, gold, silver, writer
from emaps_etl.config import get_settings

YEARS = 5
SEED = 42
STREAMS = ["electricity_mix", "electricity_flows"]

# Average power in MW: (level 5 years ago, level today). Linear trend in between.
# Today's levels are based on the real API data for France.
MIX_LEVELS = {
    "nuclear": (38000, 33000),
    "biomass": (900, 1000),
    "coal": (600, 0),
    "wind": (3000, 5500),
    "hydro": (5000, 5000),
    "gas": (4500, 2200),
    "oil": (300, 40),
}
SOLAR_PEAK = (9000, 18000)  # midday output on a clear summer day
STORAGE_LEVELS = {
    "hydro storage": {"charge": (800, 800), "discharge": (600, 600)},
    "battery storage": {"charge": (0, 50), "discharge": (0, 30)},
}
# Net export from France per neighbour today (negative = net import). 5 years ago: 60% of it.
NET_EXPORT_TODAY = {
    "BE": 1800,
    "CH": 1200,
    "DE": 750,
    "ES": 1300,
    "GB": 1450,
    "IT-NO": 2600,
    "LU": 110,
}

# Seasonal amplitude and the day of the year on which the source peaks.
SEASONALITY = {
    "nuclear": (0.12, 15),
    "wind": (0.30, 15),
    "gas": (0.40, 15),
    "hydro": (0.30, 135),
}
# Standard deviation of the multiplicative noise.
NOISE = {"wind": 0.25, "solar": 0.15}
DEFAULT_NOISE = 0.05


def trend(levels: tuple[float, float], progress: float) -> float:
    """Linear interpolation between the old and the current level (progress 0..1)."""
    start, end = levels
    return start + (end - start) * progress


def season(day_of_year: int, amplitude: float, peak_day: int) -> float:
    return 1 + amplitude * math.cos(2 * math.pi * (day_of_year - peak_day) / 365)


def daylight(hour: int, day_of_year: int) -> float:
    """Share of the solar peak at this UTC hour: 0 at night, 1 at noon (solar noon ~12 UTC)."""
    day_length = 12 + 4 * math.sin(2 * math.pi * (day_of_year - 80) / 365)
    position = (hour + 0.5 - (12 - day_length / 2)) / day_length
    return math.sin(math.pi * position) if 0 < position < 1 else 0.0


def noisy(value: float, source: str) -> float:
    return round(max(0.0, value * random.gauss(1, NOISE.get(source, DEFAULT_NOISE))), 2)


def mix_entry(ts: datetime, progress: float) -> dict:
    doy = ts.timetuple().tm_yday
    mix = {}
    for source, levels in MIX_LEVELS.items():
        value = trend(levels, progress)
        if source in SEASONALITY:
            value *= season(doy, *SEASONALITY[source])
        mix[source] = noisy(value, source)
    mix["solar"] = noisy(
        trend(SOLAR_PEAK, progress) * season(doy, 0.3, 172) * daylight(ts.hour, doy), "solar"
    )
    mix["geothermal"] = None
    mix["unknown"] = None
    for storage, directions in STORAGE_LEVELS.items():
        mix[storage] = {
            direction: noisy(trend(levels, progress), storage)
            for direction, levels in directions.items()
        }
    return mix


def flows_entry(ts: datetime, progress: float, day_shift: dict) -> tuple[dict, dict]:
    """Net export per neighbour: trend x season (lower in winter) + daily and hourly deviation.

    The daily deviation makes some days net imports; hourly noise alone averages out per day.
    """
    doy = ts.timetuple().tm_yday
    imports, exports = {}, {}
    for zone, today in NET_EXPORT_TODAY.items():
        level = trend((0.6 * today, today), progress) * season(doy, -0.6, 15)
        net_export = round(level + day_shift[zone] + random.gauss(0, 0.3 * abs(today)), 2)
        if net_export >= 0:
            exports[zone] = net_export
        else:
            imports[zone] = -net_export
    return imports, exports


def surrogate_records(start: datetime, end: datetime, now: datetime) -> list[tuple[str, dict]]:
    """One Bronze record per stream and month, for all hours in [start, end).

    Monthly files keep the number of S3 uploads small (about 120 instead of one per day).
    """
    months: dict[str, dict[str, list]] = {}  # "YYYY-MM" -> stream -> hourly entries
    day = start
    while day < end:
        hours = [day + timedelta(hours=h) for h in range(24) if day + timedelta(hours=h) < end]
        mix_history, flows_history = [], []
        day_shift = {
            zone: random.gauss(0, 0.8 * abs(today)) for zone, today in NET_EXPORT_TODAY.items()
        }
        for ts in hours:
            progress = (ts - start) / (end - start)
            stamp = ts.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            mix = mix_entry(ts, progress)
            imports, exports = flows_entry(ts, progress, day_shift)
            mix["flows"] = {
                "exports": round(sum(exports.values()), 2),
                "imports": round(sum(imports.values()), 2),
            }
            mix_history.append(
                {
                    "datetime": stamp,
                    "updatedAt": now.isoformat(),
                    "estimationMethod": "SURROGATE",
                    "breakdownType": "normal",
                    "mix": mix,
                }
            )
            flows_history.append(
                {
                    "datetime": stamp,
                    "updatedAt": now.isoformat(),
                    "import": imports,
                    "export": exports,
                }
            )

        month = months.setdefault(f"{day:%Y-%m}", {stream: [] for stream in STREAMS})
        month["electricity_mix"].extend(mix_history)
        month["electricity_flows"].extend(flows_history)
        day += timedelta(days=1)

    return [
        (
            stream,
            {
                "ingested_at": now.isoformat(),
                "source_url": f"surrogate://{stream}/{month}",
                "response": {
                    "zone": "FR",
                    "temporalGranularity": "hourly",
                    "unit": "MW",
                    "history": history,
                },
            },
        )
        for month, streams in months.items()
        for stream, history in streams.items()
    ]


def main() -> None:
    settings = get_settings()
    root = Path(settings.data_dir)
    random.seed(SEED)
    now = datetime.now(UTC)

    # Remove surrogate files from a previous run, so the script can be re-run.
    for old in (root / "bronze").rglob("*_surrogate_*.json"):
        old.unlink()
    real = {
        stream: [
            json.loads(f.read_text()) for f in sorted((root / "bronze" / stream).rglob("*.json"))
        ]
        for stream in STREAMS
    }

    # Surrogate history ends where the most recent real API call starts.
    if not real["electricity_mix"]:
        sys.exit(f"No real Bronze data in {root}; run the pipeline first.")
    latest = max(real["electricity_mix"], key=lambda record: record["ingested_at"])
    end = datetime.fromisoformat(latest["response"]["history"][0]["datetime"])
    start = (end - timedelta(days=365 * YEARS)).replace(hour=0)

    surrogate = {stream: [] for stream in STREAMS}
    folder = (f"year={now:%Y}", f"month={now:%m}", f"day={now:%d}")
    for stream, record in surrogate_records(start, end, now):
        month = record["source_url"].rsplit("/", 1)[1].replace("-", "")
        name = f"{now.strftime(bronze.FILE_TIMESTAMP)}_surrogate_{month}.json"
        writer.write_json(writer.path(settings, "bronze", stream, *folder, name), record)
        surrogate[stream].append(record)
    print(f"Wrote surrogate Bronze data from {start} to {end}")

    # Rebuild Silver and Gold from all Bronze records. The most recently ingested value per hour
    # wins, so surrogate data replaces older real hours, and the latest real call stays real.
    for layer in ("silver", "gold", "_state"):  # watermarks in _state are re-derived
        shutil.rmtree(root / layer, ignore_errors=True)
    silver.update_mix(real["electricity_mix"] + surrogate["electricity_mix"], settings)
    silver.update_flows(real["electricity_flows"] + surrogate["electricity_flows"], settings)
    gold.build_daily_relative_mix(settings)
    gold.build_daily_imports(settings)
    gold.build_daily_exports(settings)
    print(f"Rebuilt Silver and Gold in {root}")


if __name__ == "__main__":
    main()
