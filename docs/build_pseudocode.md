# Build pseudocode (scratchpad)

## 1. Directories

```
src/emaps_etl/  pipeline code
scripts/        surrogate history, charts, browse
tests/          tests + fixtures (real API responses)
docs/           charts, this file
sample_output/  committed excerpt of the data lake
data/           the data lake (local, gitignored)
```

## 2. Reusable functions

```
settings        EMAPS_* env / .env; data_dir default data/, api_key from .env
get_json        GET + retry 5x backoff on 429/5xx/timeout; 401 → fail
writer          path, write_json, table_exists                      # local data lake
reader          read_table, list_files, read_json
checks          unique_key, no_missing_hours, percentages_sum_to_100
```

## 3. Layers

```
BRONZE
  ingest(stream):
    response = get_json(<stream>/history, zone=FR, hourly)      # last 24h only (trial key)
    write {ingested_at, source_url, response}
      → bronze/<stream>/year/month/day(ingestion)/<ingested_at>.json

SILVER
  flatten mix:   1 row per hour × source (storage → _charge/_discharge, skip flows totals)
  flatten flows: 1 row per hour × neighbour (import_mw, export_mw)
  dedupe:        per key keep newest ingested_at
  upsert:        first time create table (partition year/month/day of data) + constraints
                 else MERGE on key (update matched, insert new)

GOLD (full rebuild each run, partition year, UTC days, MW summed = MWh)
  daily_relative_mix:  % per source per day (charging excluded) + zone name
  fr_daily_imports:    net MWh neighbour → FR where net import > 0
  fr_daily_exports:    net MWh FR → neighbour where net import < 0
```

## 4. High-water mark (Silver)

```
need:   load only Bronze files newer than last successful load; catch up after failures
store:  data/_state/watermark_silver_<table>.json = {ingested_at}   # 1 per table

load_new_bronze(table):
  wm = read watermark
       missing → max(ingested_at) in silver table
       no table → none (load all)
  files = bronze day folders from wm date..today, file name timestamp > wm
  none → stop
  MERGE files into silver
  write watermark = newest file                 # only after MERGE succeeded
                                                # fail before → reload next run, MERGE idempotent
```

## 5. Orchestration

```
dagster: bronze → silver (load_new_bronze) → gold
checks:  unique keys (blocking), missing hours (warn), % = 100
schedule every 12h UTC, local `dagster dev`
```

## 6. README

```
summary + diagram → layout → roles (consumer, engineer, new setup) → dagster ops → install tools
→ schemas per layer + incremental + data quality → sample output + charts
→ design decisions → limitations
keep in sync with every change
```
