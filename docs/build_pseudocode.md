# Build pseudocode (scratchpad)

## 1. Directories

```
infra/          terraform
src/emaps_etl/  pipeline code
scripts/        surrogate history, charts, browse
tests/          tests + fixtures (real API responses)
docs/           charts, this file
```

## 2. Terraform

```
manual first:   ssm put-parameter api-key (SecureString)   # keep key out of tf state
bucket          versioned, AES256, ACLs off, HTTPS only, old versions expire 30d
bucket policy   public list + read
writer role     trusted: writer_principal_arns (default: tf caller)
                allow: bucket read/write/delete, ssm get api-key
output          aws profile snippet → ~/.aws/config [profile emaps-writer]
```

## 3. Reusable functions

```
settings        EMAPS_* env / .env; storage_uri must be s3://
get_api_key     .env override else SSM
get_json        GET + retry 5x backoff on 429/5xx/timeout; 401 → fail
writer          path, write_json, delta_options (role creds), table_exists      # S3 only
reader          read_table, list_files, read_json                               # anonymous S3 or local
checks          unique_key, no_missing_hours, percentages_sum_to_100
```

## 4. Layers

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

## 5. High-water mark (Silver)

```
need:   load only Bronze files newer than last successful load; catch up after failures
store:  s3://<bucket>/_state/watermark_silver_<table>.json = {ingested_at}   # 1 per table

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

## 6. Orchestration

```
dagster: bronze → silver (load_new_bronze) → gold
checks:  unique keys (blocking), missing hours (warn), % = 100
schedule every 12h UTC, local `dagster dev`
```

## 7. README

```
summary + diagram → layout → setup (poetry, terraform, aws profile)
→ running + dagster ops → schemas per layer + incremental + data quality
→ samples + charts → design decisions → limitations
keep in sync with every change
```
