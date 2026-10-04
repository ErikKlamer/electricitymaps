# Electricity Maps ETL pipeline

ETL pipeline for France's (FR) hourly **electricity mix** and **electricity flows** from the [Electricity Maps API](https://www.electricitymaps.com/), built with Polars, Delta Lake (delta-rs) and Dagster, following the Bronze → Silver → Gold medallion architecture. Data is stored locally or in AWS S3.

```
Electricity Maps API (/v4/electricity-mix/history, /v4/electricity-flows/history)
  │
  ▼
Bronze   raw JSON responses + ingestion metadata        partitioned by ingestion date
  │
  ▼
Silver   flat, typed, deduplicated Delta tables          partitioned by data date
  │        electricity_mix, electricity_flows
  ▼
Gold     daily data products (Delta tables)              partitioned by year
           daily_relative_mix, fr_daily_imports, fr_daily_exports
```

### Project layout

| Path | Contents |
|---|---|
| [src/emaps_etl/config.py](src/emaps_etl/config.py) | Settings (`EMAPS_*` environment variables, `.env`) and API key lookup |
| [src/emaps_etl/api.py](src/emaps_etl/api.py) | API client with retries |
| [src/emaps_etl/storage.py](src/emaps_etl/storage.py) | Local / S3 file and Delta table helpers |
| [src/emaps_etl/bronze.py](src/emaps_etl/bronze.py), [silver.py](src/emaps_etl/silver.py), [gold.py](src/emaps_etl/gold.py) | The three layers |
| [src/emaps_etl/checks.py](src/emaps_etl/checks.py) | Data quality checks |
| [src/emaps_etl/definitions.py](src/emaps_etl/definitions.py) | Dagster assets, checks and schedule |
| [src/emaps_etl/\_\_main\_\_.py](src/emaps_etl/__main__.py) | Run the pipeline once without Dagster |
| [tests/](tests/) | Unit tests, with real API responses as fixtures |
| [zones.json](zones.json) | Zone metadata (name, country), a snapshot of the API's `/v4/zones` |
| [scripts/plot_gold.py](scripts/plot_gold.py), [docs/](docs/) | Charts of the Gold tables (see [Charts](#charts)) |
| [scripts/generate_surrogate_history.py](scripts/generate_surrogate_history.py) | Generates 5 years of surrogate history (see [Surrogate history](#surrogate-history)) |
| `sample_data/` | Sample output: real data plus surrogate history (not committed, ~80 MB; see [Sample outputs](#sample-outputs)) |
| [infra/](infra/) | Terraform for the AWS resources |
| [.github/workflows/ci.yml](.github/workflows/ci.yml) | CI: lint and tests |

## New deployment

Steps to set up a fresh development environment from scratch.

### Prerequisites

- Linux or macOS with `git` and `curl`
- Python 3.12 (`python3.12 --version`)
- Terraform >= 1.6 (`terraform version`), used to provision the AWS infrastructure

#### Install Terraform

Ubuntu / Debian / Linux Mint (HashiCorp apt repository):

```bash
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common
wget -O- https://apt.releases.hashicorp.com/gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
# UBUNTU_CODENAME also resolves correctly on Linux Mint (lsb_release -cs would return the Mint codename)
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") main" \
  | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform
```

macOS (Homebrew):

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
```

Verify:

```bash
terraform version
```

### 1. Clone the repository

```bash
git clone <repo-url> electricitymaps
cd electricitymaps
```

### 2. Install Poetry

Poetry manages the dependencies and the virtual environment. Install it with the official installer:

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

This installs Poetry into `~/.local/bin`. If `poetry` is not found afterwards, add that directory to your `PATH` (e.g. in `~/.bashrc`):

```bash
export PATH="$HOME/.local/bin:$PATH"
poetry --version
```

### 3. Create the virtual environment and install dependencies

The repository's `poetry.toml` sets `virtualenvs.in-project = true`, so the virtual environment is created in `.venv/` inside the repository.

```bash
poetry env use python3.12
poetry install                          # core (incl. Dagster) + dev dependencies
```

Check the environment:

```bash
poetry env info
```

### 4. Use the environment

Either activate the virtual environment:

```bash
source .venv/bin/activate
```

or prefix commands with `poetry run`, e.g. `poetry run pytest`.

In VS Code, select `.venv/bin/python` as the interpreter.

### Dependencies

| Group | Packages | Purpose |
|---|---|---|
| core | `polars`, `deltalake` | Transformations and Delta Lake tables (local and S3) |
| core | `requests`, `tenacity` | API client with retries and backoff |
| core | `pydantic-settings` | Typed configuration from environment variables and `.env` |
| core | `boto3` | AWS access (SSM Parameter Store, S3) |
| core | `dagster`, `dagster-webserver` | Orchestration on the local machine (`dagster dev`) |
| dev | `pytest`, `ruff`, `matplotlib` | Tests, linting and formatting, charts |

Add dependencies with `poetry add <package>` (or `poetry add --group dev <package>`), not with `pip install`, so that `pyproject.toml` and `poetry.lock` stay in sync.

### 5. Provision the AWS infrastructure

Terraform in [infra/](infra/) creates:

| Resource | Name | Notes |
|---|---|---|
| S3 bucket | `emaps-etl-<account_id>-<region>` | Data lake (`bronze/`, `silver/`, `gold/`); **publicly readable**, versioned, SSE-S3 encrypted, ACLs disabled, TLS-only, noncurrent versions expire after 30 days |
| IAM role | `emaps-etl-writer` | Read/write on the whole bucket, read the API key parameter. Can only be assumed by the IAM principals in `writer_principal_arns` (default: the identity that runs Terraform) |

Requirement: an AWS CLI profile with admin rights in the target account.

Store the Electricity Maps API key as an SSM `SecureString` parameter. This is a one-time manual step: the parameter is deliberately not managed by Terraform, because the `aws_ssm_parameter` resource stores the decrypted value in the Terraform state. Terraform only grants the writer role access to it.

```bash
aws ssm put-parameter --profile <profile> --region eu-central-1 \
  --name /emaps-etl/electricitymaps/api-key \
  --type SecureString --overwrite --value "<api-key>"
```

Apply:

```bash
cd infra
cp terraform.tfvars.example terraform.tfvars   # set aws_profile (and optionally writer_principal_arns)
terraform init
terraform plan -out tfplan
terraform apply tfplan
```

The pipeline writes as the writer role. Add a profile that assumes it to `~/.aws/config` (`terraform output aws_config_profile` prints it); `source_profile` is the profile whose user is allowed to assume the role:

```ini
[profile emaps-writer]
role_arn = arn:aws:iam::<account_id>:role/emaps-etl-writer
source_profile = <profile>
region = eu-central-1
```

Profile sections other than `[default]` must be written as `[profile <name>]`; delta-rs ignores sections without the `profile` prefix. Check the role:

```bash
aws sts get-caller-identity --profile emaps-writer   # ...:assumed-role/emaps-etl-writer/...
```

### 6. Read the data (no AWS account needed)

The bucket is publicly readable, so anyone can browse and read the data without credentials:

```bash
aws s3 ls s3://emaps-etl-<account_id>-eu-central-1/ --recursive --no-sign-request
```

## Running the pipeline

### Configuration

Settings are read from environment variables or a `.env` file (see [.env.example](.env.example)):

| Variable | Default | Meaning |
|---|---|---|
| `EMAPS_API_KEY` | (empty) | Electricity Maps API key. When empty, it is read from SSM (`/emaps-etl/electricitymaps/api-key`) |
| `EMAPS_STORAGE_URI` | `data` | Local folder or S3 location, e.g. `s3://emaps-etl-<account_id>-eu-central-1` |
| `EMAPS_ZONE` | `FR` | Electricity Maps zone |
| `EMAPS_AWS_PROFILE` | (empty) | AWS profile for S3 and SSM, e.g. `emaps-writer`. When empty, the standard AWS credential chain is used |

```bash
cp .env.example .env    # and set EMAPS_API_KEY
```

### With Dagster (recommended)

```bash
export DAGSTER_HOME=$PWD/.dagster && mkdir -p $DAGSTER_HOME   # keeps run history between sessions
poetry run dagster dev -m emaps_etl.definitions
```

Open http://localhost:3000:

- **Run everything:** *Jobs → etl → Materialize all*.
- **Schedule:** *Automation → etl_schedule → turn on*. It runs every 12 hours (00:00 and 12:00 UTC) while `dagster dev` is running.
- **Lineage and checks:** *Assets* shows the Bronze → Silver → Gold graph and the data quality check results.

Or run the job once from the command line:

```bash
poetry run dagster job execute -m emaps_etl.definitions -j etl
```

### Without Dagster

```bash
poetry run python -m emaps_etl
```

### Writing to S3

The data lives in the S3 bucket. Set the storage location and the writer profile in `.env`; all runs (Dagster and `python -m emaps_etl`) then write to S3 as the writer role:

```bash
EMAPS_STORAGE_URI=s3://emaps-etl-<account_id>-eu-central-1
EMAPS_AWS_PROFILE=emaps-writer
```

Leave `EMAPS_API_KEY` empty to read the API key from SSM. Runs against S3 take about a minute (the Silver and Gold tables span many small daily partitions), compared to a few seconds locally.

To get a local copy of the data, e.g. for the charts (no credentials needed):

```bash
aws s3 sync s3://emaps-etl-<account_id>-eu-central-1/ sample_data/ --no-sign-request
```

### Tests and linting

```bash
poetry run pytest
poetry run ruff check . && poetry run ruff format --check .
```

The same checks run in GitHub Actions on every push and pull request ([ci.yml](.github/workflows/ci.yml)).

## Data layers and schemas

All timestamps are UTC (see [design decisions](#all-timestamps-and-days-in-utc)). Power is in MW (average over the hour); energy is in MWh.

### Bronze: raw API responses

One JSON file per API call, stored exactly as received with minimal metadata, partitioned by **ingestion** time:

```
bronze/electricity_mix/year=2026/month=10/day=03/20261003T100410487232Z.json
bronze/electricity_flows/year=2026/month=10/day=03/20261003T100410487210Z.json
```

```json
{
  "ingested_at": "2026-10-03T10:04:10.487232+00:00",
  "source_url": "https://api.electricitymaps.com/v4/electricity-mix/history?zone=FR&temporalGranularity=hourly&disableCallerLookup=true",
  "response": { "zone": "FR", "temporalGranularity": "hourly", "unit": "MW", "history": [ ... ] }
}
```

No schema is enforced. The API key is sent as a header and never appears in the stored URL.

### Silver: clean Delta tables

Partitioned by **data** time: `year=YYYY/month=MM/day=DD` (string columns `year`, `month`, `day`). Rows are upserted with a Delta MERGE on the key, so overlapping API windows and re-runs never create duplicates; the most recent ingestion wins.

**`silver/electricity_mix`**: one row per zone, hour and source. Key: `zone, datetime_utc, source`.

| Column | Type | Description |
|---|---|---|
| `zone` | string | Zone key, e.g. `FR` |
| `datetime_utc` | timestamp (UTC) | Start of the hour |
| `source` | string | `nuclear`, `wind`, `solar`, `hydro`, `gas`, `coal`, `oil`, `biomass`, `geothermal`, `unknown`, and storage as `hydro_storage_charge`, `hydro_storage_discharge`, `battery_storage_charge`, `battery_storage_discharge` |
| `power_mw` | double | Average power in MW; null when the source is not reported |
| `estimation_method` | string | `MEASURED` or the API's estimation method |
| `is_estimated` | boolean | True when the value is estimated |
| `is_surrogate` | boolean | True for generated history, false for real API data |
| `updated_at` | timestamp (UTC) | When Electricity Maps last updated the value |
| `ingested_at` | timestamp (UTC) | When the pipeline fetched the value |
| `year`, `month`, `day` | string | Partition columns |

The import/export totals in the mix response are left out: `electricity_flows` has them per neighbouring zone.

**`silver/electricity_flows`**: one row per zone, hour and neighbouring zone. Key: `zone, datetime_utc, counterpart_zone`.

| Column | Type | Description |
|---|---|---|
| `zone` | string | Zone key, e.g. `FR` |
| `datetime_utc` | timestamp (UTC) | Start of the hour |
| `counterpart_zone` | string | Neighbouring zone, e.g. `ES`, `IT-NO` |
| `import_mw` | double | Power imported from the neighbour (0 when none) |
| `export_mw` | double | Power exported to the neighbour (0 when none) |
| `is_surrogate` | boolean | True for generated history, false for real API data |
| `updated_at` | timestamp (UTC) | When Electricity Maps last updated the value |
| `ingested_at` | timestamp (UTC) | When the pipeline fetched the value |
| `year`, `month`, `day` | string | Partition columns |

### Gold: daily data products

Rebuilt from Silver on every run (the tables are small) and partitioned by `year`. A day is a UTC day; `hours_covered` shows how many hours of data the day contains (the first and the current day are usually incomplete).

**`gold/daily_relative_mix`** (Data Product 1): percentage contribution of each energy source to the daily production.

| Column | Type | Description |
|---|---|---|
| `date_utc` | date | UTC day |
| `period_start_utc`, `period_end_utc` | timestamp (UTC) | Start and end of the day |
| `hours_covered` | int | Hours of data in the day |
| `surrogate_hours` | int | Hours of generated (surrogate) data in the day; 0 means all real |
| `zone`, `zone_name`, `country_code` | string | Zone metadata, e.g. `FR`, `France`, `FR` |
| `source` | string | Energy source (storage charging is consumption and is excluded) |
| `energy_mwh` | double | Energy produced by the source that day |
| `percentage` | double | Share of the day's total production (sums to 100 per zone and day) |
| `year` | string | Partition column |

**`gold/fr_daily_imports`** and **`gold/fr_daily_exports`** (Data Product 2): net energy exchanged with each neighbour per day. Import and export with the same neighbour within a day are netted; the neighbour appears in the imports table when France is a net importer that day, otherwise in the exports table.

| Column | Type | Description |
|---|---|---|
| `date_utc` | date | UTC day |
| `period_start_utc`, `period_end_utc` | timestamp (UTC) | Start and end of the day |
| `hours_covered` | int | Hours of data in the day |
| `surrogate_hours` | int | Hours of generated (surrogate) data in the day; 0 means all real |
| `from_zone`, `from_zone_name`, `from_country_code` | string | Exporting zone (the neighbour for imports, `FR` for exports) |
| `to_zone`, `to_zone_name`, `to_country_code` | string | Importing zone (`FR` for imports, the neighbour for exports) |
| `net_mwh` | double | Net energy from `from_zone` to `to_zone` (always > 0) |
| `year` | string | Partition column |

### Data quality

| Check | Where | On failure |
|---|---|---|
| Schema matches the table | Delta Lake (every write) | Write rejected |
| Key columns not null; `power_mw`, `import_mw`, `export_mw` ≥ 0 | Delta `CHECK` constraints (Silver) | Write rejected |
| `percentage` between 0 and 100; `net_mwh` > 0 | Delta `CHECK` constraints (Gold) | Write rejected |
| Unique key in Silver | Dagster asset check (blocking) | Gold is not built |
| No missing hours in Silver | Dagster asset check | Warning |
| Daily percentages sum to 100 | Dagster asset check | Error |

API calls are retried with exponential backoff (up to 5 attempts) on rate limiting (429), server errors (5xx), timeouts and connection errors. Other errors, such as an invalid key (401), fail immediately.

### Sample outputs

`sample_data/` holds 5 years of data: surrogate history from 4 October 2021 up to the most recent real API call, followed by the real data of that call. At about 80 MB it is not committed to git. Generate it with:

```bash
EMAPS_STORAGE_URI=sample_data poetry run python -m emaps_etl     # real data: the last 24 hours
poetry run python scripts/generate_surrogate_history.py          # surrogate history + rebuild
```

Layout:

```
sample_data/
├── bronze/electricity_mix/year=YYYY/month=MM/day=DD/<ingested_at>.json                (real)
├── bronze/electricity_mix/year=YYYY/month=MM/day=DD/<ingested_at>_surrogate_<day>.json (surrogate, 1 per day)
├── bronze/electricity_flows/...                                                        (same)
├── silver/electricity_mix/year=YYYY/month=MM/day=DD/part-*.parquet    (+ _delta_log/)
├── silver/electricity_flows/year=YYYY/month=MM/day=DD/part-*.parquet  (+ _delta_log/)
├── gold/daily_relative_mix/year=YYYY/part-*.parquet                   (+ _delta_log/)
├── gold/fr_daily_imports/year=YYYY/part-*.parquet                     (+ _delta_log/)
└── gold/fr_daily_exports/year=YYYY/part-*.parquet                     (+ _delta_log/)
```

Read them with Polars:

```python
import polars as pl

pl.read_delta("sample_data/gold/daily_relative_mix")
```

### Charts

Generated from the Gold tables with [scripts/plot_gold.py](scripts/plot_gold.py) (matplotlib, a dev dependency). Daily values are aggregated to full calendar months. Most of the data is [surrogate history](#surrogate-history), for demonstration only.

```bash
poetry run python scripts/plot_gold.py    # reads sample_data/, writes docs/*.png
```

**Production mix (`daily_relative_mix`):** the share of each energy source in the monthly production.

![France: monthly electricity production mix](docs/energy_mix_share.png)

**Net exchange per neighbour (`fr_daily_exports` and `fr_daily_imports`):** monthly net export from France (blue, above zero) or net import into France (red, below zero), on a shared scale.

![France: monthly net electricity exchange per neighbour](docs/net_exchange_by_neighbour.png)

## Design decisions

### Time window and granularity: multi-year, daily

The assignment does not specify the time window or the granularity of the analysis. Evaluating France's energy transition from fossil fuels to renewables is a long-term question, so the target is:

- **Time window: multiple years**, to show trends in the energy mix and in imports/exports over time.
- **Granularity: daily** for the data products in Gold. Hourly data is ingested and kept in Silver, so finer analyses remain possible.

Because of the API limitations (see [Limitations](#no-historical-data-api-trial-access)), the multi-year history cannot be loaded: the pipeline can only collect data from the moment it starts running. This repository is therefore an elementary setup to start with. The history builds up as the pipeline keeps running, and with an API key that includes historical access, past years could be backfilled.

To demonstrate the multi-year design, the sample data contains [surrogate history](#surrogate-history).

### Surrogate history

Because historical data cannot be loaded, [scripts/generate_surrogate_history.py](scripts/generate_surrogate_history.py) generates 5 years of hourly surrogate (fake) data for everything before the most recent real API call. Each value is:

```
trend (linear from the 2021 level to today's level) × seasonality × daily cycle (solar only) × random noise
```

- **Trend:** the energy transition: solar and wind grow, gas, coal and oil decline, nuclear decreases slightly. Today's levels are based on the real API data.
- **Seasonality:** wind, nuclear and gas higher in winter, hydro peaks in spring, solar higher in summer and zero at night.
- **Flows:** net export per neighbour with a trend, seasonality (fewer exports in winter) and a random deviation per day, so some days are net imports.
- **Reproducible:** fixed random seed; standard library only, no extra dependencies.

The surrogate data goes through the normal pipeline: the script writes Bronze files in the API's JSON format and rebuilds Silver and Gold from Bronze. It is always recognisable:

- **Bronze:** `source_url` starts with `surrogate://`, and the file name contains `_surrogate_`.
- **Silver:** `is_surrogate = true`, `estimation_method = "SURROGATE"`.
- **Gold:** `surrogate_hours` per day (0 means the day is fully real).

Real hours from earlier API calls are replaced by surrogate data (the most recent ingestion wins), so the dataset has a clean split: surrogate before the most recent real API call, real from then on. This also removes the gap of 4 hours (3 October 2026, 11:00–14:00 UTC) between the first two real runs. Surrogate data is for demonstration only and must not be used for analysis.

### Polars + delta-rs instead of PySpark or Pandas

The pipeline uses [Polars](https://pola.rs/) for transformations and [delta-rs](https://delta-io.github.io/delta-rs/) (`deltalake`) for Delta Lake tables.

- **Lightweight:** pure `pip` install, no JVM, no Spark cluster or session start-up. Runs instantly on a laptop and in CI.
- **Fits the data volume:** hourly data for a single zone is megabytes, not terabytes; distributed processing adds overhead without benefit.
- **Native S3 support:** delta-rs reads and writes Delta tables on S3 directly, without Hadoop or `s3a` configuration.
- **Fast:** Polars is multi-threaded, columnar and supports lazy evaluation.

### Local Dagster orchestration instead of AWS Step Functions / Lambda

The pipeline is orchestrated by [Dagster](https://dagster.io/), running locally (`dagster dev`). AWS is used for storage only.

- **No Step Functions:** a serverless setup needs a state machine, EventBridge schedule, extra IAM roles, log groups and a deployment pipeline. That adds complexity and running cost that this assignment does not need.
- **No Lambda:** the runtime dependencies (Polars, delta-rs) are about 324 MB, above Lambda's 250 MB limit for zip packages. Lambda would therefore require a container image, Docker and an ECR repository.
- **Dagster runs on a laptop:** no server, database or Docker needed; run history is stored in SQLite.
- **Fits the medallion architecture:** Bronze, Silver and Gold tables are modelled as Dagster assets, so the UI shows the lineage between the layers; data quality checks run as asset checks.
- **Trade-off:** schedules only run while `dagster dev` is running. Unattended production scheduling would require hosting Dagster (or moving to a managed orchestrator).

### API key in SSM Parameter Store, outside Terraform

The Electricity Maps API key is stored as an SSM `SecureString` parameter (`/emaps-etl/electricitymaps/api-key`), encrypted with the AWS-managed KMS key.

- **No secrets in the repository:** locally the key can come from a gitignored `.env`; otherwise it is read from SSM at runtime.
- **Not managed by Terraform:** the `aws_ssm_parameter` resource reads the decrypted value back into the Terraform state file. The parameter is therefore created once with `aws ssm put-parameter`, and Terraform only references its ARN to grant the writer role read access.
- **SSM instead of Secrets Manager:** Standard SSM parameters are free and sufficient for a single static key; automatic rotation is not needed.

### Publicly readable S3 bucket

The data lake bucket allows anonymous `s3:ListBucket` and `s3:GetObject`, so anyone can browse and read the Bronze, Silver and Gold data without an AWS account (`--no-sign-request`).

- **Why:** the data is non-sensitive sandbox data, and public read avoids creating and handing over user accounts or credentials.
- **Write access stays restricted:** only the writer role can write. ACLs stay disabled, access goes through the bucket policy only, and HTTPS is enforced.
- **Versioning** is enabled, so accidental overwrites or deletes can be recovered.

### Data contracts as Delta table constraints

Schemas and data quality rules are enforced by the Delta tables themselves, instead of a separate validation library (such as Pandera) or YAML contract files.

- **Explicit Polars schemas** define column names and types for the Silver tables; data is built with these schemas before writing.
- **Delta Lake enforcement:** delta-rs rejects writes whose schema does not match the table, and enforces `CHECK` constraints (e.g. key columns not null, `power_mw >= 0`, `percentage` between 0 and 100). A violating write fails as a whole and nothing is committed. The constraints are stored in the table's Delta log, so they apply to every writer, not just this pipeline.
- **Cross-row checks** that constraints cannot express (unique keys, daily percentages summing to 100%, missing hours) run as Dagster asset checks.
- **Fewer dependencies:** no Pandera or PyYAML.

### All timestamps and days in UTC

All timestamps, partitions (`year=YYYY/month=MM/day=DD`) and daily aggregations use **UTC**, the time zone the Electricity Maps API returns.

- **Consistent:** Bronze, Silver and Gold use the same clock as the source data, so no conversions are needed between layers.
- **No daylight saving issues:** every UTC day has exactly 24 hours, whereas local days in France have 23 or 25 hours on the daylight saving transitions.
- **Explicit column names:** time columns carry the time zone in their name or type (e.g. `date_utc`, `period_start_utc`).

**Note for French data consumers:** a Gold "day" is a UTC day, which runs from 01:00 to 01:00 French time in winter (CET, UTC+1) and from 02:00 to 02:00 in summer (CEST, UTC+2). For local assessments, convert the UTC timestamps to French time (`Europe/Paris`) before aggregating, e.g. in Polars:

```python
df.with_columns(pl.col("datetime_utc").dt.convert_time_zone("Europe/Paris").alias("datetime_paris"))
```

## Limitations

### No historical data (API trial access)

The API key used for this assignment only has access to the `latest` and `history` endpoints. The `past` and `past-range` endpoints return `401 Unauthorized` ("This endpoint is not included in your API trial") for every zone and data type.

As a result:

- **No backfill:** historical data cannot be loaded. The dataset starts at the first pipeline run and grows from there.
- **Ingestion uses `history`:** `GET /v4/electricity-mix/history` and `GET /v4/electricity-flows/history` return the last 24 hours of hourly data.
- **Run at least once every 24 hours:** a longer gap between runs leaves hours that can no longer be retrieved. The Dagster schedule runs every 12 hours, so a single missed run does not leave a gap.
- **Overlapping windows are expected:** consecutive runs return overlapping hours; Silver deduplicates them on the natural key and keeps the most recent ingestion.

With a key that includes `past-range` access, the pipeline could backfill by looping over windows of at most 10 days (the limit for hourly data per request).
