# Electricity Maps ETL pipeline

ETL pipeline for France's (FR) hourly **electricity mix** and **electricity flows** from the [Electricity Maps API](https://www.electricitymaps.com/), built with Polars, Delta Lake (delta-rs) and Dagster, following the Bronze → Silver → Gold medallion architecture. The data lake is an AWS S3 bucket: the pipeline writes to it as an IAM writer role, and anyone can read it.

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
| [src/emaps_etl/writer.py](src/emaps_etl/writer.py) | Writes to the S3 data lake (raw JSON, Delta tables) |
| [src/emaps_etl/reader.py](src/emaps_etl/reader.py) | Reads Delta tables and JSON files from S3 (without credentials) or a local copy |
| [src/emaps_etl/watermark.py](src/emaps_etl/watermark.py) | High-water marks for the incremental Silver load |
| [src/emaps_etl/bronze.py](src/emaps_etl/bronze.py), [silver.py](src/emaps_etl/silver.py), [gold.py](src/emaps_etl/gold.py) | The three layers |
| [src/emaps_etl/checks.py](src/emaps_etl/checks.py) | Data quality checks |
| [src/emaps_etl/definitions.py](src/emaps_etl/definitions.py) | Dagster assets, checks and schedule |
| [src/emaps_etl/\_\_main\_\_.py](src/emaps_etl/__main__.py) | Run the pipeline once without Dagster |
| [tests/](tests/) | Unit tests, with real API responses as fixtures |
| [zones.json](zones.json) | Zone metadata (name, country), a snapshot of the API's `/v4/zones` |
| [scripts/plot_gold.py](scripts/plot_gold.py), [docs/](docs/) | Charts of the Gold tables (see [Charts](#charts)) |
| [scripts/browse.py](scripts/browse.py) | Browse the Silver and Gold tables interactively (see [1. Data consumer](#1-data-consumer)) |
| [scripts/generate_surrogate_history.py](scripts/generate_surrogate_history.py) | Generates 5 years of surrogate history (see [Surrogate history](#surrogate-history)) |
| [sample_output/](sample_output/) | Sample output: partitioned Bronze files and Silver/Gold Delta tables (see [Sample output](#sample-output)) |
| `sample_data/` | Optional local copy of the full data lake (not committed; see [1. Data consumer](#1-data-consumer)) |
| [infra/](infra/) | Terraform for the AWS resources (see [3. Deployer](#3-deployer)) |
| [docs/build_pseudocode.md](docs/build_pseudocode.md) | Build order in pseudocode |
| [.github/workflows/ci.yml](.github/workflows/ci.yml) | CI: lint and tests |

## Getting started

Pick the role that matches your goal:

| Role | Goal | Needs AWS access? |
|---|---|---|
| [1. Data consumer](#1-data-consumer) | Browse and analyse the data | No: the data lake is publicly readable |
| [2. Data engineer](#2-data-engineer) | Run and maintain the existing pipeline | Yes: permission to assume the writer role |
| [3. Deployer](#3-deployer) | Deploy everything from scratch in your own AWS account | Yes: admin rights |

The tools are installed the same way for all roles: see [Install tools](#install-tools).

**Running the pipeline end to end** (API → Bronze → Silver → Gold) writes to an S3 data lake as the writer role, so it needs an AWS account with the infrastructure deployed and an Electricity Maps API key: follow [3. Deployer](#3-deployer) in your own AWS account, or [2. Data engineer](#2-data-engineer) with access to an existing deployment. Without AWS access, the output can still be inspected: in the repository ([sample_output/](sample_output/)) and in the public data lake.

## 1. Data consumer

**Goal:** browse and analyse the Silver and Gold tables.

### Prerequisites

- `git`, Python 3.12 and Poetry >= 2.0 ([Install tools](#install-tools)).
- Optional: the AWS CLI v2, only for a local copy (step 4).
- No AWS account, API key or configuration: the data lake is publicly readable.

### Steps

1. **Install the project:**

   ```bash
   git clone <repo-url> electricitymaps && cd electricitymaps
   poetry install
   ```

2. **Browse the tables** with [scripts/browse.py](scripts/browse.py). It asks for a layer (Silver or Gold) and a table, and shows its columns, row count, time range and latest rows:

   ```bash
   poetry run python scripts/browse.py s3://emaps-etl-<account_id>-eu-central-1   # the full data lake
   poetry run python scripts/browse.py sample_output                              # the sample in the repository
   ```

3. **Understand the data:** see [Data layers and schemas](#data-layers-and-schemas) for every table and column, and the [charts](#charts) for an overview. Keep in mind:
   - **All times are UTC**, and a Gold "day" is a UTC day. For French local time, convert first (see [All timestamps and days in UTC](#all-timestamps-and-days-in-utc)).
   - **Data before 3 October 2026 15:00 UTC is [surrogate history](#surrogate-history)** (generated, for demonstration only), recognisable by `estimation_method = "SURROGATE"` in `silver/electricity_mix`.
   - **The first and the current day are usually incomplete**: check `hours_covered` in Gold.

4. **Optional: work offline** with a local copy (no credentials needed):

   ```bash
   aws s3 sync s3://emaps-etl-<account_id>-eu-central-1/ sample_data/ --no-sign-request
   poetry run python scripts/browse.py sample_data
   ```

5. **Optional: use the tables in your own code**, e.g. with Polars:

   ```python
   import polars as pl

   pl.read_delta(
       "s3://emaps-etl-<account_id>-eu-central-1/gold/daily_relative_mix",
       storage_options={"aws_region": "eu-central-1", "aws_skip_signature": "true"},
   )
   ```

## 2. Data engineer

**Goal:** run the existing pipeline, keep it running on schedule and maintain it. The infrastructure and the API key in SSM already exist (see [3. Deployer](#3-deployer)).

### Prerequisites

- `git`, Python 3.12, Poetry >= 2.0 and the AWS CLI v2 ([Install tools](#install-tools)).
- An AWS CLI profile for an IAM user that is listed in the deployment's `writer_principal_arns`, so it may assume the writer role `emaps-etl-writer`. Ask the deployer to add your user.

### Steps

1. **Install the project:**

   ```bash
   git clone <repo-url> electricitymaps && cd electricitymaps
   poetry install
   ```

2. **Add the writer profile** to `~/.aws/config`; `source_profile` is your own profile:

   ```ini
   [profile emaps-writer]
   role_arn = arn:aws:iam::<account_id>:role/emaps-etl-writer
   source_profile = <your-profile>
   region = eu-central-1
   ```

   Profile sections other than `[default]` must be written as `[profile <name>]`; delta-rs ignores sections without the `profile` prefix. Check that the role works:

   ```bash
   aws sts get-caller-identity --profile emaps-writer   # ...:assumed-role/emaps-etl-writer/...
   ```

3. **Configure** the pipeline in `.env` (gitignored; template: [.env.example](.env.example)):

   ```bash
   cp .env.example .env    # set EMAPS_STORAGE_URI and EMAPS_AWS_PROFILE
   ```

   | Variable | Default | Meaning |
   |---|---|---|
   | `EMAPS_STORAGE_URI` | (required) | S3 location of the data lake, e.g. `s3://emaps-etl-<account_id>-eu-central-1`. Must start with `s3://` |
   | `EMAPS_AWS_PROFILE` | (empty) | AWS profile for S3 and SSM, e.g. `emaps-writer`. When empty, the standard AWS credential chain is used |
   | `EMAPS_API_KEY` | (empty) | Leave empty: the key is read from SSM (`/emaps-etl/electricitymaps/api-key`). Set it only as a local override |
   | `EMAPS_ZONE` | `FR` | Electricity Maps zone |

   The pipeline writes only to the S3 data lake ([writer.py](src/emaps_etl/writer.py)), as the writer role. Reading needs no credentials ([reader.py](src/emaps_etl/reader.py)).

4. **Run the pipeline once** to check the setup (about a minute: the Silver and Gold tables span many small daily partitions on S3):

   ```bash
   poetry run dagster job execute -m emaps_etl.definitions -j etl   # with data quality checks
   poetry run python -m emaps_etl                                   # or without Dagster
   ```

5. **Keep it running with Dagster** and its schedule; see [Dagster operations](#dagster-operations) below.

6. **Check the result** with `poetry run python scripts/browse.py` (uses `EMAPS_STORAGE_URI`), and the check results in the Dagster UI.

7. **When changing the code:** run the tests and the linter (the same checks run in GitHub Actions on every push and pull request, [ci.yml](.github/workflows/ci.yml)), regenerate the [charts](#charts) when needed, and keep this README and [docs/build_pseudocode.md](docs/build_pseudocode.md) up to date.

   ```bash
   poetry run pytest
   poetry run ruff check . && poetry run ruff format --check .
   ```

### Dagster operations

Dagster runs locally (`dagster dev`): a webserver (the UI), a daemon that starts scheduled runs, and a code server that loads [definitions.py](src/emaps_etl/definitions.py). Settings come from `.env`, which `dagster dev` loads at start-up. All Dagster state (run history, schedule on/off, logs) lives in `DAGSTER_HOME` (`.dagster/`, gitignored).

All commands below run from the repository root with:

```bash
export DAGSTER_HOME=$PWD/.dagster
```

#### Start

In the foreground (stops when the terminal closes):

```bash
mkdir -p $DAGSTER_HOME
poetry run dagster dev -m emaps_etl.definitions
```

In the background, detached from the terminal, with logs in `.dagster/dagster.log`:

```bash
mkdir -p $DAGSTER_HOME
setsid nohup .venv/bin/dagster dev -m emaps_etl.definitions > $DAGSTER_HOME/dagster.log 2>&1 < /dev/null &
```

The UI is at http://localhost:3000 once the log shows `Serving dagster-webserver`.

#### Schedule

`etl_schedule` runs the whole job (Bronze → Silver → Gold and all checks) every 12 hours, at 00:00 and 12:00 UTC. Turn it on once; the setting is stored in `DAGSTER_HOME` and survives restarts.

```bash
poetry run dagster schedule start etl_schedule -m emaps_etl.definitions   # turn on
poetry run dagster schedule stop etl_schedule -m emaps_etl.definitions    # turn off
poetry run dagster schedule list -m emaps_etl.definitions                 # status: RUNNING / STOPPED
```

Or in the UI: *Automation → etl_schedule*.

**Scheduled runs only happen while `dagster dev` is running.** When the laptop is off, asleep or Dagster is stopped, runs are skipped and not caught up later. Because the API only returns the last 24 hours, a break of more than about 24 hours leaves a permanent gap in the data (reported by the `silver_mix_no_missing_hours` check). After a break, start a manual run as soon as possible.

#### Run manually

- **UI:** *Jobs → etl → Materialize all*, or select individual assets under *Assets* and choose *Materialize selected*.
- **Command line** (does not need `dagster dev`):

  ```bash
  poetry run dagster job execute -m emaps_etl.definitions -j etl
  ```

#### Monitor

| What | Where |
|---|---|
| Run history, status and logs per step | UI: *Runs* |
| Data quality check results | UI: *Assets* → asset → *Checks* |
| Bronze files loaded into Silver per run | UI: *Assets* → Silver asset → metadata `bronze_files_loaded` |
| Next scheduled run | UI: *Automation → etl_schedule* |
| Is Dagster running? | `pgrep -af "dagster dev"`, or `curl -s -o /dev/null -w "%{http_code}" http://localhost:3000` (200 = up) |
| Process log (background start) | `tail -f .dagster/dagster.log` |

A failed blocking check (unique keys in Silver) stops the Gold assets for that run; the run shows as failed in *Runs*. A warning check (missing hours) does not stop the run.

#### Stop

```bash
pkill -f "dagster dev -m emaps_etl.definitions"
```

or `Ctrl+C` in the terminal when started in the foreground. The schedule stays turned on and resumes when Dagster starts again.

#### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Runs fail at Bronze with `401 Unauthorized` | Invalid API key, or the endpoint is not included in the API plan. Check the SSM parameter (or `EMAPS_API_KEY` in `.env` when set). |
| Runs fail with an AWS `AccessDenied` or credentials error | The `emaps-writer` profile cannot assume the writer role. Check with `aws sts get-caller-identity --profile emaps-writer`. |
| `EMAPS_STORAGE_URI` missing or "must be an S3 location" | `.env` is missing, or the command does not run from the repository root. |
| Changes to `.env` or the code have no effect | `.env` is read when `dagster dev` starts: restart it. Code changes are picked up via *Deployment* → code location `emaps_etl.definitions` → *Reload* in the UI, or by restarting. |
| `silver_mix_no_missing_hours` warns | Dagster did not run for more than 24 hours; the missing hours cannot be retrieved anymore with the trial API key. |
| Port 3000 already in use | Another `dagster dev` is running: stop it first, or start with `-p 3001`. |
| `The Poetry configuration is invalid` | An old Poetry (e.g. the Ubuntu package `python3-poetry`, 1.8) is used instead of Poetry >= 2.0: see [Install tools](#install-tools). |

## 3. Deployer

**Goal:** deploy the complete setup from scratch in your own AWS account: infrastructure, API key, data lake with sample data, and the scheduled pipeline.

### Prerequisites

- `git`, Python 3.12, Poetry >= 2.0, Terraform >= 1.6 and the AWS CLI v2 ([Install tools](#install-tools)).
- An AWS CLI profile with admin rights in the target account.
- An Electricity Maps API key ([sandbox key](https://help.electricitymaps.com/en/articles/13169368-using-a-sandbox-api-key)).

### Steps

1. **Install the project:**

   ```bash
   git clone <repo-url> electricitymaps && cd electricitymaps
   poetry install
   ```

2. **Store the API key in SSM Parameter Store.** This is a one-time manual step: the parameter is deliberately not managed by Terraform, because the `aws_ssm_parameter` resource stores the decrypted value in the Terraform state (see [API key in SSM Parameter Store](#api-key-in-ssm-parameter-store-outside-terraform)).

   ```bash
   aws ssm put-parameter --profile <admin-profile> --region eu-central-1 \
     --name /emaps-etl/electricitymaps/api-key \
     --type SecureString --overwrite --value "<api-key>"
   ```

3. **Create the infrastructure** with Terraform in [infra/](infra/):

   | Resource | Name | Notes |
   |---|---|---|
   | S3 bucket | `emaps-etl-<account_id>-<region>` | Data lake (`bronze/`, `silver/`, `gold/`, `_state/`); **publicly readable**, versioned, SSE-S3 encrypted, ACLs disabled, TLS-only, noncurrent versions expire after 30 days |
   | IAM role | `emaps-etl-writer` | Read/write on the whole bucket, read the API key parameter. Can only be assumed by the IAM principals in `writer_principal_arns` (default: the identity that runs Terraform) |

   ```bash
   cd infra
   cp terraform.tfvars.example terraform.tfvars   # set aws_profile (and optionally writer_principal_arns)
   terraform init
   terraform plan -out tfplan
   terraform apply tfplan
   cd ..
   ```

   The Terraform state is stored locally in `infra/terraform.tfstate` and is **not** in git. Keep it safe: without it, Terraform does not know that the bucket and role exist, and `terraform apply` from another machine fails with "already exists". To manage the infrastructure from another machine, copy this file, or set up a remote backend (the `backend "s3"` block in [infra/versions.tf](infra/versions.tf) is prepared for this).

4. **Set up the writer profile and `.env`** as in steps 2 and 3 of [2. Data engineer](#2-data-engineer). `terraform output aws_config_profile` prints the profile, and `terraform output storage_uri` the value for `EMAPS_STORAGE_URI`.

5. **Load the first real data** (the last 24 hours):

   ```bash
   poetry run python -m emaps_etl
   ```

6. **Optional: add 5 years of [surrogate history](#surrogate-history)**, so the data products show multi-year trends. It writes surrogate Bronze files, then rebuilds Silver and Gold from all Bronze data; it takes about 10 minutes.

   ```bash
   poetry run python scripts/generate_surrogate_history.py
   ```

7. **Start Dagster and turn on the schedule** (see [Dagster operations](#dagster-operations)). From now on the pipeline runs every 12 hours while Dagster is running.

8. **Verify the deployment:**

   ```bash
   aws s3 ls s3://<bucket>/ --no-sign-request                       # public read works
   poetry run python scripts/browse.py s3://<bucket>                 # tables are readable
   poetry run dagster job execute -m emaps_etl.definitions -j etl    # all checks pass
   ```

9. **Hand over:** add the IAM users of the data engineers to `writer_principal_arns` and apply again; share the bucket name with data consumers.

## Install tools

### Poetry

Poetry >= 2.0 manages the dependencies and the virtual environment. Install it with the official installer (Linux and macOS):

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

This installs Poetry into `~/.local/bin`. If `poetry` is not found afterwards, add that directory to your `PATH` (e.g. in `~/.bashrc` or `~/.zshrc`):

```bash
export PATH="$HOME/.local/bin:$PATH"
poetry --version
```

Do not use the Ubuntu/Debian package `python3-poetry`: it is version 1.8, which cannot read this project's `pyproject.toml`. Make sure `~/.local/bin` comes first in your `PATH`.

### Terraform (deployer only)

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

Verify with `terraform version`.

### The project environment

`poetry install` creates the virtual environment in `.venv/` inside the repository (`poetry.toml` sets `virtualenvs.in-project = true`) and installs all dependencies:

| Group | Packages | Purpose |
|---|---|---|
| core | `polars`, `deltalake` | Transformations and Delta Lake tables on S3 |
| core | `requests`, `tenacity` | API client with retries and backoff |
| core | `pydantic-settings` | Typed configuration from environment variables and `.env` |
| core | `boto3` | AWS access (SSM Parameter Store, S3) |
| core | `dagster`, `dagster-webserver` | Orchestration on the local machine (`dagster dev`) |
| dev | `pytest`, `ruff`, `matplotlib` | Tests, linting and formatting, charts |

Run commands with `poetry run …`, or activate the environment with `source .venv/bin/activate`. In VS Code, select `.venv/bin/python` as the interpreter. Add dependencies with `poetry add <package>` (or `poetry add --group dev <package>`), not with `pip install`, so that `pyproject.toml` and `poetry.lock` stay in sync.

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
| `estimation_method` | string | `MEASURED` or the API's estimation method; `SURROGATE` for [surrogate history](#surrogate-history) |
| `is_estimated` | boolean | True when the value is estimated |
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
| `updated_at` | timestamp (UTC) | When Electricity Maps last updated the value |
| `ingested_at` | timestamp (UTC) | When the pipeline fetched the value |
| `year`, `month`, `day` | string | Partition columns |

### Incremental loading

Silver is loaded incrementally from Bronze with a **high-water mark** per table: the `ingested_at` of the last Bronze file loaded into it. It is stored as a small JSON file in the data lake, one per table (the two Silver tables load in parallel):

```
s3://emaps-etl-<account_id>-eu-central-1/_state/watermark_silver_electricity_mix.json
{"ingested_at": "2026-10-04T18:48:47.910184+00:00"}
```

Each run ([silver.load_new_bronze](src/emaps_etl/silver.py)):

1. Reads the watermark. Without a watermark file (first run, or after the [surrogate history](#surrogate-history) rebuild), it is derived from the latest `ingested_at` in the Silver table.
2. Lists only the Bronze day folders from the watermark's date until today (Bronze is partitioned by ingestion date) and selects the files whose name, which starts with the ingestion timestamp, is newer than the watermark. Only those files are read.
3. MERGEs them into Silver.
4. Moves the watermark to the newest loaded file, **only after a successful MERGE**. If that update fails, the next run loads the same files again, which is harmless because the MERGE is idempotent.

So Bronze files are never lost between the layers: when a Silver step fails, the next run catches up with all Bronze files ingested since. In Dagster, the number of loaded files shows as `bronze_files_loaded` in the materialization metadata of the Silver assets. A run without new Bronze files loads nothing and leaves Silver unchanged.

Gold is not incremental: it is rebuilt from Silver on every run (the tables are small). The API side is not incremental either: with the trial API key, every run fetches the last 24 hours (see [Limitations](#no-historical-data-api-trial-access)).

### Gold: daily data products

Rebuilt from Silver on every run (the tables are small) and partitioned by `year`. A day is a UTC day; `hours_covered` shows how many hours of data the day contains (the first and the current day are usually incomplete).

**`gold/daily_relative_mix`** (Data Product 1): percentage contribution of each energy source to the daily production.

| Column | Type | Description |
|---|---|---|
| `date_utc` | date | UTC day |
| `period_start_utc`, `period_end_utc` | timestamp (UTC) | Start and end of the day |
| `hours_covered` | int | Hours of data in the day |
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

### Sample output

[sample_output/](sample_output/) in the repository is an excerpt of the data lake (about 400 KB), with the same partitioned layout:

| Layer | Contents |
|---|---|
| `bronze/` | All real API responses (JSON), partitioned by ingestion date |
| `silver/` | Delta tables with all real hours (3 October 2026 15:00 UTC onwards), partitioned by data date |
| `gold/` | Delta tables from 1 September 2026: [surrogate history](#surrogate-history) until 3 October 2026, real data after. The surrogate days are included so that `fr_daily_imports` has rows: in the real data, France is a net exporter to every neighbour |
| `_state/` | The high-water marks of the Silver tables |

It was copied from the data lake on 4 October 2026 and is not updated by the pipeline. Browse it with `poetry run python scripts/browse.py sample_output`.

### Data lake layout

The data lake (`s3://emaps-etl-<account_id>-eu-central-1`, publicly readable) holds 5 years of data: [surrogate history](#surrogate-history) from 4 October 2021 up to 3 October 2026 15:00 UTC, followed by the real data collected since. The full data lake is about 55 MB and not committed to git; [sample_output/](sample_output/) is an excerpt, and [3. Deployer](#3-deployer) describes how to build it.

```
s3://emaps-etl-<account_id>-eu-central-1/
├── bronze/electricity_mix/year=YYYY/month=MM/day=DD/<ingested_at>.json                  (real, 1 per API call)
├── bronze/electricity_mix/year=YYYY/month=MM/day=DD/<ingested_at>_surrogate_<YYYYMM>.json (surrogate, 1 per month)
├── bronze/electricity_flows/...                                                        (same)
├── silver/electricity_mix/year=YYYY/month=MM/day=DD/part-*.parquet    (+ _delta_log/)
├── silver/electricity_flows/year=YYYY/month=MM/day=DD/part-*.parquet  (+ _delta_log/)
├── gold/daily_relative_mix/year=YYYY/part-*.parquet                   (+ _delta_log/)
├── gold/fr_daily_imports/year=YYYY/part-*.parquet                     (+ _delta_log/)
├── gold/fr_daily_exports/year=YYYY/part-*.parquet                     (+ _delta_log/)
└── _state/watermark_silver_<table>.json                              (high-water marks, see Incremental loading)
```

### Charts

Generated from the Gold tables with [scripts/plot_gold.py](scripts/plot_gold.py) (matplotlib, a dev dependency). Daily values are aggregated to full calendar months. Most of the data is [surrogate history](#surrogate-history), for demonstration only.

```bash
poetry run python scripts/plot_gold.py    # reads the S3 data lake, writes docs/*.png
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

The surrogate data goes through the normal pipeline: the script writes Bronze files in the API's JSON format to the S3 data lake (one file per stream and month), deletes the Silver and Gold tables and the watermarks, and rebuilds the tables from all Bronze data (the watermarks are re-derived on the next run). It runs as the writer role and takes about 10 minutes, mostly for writing and reading the thousands of small daily partitions of Silver on S3. It arrives in the standard columns; no extra columns are added. It can be recognised by:

- **Bronze:** `source_url` starts with `surrogate://`, and the file name contains `_surrogate_`.
- **Silver:** `estimation_method = "SURROGATE"` in `electricity_mix` (`electricity_flows` has no such column).
- **Time:** all data before **3 October 2026 15:00 UTC** is surrogate; real API data starts there. In Gold, every day before 3 October 2026 is fully surrogate, 3 October is mixed (15 surrogate hours, 9 real), and later days are real.

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

### Incremental Silver load with a high-water mark in the bucket

Silver loads only the Bronze files ingested since the last successful load, tracked by a high-water mark per table (see [Incremental loading](#incremental-loading)).

- **Why a high-water mark:** Silver reads its input from Bronze in the data lake instead of receiving it directly from the Bronze step of the same run. When a Silver step fails, the next run catches up with all Bronze files since the last successful load, and Silver can also be run on its own.
- **Stored as a small JSON file in the bucket** (`_state/watermark_silver_<table>.json`):
  - the state lives next to the data it describes;
  - the writer role can already write there, so no extra infrastructure or permissions are needed.
- **Not in SSM Parameter Store:** SSM is meant for configuration, not for state that changes on every run, and it would need an extra `ssm:PutParameter` permission and a Terraform change.
- **Not derived from Silver on every run:** reading `max(ingested_at)` scans the `ingested_at` column across all daily partitions of the Silver table on S3, which is slow. It is only used as a fallback when the watermark file is missing.
- **One file per table:** the two Silver tables load in parallel; separate files avoid one run overwriting the other's watermark.
- **Safe order:** the watermark moves only after a successful MERGE; if that update fails, the files are loaded again, which is harmless because the MERGE is idempotent.
- **Trade-off:** the watermark files are publicly readable like the rest of the bucket. They only contain a timestamp.

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
