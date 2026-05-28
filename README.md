# Riot Lakehouse Platform

Docker-first lakehouse scaffold for Riot Games API data. The pipeline follows raw JSON to Bronze, Silver, Gold, and Platinum layers using PySpark-compatible jobs, checkpoints, validation reports, and optional Airflow DAGs.

## Raw Inputs

Current raw datasets are expected under:

- `raw/matches/*.json`
- `raw/timelines/*.json`
- `raw/summoners/*.json`
- `raw/ranked/*.json`

Raw JSON is ignored by git. Keep only `.gitkeep` files in source control.

## Quick Start

Create `.env` from `.env.example`, then run the Bronze pipeline with Docker Compose:

```bash
docker compose build
docker compose run --rm lakehouse
docker compose run --rm lakehouse pytest -q
```

The default container command runs:

```bash
python -m lakehouse.jobs.run_bronze
```

The job reads `LAKEHOUSE_ENV`, `LAKEHOUSE_CONFIG_DIR`, and `LAKEHOUSE_ENV_FILE` from `.env`.
By default `LAKEHOUSE_ENV=dev`, so it loads `configs/dev.yaml`.

Bronze Parquet output is written to `data/lakehouse/bronze/raw_json`, partitioned by `dataset` and `ingest_date`.

By default, `configs/dev.yaml` ingests all datasets but caps `timelines` at 500 checkpointed files total with smaller timeline batches:

```yaml
bronze:
  max_records_per_batch: 1000
  max_bytes_per_batch: 134217728
  output_partitions: 1
  dataset_max_files:
    timelines: 500
  dataset_batch_sizes:
    matches: 1000
    timelines: 25
    summoners: 1000
    ranked: 20
  dataset_max_bytes_per_batch:
    matches: 134217728
    timelines: 67108864
    summoners: 1048576
    ranked: 33554432
```

The configured timeline cap is enforced against the checkpoint count. If 5 timeline files are already checkpointed, the next default run ingests at most 495 more timeline files.
When files live under a known raw dataset folder, Bronze does not parse JSON; it stores the original UTF-8 text plus metadata in Parquet.

For an explicit large timeline backfill, override the default with CLI options:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_bronze --env dev --datasets timelines --max-files 500 --batch-size 5
```

Checkpoints are saved after each successful batch, so rerunning the same command resumes from the remaining unprocessed files.

Run Silver after Bronze to materialize cleaned domain tables:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_silver --env dev
docker compose run --rm lakehouse python -m lakehouse.jobs.run_silver --env dev --datasets matches,timelines
docker compose run --rm lakehouse python -m lakehouse.jobs.run_silver --env dev --tables matches,participants,teams
```

When running from an installed local environment instead of Docker:

```bash
python -m lakehouse.jobs.run_silver --env dev
python -m lakehouse.jobs.run_silver --env dev --datasets matches,timelines
python -m lakehouse.jobs.run_silver --env dev --tables matches,participants,teams
```

Silver reads `data/lakehouse/bronze/raw_json`, parses valid Riot payloads, keeps Bronze lineage columns (`source_file`, `file_hash`, `ingest_ts`, `ingest_date`, `dataset`), derives `game_date`, and overwrites the cleaned Parquet tables under `data/lakehouse/silver/{matches,participants,teams,summoners,ranked,timeline_frames,timeline_events}`. Silver is partitioned by `dataset` and `game_date`, for example `data/lakehouse/silver/matches/dataset=matches/game_date=YYYY-MM-DD/*.parquet`.

Run Gold after Silver to build the dimensional analytics model:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_gold --env dev
docker compose run --rm lakehouse python -m lakehouse.jobs.run_gold --env dev --tables dim_summoner,mart_player_daily_performance
```

Gold reads Silver Parquet and overwrites dimension, fact, and mart tables under `data/lakehouse/gold/`:

- Dimensions: `dim_date`, `dim_match`, `dim_summoner`, `dim_champion`, `dim_team`, `dim_rank`
- Facts: `fact_participant_performance`, `fact_team_objectives`, `fact_rank_snapshot`, `fact_timeline_frames`, `fact_timeline_events`
- Analytics marts: `mart_player_daily_performance`, `mart_champion_daily_performance`, `mart_role_daily_performance`, `mart_rank_daily_summary`, `mart_team_objective_daily_summary`

Gold tables are partitioned by `game_date` when that column exists.

Run Data Quality after Gold to profile and validate Silver/Gold tables:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev --layers gold --gold-tables mart_player_daily_performance,mart_champion_daily_performance
```

The job writes JSON and Markdown reports under `reports/data_quality/`, including `data_quality_latest.json` and `data_quality_latest.md`. It checks table existence, row counts, expected columns, required values, uniqueness, non-negative metrics, win-rate ranges, and basic aggregate consistency without calling any AI API.

Quick DuckDB check:

```sql
SELECT dataset, game_date, COUNT(*)
FROM read_parquet('data/lakehouse/silver/matches/**/*.parquet')
GROUP BY dataset, game_date;
```

## Layers

- Bronze: append-only Parquet ingestion from raw JSON with dataset, file path, file hash, ingestion timestamp, ingestion date, and original JSON payload string.
- Silver: cleaned domain tables for matches, participants, teams, summoners, ranked, timeline frames, and timeline events.
- Gold: conformed dimensions, event/snapshot facts, and analytics marts for players, champions, roles, ranks, and team objectives.
- Platinum: ML-ready feature tables for match win, player performance, and champion meta modeling.

Current work has runnable Bronze, Silver, and Gold layers. Platinum scaffolds remain available for later stages.

TODO: Silver and Gold can be migrated from Parquet to Delta Lake in a later version.

## Configuration

Edit `.env` for runtime settings, `configs/dev.yaml` / `configs/prod.yaml` for environment
defaults, and `configs/tables.yaml` for table metadata. Config YAML values support
`${VAR}`, `${VAR:-default}`, and `${VAR:?message}` interpolation from `.env`.

Local development uses folders under the repo:

```env
LAKEHOUSE_ENV=dev
LAKEHOUSE_RAW_ROOT=raw
LAKEHOUSE_ROOT=data/lakehouse
LAKEHOUSE_CHECKPOINT_ROOT=metadata/checkpoints
LAKEHOUSE_REPORT_ROOT=reports
```

S3 runs use `configs/prod.yaml`:

```env
LAKEHOUSE_ENV=prod
AWS_REGION=ap-southeast-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
S3_BUCKET=my-riot-lakehouse-bucket
S3_RAW_PREFIX=raw
S3_LAKEHOUSE_PREFIX=lakehouse
S3_CHECKPOINT_PREFIX=metadata/checkpoints
S3_REPORT_PREFIX=reports
```

Run with a different env file without editing Compose:

```powershell
$env:LAKEHOUSE_ENV_FILE=".env.prod"
docker compose --env-file .env.prod run --rm lakehouse python -m lakehouse.jobs.run_full_pipeline
```

You can also set `LAKEHOUSE_ENV_FILE=.env.prod` inside `.env.prod`.

Start the local Airflow UI:

```bash
docker compose --profile airflow up --build airflow
```

Then open `http://localhost:8080` and sign in with `_AIRFLOW_WWW_USER_USERNAME` /
`_AIRFLOW_WWW_USER_PASSWORD` from `.env` (`admin` / `admin` in `.env.example`).
The DAGs inherit the same `.env` settings and run the Bronze -> Silver -> Gold -> Data Quality
chain without hard-coded `--env dev`.

Runtime data is intentionally not committed:

- raw Riot API JSON under `raw/**/*.json`
- lakehouse outputs under `data/lakehouse`
- checkpoints under `metadata/checkpoints`
- reports under `reports`
