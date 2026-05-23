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

Create `.env` from `.env.example` if you need local secrets, then run the Bronze pipeline with Docker Compose:

```bash
docker compose build
docker compose run --rm lakehouse
docker compose run --rm lakehouse pytest -q
```

The default container command runs:

```bash
python -m lakehouse.jobs.run_bronze --env dev
```

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

Run Gold after Silver to build dashboard-friendly aggregate tables:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_gold --env dev
docker compose run --rm lakehouse python -m lakehouse.jobs.run_gold --env dev --tables player_metrics,champion_metrics
```

Gold reads Silver Parquet and overwrites `data/lakehouse/gold/{player_metrics,champion_metrics,role_metrics,rank_metrics,team_objective_metrics}`. Gold v1 is partitioned by `game_date` for all five tables.

Run Data Quality after Gold to profile and validate Silver/Gold tables:

```bash
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev
docker compose run --rm lakehouse python -m lakehouse.jobs.run_data_quality --env dev --layers gold --gold-tables player_metrics,champion_metrics
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
- Gold: analytics aggregates for players, champions, roles, ranks, and team objectives, written as Parquet from Silver.
- Platinum: ML-ready feature tables for match win, player performance, and champion meta modeling.

Current work has runnable Bronze, Silver, and Gold layers. Platinum scaffolds remain available for later stages.

TODO: Silver and Gold can be migrated from Parquet to Delta Lake in a later version.

## Configuration

Edit `configs/dev.yaml` for local paths and `configs/tables.yaml` for table metadata. Copy `.env.example` to `.env` for local secrets.

Runtime data is intentionally not committed:

- raw Riot API JSON under `raw/**/*.json`
- lakehouse outputs under `data/lakehouse`
- checkpoints under `metadata/checkpoints`
- reports under `reports`
