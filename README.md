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

## Layers

- Bronze: append-only Parquet ingestion from raw JSON with dataset, file path, file hash, ingestion timestamp, ingestion date, and original JSON payload string.
- Silver: cleaned domain tables for matches, participants, teams, summoners, ranked, timeline frames, and timeline events.
- Gold: analytics aggregates for players, champions, roles, ranks, and team objectives.
- Platinum: ML-ready feature tables for match win, player performance, and champion meta modeling.

Current work focuses on Bronze only. Silver, Gold, and Platinum scaffolds remain available for later stages.

## Configuration

Edit `configs/dev.yaml` for local paths and `configs/tables.yaml` for table metadata. Copy `.env.example` to `.env` for local secrets.

Runtime data is intentionally not committed:

- raw Riot API JSON under `raw/**/*.json`
- lakehouse outputs under `data/lakehouse`
- checkpoints under `metadata/checkpoints`
- reports under `reports`
