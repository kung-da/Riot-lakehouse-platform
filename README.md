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
