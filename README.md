# Riot Lakehouse Platform

Local-first lakehouse scaffold for Riot Games API data. The pipeline follows raw JSON to Bronze, Silver, Gold, and Platinum layers using PySpark-compatible jobs, checkpoints, validation reports, and optional Airflow DAGs.

## Raw Inputs

Current raw datasets are expected under:

- `raw/matches/*.json`
- `raw/timelines/*.json`
- `raw/summoners/*.json`
- `raw/ranked/*.json`

Raw JSON is ignored by git. Keep only `.gitkeep` files in source control.

## Quick Start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
python -m lakehouse.jobs.run_full_pipeline --env dev
```

## Layers

- Bronze: append-only ingestion from raw JSON with dataset, file path, file hash, and ingestion timestamp.
- Silver: cleaned domain tables for matches, participants, teams, summoners, ranked, timeline frames, and timeline events.
- Gold: analytics aggregates for players, champions, roles, ranks, and team objectives.
- Platinum: ML-ready feature tables for match win, player performance, and champion meta modeling.

## Configuration

Edit `configs/dev.yaml` for local paths and `configs/tables.yaml` for table metadata. Copy `.env.example` to `.env` for local secrets.
