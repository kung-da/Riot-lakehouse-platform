from __future__ import annotations

import argparse

from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.jobs._cli import add_config_args, load_config_from_args


DATASET_ALIASES = {
    "match": "matches",
    "matches": "matches",
    "timeline": "timelines",
    "timelines": "timelines",
    "summoner": "summoners",
    "summoners": "summoners",
    "ranked": "ranked",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Bronze ingestion")
    add_config_args(parser)
    parser.add_argument(
        "--datasets",
        nargs="+",
        default=None,
        help="Optional datasets to ingest: matches timelines summoners ranked",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=None,
        help="Maximum new raw files to ingest",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Maximum raw files to collect and write per Spark batch",
    )
    return parser.parse_args()


def _normalize_datasets(datasets: list[str] | None) -> list[str] | None:
    if not datasets:
        return None

    normalized: list[str] = []
    for dataset in datasets:
        key = dataset.lower()
        if key not in DATASET_ALIASES:
            valid = ", ".join(sorted(set(DATASET_ALIASES.values())))
            raise ValueError(f"Unknown dataset '{dataset}'. Valid datasets: {valid}")
        normalized.append(DATASET_ALIASES[key])
    return normalized


def main() -> None:
    args = _parse_args()
    config = load_config_from_args(args)
    count = run_bronze_ingestion(
        config=config,
        datasets=_normalize_datasets(args.datasets),
        max_files=args.max_files,
        max_records_per_batch=args.batch_size,
    )
    print(f"Bronze ingested {count} new raw files")


if __name__ == "__main__":
    main()
