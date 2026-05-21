from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterator

from lakehouse.common.checkpoint import FileCheckpoint
from lakehouse.common.io import iter_json_files
from lakehouse.common.logging import get_logger
from lakehouse.raw.raw_file_reader import read_raw_record


LOGGER = get_logger(__name__)
DEFAULT_DATASETS = ["matches", "timelines", "summoners", "ranked"]
DEFAULT_MAX_RECORDS_PER_BATCH = 100
DEFAULT_MAX_BYTES_PER_BATCH = 64 * 1024 * 1024


@dataclass(frozen=True)
class BronzeRecordBatch:
    records: list[dict[str, str]]
    checkpoints: dict[str, FileCheckpoint]

    def save_checkpoints(self, checkpoint_root: Path) -> None:
        for checkpoint in self.checkpoints.values():
            checkpoint.save(checkpoint_root)

    def __len__(self) -> int:
        return len(self.records)


def _relative_source_file(raw_root: Path, path: Path) -> str:
    try:
        return path.relative_to(raw_root.parent).as_posix()
    except ValueError:
        return path.as_posix()


def collect_new_bronze_records(
    raw_root: Path,
    checkpoint_root: Path,
    datasets: list[str] | None = None,
    max_records_per_batch: int = DEFAULT_MAX_RECORDS_PER_BATCH,
    max_files: int | None = None,
    dataset_max_files: dict[str, int] | None = None,
    dataset_batch_sizes: dict[str, int] | None = None,
    max_bytes_per_batch: int = DEFAULT_MAX_BYTES_PER_BATCH,
    dataset_max_bytes_per_batch: dict[str, int] | None = None,
) -> BronzeRecordBatch:
    records: list[dict[str, str]] = []
    checkpoints: dict[str, FileCheckpoint] = {}
    for batch in iter_new_bronze_record_batches(
        raw_root=raw_root,
        checkpoint_root=checkpoint_root,
        datasets=datasets,
        max_records_per_batch=max_records_per_batch,
        max_files=max_files,
        dataset_max_files=dataset_max_files,
        dataset_batch_sizes=dataset_batch_sizes,
        max_bytes_per_batch=max_bytes_per_batch,
        dataset_max_bytes_per_batch=dataset_max_bytes_per_batch,
    ):
        records.extend(batch.records)
        checkpoints.update(batch.checkpoints)

    return BronzeRecordBatch(records=records, checkpoints=checkpoints)


def iter_new_bronze_record_batches(
    raw_root: Path,
    checkpoint_root: Path,
    datasets: list[str] | None = None,
    max_records_per_batch: int = DEFAULT_MAX_RECORDS_PER_BATCH,
    max_files: int | None = None,
    dataset_max_files: dict[str, int] | None = None,
    dataset_batch_sizes: dict[str, int] | None = None,
    max_bytes_per_batch: int = DEFAULT_MAX_BYTES_PER_BATCH,
    dataset_max_bytes_per_batch: dict[str, int] | None = None,
) -> Iterator[BronzeRecordBatch]:
    if max_records_per_batch < 1:
        raise ValueError("max_records_per_batch must be greater than zero")
    if max_files is not None and max_files < 0:
        raise ValueError("max_files must be greater than or equal to zero")
    for dataset, limit in (dataset_max_files or {}).items():
        if limit < 0:
            raise ValueError(f"dataset_max_files[{dataset}] must be greater than or equal to zero")
    for dataset, batch_size in (dataset_batch_sizes or {}).items():
        if batch_size < 1:
            raise ValueError(f"dataset_batch_sizes[{dataset}] must be greater than zero")
    if max_bytes_per_batch < 1:
        raise ValueError("max_bytes_per_batch must be greater than zero")
    for dataset, byte_limit in (dataset_max_bytes_per_batch or {}).items():
        if byte_limit < 1:
            raise ValueError(f"dataset_max_bytes_per_batch[{dataset}] must be greater than zero")
    if max_files == 0:
        return

    selected = datasets or DEFAULT_DATASETS
    total_collected = 0
    for dataset in selected:
        dataset_limit = (dataset_max_files or {}).get(dataset)
        batch_size = (dataset_batch_sizes or {}).get(dataset, max_records_per_batch)
        byte_limit = (dataset_max_bytes_per_batch or {}).get(dataset, max_bytes_per_batch)
        checkpoint = FileCheckpoint.load(checkpoint_root, dataset)
        dataset_limit_remaining = (
            None if dataset_limit is None else dataset_limit - len(checkpoint.processed_files)
        )
        if dataset_limit_remaining is not None and dataset_limit_remaining <= 0:
            continue

        records: list[dict[str, str]] = []
        batch_bytes = 0
        dataset_collected = 0
        for path in iter_json_files(raw_root / dataset):
            if max_files is not None and total_collected >= max_files:
                break
            if dataset_limit_remaining is not None and dataset_collected >= dataset_limit_remaining:
                break

            source_file = _relative_source_file(raw_root, path)
            if checkpoint.is_processed(source_file):
                continue
            file_size = path.stat().st_size
            if records and batch_bytes + file_size > byte_limit:
                yield BronzeRecordBatch(records=records, checkpoints={dataset: checkpoint})
                records = []
                batch_bytes = 0

            try:
                record = read_raw_record(path, source_file=source_file)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                LOGGER.warning("Skipping invalid raw JSON file %s: %s", path, exc)
                continue
            row: dict[str, str] = asdict(record)
            records.append(row)
            checkpoint.mark_processed(source_file)
            total_collected += 1
            dataset_collected += 1
            batch_bytes += file_size

            if len(records) >= batch_size:
                yield BronzeRecordBatch(records=records, checkpoints={dataset: checkpoint})
                records = []
                batch_bytes = 0

        if records:
            yield BronzeRecordBatch(records=records, checkpoints={dataset: checkpoint})

        if max_files is not None and total_collected >= max_files:
            break
