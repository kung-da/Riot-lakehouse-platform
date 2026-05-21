from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from lakehouse.common.checkpoint import FileCheckpoint
from lakehouse.common.io import iter_json_files
from lakehouse.common.logging import get_logger
from lakehouse.raw.raw_file_reader import read_raw_record


LOGGER = get_logger(__name__)
DEFAULT_DATASETS = ["matches", "timelines", "summoners", "ranked"]


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
) -> BronzeRecordBatch:
    selected = datasets or DEFAULT_DATASETS
    records: list[dict[str, str]] = []
    checkpoints: dict[str, FileCheckpoint] = {}

    for dataset in selected:
        checkpoint = FileCheckpoint.load(checkpoint_root, dataset)
        checkpoints[dataset] = checkpoint
        for path in iter_json_files(raw_root / dataset):
            source_file = _relative_source_file(raw_root, path)
            if checkpoint.is_processed(source_file):
                continue
            try:
                record = read_raw_record(path, source_file=source_file)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                LOGGER.warning("Skipping invalid raw JSON file %s: %s", path, exc)
                continue
            row: dict[str, str] = asdict(record)
            records.append(row)
            checkpoint.mark_processed(source_file)

    return BronzeRecordBatch(records=records, checkpoints=checkpoints)
