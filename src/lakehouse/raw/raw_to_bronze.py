from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from lakehouse.common.checkpoint import FileCheckpoint
from lakehouse.common.io import iter_json_files
from lakehouse.raw.raw_file_reader import read_raw_record


def collect_new_bronze_records(
    raw_root: Path,
    checkpoint_root: Path,
    datasets: list[str] | None = None,
) -> list[dict]:
    selected = datasets or ["matches", "timelines", "summoners", "ranked"]
    records: list[dict] = []
    checkpoints: dict[str, FileCheckpoint] = {}

    for dataset in selected:
        checkpoint = FileCheckpoint.load(checkpoint_root, dataset)
        checkpoints[dataset] = checkpoint
        for path in iter_json_files(raw_root / dataset):
            if checkpoint.is_processed(path):
                continue
            record = read_raw_record(path)
            row = asdict(record)
            row["payload_json"] = json.dumps(row.pop("payload"), ensure_ascii=False)
            records.append(row)
            checkpoint.mark_processed(path)

    for checkpoint in checkpoints.values():
        checkpoint.save(checkpoint_root)
    return records
