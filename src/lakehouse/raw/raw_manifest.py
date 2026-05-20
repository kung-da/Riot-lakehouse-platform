from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from lakehouse.common.io import iter_json_files
from lakehouse.raw.raw_file_reader import read_raw_record


def build_raw_manifest(raw_root: Path, datasets: list[str] | None = None) -> list[dict]:
    selected = datasets or ["matches", "timelines", "summoners", "ranked"]
    records: list[dict] = []
    for dataset in selected:
        for path in iter_json_files(raw_root / dataset):
            record = read_raw_record(path)
            row = asdict(record)
            row["payload"] = None
            records.append(row)
    return records
