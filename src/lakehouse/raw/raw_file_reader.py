from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.common.storage import S3Path
from lakehouse.raw.detect_dataset import detect_dataset, detect_dataset_from_path


@dataclass(frozen=True)
class RawRecord:
    dataset: str
    source_file: str
    file_hash: str
    ingest_ts: str
    ingest_date: str
    payload_json: str


def read_raw_record(path: Path | S3Path, source_file: str | None = None) -> RawRecord:
    raw_bytes = path.read_bytes()
    payload_json = raw_bytes.decode("utf-8")
    dataset = detect_dataset_from_path(path)
    if dataset is None:
        dataset = detect_dataset(path, json.loads(payload_json))
    ingest_time = datetime.now(timezone.utc)
    return RawRecord(
        dataset=dataset,
        source_file=source_file or path.as_posix(),
        file_hash=hashlib.sha256(raw_bytes).hexdigest(),
        ingest_ts=ingest_time.isoformat(),
        ingest_date=ingest_time.date().isoformat(),
        payload_json=payload_json,
    )
