from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse.raw.detect_dataset import detect_dataset


@dataclass(frozen=True)
class RawRecord:
    dataset: str
    source_file: str
    file_hash: str
    ingest_ts: str
    payload: dict[str, Any] | list[Any]


def read_raw_record(path: Path) -> RawRecord:
    raw_bytes = path.read_bytes()
    payload = json.loads(raw_bytes.decode("utf-8"))
    return RawRecord(
        dataset=detect_dataset(path, payload),
        source_file=path.as_posix(),
        file_hash=hashlib.sha256(raw_bytes).hexdigest(),
        ingest_ts=datetime.now(timezone.utc).isoformat(),
        payload=payload,
    )
