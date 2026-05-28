from __future__ import annotations

import json
from pathlib import Path

from lakehouse.common.storage import S3Path


def write_quality_report(report_root: Path | S3Path, name: str, payload: dict) -> Path | S3Path:
    report_root.mkdir(parents=True, exist_ok=True)
    path = report_root / f"{name}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path
