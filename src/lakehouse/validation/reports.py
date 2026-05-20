from __future__ import annotations

import json
from pathlib import Path


def write_quality_report(report_root: Path, name: str, payload: dict) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    path = report_root / f"{name}.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return path
