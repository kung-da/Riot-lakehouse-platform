from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

from lakehouse.common.storage import S3Path


def iter_json_files(path: Path | S3Path) -> Iterable[Path | S3Path]:
    if not path.exists():
        return []
    return sorted(
        (file for file in path.glob("*.json") if file.name != ".gitkeep"),
        key=lambda file: file.as_posix(),
    )


def read_json(path: Path | S3Path) -> dict[str, Any] | list[Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
