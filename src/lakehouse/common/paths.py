from __future__ import annotations

from pathlib import Path
from typing import Any

from lakehouse.common.storage import S3Path, is_s3_path


def ensure_directories(paths: list[Any]) -> None:
    for path in paths:
        if not is_s3_path(path):
            path.mkdir(parents=True, exist_ok=True)


def dataset_raw_path(raw_root: Path | S3Path, dataset: str) -> Path | S3Path:
    return raw_root / dataset
