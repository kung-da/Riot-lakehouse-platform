from __future__ import annotations

from pathlib import Path


def ensure_directories(paths: list[Path]) -> None:
    for path in paths:
        if not str(path).startswith("s3:/"):
            path.mkdir(parents=True, exist_ok=True)


def dataset_raw_path(raw_root: Path, dataset: str) -> Path:
    return raw_root / dataset
