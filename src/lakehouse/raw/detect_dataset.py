from __future__ import annotations

from pathlib import Path
from typing import Any

from lakehouse.common.storage import S3Path


DATASET_BY_FOLDER = {
    "matches": "matches",
    "timelines": "timelines",
    "summoners": "summoners",
    "ranked": "ranked",
}


def detect_dataset_from_path(path: Path | S3Path) -> str | None:
    folder_name = path.parent.name.lower()
    if folder_name in DATASET_BY_FOLDER:
        return DATASET_BY_FOLDER[folder_name]
    return None


def detect_dataset(path: Path | S3Path, payload: dict[str, Any] | list[Any] | None = None) -> str:
    dataset = detect_dataset_from_path(path)
    if dataset:
        return dataset

    if payload is None:
        return "unknown"
    if isinstance(payload, dict):
        if {"metadata", "info"} <= payload.keys():
            info = payload.get("info", {})
            return "timelines" if "frames" in info else "matches"
        if "entries" in payload and "tier" in payload:
            return "ranked"
        if "puuid" in payload and "summonerLevel" in payload:
            return "summoners"

    return "unknown"
