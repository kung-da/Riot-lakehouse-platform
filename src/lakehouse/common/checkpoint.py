from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


def _checkpoint_key(file_path: Path | str) -> str:
    if isinstance(file_path, Path):
        return file_path.as_posix()
    return str(file_path).replace("\\", "/")


@dataclass
class FileCheckpoint:
    dataset: str
    processed_files: set[str] = field(default_factory=set)

    @classmethod
    def load(cls, checkpoint_root: Path, dataset: str) -> "FileCheckpoint":
        path = checkpoint_root / f"{dataset}.json"
        if not path.exists():
            return cls(dataset=dataset)
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return cls(dataset=dataset, processed_files=set(payload.get("processed_files", [])))

    def save(self, checkpoint_root: Path) -> None:
        checkpoint_root.mkdir(parents=True, exist_ok=True)
        path = checkpoint_root / f"{self.dataset}.json"
        payload = {"dataset": self.dataset, "processed_files": sorted(self.processed_files)}
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    def is_processed(self, file_path: Path | str) -> bool:
        return _checkpoint_key(file_path) in self.processed_files

    def mark_processed(self, file_path: Path | str) -> None:
        self.processed_files.add(_checkpoint_key(file_path))
