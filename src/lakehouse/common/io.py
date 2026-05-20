from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


def iter_json_files(path: Path) -> Iterable[Path]:
    if not path.exists():
        return []
    return sorted(file for file in path.glob("*.json") if file.name != ".gitkeep")


def read_json(path: Path) -> dict[str, Any] | list[Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
