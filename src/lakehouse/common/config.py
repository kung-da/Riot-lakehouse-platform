from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class LakehouseConfig:
    environment: str
    project_root: Path
    raw_root: Path
    lakehouse_root: Path
    checkpoint_root: Path
    report_root: Path
    default_format: str
    write_mode: str
    values: dict[str, Any]

    def layer_path(self, layer: str, table: str | None = None) -> Path:
        path = self.lakehouse_root / layer
        return path / table if table else path


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if value in {"true", "false"}:
        return value == "true"
    if value in {"null", "~"}:
        return None
    return value.strip("\"'")


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        values: dict[str, Any] = {}
        stack: list[tuple[int, Any]] = [(-1, values)]
        last_key_by_indent: dict[int, str] = {}
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            if not raw_line.strip() or raw_line.lstrip().startswith("#"):
                continue
            indent = len(raw_line) - len(raw_line.lstrip(" "))
            line = raw_line.strip()
            while stack and indent <= stack[-1][0]:
                stack.pop()
            parent = stack[-1][1]
            if line.startswith("- "):
                item = _parse_scalar(line[2:])
                if isinstance(parent, list):
                    parent.append(item)
                continue
            key, _, raw_value = line.partition(":")
            key = key.strip()
            raw_value = raw_value.strip()
            if raw_value:
                parent[key] = _parse_scalar(raw_value)
                last_key_by_indent[indent] = key
                continue
            next_container: dict[str, Any] | list[Any] = {}
            parent[key] = next_container
            stack.append((indent, next_container))
            last_key_by_indent[indent] = key
        return values
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_config(env: str = "dev", config_dir: str | Path = "configs") -> LakehouseConfig:
    config_path = Path(config_dir) / f"{env}.yaml"
    values = _load_yaml(config_path)

    project_root = Path(values.get("project_root", ".")).resolve()

    def resolve_path(key: str) -> Path:
        raw_value = str(values[key])
        if raw_value.startswith("s3://"):
            return Path(raw_value)
        path = Path(raw_value)
        return path if path.is_absolute() else project_root / path

    return LakehouseConfig(
        environment=values["environment"],
        project_root=project_root,
        raw_root=resolve_path("raw_root"),
        lakehouse_root=resolve_path("lakehouse_root"),
        checkpoint_root=resolve_path("checkpoint_root"),
        report_root=resolve_path("report_root"),
        default_format=values.get("default_format", "parquet"),
        write_mode=values.get("write_mode", "append"),
        values=values,
    )
