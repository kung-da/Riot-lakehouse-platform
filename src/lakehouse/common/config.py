from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lakehouse.common.storage import S3Path, is_s3_uri

@dataclass(frozen=True)
class LakehouseConfig:
    environment: str
    project_root: Path
    raw_root: Path | S3Path
    lakehouse_root: Path | S3Path
    checkpoint_root: Path | S3Path
    report_root: Path | S3Path
    default_format: str
    write_mode: str
    values: dict[str, Any]

    def layer_path(self, layer: str, table: str | None = None) -> Path | S3Path:
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


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _read_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, separator, raw_value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        value = raw_value.strip().strip("\"'")
        os.environ.setdefault(key, value)


def _load_env_file(path: str | Path | None) -> None:
    env_path = Path(path or os.getenv("LAKEHOUSE_ENV_FILE", ".env"))
    _read_env_file(env_path)
    nested_env_file = os.getenv("LAKEHOUSE_ENV_FILE")
    if path is None and nested_env_file and Path(nested_env_file) != env_path:
        _read_env_file(Path(nested_env_file))


def _resolve_env_expression(expression: str) -> str:
    name = expression
    default: str | None = None
    required_message: str | None = None
    if ":?" in expression:
        name, required_message = expression.split(":?", 1)
    elif ":-" in expression:
        name, default = expression.split(":-", 1)
    name = name.strip()

    value = os.getenv(name)
    if value not in {None, ""}:
        return str(value)
    if required_message is not None:
        message = required_message or f"Set {name}"
        raise ValueError(f"Missing required environment variable {name}: {message}")
    return default or ""


def _coerce_scalar(value: str) -> Any:
    normalized = value.strip()
    lower_value = normalized.lower()
    if lower_value in {"true", "false"}:
        return lower_value == "true"
    if lower_value in {"null", "~", "none"}:
        return None
    try:
        return int(normalized)
    except ValueError:
        return value


def _interpolate_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _interpolate_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(item) for item in value]
    if not isinstance(value, str):
        return value

    replaced = _ENV_PATTERN.sub(lambda match: _resolve_env_expression(match.group(1)), value)
    return _coerce_scalar(replaced) if replaced != value or _ENV_PATTERN.search(value) else value


def _config_path(env: str, config_dir: str | Path) -> Path:
    env_path = Path(env)
    if env_path.suffix in {".yaml", ".yml"} or env_path.exists():
        return env_path
    return Path(config_dir) / f"{env}.yaml"


def load_config(
    env: str | None = None,
    config_dir: str | Path | None = None,
    env_file: str | Path | None = None,
) -> LakehouseConfig:
    _load_env_file(env_file)
    selected_env = env or os.getenv("LAKEHOUSE_ENV", "dev")
    selected_config_dir = config_dir or os.getenv("LAKEHOUSE_CONFIG_DIR", "configs")
    config_path = _config_path(selected_env, selected_config_dir)
    values = _load_yaml(config_path)
    values = _interpolate_env(values)

    project_root = Path(values.get("project_root", ".")).resolve()

    def resolve_path(key: str) -> Path | S3Path:
        raw_value = str(values[key])
        if is_s3_uri(raw_value):
            return S3Path(raw_value)
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
