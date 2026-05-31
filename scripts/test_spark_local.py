"""Smoke test the local Spark runtime used by the Airflow lakehouse jobs."""

from __future__ import annotations

import argparse
import os
import platform
import subprocess
import sys
from pathlib import Path


def load_env_file(path: Path) -> None:
    if not path.exists():
        print(f"[WARN] env file not found: {path}")
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if value:
            os.environ.setdefault(key, value)


def print_java_version() -> None:
    try:
        completed = subprocess.run(
            ["java", "-version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("[FAIL] java: not found on PATH")
        return

    output = (completed.stderr or completed.stdout).splitlines()
    version = output[0] if output else "<unknown>"
    print(f"[INFO] java: {version}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a minimal local PySpark smoke test.")
    parser.add_argument("--env-file", default=".env", help="Path to the env file to load.")
    parser.add_argument("--master", default=None, help="Spark master override, e.g. local[2].")
    args = parser.parse_args()

    load_env_file(Path(args.env_file))

    print(f"[INFO] python: {platform.python_version()} ({sys.executable})")
    print(f"[INFO] LAKEHOUSE_ENV: {os.getenv('LAKEHOUSE_ENV', '<unset>')}")
    print_java_version()

    try:
        from lakehouse.common.config import load_config
        from lakehouse.common.spark import get_spark
    except Exception as exc:  # noqa: BLE001 - this is a diagnostic CLI.
        print(f"[FAIL] lakehouse import: {type(exc).__name__} - {exc}")
        return 1

    try:
        config = load_config(os.getenv("LAKEHOUSE_ENV") or "dev")
        if args.master:
            config.values.setdefault("spark", {})["master"] = args.master
        spark = get_spark(config=config)
        count = spark.range(5).where("id >= 2").count()
        spark.stop()
    except Exception as exc:  # noqa: BLE001 - this is a diagnostic CLI.
        print(f"[FAIL] spark smoke test: {type(exc).__name__} - {exc}")
        return 1

    if count != 3:
        print(f"[FAIL] spark smoke test: expected count=3, got {count}")
        return 1

    print("[OK] spark smoke test: SparkSession started and executed a small job")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
