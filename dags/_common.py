from __future__ import annotations

from datetime import datetime
from pathlib import Path
import shlex

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None
    BashOperator = None


DEFAULT_START_DATE = datetime(2026, 1, 1)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def lakehouse_command(module: str) -> str:
    return f"cd {shlex.quote(str(PROJECT_ROOT))} && python -m {module}"
