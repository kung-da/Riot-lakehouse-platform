from __future__ import annotations

from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None
    BashOperator = None


DEFAULT_START_DATE = datetime(2026, 1, 1)


def lakehouse_command(module: str) -> str:
    return f"python -m {module}"
