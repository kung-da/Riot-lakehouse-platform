from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import shlex

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None
    BashOperator = None


DEFAULT_START_DATE = datetime(2026, 1, 1)
DEFAULT_TASK_RETRIES = 3
DEFAULT_TASK_RETRY_DELAY = timedelta(minutes=3)
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def lakehouse_command(module: str) -> str:
    python_module = f"python -m {shlex.quote(module)}"
    return (
        f"cd {shlex.quote(str(PROJECT_ROOT))} && "
        'lakehouse_env="${LAKEHOUSE_ENV:-prod}" && '
        f'echo "Running lakehouse command: {python_module} --env ${{lakehouse_env}}" && '
        f'{python_module} --env "${{lakehouse_env}}"'
    )


def lakehouse_task(task_id: str, module: str, **kwargs):
    task_kwargs = {
        "retries": DEFAULT_TASK_RETRIES,
        "retry_delay": DEFAULT_TASK_RETRY_DELAY,
    }
    task_kwargs.update(kwargs)
    return BashOperator(
        task_id=task_id,
        bash_command=lakehouse_command(module),
        **task_kwargs,
    )
