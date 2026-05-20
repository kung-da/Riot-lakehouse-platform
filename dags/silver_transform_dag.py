from __future__ import annotations

from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None


if DAG:
    with DAG("riot_silver_transform", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
        BashOperator(task_id="run_silver", bash_command="python -m lakehouse.jobs.run_silver --env dev")
