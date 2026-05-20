from __future__ import annotations

from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None


if DAG:
    with DAG("riot_full_lakehouse_pipeline", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
        BashOperator(task_id="run_full_pipeline", bash_command="python -m lakehouse.jobs.run_full_pipeline --env dev")
