from __future__ import annotations

from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None


if DAG:
    with DAG(
        "riot_gold_model",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
    ) as dag:
        BashOperator(task_id="run_gold", bash_command="python -m lakehouse.jobs.run_gold --env dev")
