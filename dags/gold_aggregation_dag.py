from __future__ import annotations

from dags._common import DEFAULT_START_DATE, DAG, BashOperator, lakehouse_command


if DAG:
    with DAG(
        "riot_gold_model",
        start_date=DEFAULT_START_DATE,
        schedule=None,
        catchup=False,
    ) as dag:
        BashOperator(
            task_id="run_gold",
            bash_command=lakehouse_command("lakehouse.jobs.run_gold"),
        )
