from __future__ import annotations

from dags._common import DEFAULT_START_DATE, DAG, lakehouse_task


if DAG:
    with DAG(
        "riot_bronze_ingestion",
        start_date=DEFAULT_START_DATE,
        schedule=None,
        catchup=False,
    ) as dag:
        lakehouse_task(
            task_id="run_bronze",
            module="lakehouse.jobs.run_bronze",
        )
