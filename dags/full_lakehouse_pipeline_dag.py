from __future__ import annotations

from dags._common import DEFAULT_START_DATE, DAG, lakehouse_task


if DAG:
    with DAG(
        "riot_full_lakehouse_pipeline",
        start_date=DEFAULT_START_DATE,
        schedule=None,
        catchup=False,
    ) as dag:
        check_environment = lakehouse_task(
            task_id="check_environment",
            module="lakehouse.jobs.check_environment",
        )
        run_bronze = lakehouse_task(
            task_id="run_bronze",
            module="lakehouse.jobs.run_bronze",
        )
        run_silver = lakehouse_task(
            task_id="run_silver",
            module="lakehouse.jobs.run_silver",
        )
        run_gold = lakehouse_task(
            task_id="run_gold",
            module="lakehouse.jobs.run_gold",
        )
        run_platinum = lakehouse_task(
            task_id="run_platinum",
            module="lakehouse.jobs.run_platinum",
        )
        run_data_quality = lakehouse_task(
            task_id="run_data_quality",
            module="lakehouse.jobs.run_data_quality",
        )

        (
            check_environment
            >> run_bronze
            >> run_silver
            >> run_gold
            >> run_platinum
            >> run_data_quality
        )
