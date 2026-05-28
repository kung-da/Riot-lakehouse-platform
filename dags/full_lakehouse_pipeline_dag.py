from __future__ import annotations

from dags._common import DEFAULT_START_DATE, DAG, BashOperator, lakehouse_command


if DAG:
    with DAG(
        "riot_full_lakehouse_pipeline",
        start_date=DEFAULT_START_DATE,
        schedule=None,
        catchup=False,
    ) as dag:
        run_bronze = BashOperator(
            task_id="run_bronze",
            bash_command=lakehouse_command("lakehouse.jobs.run_bronze"),
        )
        run_silver = BashOperator(
            task_id="run_silver",
            bash_command=lakehouse_command("lakehouse.jobs.run_silver"),
        )
        run_gold = BashOperator(
            task_id="run_gold",
            bash_command=lakehouse_command("lakehouse.jobs.run_gold"),
        )
        run_platinum = BashOperator(
            task_id="run_platinum",
            bash_command=lakehouse_command("lakehouse.jobs.run_platinum"),
        )
        run_data_quality = BashOperator(
            task_id="run_data_quality",
            bash_command=lakehouse_command("lakehouse.jobs.run_data_quality"),
        )

        run_bronze >> run_silver >> run_gold >> run_platinum >> run_data_quality
