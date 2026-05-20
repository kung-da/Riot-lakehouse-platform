from lakehouse.gold.gold_builder import gold_sql_registry
from lakehouse.jobs._cli import load_job_config


def main() -> None:
    load_job_config("Run Gold aggregations")
    print(gold_sql_registry())


if __name__ == "__main__":
    main()
