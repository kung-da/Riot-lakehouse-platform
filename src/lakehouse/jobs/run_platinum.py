from lakehouse.jobs._cli import load_job_config
from lakehouse.platinum.platinum_builder import platinum_sql_registry


def main() -> None:
    load_job_config("Run Platinum features")
    print(platinum_sql_registry())


if __name__ == "__main__":
    main()
