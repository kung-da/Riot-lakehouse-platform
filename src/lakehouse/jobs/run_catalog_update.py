from lakehouse.catalog.register_tables import planned_catalog_tables
from lakehouse.jobs._cli import load_job_config


def main() -> None:
    load_job_config("Run catalog update")
    print(planned_catalog_tables())


if __name__ == "__main__":
    main()
