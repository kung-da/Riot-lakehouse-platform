from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.jobs._cli import load_job_config


def main() -> None:
    config = load_job_config("Run Bronze ingestion")
    count = run_bronze_ingestion(config)
    print(f"Bronze ingested {count} new raw files")


if __name__ == "__main__":
    main()
