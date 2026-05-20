from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.common.paths import ensure_directories
from lakehouse.jobs._cli import load_job_config


def main() -> None:
    config = load_job_config("Run full Riot lakehouse pipeline")
    ensure_directories(
        [
            config.layer_path("bronze"),
            config.layer_path("silver"),
            config.layer_path("gold"),
            config.layer_path("platinum"),
            config.checkpoint_root,
            config.report_root,
        ]
    )
    bronze_count = run_bronze_ingestion(config)
    print(f"Full pipeline scaffold completed. Bronze ingested {bronze_count} new raw files.")


if __name__ == "__main__":
    main()
