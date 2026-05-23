from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.common.paths import ensure_directories
from lakehouse.gold.gold_transformer import run_gold_transform
from lakehouse.jobs._cli import load_job_config
from lakehouse.silver.silver_transformer import run_silver_transform


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
    silver_counts = run_silver_transform(config)
    gold_counts = run_gold_transform(config)
    print(
        "Full pipeline completed. "
        f"Bronze ingested {bronze_count} new raw files. "
        f"Silver rows: {silver_counts}. "
        f"Gold rows: {gold_counts}"
    )


if __name__ == "__main__":
    main()
