from __future__ import annotations

from lakehouse.bronze.bronze_writer import write_parquet
from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.raw.raw_to_bronze import collect_new_bronze_records


DEFAULT_PARTITION_COLUMNS = ["dataset", "ingest_date"]


def _bronze_partition_columns(config: LakehouseConfig) -> list[str]:
    partition_config = config.values.get("partition_columns", {})
    return partition_config.get("bronze", DEFAULT_PARTITION_COLUMNS)


def run_bronze_ingestion(config: LakehouseConfig, datasets: list[str] | None = None) -> int:
    batch = collect_new_bronze_records(config.raw_root, config.checkpoint_root, datasets)
    if not batch.records:
        return 0

    spark = get_spark(config=config)
    try:
        write_parquet(
            spark=spark,
            records=batch.records,
            output_path=config.layer_path("bronze", "raw_json"),
            partition_columns=_bronze_partition_columns(config),
        )
        batch.save_checkpoints(config.checkpoint_root)
        return len(batch)
    finally:
        spark.stop()
