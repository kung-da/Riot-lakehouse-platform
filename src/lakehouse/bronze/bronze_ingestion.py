from __future__ import annotations

from lakehouse.bronze.bronze_writer import cleanup_temporary_output, write_parquet
from lakehouse.common.config import LakehouseConfig
from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark
from lakehouse.raw.raw_to_bronze import (
    DEFAULT_MAX_BYTES_PER_BATCH,
    DEFAULT_MAX_RECORDS_PER_BATCH,
    iter_new_bronze_record_batches,
)


DEFAULT_PARTITION_COLUMNS = ["dataset", "ingest_date"]
LOGGER = get_logger(__name__)


def _bronze_partition_columns(config: LakehouseConfig) -> list[str]:
    partition_config = config.values.get("partition_columns", {})
    return partition_config.get("bronze", DEFAULT_PARTITION_COLUMNS)


def _max_records_per_batch(config: LakehouseConfig) -> int:
    bronze_config = config.values.get("bronze", {})
    return int(bronze_config.get("max_records_per_batch", DEFAULT_MAX_RECORDS_PER_BATCH))


def _max_bytes_per_batch(config: LakehouseConfig) -> int:
    bronze_config = config.values.get("bronze", {})
    return int(bronze_config.get("max_bytes_per_batch", DEFAULT_MAX_BYTES_PER_BATCH))


def _dataset_max_files(config: LakehouseConfig) -> dict[str, int]:
    bronze_config = config.values.get("bronze", {})
    return {
        str(dataset): int(limit)
        for dataset, limit in bronze_config.get("dataset_max_files", {}).items()
    }


def _dataset_batch_sizes(config: LakehouseConfig) -> dict[str, int]:
    bronze_config = config.values.get("bronze", {})
    return {
        str(dataset): int(batch_size)
        for dataset, batch_size in bronze_config.get("dataset_batch_sizes", {}).items()
    }


def _dataset_max_bytes_per_batch(config: LakehouseConfig) -> dict[str, int]:
    bronze_config = config.values.get("bronze", {})
    return {
        str(dataset): int(byte_limit)
        for dataset, byte_limit in bronze_config.get("dataset_max_bytes_per_batch", {}).items()
    }


def _output_partitions(config: LakehouseConfig) -> int:
    bronze_config = config.values.get("bronze", {})
    return int(bronze_config.get("output_partitions", 1))


def run_bronze_ingestion(
    config: LakehouseConfig,
    datasets: list[str] | None = None,
    max_files: int | None = None,
    max_records_per_batch: int | None = None,
    dataset_max_files: dict[str, int] | None = None,
    dataset_batch_sizes: dict[str, int] | None = None,
    max_bytes_per_batch: int | None = None,
    dataset_max_bytes_per_batch: dict[str, int] | None = None,
) -> int:
    spark = None
    total_records = 0
    batch_size = (
        _max_records_per_batch(config)
        if max_records_per_batch is None
        else max_records_per_batch
    )
    byte_limit = (
        _max_bytes_per_batch(config)
        if max_bytes_per_batch is None
        else max_bytes_per_batch
    )
    effective_dataset_max_files = (
        dataset_max_files
        if dataset_max_files is not None
        else {} if max_files is not None else _dataset_max_files(config)
    )
    effective_dataset_batch_sizes = (
        dataset_batch_sizes
        if dataset_batch_sizes is not None
        else {} if max_records_per_batch is not None else _dataset_batch_sizes(config)
    )
    effective_dataset_max_bytes_per_batch = (
        dataset_max_bytes_per_batch
        if dataset_max_bytes_per_batch is not None
        else {} if max_bytes_per_batch is not None else _dataset_max_bytes_per_batch(config)
    )
    output_path = config.layer_path("bronze", "raw_json")
    cleanup_temporary_output(output_path)
    try:
        for batch in iter_new_bronze_record_batches(
            raw_root=config.raw_root,
            checkpoint_root=config.checkpoint_root,
            datasets=datasets,
            max_records_per_batch=batch_size,
            max_bytes_per_batch=byte_limit,
            max_files=max_files,
            dataset_max_files=effective_dataset_max_files,
            dataset_batch_sizes=effective_dataset_batch_sizes,
            dataset_max_bytes_per_batch=effective_dataset_max_bytes_per_batch,
        ):
            if spark is None:
                spark = get_spark(config=config)

            write_parquet(
                spark=spark,
                records=batch.records,
                output_path=output_path,
                partition_columns=_bronze_partition_columns(config),
                output_partitions=_output_partitions(config),
            )
            batch.save_checkpoints(config.checkpoint_root)
            total_records += len(batch)
            LOGGER.info("Bronze ingested %s records in this batch (%s total)", len(batch), total_records)

        return total_records
    finally:
        if spark is not None:
            spark.stop()
