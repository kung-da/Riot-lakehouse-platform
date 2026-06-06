from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from lakehouse.common.storage import S3Path, is_s3_path, to_spark_path, write_parquet_dataset


def _bronze_schema() -> Any:
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("dataset", StringType(), nullable=False),
            StructField("source_file", StringType(), nullable=False),
            StructField("file_hash", StringType(), nullable=False),
            StructField("ingest_ts", StringType(), nullable=False),
            StructField("ingest_date", StringType(), nullable=False),
            StructField("payload_json", StringType(), nullable=False),
        ]
    )


def _spark_path(path: Path | S3Path) -> str:
    return to_spark_path(path)


def cleanup_temporary_output(output_path: Path | S3Path) -> None:
    if is_s3_path(output_path):
        return
    temporary_path = output_path / "_temporary"
    if temporary_path.exists():
        shutil.rmtree(temporary_path)


def write_parquet(
    spark: Any,
    records: list[dict[str, str]],
    output_path: Path | S3Path,
    partition_columns: list[str],
    output_partitions: int = 1,
) -> Path | S3Path:
    if not records:
        return output_path

    if output_partitions < 1:
        raise ValueError("output_partitions must be greater than zero")

    dataframe = spark.createDataFrame(records, schema=_bronze_schema())
    write_parquet_dataset(
        dataframe=dataframe,
        output_path=output_path,
        mode="append",
        partition_columns=partition_columns,
        output_partitions=output_partitions,
    )
    return output_path
