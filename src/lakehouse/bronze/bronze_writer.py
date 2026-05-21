from __future__ import annotations

from pathlib import Path
from typing import Any


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


def _spark_path(path: Path) -> str:
    return path.as_posix()


def write_parquet(
    spark: Any,
    records: list[dict[str, str]],
    output_path: Path,
    partition_columns: list[str],
) -> Path:
    if not records:
        return output_path

    dataframe = spark.createDataFrame(records, schema=_bronze_schema())
    (
        dataframe.write.mode("append")
        .option("compression", "snappy")
        .partitionBy(*partition_columns)
        .parquet(_spark_path(output_path))
    )
    return output_path
