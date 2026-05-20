from __future__ import annotations

from typing import Any


def get_spark(app_name: str = "riot-lakehouse", master: str = "local[*]", enable_delta: bool = False) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("Install the spark extra to run Spark jobs: pip install -e '.[spark]'") from exc

    builder = SparkSession.builder.appName(app_name).master(master)
    if enable_delta:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    return builder.getOrCreate()
