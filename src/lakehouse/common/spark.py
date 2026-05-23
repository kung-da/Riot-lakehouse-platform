from __future__ import annotations

from typing import Any


def get_spark(
    app_name: str = "riot-lakehouse",
    master: str = "local[*]",
    enable_delta: bool = False,
    config: Any | None = None,
) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("Install the spark extra to run Spark jobs: pip install -e '.[spark]'") from exc

    if config is not None:
        spark_config = config.values.get("spark", {}) if hasattr(config, "values") else {}
        app_name = spark_config.get("app_name", app_name)
        master = spark_config.get("master", master)
        enable_delta = spark_config.get("enable_delta", enable_delta)
    else:
        spark_config = {}

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", str(spark_config.get("shuffle_partitions", 2)))
        .config("spark.default.parallelism", str(spark_config.get("default_parallelism", 2)))
        .config("spark.driver.memory", str(spark_config.get("driver_memory", "4g")))
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    )
    for key, value in spark_config.get("conf", {}).items():
        builder = builder.config(str(key), str(value))
    if enable_delta:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
    return builder.getOrCreate()
