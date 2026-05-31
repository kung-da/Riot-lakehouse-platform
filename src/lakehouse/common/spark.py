from __future__ import annotations

import os
from typing import Any

from lakehouse.common.storage import is_s3_path


def _config_uses_s3(config: Any | None) -> bool:
    if config is None:
        return False
    return any(
        is_s3_path(path)
        for path in [
            getattr(config, "raw_root", ""),
            getattr(config, "lakehouse_root", ""),
            getattr(config, "checkpoint_root", ""),
            getattr(config, "report_root", ""),
        ]
    )


def _aws_setting(aws_config: dict[str, Any], key: str, env_name: str) -> Any:
    return os.getenv(env_name) or aws_config.get(key)


def _apply_s3_defaults(builder: Any, config: Any | None, spark_config: dict[str, Any]) -> Any:
    if config is not None and hasattr(config, "values"):
        aws_config = config.values.get("aws", {}) or {}
    else:
        aws_config = {}
    if not _config_uses_s3(config) and not aws_config:
        return builder

    if spark_config.get("include_hadoop_aws_package", False):
        package = spark_config.get("hadoop_aws_package", "org.apache.hadoop:hadoop-aws:3.4.2")
        builder = builder.config("spark.jars.packages", str(package))

    region = _aws_setting(aws_config, "region", "AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    endpoint = _aws_setting(aws_config, "s3_endpoint_url", "AWS_S3_ENDPOINT_URL")
    path_style_access = aws_config.get("path_style_access", os.getenv("AWS_S3_PATH_STYLE_ACCESS"))
    access_key = _aws_setting(aws_config, "access_key_id", "AWS_ACCESS_KEY_ID")
    secret_key = _aws_setting(aws_config, "secret_access_key", "AWS_SECRET_ACCESS_KEY")
    session_token = _aws_setting(aws_config, "session_token", "AWS_SESSION_TOKEN")

    builder = (
        builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )
    credentials_provider = spark_config.get("aws_credentials_provider")
    if credentials_provider:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            str(credentials_provider),
        )
    if region:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", str(region))
    if endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", str(endpoint))
    if path_style_access not in {None, ""}:
        builder = builder.config(
            "spark.hadoop.fs.s3a.path.style.access",
            str(path_style_access).lower(),
        )
    if access_key and secret_key:
        builder = (
            builder.config("spark.hadoop.fs.s3a.access.key", str(access_key))
            .config("spark.hadoop.fs.s3a.secret.key", str(secret_key))
        )
    if session_token:
        builder = builder.config("spark.hadoop.fs.s3a.session.token", str(session_token))
    return builder


def get_spark(
    app_name: str = "riot-lakehouse",
    master: str = "local[*]",
    enable_delta: bool = False,
    config: Any | None = None,
) -> Any:
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "Install the spark extra to run Spark jobs: pip install -e '.[spark]'"
        ) from exc

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
    builder = _apply_s3_defaults(builder, config, spark_config)
    for key, value in spark_config.get("conf", {}).items():
        builder = builder.config(str(key), str(value))
    if enable_delta:
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    return builder.getOrCreate()
