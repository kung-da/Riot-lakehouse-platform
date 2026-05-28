from __future__ import annotations

from typing import Any

from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark
from lakehouse.common.storage import has_files, to_spark_path
from lakehouse.gold.aggregations import AGGREGATION_BUILDERS, GOLD_TABLE_SOURCES
from lakehouse.gold.schemas import GOLD_COLUMNS, GOLD_TABLES, gold_schema


LOGGER = get_logger(__name__)


def _spark_path(path: Any) -> str:
    return to_spark_path(path)


def _has_parquet_files(path: Any) -> bool:
    return has_files(path, "*.parquet")


def _selected_tables(tables: list[str] | None) -> list[str]:
    selected = tables or GOLD_TABLES
    unknown = sorted(set(selected) - set(GOLD_TABLES))
    if unknown:
        raise ValueError(f"Unknown gold tables: {', '.join(unknown)}")
    return selected


def _gold_write_mode(config: Any, write_mode: str | None) -> str:
    if write_mode:
        return write_mode
    gold_config = config.values.get("gold", {}) if hasattr(config, "values") else {}
    return str(gold_config.get("write_mode", "overwrite"))


def _gold_output_partitions(config: Any) -> int:
    gold_config = config.values.get("gold", {}) if hasattr(config, "values") else {}
    return int(gold_config.get("output_partitions", 1))


def _gold_partition_columns(config: Any, dataframe: Any) -> list[str]:
    partition_config = (
        config.values.get("partition_columns", {}) if hasattr(config, "values") else {}
    )
    configured_columns = partition_config.get("gold")
    if configured_columns is None:
        configured_columns = ["game_date"] if "game_date" in dataframe.columns else []
    return [str(column) for column in configured_columns if str(column) in dataframe.columns]


def _apply_schema(dataframe: Any, table: str) -> Any:
    from pyspark.sql import functions as F

    schema = gold_schema(table)
    for field in schema.fields:
        if field.name not in dataframe.columns:
            dataframe = dataframe.withColumn(field.name, F.lit(None).cast(field.dataType))
        else:
            dataframe = dataframe.withColumn(field.name, F.col(field.name).cast(field.dataType))
    return dataframe.select(*GOLD_COLUMNS[table])


def _write_table(
    dataframe: Any,
    output_path: Any,
    mode: str,
    partition_columns: list[str],
    output_partitions: int,
) -> None:
    if output_partitions < 1:
        raise ValueError("gold.output_partitions must be greater than zero")

    dataframe = dataframe.coalesce(output_partitions)
    writer = dataframe.write.mode(mode).option("compression", "snappy")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(_spark_path(output_path))


def run_gold_transform(
    config: Any,
    tables: list[str] | None = None,
    write_mode: str | None = None,
) -> dict[str, int]:
    selected_tables = _selected_tables(tables)
    counts = {table: 0 for table in selected_tables}
    required_sources = sorted(
        {source for table in selected_tables for source in GOLD_TABLE_SOURCES[table]}
    )
    available_source_paths = {}
    missing_sources = set()

    for source in required_sources:
        source_path = config.layer_path("silver", source)
        LOGGER.info("Reading Silver table %s from %s", source, source_path)
        if _has_parquet_files(source_path):
            available_source_paths[source] = source_path
        else:
            missing_sources.add(source)
            LOGGER.warning("Silver input path has no Parquet files: %s", source_path)

    if not available_source_paths:
        return counts

    spark = get_spark(config=config)
    mode = _gold_write_mode(config, write_mode)
    output_partitions = _gold_output_partitions(config)
    silver_tables: dict[str, Any] = {}
    try:
        for source, source_path in available_source_paths.items():
            silver_tables[source] = spark.read.parquet(_spark_path(source_path))

        for table in selected_tables:
            table_sources = GOLD_TABLE_SOURCES[table]
            unavailable = sorted(set(table_sources).intersection(missing_sources))
            if unavailable:
                LOGGER.warning(
                    "Skipping Gold table %s because missing Silver sources: %s",
                    table,
                    ", ".join(unavailable),
                )
                continue

            LOGGER.info("Building Gold table %s from Silver sources: %s", table, table_sources)
            dataframe = _apply_schema(AGGREGATION_BUILDERS[table](silver_tables), table).cache()
            try:
                row_count = dataframe.count()
                output_path = config.layer_path("gold", table)
                partition_columns = _gold_partition_columns(config, dataframe)
                LOGGER.info(
                    "Writing Gold table %s to %s with mode=%s partition_columns=%s",
                    table,
                    output_path,
                    mode,
                    partition_columns,
                )
                if row_count > 0 or mode == "overwrite":
                    _write_table(
                        dataframe=dataframe,
                        output_path=output_path,
                        mode=mode,
                        partition_columns=partition_columns,
                        output_partitions=output_partitions,
                    )
                counts[table] = row_count
                LOGGER.info("Gold table %s wrote %s rows to %s", table, row_count, output_path)
            finally:
                dataframe.unpersist()
        return counts
    finally:
        spark.stop()
