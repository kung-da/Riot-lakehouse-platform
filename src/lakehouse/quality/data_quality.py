from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark
from lakehouse.quality.report_writer import write_quality_reports
from lakehouse.quality.rules import (
    FAIL,
    PASS,
    WARN,
    RuleResult,
    evaluate_rules,
    expected_tables,
)


LOGGER = get_logger(__name__)
SUPPORTED_LAYERS = ("silver", "gold")
READY = "READY"
READY_WITH_WARNINGS = "READY_WITH_WARNINGS"


def run_data_quality(
    config: Any,
    layers: list[str] | None = None,
    tables_by_layer: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    selected_layers = _selected_layers(config, layers)
    selected_tables = _selected_tables(selected_layers, tables_by_layer)

    spark = get_spark(config=config)
    try:
        report = build_data_quality_report(
            spark=spark,
            config=config,
            layers=selected_layers,
            tables_by_layer=selected_tables,
        )
    finally:
        spark.stop()

    output_dir = _quality_output_dir(config)
    paths = write_quality_reports(output_dir, report)
    return {"report": report, "paths": {key: str(path) for key, path in paths.items()}}


def build_data_quality_report(
    spark: Any,
    config: Any,
    layers: list[str],
    tables_by_layer: dict[str, list[str]],
) -> dict[str, Any]:
    generated_at = _utc_now()
    report = {
        "run_id": _run_id(generated_at),
        "generated_at": generated_at,
        "environment": getattr(config, "environment", "unknown"),
        "lakehouse_root": str(getattr(config, "lakehouse_root", "")),
        "layers": layers,
        "status": PASS,
        "summary": {},
        "tables": [],
    }

    for layer in layers:
        for table in tables_by_layer[layer]:
            table_path = config.layer_path(layer, table)
            LOGGER.info("Running quality checks for %s.%s at %s", layer, table, table_path)
            report["tables"].append(_analyze_table(spark, layer, table, table_path))

    report["status"] = _overall_status(report["tables"])
    report["summary"] = _summary(report["tables"], report["status"])
    return report


def _analyze_table(spark: Any, layer: str, table: str, table_path: Path) -> dict[str, Any]:
    if not _has_parquet_files(table_path):
        check = RuleResult(
            name="table_exists",
            description="Expected Parquet table exists",
            severity="error",
            status=FAIL,
            passed=False,
            failed_rows=None,
            details={"path": str(table_path)},
        ).as_dict()
        return {
            "layer": layer,
            "table": table,
            "path": str(table_path),
            "status": FAIL,
            "row_count": 0,
            "profile": {},
            "checks": [check],
        }

    dataframe = spark.read.parquet(_spark_path(table_path)).cache()
    try:
        row_count = int(dataframe.count())
        checks = [result.as_dict() for result in evaluate_rules(dataframe, layer, table, row_count)]
        profile = _profile_table(dataframe, row_count)
        status = _overall_status_from_checks(checks)
        return {
            "layer": layer,
            "table": table,
            "path": str(table_path),
            "status": status,
            "row_count": row_count,
            "profile": profile,
            "checks": checks,
        }
    finally:
        dataframe.unpersist()


def _profile_table(dataframe: Any, row_count: int) -> dict[str, Any]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import NumericType, StringType

    fields = dataframe.schema.fields
    columns = [field.name for field in fields]
    null_counts = _aggregate_dict(
        dataframe,
        [F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column) for column in columns],
    )
    distinct_counts = _aggregate_dict(
        dataframe,
        [F.approx_count_distinct(F.col(column)).alias(column) for column in columns],
    )
    string_columns = [field.name for field in fields if isinstance(field.dataType, StringType)]
    empty_counts = _aggregate_dict(
        dataframe,
        [
            F.sum(F.when(F.length(F.trim(F.col(column))) == 0, 1).otherwise(0)).alias(column)
            for column in string_columns
        ],
    )

    numeric_fields = [field for field in fields if isinstance(field.dataType, NumericType)]
    numeric_stats = _numeric_stats(dataframe, numeric_fields)

    column_profiles = []
    for field in fields:
        null_count = int(null_counts.get(field.name) or 0)
        column_profile = {
            "name": field.name,
            "data_type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
            "null_count": null_count,
            "null_ratio": _ratio(null_count, row_count),
            "approx_distinct_count": int(distinct_counts.get(field.name) or 0),
        }
        if field.name in empty_counts:
            column_profile["empty_string_count"] = int(empty_counts.get(field.name) or 0)
        if field.name in numeric_stats:
            column_profile["numeric"] = numeric_stats[field.name]
        column_profiles.append(column_profile)

    profile = {
        "row_count": row_count,
        "column_count": len(columns),
        "columns": column_profiles,
    }
    if "game_date" in columns:
        profile["game_date_coverage"] = _game_date_coverage(dataframe)
    return profile


def _numeric_stats(dataframe: Any, fields: list[Any]) -> dict[str, dict[str, Any]]:
    from pyspark.sql import functions as F

    if not fields:
        return {}

    expressions = []
    for field in fields:
        expressions.extend(
            [
                F.min(F.col(field.name)).alias(f"{field.name}__min"),
                F.max(F.col(field.name)).alias(f"{field.name}__max"),
                F.avg(F.col(field.name)).alias(f"{field.name}__avg"),
            ]
        )
    values = _aggregate_dict(dataframe, expressions)
    stats = {}
    for field in fields:
        stats[field.name] = {
            "min": _json_value(values.get(f"{field.name}__min")),
            "max": _json_value(values.get(f"{field.name}__max")),
            "avg": _json_value(values.get(f"{field.name}__avg")),
        }
    return stats


def _game_date_coverage(dataframe: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    coverage = _aggregate_dict(
        dataframe,
        [
            F.min(F.col("game_date")).alias("min"),
            F.max(F.col("game_date")).alias("max"),
            F.countDistinct(F.col("game_date")).alias("distinct_count"),
        ],
    )
    top_dates = [
        {"game_date": row["game_date"], "row_count": int(row["count"])}
        for row in (
            dataframe.groupBy("game_date")
            .count()
            .orderBy(F.desc("count"))
            .limit(20)
            .collect()
        )
    ]
    return {
        "min": coverage.get("min"),
        "max": coverage.get("max"),
        "distinct_count": int(coverage.get("distinct_count") or 0),
        "top_dates": top_dates,
    }


def _aggregate_dict(dataframe: Any, expressions: list[Any]) -> dict[str, Any]:
    if not expressions:
        return {}
    row = dataframe.agg(*expressions).collect()[0]
    return {key: _json_value(value) for key, value in row.asDict().items()}


def _selected_layers(config: Any, layers: list[str] | None) -> list[str]:
    if layers is None:
        quality_config = config.values.get("quality", {}) if hasattr(config, "values") else {}
        layers = quality_config.get("layers") or list(SUPPORTED_LAYERS)
    selected = [str(layer).strip().lower() for layer in layers if str(layer).strip()]
    unknown = sorted(set(selected) - set(SUPPORTED_LAYERS))
    if unknown:
        raise ValueError(f"Unknown quality layers: {', '.join(unknown)}")
    return selected or list(SUPPORTED_LAYERS)


def _selected_tables(
    layers: list[str],
    tables_by_layer: dict[str, list[str]] | None,
) -> dict[str, list[str]]:
    selected = {}
    tables_by_layer = tables_by_layer or {}
    for layer in layers:
        configured = tables_by_layer.get(layer)
        if configured is None:
            selected[layer] = expected_tables(layer)
            continue

        requested = [str(table).strip() for table in configured if str(table).strip()]
        available = set(expected_tables(layer))
        unknown = sorted(set(requested) - available)
        if unknown:
            raise ValueError(f"Unknown {layer} quality tables: {', '.join(unknown)}")
        selected[layer] = requested or expected_tables(layer)
    return selected


def _summary(tables: list[dict[str, Any]], status: str) -> dict[str, Any]:
    return {
        "tables_expected": len(tables),
        "tables_analyzed": sum(1 for table in tables if table.get("profile")),
        "missing_tables": sum(1 for table in tables if not table.get("profile")),
        "passed_tables": sum(1 for table in tables if table["status"] == PASS),
        "warning_tables": sum(1 for table in tables if table["status"] == WARN),
        "failed_tables": sum(1 for table in tables if table["status"] == FAIL),
        "ready_for_dashboard": status in {READY, READY_WITH_WARNINGS},
        "recommendation": _recommendation(status),
    }


def _recommendation(status: str) -> str:
    if status == READY:
        return "ready_for_dashboard_review"
    if status == READY_WITH_WARNINGS:
        return "usable_with_manual_review"
    return "blocked_until_failed_checks_are_fixed"


def _overall_status(tables: list[dict[str, Any]]) -> str:
    statuses = [table["status"] for table in tables]
    if any(status == FAIL for status in statuses):
        return FAIL
    if any(status == WARN for status in statuses):
        return READY_WITH_WARNINGS
    return READY


def _overall_status_from_checks(checks: list[dict[str, Any]]) -> str:
    statuses = [check["status"] for check in checks]
    if any(status == FAIL for status in statuses):
        return FAIL
    if any(status == WARN for status in statuses):
        return WARN
    return PASS


def _quality_output_dir(config: Any) -> Path:
    quality_config = config.values.get("quality", {}) if hasattr(config, "values") else {}
    output_dir = quality_config.get("output_dir", "data_quality")
    return config.report_root / str(output_dir)


def _spark_path(path: Any) -> str:
    path_text = path.as_posix()
    if path_text.startswith("s3:/") and not path_text.startswith("s3://"):
        return "s3://" + path_text.removeprefix("s3:/").lstrip("/")
    return path_text


def _has_parquet_files(path: Any) -> bool:
    path_text = path.as_posix()
    if path_text.startswith("s3:/"):
        return True
    return path.exists() and any(path.rglob("*.parquet"))


def _ratio(value: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(value / total, 6)


def _json_value(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _run_id(generated_at: str) -> str:
    return (
        generated_at.replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
        .replace("Z", "Z")
    )
