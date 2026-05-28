from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark
from lakehouse.common.storage import S3Path, has_files, to_spark_path
from lakehouse.quality.report_writer import write_quality_reports
from lakehouse.quality.rules import (
    FAIL,
    PASS,
    SKIP,
    WARN,
    WARNING,
    RuleResult,
    evaluate_rules,
    expected_tables,
)


LOGGER = get_logger(__name__)
SUPPORTED_LAYERS = ("silver", "gold")
READY = "READY"
READY_WITH_WARNINGS = "READY_WITH_WARNINGS"

GOLD_REFERENCE_CHECKS = (
    (
        "fact_participant_performance",
        ("match_id",),
        "dim_match",
        ("match_id",),
        "participant_match_fk",
    ),
    (
        "fact_participant_performance",
        ("puuid",),
        "dim_summoner",
        ("puuid",),
        "participant_summoner_fk",
    ),
    (
        "fact_participant_performance",
        ("champion_id",),
        "dim_champion",
        ("champion_id",),
        "participant_champion_fk",
    ),
    (
        "fact_participant_performance",
        ("team_id",),
        "dim_team",
        ("team_id",),
        "participant_team_fk",
    ),
    (
        "fact_team_objectives",
        ("match_id",),
        "dim_match",
        ("match_id",),
        "team_objectives_match_fk",
    ),
    (
        "fact_team_objectives",
        ("team_id",),
        "dim_team",
        ("team_id",),
        "team_objectives_team_fk",
    ),
    (
        "fact_rank_snapshot",
        ("puuid",),
        "dim_summoner",
        ("puuid",),
        "rank_snapshot_summoner_fk",
    ),
    (
        "fact_rank_snapshot",
        ("queue", "tier", "rank"),
        "dim_rank",
        ("queue", "tier", "rank"),
        "rank_snapshot_rank_fk",
    ),
    (
        "fact_timeline_frames",
        ("match_id",),
        "dim_match",
        ("match_id",),
        "timeline_frames_match_fk",
    ),
    (
        "fact_timeline_events",
        ("match_id",),
        "dim_match",
        ("match_id",),
        "timeline_events_match_fk",
    ),
)


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

    if "gold" in layers:
        _append_gold_star_schema_checks(spark, config, report, tables_by_layer)

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
        checks = [
            RuleResult(
                name="table_exists",
                description="Expected Parquet table exists",
                severity="error",
                status=PASS,
                passed=True,
                failed_rows=0,
                details={"path": str(table_path)},
            ).as_dict(),
            *[result.as_dict() for result in evaluate_rules(dataframe, layer, table, row_count)],
        ]
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


def _append_gold_star_schema_checks(
    spark: Any,
    config: Any,
    report: dict[str, Any],
    tables_by_layer: dict[str, list[str]],
) -> None:
    selected_gold_tables = set(tables_by_layer.get("gold", []))
    table_reports = {
        table["table"]: table for table in report["tables"] if table["layer"] == "gold"
    }

    for fact_table, fact_columns, dim_table, dim_columns, check_name in GOLD_REFERENCE_CHECKS:
        if fact_table not in selected_gold_tables:
            continue
        table_report = table_reports.get(fact_table)
        if not table_report or not table_report.get("profile"):
            continue
        check = _gold_reference_check(
            spark=spark,
            config=config,
            fact_table=fact_table,
            fact_columns=fact_columns,
            dim_table=dim_table,
            dim_columns=dim_columns,
            check_name=check_name,
        )
        _append_check(table_report, check)

    _append_dim_match_row_count_alignment(report)


def _gold_reference_check(
    spark: Any,
    config: Any,
    fact_table: str,
    fact_columns: tuple[str, ...],
    dim_table: str,
    dim_columns: tuple[str, ...],
    check_name: str,
) -> dict[str, Any]:
    fact_path = config.layer_path("gold", fact_table)
    dim_path = config.layer_path("gold", dim_table)
    if not _has_parquet_files(fact_path) or not _has_parquet_files(dim_path):
        return RuleResult(
            name=check_name,
            description=f"{fact_table} references existing {dim_table} keys",
            severity=WARNING,
            status=SKIP,
            passed=False,
            details={"fact_path": str(fact_path), "dimension_path": str(dim_path)},
        ).as_dict()
    if len(fact_columns) != len(dim_columns):
        return RuleResult(
            name=check_name,
            description=f"{fact_table} references existing {dim_table} keys",
            severity=WARNING,
            status=SKIP,
            passed=False,
            details={
                "fact_columns": list(fact_columns),
                "dimension_columns": list(dim_columns),
            },
        ).as_dict()

    fact_dataframe = spark.read.parquet(_spark_path(fact_path))
    dimension_dataframe = spark.read.parquet(_spark_path(dim_path))
    missing_fact_columns = [
        column for column in fact_columns if column not in fact_dataframe.columns
    ]
    missing_dim_columns = [
        column for column in dim_columns if column not in dimension_dataframe.columns
    ]
    if missing_fact_columns or missing_dim_columns:
        return RuleResult(
            name=check_name,
            description=f"{fact_table} references existing {dim_table} keys",
            severity=WARNING,
            status=SKIP,
            passed=False,
            details={
                "missing_fact_columns": missing_fact_columns,
                "missing_dimension_columns": missing_dim_columns,
            },
        ).as_dict()

    fact = fact_dataframe.select(*fact_columns)
    dimension = dimension_dataframe.select(*dim_columns)
    fact_keys = _nonnull_distinct_keys(fact, fact_columns, "fact")
    dim_keys = _nonnull_distinct_keys(dimension, dim_columns, "dim")
    checked_keys = int(fact_keys.count())
    invalid_keys = int(
        fact_keys.join(
            dim_keys,
            _join_condition(len(fact_columns)),
            "left_anti",
        ).count()
    )
    invalid_rate = _ratio(invalid_keys, checked_keys)
    status = WARN if invalid_keys else PASS
    return RuleResult(
        name=check_name,
        description=f"{fact_table} references existing {dim_table} keys",
        severity=WARNING,
        status=status,
        passed=invalid_keys == 0,
        failed_rows=invalid_keys,
        details={
            "fact_table": fact_table,
            "fact_columns": list(fact_columns),
            "dimension_table": dim_table,
            "dimension_columns": list(dim_columns),
            "checked_distinct_keys": checked_keys,
            "invalid_distinct_keys": invalid_keys,
            "invalid_rate": invalid_rate,
        },
    ).as_dict()


def _nonnull_distinct_keys(dataframe: Any, columns: tuple[str, ...], prefix: str) -> Any:
    from pyspark.sql import functions as F

    condition = None
    for column in columns:
        column_condition = F.col(column).isNotNull()
        condition = column_condition if condition is None else condition & column_condition
    selected = dataframe.where(condition) if condition is not None else dataframe
    return selected.select(
        *[F.col(column).alias(f"{prefix}_{index}") for index, column in enumerate(columns)]
    ).dropDuplicates()


def _join_condition(column_count: int) -> Any:
    from functools import reduce
    from operator import and_

    from pyspark.sql import functions as F

    conditions = [
        F.col(f"fact_{index}").eqNullSafe(F.col(f"dim_{index}"))
        for index in range(column_count)
    ]
    return reduce(and_, conditions)


def _append_dim_match_row_count_alignment(report: dict[str, Any]) -> None:
    silver_match = _table_report(report, "silver", "matches")
    dim_match = _table_report(report, "gold", "dim_match")
    if not silver_match or not dim_match or not dim_match.get("profile"):
        return

    silver_count = int(silver_match.get("row_count", 0))
    dim_count = int(dim_match.get("row_count", 0))
    delta = abs(dim_count - silver_count)
    delta_rate = _ratio(delta, silver_count)
    status = WARN if delta_rate > 0.01 else PASS
    _append_check(
        dim_match,
        RuleResult(
            name="silver_match_row_count_alignment",
            description="dim_match row count is close to silver.matches row count",
            severity=WARNING,
            status=status,
            passed=status == PASS,
            failed_rows=delta,
            details={
                "silver_matches_row_count": silver_count,
                "dim_match_row_count": dim_count,
                "delta": delta,
                "delta_rate": delta_rate,
                "warn_threshold": 0.01,
            },
        ).as_dict(),
    )


def _table_report(
    report: dict[str, Any],
    layer: str,
    table: str,
) -> dict[str, Any] | None:
    return next(
        (
            table_report
            for table_report in report["tables"]
            if table_report["layer"] == layer and table_report["table"] == table
        ),
        None,
    )


def _append_check(table_report: dict[str, Any], check: dict[str, Any]) -> None:
    table_report.setdefault("checks", []).append(check)
    table_report["status"] = _overall_status_from_checks(table_report["checks"])


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


def _quality_output_dir(config: Any) -> Path | S3Path:
    quality_config = config.values.get("quality", {}) if hasattr(config, "values") else {}
    output_dir = quality_config.get("output_dir", "data_quality")
    return config.report_root / str(output_dir)


def _spark_path(path: Any) -> str:
    return to_spark_path(path)


def _has_parquet_files(path: Any) -> bool:
    return has_files(path, "*.parquet")


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
