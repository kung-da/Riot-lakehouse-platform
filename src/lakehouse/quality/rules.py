from __future__ import annotations

from dataclasses import dataclass, field
from functools import reduce
from operator import or_
from typing import Any

from lakehouse.gold.schemas import DOUBLE_COLUMNS, GOLD_COLUMNS, GOLD_TABLES, LONG_COLUMNS
from lakehouse.silver.silver_transformer import SILVER_TABLES, TABLE_FIELDS


PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"
SKIP = "SKIP"

ERROR = "error"
WARNING = "warning"

EXPECTED_TABLES = {
    "silver": SILVER_TABLES,
    "gold": GOLD_TABLES,
}

EXPECTED_COLUMNS = {
    "silver": TABLE_FIELDS,
    "gold": GOLD_COLUMNS,
}

SILVER_KEYS = {
    "matches": ("match_id",),
    "participants": ("match_id", "participant_id"),
    "teams": ("match_id", "team_id"),
    "summoners": ("puuid",),
    "timeline_frames": ("match_id", "frame_index"),
    "timeline_events": ("match_id", "frame_index", "event_index"),
}

GOLD_KEYS = {
    "dim_date": ("game_date",),
    "dim_match": ("match_id",),
    "dim_summoner": ("puuid",),
    "dim_champion": ("champion_id",),
    "dim_team": ("team_id",),
    "dim_rank": ("queue", "tier", "rank"),
    "fact_participant_performance": ("match_id", "participant_id"),
    "fact_team_objectives": ("match_id", "team_id"),
    "fact_rank_snapshot": ("game_date", "queue", "tier", "rank", "puuid"),
    "fact_timeline_frames": ("match_id", "frame_index"),
    "fact_timeline_events": ("match_id", "frame_index", "event_index"),
    "mart_player_daily_performance": ("game_date", "puuid"),
    "mart_champion_daily_performance": ("game_date", "champion_id"),
    "mart_role_daily_performance": ("game_date", "team_position"),
    "mart_rank_daily_summary": ("game_date", "queue", "tier", "rank"),
    "mart_team_objective_daily_summary": ("game_date", "team_id"),
}

SILVER_REQUIRED = {
    "matches": ("match_id", "dataset", "game_date"),
    "participants": (
        "match_id",
        "participant_id",
        "puuid",
        "champion_id",
        "team_id",
        "dataset",
        "game_date",
    ),
    "teams": ("match_id", "team_id", "dataset", "game_date"),
    "summoners": ("puuid", "dataset", "game_date"),
    "timeline_frames": ("match_id", "frame_index", "dataset", "game_date"),
    "timeline_events": (
        "match_id",
        "frame_index",
        "event_index",
        "event_type",
        "dataset",
        "game_date",
    ),
}

GOLD_REQUIRED = {
    "dim_date": ("date_key", "game_date"),
    "dim_match": ("match_id", "game_date"),
    "dim_summoner": ("puuid",),
    "dim_champion": ("champion_id", "champion_name"),
    "dim_team": ("team_id", "team_side"),
    "dim_rank": ("queue", "tier", "rank"),
    "fact_participant_performance": (
        "game_date",
        "match_id",
        "participant_id",
        "puuid",
        "champion_id",
    ),
    "fact_team_objectives": ("game_date", "match_id", "team_id"),
    "fact_rank_snapshot": ("game_date", "queue", "tier", "rank", "puuid"),
    "fact_timeline_frames": ("game_date", "match_id", "frame_index"),
    "fact_timeline_events": (
        "game_date",
        "match_id",
        "frame_index",
        "event_index",
        "event_type",
    ),
    "mart_player_daily_performance": (
        "game_date",
        "puuid",
        "matches_played",
        "wins",
        "losses",
        "win_rate",
    ),
    "mart_champion_daily_performance": (
        "game_date",
        "champion_id",
        "champion_name",
        "matches_played",
        "wins",
        "losses",
        "win_rate",
    ),
    "mart_role_daily_performance": (
        "game_date",
        "team_position",
        "matches_played",
        "wins",
        "losses",
        "win_rate",
    ),
    "mart_rank_daily_summary": ("game_date", "queue", "tier", "rank", "players", "avg_win_rate"),
    "mart_team_objective_daily_summary": (
        "game_date",
        "team_id",
        "games_played",
        "wins",
        "losses",
        "win_rate",
        "avg_baron_kills",
        "avg_dragon_kills",
        "avg_rift_herald_kills",
        "avg_tower_kills",
        "avg_inhibitor_kills",
        "avg_champion_kills",
    ),
}

SILVER_NON_NEGATIVE = {
    "matches": (
        "game_id",
        "queue_id",
        "game_creation",
        "game_start_timestamp",
        "game_end_timestamp",
        "game_duration",
        "participant_count",
    ),
    "participants": (
        "participant_id",
        "champion_id",
        "team_id",
        "kills",
        "deaths",
        "assists",
        "kda",
        "gold_earned",
        "total_damage_dealt_to_champions",
        "total_damage_taken",
        "vision_score",
        "total_minions_killed",
        "neutral_minions_killed",
    ),
    "teams": (
        "team_id",
        "baron_kills",
        "dragon_kills",
        "rift_herald_kills",
        "tower_kills",
        "inhibitor_kills",
        "champion_kills",
    ),
    "summoners": ("profile_icon_id", "revision_date", "summoner_level"),
    "ranked": ("league_points", "wins", "losses", "win_rate"),
    "timeline_frames": ("frame_index", "frame_timestamp", "participant_frame_count", "event_count"),
    "timeline_events": (
        "frame_index",
        "event_index",
        "event_timestamp",
        "participant_id",
        "killer_id",
        "victim_id",
        "team_id",
    ),
}

GOLD_NON_NEGATIVE = {
    table: tuple(
        column
        for column in GOLD_COLUMNS[table]
        if column in LONG_COLUMNS or column in DOUBLE_COLUMNS
    )
    for table in GOLD_TABLES
}

RATE_COLUMNS = {
    "silver": {"ranked": ("win_rate",)},
    "gold": {
        "fact_rank_snapshot": ("win_rate",),
        "mart_player_daily_performance": ("win_rate",),
        "mart_champion_daily_performance": ("win_rate",),
        "mart_role_daily_performance": ("win_rate",),
        "mart_rank_daily_summary": ("avg_win_rate",),
        "mart_team_objective_daily_summary": ("win_rate",),
    },
}

TEAM_ID_TABLES = {
    "silver": {"participants": ("team_id",), "teams": ("team_id",)},
    "gold": {
        "dim_team": ("team_id",),
        "fact_participant_performance": ("team_id",),
        "fact_team_objectives": ("team_id",),
        "mart_team_objective_daily_summary": ("team_id",),
    },
}

ROLE_VALUES = ("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY", "UNKNOWN")

RANKED_KEY_CANDIDATES = (
    ("queue", "tier", "rank", "puuid", "game_date"),
    ("queue", "tier", "rank", "puuid", "ingest_date"),
)

TEAM_OBJECTIVE_POSITIVE_METRICS = {
    "team_count": ("team_count", "games_played"),
}

TEAM_OBJECTIVE_NON_NEGATIVE_METRICS = {
    "win_count": ("win_count", "wins"),
    "avg_baron_kills": ("avg_baron_kills",),
    "avg_dragon_kills": ("avg_dragon_kills",),
    "avg_rift_herald_kills": ("avg_rift_herald_kills",),
    "avg_tower_kills": ("avg_tower_kills",),
    "avg_inhibitor_kills": ("avg_inhibitor_kills",),
    "avg_champion_kills": ("avg_champion_kills",),
    "avg_objective_score": ("avg_objective_score",),
}

RANK_TIERS = (
    "IRON",
    "BRONZE",
    "SILVER",
    "GOLD",
    "PLATINUM",
    "EMERALD",
    "DIAMOND",
    "MASTER",
    "GRANDMASTER",
    "CHALLENGER",
)


@dataclass(frozen=True)
class RuleSpec:
    name: str
    rule_type: str
    description: str
    severity: str = ERROR
    columns: tuple[str, ...] = ()
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuleResult:
    name: str
    description: str
    severity: str
    status: str
    passed: bool
    failed_rows: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "severity": self.severity,
            "status": self.status,
            "passed": self.passed,
            "failed_rows": self.failed_rows,
            "details": self.details,
        }


def expected_tables(layer: str) -> list[str]:
    if layer not in EXPECTED_TABLES:
        raise ValueError(f"Unknown quality layer: {layer}")
    return list(EXPECTED_TABLES[layer])


def expected_columns(layer: str, table: str) -> list[str]:
    try:
        return list(EXPECTED_COLUMNS[layer][table])
    except KeyError as exc:
        raise ValueError(f"Unknown table for {layer}: {table}") from exc


def rules_for_table(layer: str, table: str) -> list[RuleSpec]:
    rules = [
        RuleSpec(
            name="required_columns",
            rule_type="required_columns",
            description="Expected schema columns are present",
            columns=tuple(expected_columns(layer, table)),
        ),
        RuleSpec(
            name="table_not_empty",
            rule_type="row_count_min",
            description="Table contains at least one row",
            params={"min_count": 1},
        ),
    ]

    if layer == "silver" and table == "ranked":
        rules.append(
            RuleSpec(
                name="required_values_not_null",
                rule_type="not_null_with_first_available",
                description="Required ranked columns are not null",
                columns=("queue", "tier", "rank", "puuid", "dataset"),
                params={"candidates": RANKED_KEY_CANDIDATES},
            )
        )
    required = SILVER_REQUIRED.get(table, ()) if layer == "silver" else GOLD_REQUIRED.get(table, ())
    if required:
        rules.append(
            RuleSpec(
                name="required_values_not_null",
                rule_type="not_null",
                description="Required business columns are not null",
                columns=required,
            )
        )

    if layer == "silver" and table == "ranked":
        rules.append(
            RuleSpec(
                name="unique_business_key",
                rule_type="first_available_unique_key",
                description="Business key is unique",
                params={"candidates": RANKED_KEY_CANDIDATES},
            )
        )
    key_columns = SILVER_KEYS.get(table, ()) if layer == "silver" else GOLD_KEYS.get(table, ())
    if key_columns:
        rules.append(
            RuleSpec(
                name="unique_business_key",
                rule_type="unique_key",
                description="Business key is unique",
                columns=key_columns,
            )
        )

    if layer == "silver":
        non_negative = SILVER_NON_NEGATIVE.get(table, ())
    else:
        non_negative = GOLD_NON_NEGATIVE.get(table, ())
    if non_negative:
        rules.append(
            RuleSpec(
                name="non_negative_metrics",
                rule_type="non_negative",
                description="Metric and count columns are non-negative when present",
                columns=non_negative,
            )
        )
    if layer == "gold" and table == "mart_team_objective_daily_summary":
        rules.extend(
            [
                RuleSpec(
                    name="team_count_positive",
                    rule_type="positive_metric_candidates",
                    description="Team count metric is greater than zero",
                    params={"metrics": TEAM_OBJECTIVE_POSITIVE_METRICS},
                ),
                RuleSpec(
                    name="team_objective_summary_non_negative",
                    rule_type="non_negative_metric_candidates",
                    description="Team objective metrics are non-negative when present",
                    params={"metrics": TEAM_OBJECTIVE_NON_NEGATIVE_METRICS},
                ),
            ]
        )

    rate_columns = RATE_COLUMNS.get(layer, {}).get(table, ())
    if rate_columns:
        rules.append(
            RuleSpec(
                name="rates_between_zero_and_one",
                rule_type="between",
                description="Rate columns are between 0 and 1",
                columns=rate_columns,
                params={"min_value": 0.0, "max_value": 1.0},
            )
        )

    if table in {
        "mart_player_daily_performance",
        "mart_champion_daily_performance",
        "mart_role_daily_performance",
    }:
        rules.append(
            RuleSpec(
                name="wins_losses_match_games",
                rule_type="sum_equals",
                description="wins + losses equals matches_played",
                params={"addends": ("wins", "losses"), "target": "matches_played"},
            )
        )
    if table == "mart_team_objective_daily_summary":
        rules.append(
            RuleSpec(
                name="wins_losses_match_games",
                rule_type="sum_equals",
                description="wins + losses equals games_played",
                params={"addends": ("wins", "losses"), "target": "games_played"},
            )
        )

    team_id_columns = TEAM_ID_TABLES.get(layer, {}).get(table, ())
    if team_id_columns:
        rules.append(
            RuleSpec(
                name="team_id_known_values",
                rule_type="accepted_values",
                description="Team identifiers use standard Riot team IDs",
                severity=WARNING,
                columns=team_id_columns,
                params={"values": (100, 200)},
            )
        )

    if layer == "gold" and table in {
        "fact_participant_performance",
        "mart_role_daily_performance",
    }:
        rules.append(
            RuleSpec(
                name="role_known_values",
                rule_type="accepted_values",
                description="Gold role buckets use expected role names",
                severity=WARNING,
                columns=("team_position",),
                params={"values": ROLE_VALUES},
            )
        )

    if table in {"dim_rank", "fact_rank_snapshot", "mart_rank_daily_summary"}:
        rules.append(
            RuleSpec(
                name="tier_known_values",
                rule_type="accepted_values",
                description="Rank tiers use expected Riot tier names",
                severity=WARNING,
                columns=("tier",),
                params={"values": RANK_TIERS},
            )
        )

    return rules


def evaluate_rules(dataframe: Any, layer: str, table: str, row_count: int) -> list[RuleResult]:
    return [_evaluate_rule(dataframe, rule, row_count) for rule in rules_for_table(layer, table)]


def _evaluate_rule(dataframe: Any, rule: RuleSpec, row_count: int) -> RuleResult:
    if rule.rule_type == "required_columns":
        missing = [column for column in rule.columns if column not in dataframe.columns]
        return _result(
            rule,
            failed=bool(missing),
            details={
                "expected_column_count": len(rule.columns),
                "missing_columns": missing,
            },
        )

    if rule.rule_type == "row_count_min":
        min_count = int(rule.params["min_count"])
        return _result(
            rule,
            failed=row_count < min_count,
            details={"row_count": row_count, "min_count": min_count},
        )

    if rule.rule_type == "not_null_with_first_available":
        return _evaluate_not_null_with_first_available(dataframe, rule, row_count)

    if rule.rule_type == "first_available_unique_key":
        return _evaluate_first_available_unique_key(dataframe, rule, row_count)

    if rule.rule_type == "positive_metric_candidates":
        return _evaluate_metric_candidates(dataframe, rule, row_count, min_value=0, strict=True)

    if rule.rule_type == "non_negative_metric_candidates":
        return _evaluate_metric_candidates(dataframe, rule, row_count, min_value=0, strict=False)

    required_columns = _columns_for_rule(rule)
    missing = [column for column in required_columns if column not in dataframe.columns]
    if missing:
        return RuleResult(
            name=rule.name,
            description=rule.description,
            severity=rule.severity,
            status=SKIP,
            passed=False,
            failed_rows=None,
            details={"missing_columns": missing},
        )

    if row_count == 0:
        return _result(rule, failed=False, failed_rows=0, details={"row_count": row_count})

    if rule.rule_type == "not_null":
        failed_rows = _count_condition(dataframe, _any_null(dataframe, rule.columns))
        return _result(rule, failed=failed_rows > 0, failed_rows=failed_rows)

    if rule.rule_type == "non_negative":
        failed_rows = _count_condition(dataframe, _any_negative(dataframe, rule.columns))
        return _result(rule, failed=failed_rows > 0, failed_rows=failed_rows)

    if rule.rule_type == "between":
        failed_rows = _count_condition(
            dataframe,
            _any_outside_range(
                dataframe,
                rule.columns,
                min_value=float(rule.params["min_value"]),
                max_value=float(rule.params["max_value"]),
            ),
        )
        return _result(rule, failed=failed_rows > 0, failed_rows=failed_rows)

    if rule.rule_type == "accepted_values":
        failed_rows = _count_condition(
            dataframe,
            _any_unaccepted_value(dataframe, rule.columns, tuple(rule.params["values"])),
        )
        return _result(
            rule,
            failed=failed_rows > 0,
            failed_rows=failed_rows,
            details={
                "accepted_values": list(rule.params["values"]),
                "invalid_rate": _ratio(failed_rows, row_count),
                "top_invalid_values": _top_invalid_values(
                    dataframe,
                    rule.columns,
                    tuple(rule.params["values"]),
                ),
            },
        )

    if rule.rule_type == "unique_key":
        distinct_keys = dataframe.select(*rule.columns).distinct().count()
        duplicate_rows = max(int(row_count - distinct_keys), 0)
        return _result(
            rule,
            failed=duplicate_rows > 0,
            failed_rows=duplicate_rows,
            details={"key_columns": list(rule.columns), "distinct_keys": int(distinct_keys)},
        )

    if rule.rule_type == "sum_equals":
        addends = tuple(rule.params["addends"])
        target = str(rule.params["target"])
        failed_rows = _count_condition(dataframe, _sum_not_equal(dataframe, addends, target))
        return _result(
            rule,
            failed=failed_rows > 0,
            failed_rows=failed_rows,
            details={"addends": list(addends), "target": target},
        )

    raise ValueError(f"Unknown quality rule type: {rule.rule_type}")


def _result(
    rule: RuleSpec,
    failed: bool,
    failed_rows: int | None = None,
    details: dict[str, Any] | None = None,
) -> RuleResult:
    status = PASS
    if failed:
        status = WARN if rule.severity == WARNING else FAIL
    return RuleResult(
        name=rule.name,
        description=rule.description,
        severity=rule.severity,
        status=status,
        passed=not failed,
        failed_rows=failed_rows,
        details=details or {},
    )


def _columns_for_rule(rule: RuleSpec) -> tuple[str, ...]:
    if rule.rule_type == "sum_equals":
        return (*tuple(rule.params["addends"]), str(rule.params["target"]))
    return rule.columns


def _evaluate_not_null_with_first_available(
    dataframe: Any,
    rule: RuleSpec,
    row_count: int,
) -> RuleResult:
    key_columns = tuple(dict.fromkeys((*rule.columns, *_first_available_columns(dataframe, rule))))
    missing = [column for column in key_columns if column not in dataframe.columns]
    if missing:
        return _skipped_for_missing(rule, missing)

    failed_rows = 0
    if row_count > 0:
        failed_rows = _count_condition(dataframe, _any_null(dataframe, key_columns))
    return _result(
        rule,
        failed=failed_rows > 0,
        failed_rows=failed_rows,
        details={"checked_columns": list(key_columns)},
    )


def _evaluate_first_available_unique_key(
    dataframe: Any,
    rule: RuleSpec,
    row_count: int,
) -> RuleResult:
    key_columns = _first_available_columns(dataframe, rule)
    missing = [column for column in key_columns if column not in dataframe.columns]
    if missing:
        return _skipped_for_missing(rule, missing)

    distinct_keys = dataframe.select(*key_columns).distinct().count()
    duplicate_rows = max(int(row_count - distinct_keys), 0)
    return _result(
        rule,
        failed=duplicate_rows > 0,
        failed_rows=duplicate_rows,
        details={"key_columns": list(key_columns), "distinct_keys": int(distinct_keys)},
    )


def _evaluate_metric_candidates(
    dataframe: Any,
    rule: RuleSpec,
    row_count: int,
    min_value: int | float,
    strict: bool,
) -> RuleResult:
    metric_columns = _metric_columns(dataframe, rule)
    if not metric_columns:
        return _skipped_for_missing(
            rule,
            [column for columns in rule.params["metrics"].values() for column in columns],
        )

    failed_rows = 0
    if row_count > 0:
        condition = _any_not_greater_than(dataframe, metric_columns, min_value)
        if not strict:
            condition = _any_less_than(dataframe, metric_columns, min_value)
        failed_rows = _count_condition(dataframe, condition)
    return _result(
        rule,
        failed=failed_rows > 0,
        failed_rows=failed_rows,
        details={
            "checked_metrics": {
                metric: column for metric, column in metric_columns.items()
            },
            "missing_optional_metrics": _missing_metric_candidates(dataframe, rule),
        },
    )


def _first_available_columns(dataframe: Any, rule: RuleSpec) -> tuple[str, ...]:
    for columns in rule.params["candidates"]:
        if all(column in dataframe.columns for column in columns):
            return tuple(columns)
    return tuple(rule.params["candidates"][0])


def _metric_columns(dataframe: Any, rule: RuleSpec) -> dict[str, str]:
    selected = {}
    for metric, candidates in rule.params["metrics"].items():
        for column in candidates:
            if column in dataframe.columns:
                selected[metric] = column
                break
    return selected


def _missing_metric_candidates(dataframe: Any, rule: RuleSpec) -> dict[str, list[str]]:
    missing = {}
    for metric, candidates in rule.params["metrics"].items():
        if not any(column in dataframe.columns for column in candidates):
            missing[metric] = list(candidates)
    return missing


def _skipped_for_missing(rule: RuleSpec, missing: list[str]) -> RuleResult:
    return RuleResult(
        name=rule.name,
        description=rule.description,
        severity=rule.severity,
        status=SKIP,
        passed=False,
        failed_rows=None,
        details={"missing_columns": missing},
    )


def _count_condition(dataframe: Any, condition: Any) -> int:
    return int(dataframe.where(condition).count())


def _ratio(value: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round(value / total, 6)


def _any_null(dataframe: Any, columns: tuple[str, ...]) -> Any:
    return reduce(or_, [dataframe[column].isNull() for column in columns])


def _any_negative(dataframe: Any, columns: tuple[str, ...]) -> Any:
    return reduce(
        or_,
        [dataframe[column].isNotNull() & (dataframe[column] < 0) for column in columns],
    )


def _any_outside_range(
    dataframe: Any,
    columns: tuple[str, ...],
    min_value: float,
    max_value: float,
) -> Any:
    return reduce(
        or_,
        [
            dataframe[column].isNotNull()
            & ((dataframe[column] < min_value) | (dataframe[column] > max_value))
            for column in columns
        ],
    )


def _any_unaccepted_value(dataframe: Any, columns: tuple[str, ...], values: tuple[Any, ...]) -> Any:
    return reduce(
        or_,
        [dataframe[column].isNotNull() & ~dataframe[column].isin(*values) for column in columns],
    )


def _any_not_greater_than(
    dataframe: Any,
    metric_columns: dict[str, str],
    min_value: int | float,
) -> Any:
    return reduce(
        or_,
        [
            dataframe[column].isNotNull() & (dataframe[column] <= min_value)
            for column in metric_columns.values()
        ],
    )


def _any_less_than(
    dataframe: Any,
    metric_columns: dict[str, str],
    min_value: int | float,
) -> Any:
    return reduce(
        or_,
        [
            dataframe[column].isNotNull() & (dataframe[column] < min_value)
            for column in metric_columns.values()
        ],
    )


def _top_invalid_values(
    dataframe: Any,
    columns: tuple[str, ...],
    values: tuple[Any, ...],
    limit: int = 10,
) -> list[dict[str, Any]]:
    from pyspark.sql import functions as F

    top_values = []
    for column in columns:
        rows = (
            dataframe.where(dataframe[column].isNotNull() & ~dataframe[column].isin(*values))
            .groupBy(column)
            .count()
            .orderBy(F.desc("count"))
            .limit(limit)
            .collect()
        )
        top_values.extend(
            {
                "column": column,
                "value": row[column],
                "row_count": int(row["count"]),
            }
            for row in rows
        )
    return top_values


def _sum_not_equal(dataframe: Any, addends: tuple[str, ...], target: str) -> Any:
    from pyspark.sql import functions as F

    sum_expression = reduce(
        lambda left, right: left + right,
        [F.coalesce(dataframe[column], F.lit(0)) for column in addends],
    )
    return dataframe[target].isNotNull() & (sum_expression != dataframe[target])
