import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.gold.schemas import gold_schema
from lakehouse.quality.data_quality import run_data_quality
from lakehouse.quality.report_writer import render_markdown_report
from lakehouse.quality.rules import evaluate_rules, rules_for_table


def _config(tmp_path: Path) -> LakehouseConfig:
    return LakehouseConfig(
        environment="test",
        project_root=tmp_path,
        raw_root=tmp_path / "raw",
        lakehouse_root=tmp_path / "data" / "lakehouse",
        checkpoint_root=tmp_path / "metadata" / "checkpoints",
        report_root=tmp_path / "reports",
        default_format="parquet",
        write_mode="append",
        values={
            "quality": {"layers": ["gold"], "output_dir": "data_quality"},
            "spark": {
                "app_name": "data-quality-test",
                "master": "local[1]",
                "enable_delta": False,
            },
        },
    )


def _player_metric_rows() -> list[dict]:
    base = {
        "game_date": "2024-03-09",
        "puuid": "p1",
        "summoner_id": "s1",
        "summoner_name": "PlayerOne",
        "riot_id_game_name": "PlayerOne",
        "riot_id_tagline": "VN2",
        "matches_played": 1,
        "wins": 1,
        "losses": 0,
        "win_rate": 1.0,
        "unique_champions": 1,
        "total_kills": 5,
        "total_deaths": 1,
        "total_assists": 7,
        "avg_kills": 5.0,
        "avg_deaths": 1.0,
        "avg_assists": 7.0,
        "avg_kda": 12.0,
        "avg_gold_earned": 12000.0,
        "avg_damage_dealt_to_champions": 20000.0,
        "avg_damage_taken": 11000.0,
        "avg_vision_score": 18.0,
        "avg_cs": 194.0,
    }
    bad = {
        **base,
        "matches_played": 1,
        "wins": 2,
        "win_rate": 1.2,
        "avg_kills": -1.0,
    }
    return [base, bad]


def _silver_team_rows() -> list[dict]:
    base = {
        "match_id": "VN2_1",
        "team_id": 100,
        "win": True,
        "baron_kills": 1,
        "dragon_kills": 3,
        "rift_herald_kills": 1,
        "tower_kills": 8,
        "inhibitor_kills": 1,
        "champion_kills": 25,
        "source_file": "raw/matches/VN2_1.json",
        "file_hash": "abc123",
        "ingest_ts": "2026-05-22T00:00:00Z",
        "ingest_date": "2026-05-22",
        "dataset": "matches",
        "game_date": "2024-03-09",
    }
    return [
        base,
        {**base, "match_id": "VN2_2", "team_id": 0},
        {**base, "match_id": "VN2_3", "team_id": 0},
        {**base, "match_id": "VN2_4", "team_id": 300},
    ]


def _rule_by_name(layer: str, table: str, name: str):
    return next(rule for rule in rules_for_table(layer, table) if rule.name == name)


def _check_by_name(checks, name: str):
    return next(check for check in checks if check.name == name)


def test_quality_rules_use_riot_stable_identifiers():
    summoner_required = _rule_by_name("silver", "summoners", "required_values_not_null")
    summoner_key = _rule_by_name("silver", "summoners", "unique_business_key")
    assert summoner_required.columns == ("puuid", "dataset", "game_date")
    assert summoner_key.columns == ("puuid",)

    ranked_required = _rule_by_name("silver", "ranked", "required_values_not_null")
    ranked_key = _rule_by_name("silver", "ranked", "unique_business_key")
    assert ranked_required.columns == ("queue", "tier", "rank", "puuid", "dataset")
    assert ranked_key.rule_type == "first_available_unique_key"
    assert ("queue", "tier", "rank", "puuid", "game_date") in ranked_key.params["candidates"]
    assert "summoner_id" not in ranked_required.columns
    assert "league_id" not in ranked_required.columns

    team_objective_rules = rules_for_table("gold", "mart_team_objective_daily_summary")
    team_objective_rule_names = {rule.name for rule in team_objective_rules}
    assert "team_id_known_values" in team_objective_rule_names
    assert "team_count_positive" in team_objective_rule_names
    assert "team_objective_summary_non_negative" in team_objective_rule_names

    dim_date_key = _rule_by_name("gold", "dim_date", "unique_business_key")
    assert dim_date_key.columns == ("date_key",)


def test_gold_dimension_unique_key_validation(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        dataframe = spark.createDataFrame(
            [
                {
                    "date_key": "20240309",
                    "game_date": "2024-03-09",
                    "date_year": 2024,
                    "date_month": 3,
                    "date_day": 9,
                    "day_of_week": 7,
                },
                {
                    "date_key": "20240309",
                    "game_date": "2024-03-09",
                    "date_year": 2024,
                    "date_month": 3,
                    "date_day": 9,
                    "day_of_week": 7,
                },
            ],
            schema=gold_schema("dim_date"),
        )
        checks = evaluate_rules(dataframe, "gold", "dim_date", dataframe.count())
    finally:
        spark.stop()

    assert _check_by_name(checks, "unique_business_key").status == "FAIL"


def test_gold_fact_primary_key_duplicate_detection(tmp_path: Path):
    pytest.importorskip("pyspark")

    row = {
        "game_date": "2024-03-09",
        "match_id": "VN2_1",
        "participant_id": 1,
        "puuid": "p1",
        "summoner_id": "s1",
        "champion_id": 1,
        "team_id": 100,
        "team_position": "MIDDLE",
        "win": True,
        "kills": 1,
        "deaths": 0,
        "assists": 2,
        "kda": 3.0,
        "gold_earned": 500,
        "total_damage_dealt_to_champions": 1000,
        "total_damage_taken": 800,
        "vision_score": 5,
        "total_minions_killed": 20,
        "neutral_minions_killed": 1,
        "cs": 21,
    }
    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        dataframe = spark.createDataFrame(
            [row, row],
            schema=gold_schema("fact_participant_performance"),
        )
        checks = evaluate_rules(
            dataframe,
            "gold",
            "fact_participant_performance",
            dataframe.count(),
        )
    finally:
        spark.stop()

    assert _check_by_name(checks, "unique_business_key").status == "FAIL"


def test_gold_numeric_non_negative_validation(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        dataframe = spark.createDataFrame(
            [
                {
                    "game_date": "2024-03-09",
                    "match_id": "VN2_1",
                    "team_id": 100,
                    "team_side": "BLUE",
                    "win": True,
                    "baron_kills": 0,
                    "dragon_kills": 1,
                    "rift_herald_kills": 0,
                    "tower_kills": 1,
                    "inhibitor_kills": 0,
                    "champion_kills": 3,
                    "objective_score": -1,
                }
            ],
            schema=gold_schema("fact_team_objectives"),
        )
        checks = evaluate_rules(dataframe, "gold", "fact_team_objectives", dataframe.count())
    finally:
        spark.stop()

    assert _check_by_name(checks, "non_negative_metrics").status == "FAIL"


def test_gold_dim_team_expected_values_warn(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        dataframe = spark.createDataFrame(
            [
                {
                    "team_id": 300,
                    "team_side": "UNKNOWN",
                    "first_seen_game_date": "2024-03-09",
                    "last_seen_game_date": "2024-03-09",
                }
            ],
            schema=gold_schema("dim_team"),
        )
        checks = evaluate_rules(dataframe, "gold", "dim_team", dataframe.count())
    finally:
        spark.stop()

    assert _check_by_name(checks, "team_id_known_values").status == "WARN"
    assert _check_by_name(checks, "row_count_expectation").status == "WARN"


def test_gold_mart_champion_win_rate_validation(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        dataframe = spark.createDataFrame(
            [
                {
                    "game_date": "2024-03-09",
                    "champion_id": 1,
                    "champion_name": "Annie",
                    "matches_played": 1,
                    "unique_players": 1,
                    "wins": 2,
                    "losses": 0,
                    "win_rate": 1.2,
                    "total_kills": 1,
                    "total_deaths": 0,
                    "total_assists": 1,
                    "avg_kills": 1.0,
                    "avg_deaths": 0.0,
                    "avg_assists": 1.0,
                    "avg_kda": 2.0,
                    "avg_gold_earned": 500.0,
                    "avg_damage_dealt_to_champions": 1000.0,
                    "avg_damage_taken": 800.0,
                    "avg_vision_score": 5.0,
                    "avg_cs": 21.0,
                }
            ],
            schema=gold_schema("mart_champion_daily_performance"),
        )
        checks = evaluate_rules(
            dataframe,
            "gold",
            "mart_champion_daily_performance",
            dataframe.count(),
        )
    finally:
        spark.stop()

    assert _check_by_name(checks, "rates_between_zero_and_one").status == "FAIL"


def test_run_data_quality_help():
    env = os.environ.copy()
    src_path = str(Path.cwd() / "src")
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-m", "lakehouse.jobs.run_data_quality", "--help"],
        cwd=Path.cwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--layers" in result.stdout
    assert "--fail-on-error" in result.stdout


def test_data_quality_writes_reports_and_flags_failed_gold_checks(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        (
            spark.createDataFrame(
                _player_metric_rows(),
                schema=gold_schema("mart_player_daily_performance"),
            )
            .write.mode("overwrite")
            .partitionBy("game_date")
            .parquet(config.layer_path("gold", "mart_player_daily_performance").as_posix())
        )
    finally:
        spark.stop()

    result = run_data_quality(
        config,
        layers=["gold"],
        tables_by_layer={"gold": ["mart_player_daily_performance"]},
    )
    report = result["report"]

    assert report["status"] == "FAIL"
    assert report["summary"]["ready_for_dashboard"] is False
    assert report["tables"][0]["row_count"] == 2

    checks = {check["name"]: check for check in report["tables"][0]["checks"]}
    assert checks["unique_business_key"]["status"] == "FAIL"
    assert checks["rates_between_zero_and_one"]["status"] == "FAIL"
    assert checks["non_negative_metrics"]["status"] == "FAIL"
    assert checks["wins_losses_match_games"]["status"] == "FAIL"

    json_path = Path(result["paths"]["json"])
    markdown_path = Path(result["paths"]["markdown"])
    assert json_path.exists()
    assert markdown_path.exists()
    assert (config.report_root / "data_quality" / "data_quality_latest.json").exists()
    assert json.loads(json_path.read_text(encoding="utf-8"))["status"] == "FAIL"
    assert "mart_player_daily_performance" in markdown_path.read_text(encoding="utf-8")


def test_accepted_value_warnings_include_top_invalid_values(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        dataframe = spark.createDataFrame(_silver_team_rows())
        checks = {
            check.name: check
            for check in evaluate_rules(dataframe, "silver", "teams", dataframe.count())
        }
    finally:
        spark.stop()

    team_check = checks["team_id_known_values"]
    assert team_check.status == "WARN"
    assert team_check.details["invalid_rate"] == 0.75
    assert team_check.details["top_invalid_values"][0] == {
        "column": "team_id",
        "value": 0,
        "row_count": 2,
    }


def test_markdown_issue_details_include_invalid_value_profile():
    report = {
        "run_id": "test",
        "generated_at": "2026-05-23T00:00:00Z",
        "environment": "test",
        "status": "READY_WITH_WARNINGS",
        "summary": {
            "ready_for_dashboard": True,
            "tables_expected": 1,
            "tables_analyzed": 1,
            "missing_tables": 0,
            "passed_tables": 0,
            "warning_tables": 1,
            "failed_tables": 0,
        },
        "tables": [
            {
                "layer": "silver",
                "table": "teams",
                "status": "WARN",
                "row_count": 4,
                "profile": {"row_count": 4, "column_count": 15},
                "checks": [
                    {
                        "name": "team_id_known_values",
                        "status": "WARN",
                        "failed_rows": 3,
                        "details": {
                            "accepted_values": [100, 200],
                            "invalid_rate": 0.75,
                            "top_invalid_values": [
                                {"column": "team_id", "value": 0, "row_count": 2}
                            ],
                        },
                    }
                ],
            }
        ],
    }

    markdown = render_markdown_report(report)

    assert "invalid_rate" in markdown
    assert "top_invalid_values" in markdown
    assert '"value": 0' in markdown
