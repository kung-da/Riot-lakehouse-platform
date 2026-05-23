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

    team_objective_rules = rules_for_table("gold", "team_objective_metrics")
    team_objective_rule_names = {rule.name for rule in team_objective_rules}
    assert "team_id_known_values" not in team_objective_rule_names
    assert "team_count_positive" in team_objective_rule_names
    assert "team_objective_metrics_non_negative" in team_objective_rule_names


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
            spark.createDataFrame(_player_metric_rows(), schema=gold_schema("player_metrics"))
            .write.mode("overwrite")
            .partitionBy("game_date")
            .parquet(config.layer_path("gold", "player_metrics").as_posix())
        )
    finally:
        spark.stop()

    result = run_data_quality(
        config,
        layers=["gold"],
        tables_by_layer={"gold": ["player_metrics"]},
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
    assert "player_metrics" in markdown_path.read_text(encoding="utf-8")


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
