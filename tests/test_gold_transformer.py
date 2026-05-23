import math
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.gold.aggregations import build_player_metrics
from lakehouse.gold.gold_transformer import GOLD_TABLES, _selected_tables, run_gold_transform


GAME_DATE = "2024-03-09"


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
            "partition_columns": {
                "bronze": ["dataset", "ingest_date"],
                "silver": ["dataset", "game_date"],
                "gold": ["game_date"],
            },
            "gold": {"write_mode": "overwrite", "output_partitions": 1},
            "spark": {
                "app_name": "gold-transform-test",
                "master": "local[1]",
                "enable_delta": False,
            },
        },
    )


def _participant_rows() -> list[dict]:
    return [
        {
            "match_id": "VN2_1",
            "participant_id": 1,
            "puuid": "p1",
            "summoner_id": "s1",
            "riot_id_game_name": "PlayerOne",
            "riot_id_tagline": "VN2",
            "summoner_name": "PlayerOne",
            "champion_id": 1,
            "champion_name": "Annie",
            "team_id": 100,
            "team_position": "MIDDLE",
            "individual_position": "MIDDLE",
            "lane": "MIDDLE",
            "role": "SOLO",
            "win": True,
            "kills": 5,
            "deaths": 1,
            "assists": 7,
            "kda": 12.0,
            "gold_earned": 12000,
            "total_damage_dealt_to_champions": 20000,
            "total_damage_taken": 11000,
            "vision_score": 18,
            "total_minions_killed": 190,
            "neutral_minions_killed": 4,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_2",
            "participant_id": 1,
            "puuid": "p1",
            "summoner_id": "s1",
            "riot_id_game_name": "PlayerOne",
            "riot_id_tagline": "VN2",
            "summoner_name": "PlayerOne",
            "champion_id": 1,
            "champion_name": "Annie",
            "team_id": 100,
            "team_position": "MIDDLE",
            "individual_position": "MIDDLE",
            "lane": "MIDDLE",
            "role": "SOLO",
            "win": False,
            "kills": 3,
            "deaths": 4,
            "assists": 5,
            "kda": 2.0,
            "gold_earned": 9800,
            "total_damage_dealt_to_champions": 15000,
            "total_damage_taken": 14000,
            "vision_score": 14,
            "total_minions_killed": 145,
            "neutral_minions_killed": 5,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_1",
            "participant_id": 2,
            "puuid": "p2",
            "summoner_id": "s2",
            "riot_id_game_name": "JungleTwo",
            "riot_id_tagline": "VN2",
            "summoner_name": "JungleTwo",
            "champion_id": 2,
            "champion_name": "LeeSin",
            "team_id": 200,
            "team_position": "JUNGLE",
            "individual_position": "JUNGLE",
            "lane": "JUNGLE",
            "role": "NONE",
            "win": False,
            "kills": 2,
            "deaths": 6,
            "assists": 8,
            "kda": 1.6666666667,
            "gold_earned": 9000,
            "total_damage_dealt_to_champions": 11000,
            "total_damage_taken": 21000,
            "vision_score": 22,
            "total_minions_killed": 20,
            "neutral_minions_killed": 120,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
    ]


def _team_rows() -> list[dict]:
    return [
        {
            "match_id": "VN2_1",
            "team_id": 100,
            "win": True,
            "baron_kills": 1,
            "dragon_kills": 3,
            "rift_herald_kills": 1,
            "tower_kills": 8,
            "inhibitor_kills": 1,
            "champion_kills": 25,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_1",
            "team_id": 200,
            "win": False,
            "baron_kills": 0,
            "dragon_kills": 1,
            "rift_herald_kills": 0,
            "tower_kills": 2,
            "inhibitor_kills": 0,
            "champion_kills": 12,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_2",
            "team_id": 100,
            "win": False,
            "baron_kills": 0,
            "dragon_kills": 2,
            "rift_herald_kills": 1,
            "tower_kills": 5,
            "inhibitor_kills": 0,
            "champion_kills": 18,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
    ]


def _ranked_rows() -> list[dict]:
    return [
        {
            "league_id": "league-1",
            "queue": "RANKED_SOLO_5x5",
            "tier": "DIAMOND",
            "rank": "I",
            "summoner_id": "s1",
            "puuid": "p1",
            "league_points": 90,
            "wins": 30,
            "losses": 10,
            "win_rate": 0.75,
            "hot_streak": False,
            "veteran": True,
            "fresh_blood": False,
            "inactive": False,
            "dataset": "ranked",
            "game_date": GAME_DATE,
        },
        {
            "league_id": "league-2",
            "queue": "RANKED_SOLO_5x5",
            "tier": "GOLD",
            "rank": "II",
            "summoner_id": "s2",
            "puuid": "p2",
            "league_points": 40,
            "wins": 12,
            "losses": 13,
            "win_rate": 0.48,
            "hot_streak": True,
            "veteran": False,
            "fresh_blood": True,
            "inactive": False,
            "dataset": "ranked",
            "game_date": GAME_DATE,
        },
    ]


def _write_partitioned_silver_table(spark, path: Path, rows: list[dict]) -> None:
    (
        spark.createDataFrame(rows)
        .write.mode("overwrite")
        .partitionBy("dataset", "game_date")
        .parquet(path.as_posix())
    )


def test_selected_tables_rejects_unknown_gold_table():
    with pytest.raises(ValueError, match="Unknown gold tables"):
        _selected_tables(["player_metrics", "not_a_table"])


def test_player_metrics_aggregation_logic(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        participants = spark.createDataFrame(_participant_rows())
        metrics = {
            row["puuid"]: row.asDict()
            for row in build_player_metrics({"participants": participants}).collect()
        }
    finally:
        spark.stop()

    assert metrics["p1"]["matches_played"] == 2
    assert metrics["p1"]["wins"] == 1
    assert metrics["p1"]["losses"] == 1
    assert metrics["p1"]["unique_champions"] == 1
    assert metrics["p1"]["total_kills"] == 8
    assert math.isclose(metrics["p1"]["win_rate"], 0.5)
    assert math.isclose(metrics["p1"]["avg_kills"], 4.0)
    assert math.isclose(metrics["p1"]["avg_cs"], 172.0)


def test_run_gold_help():
    env = os.environ.copy()
    src_path = str(Path.cwd() / "src")
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-m", "lakehouse.jobs.run_gold", "--help"],
        cwd=Path.cwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--tables" in result.stdout
    assert "--write-mode" in result.stdout


def test_run_gold_transform_writes_partitioned_aggregate_tables(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "participants"),
            _participant_rows(),
        )
        _write_partitioned_silver_table(spark, config.layer_path("silver", "teams"), _team_rows())
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "ranked"),
            _ranked_rows(),
        )
    finally:
        spark.stop()

    counts = run_gold_transform(config)
    assert counts == {
        "player_metrics": 2,
        "champion_metrics": 2,
        "role_metrics": 2,
        "rank_metrics": 2,
        "team_objective_metrics": 2,
    }

    for table in GOLD_TABLES:
        assert list(config.layer_path("gold", table).rglob("*.parquet"))
        assert (config.layer_path("gold", table) / f"game_date={GAME_DATE}").exists()

    spark = get_spark(config=config)
    try:
        player = (
            spark.read.parquet(config.layer_path("gold", "player_metrics").as_posix())
            .where("puuid = 'p1'")
            .collect()[0]
            .asDict()
        )
        team = (
            spark.read.parquet(config.layer_path("gold", "team_objective_metrics").as_posix())
            .where("team_id = 100")
            .collect()[0]
            .asDict()
        )
        assert player["matches_played"] == 2
        assert math.isclose(player["win_rate"], 0.5)
        assert team["games_played"] == 2
        assert team["wins"] == 1
        assert math.isclose(team["avg_dragon_kills"], 2.5)
    finally:
        spark.stop()
