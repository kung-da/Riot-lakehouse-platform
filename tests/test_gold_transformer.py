import math
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.common.storage import has_table_data, read_table_dataset, write_table_dataset
from lakehouse.gold.aggregations import (
    build_fact_participant_performance,
    build_fact_team_objectives,
    build_mart_player_daily_performance,
    build_mart_role_daily_performance,
    build_mart_team_objective_daily_summary,
)
from lakehouse.gold.gold_transformer import GOLD_TABLES, _selected_tables, run_gold_transform
from lakehouse.gold.schemas import GOLD_COLUMNS


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
            "table_formats": {
                "bronze": "parquet",
                "silver": "delta",
                "gold": "delta",
            },
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


def _match_rows() -> list[dict]:
    return [
        {
            "match_id": "VN2_1",
            "game_id": 1,
            "platform_id": "VN2",
            "queue_id": 420,
            "game_mode": "CLASSIC",
            "game_type": "MATCHED_GAME",
            "game_version": "14.5.1",
            "game_creation": 1709942400000,
            "game_start_timestamp": 1709942410000,
            "game_end_timestamp": 1709944210000,
            "game_duration": 1800,
            "participant_count": 2,
            "dataset": "matches",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_2",
            "game_id": 2,
            "platform_id": "VN2",
            "queue_id": 420,
            "game_mode": "CLASSIC",
            "game_type": "MATCHED_GAME",
            "game_version": "14.5.1",
            "game_creation": 1709946000000,
            "game_start_timestamp": 1709946010000,
            "game_end_timestamp": 1709947810000,
            "game_duration": 1800,
            "participant_count": 1,
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


def _summoner_rows() -> list[dict]:
    return [
        {
            "puuid": "p1",
            "summoner_id": "s1",
            "account_id": "a1",
            "profile_icon_id": 1,
            "revision_date": 1709942400000,
            "summoner_level": 300,
            "dataset": "summoners",
            "game_date": GAME_DATE,
        },
        {
            "puuid": "p2",
            "summoner_id": "s2",
            "account_id": "a2",
            "profile_icon_id": 2,
            "revision_date": 1709942400000,
            "summoner_level": 250,
            "dataset": "summoners",
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


def _timeline_event_rows() -> list[dict]:
    return [
        {
            "match_id": "VN2_1",
            "frame_index": 0,
            "event_index": 0,
            "event_timestamp": 1000,
            "event_type": "CHAMPION_KILL",
            "participant_id": 1,
            "killer_id": 1,
            "victim_id": 2,
            "team_id": 100,
            "monster_type": "NONE",
            "building_type": "NONE",
            "lane_type": "MID_LANE",
            "dataset": "timelines",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_1",
            "frame_index": 1,
            "event_index": 0,
            "event_timestamp": 60000,
            "event_type": "ELITE_MONSTER_KILL",
            "participant_id": 2,
            "killer_id": 2,
            "victim_id": 0,
            "team_id": 200,
            "monster_type": "DRAGON",
            "building_type": "NONE",
            "lane_type": "NONE",
            "dataset": "timelines",
            "game_date": GAME_DATE,
        },
    ]


def _timeline_frame_rows() -> list[dict]:
    return [
        {
            "match_id": "VN2_1",
            "frame_index": 0,
            "frame_timestamp": 0,
            "participant_frame_count": 2,
            "event_count": 1,
            "dataset": "timelines",
            "game_date": GAME_DATE,
        },
        {
            "match_id": "VN2_1",
            "frame_index": 1,
            "frame_timestamp": 60000,
            "participant_frame_count": 2,
            "event_count": 1,
            "dataset": "timelines",
            "game_date": GAME_DATE,
        },
    ]


def _write_partitioned_silver_table(spark, path: Path, rows: list[dict]) -> None:
    write_table_dataset(
        dataframe=spark.createDataFrame(rows),
        output_path=path,
        mode="overwrite",
        partition_columns=["dataset", "game_date"],
        output_partitions=1,
        table_format="delta",
    )


def test_selected_tables_rejects_unknown_gold_table():
    with pytest.raises(ValueError, match="Unknown gold tables"):
        _selected_tables(["mart_player_daily_performance", "not_a_table"])


def test_player_daily_mart_aggregation_logic(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        participants = spark.createDataFrame(_participant_rows())
        metrics = {
            row["puuid"]: row.asDict()
            for row in build_mart_player_daily_performance({"participants": participants}).collect()
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


def test_gold_builders_deduplicate_business_keys_before_metrics(tmp_path: Path):
    pytest.importorskip("pyspark")

    participant_row = _participant_rows()[0]
    team_row = _team_rows()[0]
    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        participants = spark.createDataFrame([participant_row, dict(participant_row)])
        teams = spark.createDataFrame([team_row, dict(team_row)])

        participant_fact = build_fact_participant_performance(
            {"participants": participants}
        ).collect()
        team_fact = build_fact_team_objectives({"teams": teams}).collect()
        player_metrics = build_mart_player_daily_performance(
            {"participants": participants}
        ).collect()[0].asDict()
        team_metrics = (
            build_mart_team_objective_daily_summary({"teams": teams})
            .collect()[0]
            .asDict()
        )
    finally:
        spark.stop()

    assert len(participant_fact) == 1
    assert len(team_fact) == 1
    assert player_metrics["matches_played"] == 1
    assert player_metrics["wins"] == 1
    assert player_metrics["losses"] == 0
    assert math.isclose(player_metrics["win_rate"], 1.0)
    assert team_metrics["games_played"] == 1
    assert team_metrics["wins"] == 1
    assert team_metrics["losses"] == 0
    assert math.isclose(team_metrics["win_rate"], 1.0)


def test_role_daily_mart_normalizes_invalid_role_values(tmp_path: Path):
    pytest.importorskip("pyspark")

    invalid_role_row = {
        **_participant_rows()[0],
        "match_id": "VN2_3",
        "puuid": "p3",
        "summoner_id": "s3",
        "team_position": "",
        "individual_position": "Invalid",
        "lane": "NONE",
        "role": "NONE",
    }
    config = _config(tmp_path)
    spark = get_spark(config=config)
    try:
        participants = spark.createDataFrame([_participant_rows()[0], invalid_role_row])
        roles = {
            row["team_position"]: row.asDict()
            for row in build_mart_role_daily_performance({"participants": participants}).collect()
        }
    finally:
        spark.stop()

    assert "MIDDLE" in roles
    assert "UNKNOWN" in roles
    assert "Invalid" not in roles
    assert "NONE" not in roles


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


def test_run_gold_transform_writes_dimensional_model_tables(tmp_path: Path):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")

    config = _config(tmp_path)
    spark = get_spark(config=config, enable_delta=True)
    try:
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "matches"),
            _match_rows(),
        )
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "participants"),
            _participant_rows(),
        )
        _write_partitioned_silver_table(spark, config.layer_path("silver", "teams"), _team_rows())
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "summoners"),
            _summoner_rows(),
        )
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "ranked"),
            _ranked_rows(),
        )
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "timeline_events"),
            _timeline_event_rows(),
        )
        _write_partitioned_silver_table(
            spark,
            config.layer_path("silver", "timeline_frames"),
            _timeline_frame_rows(),
        )
    finally:
        spark.stop()

    counts = run_gold_transform(config)
    assert counts == {
        "dim_date": 1,
        "dim_match": 2,
        "dim_summoner": 2,
        "dim_champion": 2,
        "dim_team": 2,
        "dim_rank": 2,
        "fact_participant_performance": 3,
        "fact_team_objectives": 3,
        "fact_rank_snapshot": 2,
        "fact_timeline_frames": 2,
        "fact_timeline_events": 2,
        "mart_player_daily_performance": 2,
        "mart_champion_daily_performance": 2,
        "mart_role_daily_performance": 2,
        "mart_rank_daily_summary": 2,
        "mart_team_objective_daily_summary": 2,
    }

    for table in GOLD_TABLES:
        assert has_table_data(config.layer_path("gold", table), "delta")
        if "game_date" in GOLD_COLUMNS[table]:
            assert (config.layer_path("gold", table) / f"game_date={GAME_DATE}").exists()

    spark = get_spark(config=config, enable_delta=True)
    try:
        player = (
            read_table_dataset(
                spark,
                config.layer_path("gold", "mart_player_daily_performance"),
                "delta",
            )
            .where("puuid = 'p1'")
            .collect()[0]
            .asDict()
        )
        team = (
            read_table_dataset(
                spark,
                config.layer_path("gold", "mart_team_objective_daily_summary"),
                "delta",
            )
            .where("team_id = 100")
            .collect()[0]
            .asDict()
        )
        participant_fact = (
            read_table_dataset(
                spark,
                config.layer_path("gold", "fact_participant_performance"),
                "delta",
            )
            .where("match_id = 'VN2_1' and participant_id = 1")
            .collect()[0]
            .asDict()
        )
        assert player["matches_played"] == 2
        assert math.isclose(player["win_rate"], 0.5)
        assert team["games_played"] == 2
        assert team["wins"] == 1
        assert math.isclose(team["avg_dragon_kills"], 2.5)
        assert participant_fact["cs"] == 194
        assert participant_fact["team_position"] == "MIDDLE"
    finally:
        spark.stop()
