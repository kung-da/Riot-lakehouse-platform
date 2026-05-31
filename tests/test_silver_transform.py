import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.silver.silver_transformer import (
    SILVER_TABLES,
    _selected_tables,
    derive_game_date_from_ms,
    run_silver_transform,
    transform_bronze_record,
    transform_payload,
)


def _match_payload() -> dict:
    return {
        "metadata": {"matchId": "VN2_1"},
        "info": {
            "gameId": 1,
            "platformId": "VN2",
            "queueId": 420,
            "gameMode": "CLASSIC",
            "gameType": "MATCHED_GAME",
            "gameVersion": "15.1.1",
            "gameCreation": 1710000000000,
            "gameStartTimestamp": 1710000060000,
            "gameEndTimestamp": 1710001860000,
            "gameDuration": 1800,
            "participants": [
                {
                    "participantId": 1,
                    "puuid": "p1",
                    "summonerId": "s1",
                    "riotIdGameName": "RiotName",
                    "riotIdTagline": "VN2",
                    "summonerName": "LegacyName",
                    "championId": 1,
                    "championName": "Annie",
                    "teamId": 100,
                    "teamPosition": "MIDDLE",
                    "individualPosition": "MIDDLE",
                    "lane": "MIDDLE",
                    "role": "SOLO",
                    "win": True,
                    "kills": 5,
                    "deaths": 1,
                    "assists": 7,
                    "goldEarned": 12000,
                    "totalDamageDealtToChampions": 20000,
                    "totalDamageTaken": 11000,
                    "visionScore": 18,
                    "totalMinionsKilled": 190,
                    "neutralMinionsKilled": 4,
                }
            ],
            "teams": [
                {
                    "teamId": 100,
                    "win": True,
                    "objectives": {
                        "baron": {"kills": 1},
                        "champion": {"kills": 25},
                        "dragon": {"kills": 3},
                        "riftHerald": {"kills": 1},
                        "tower": {"kills": 8},
                        "inhibitor": {"kills": 1},
                    },
                }
            ],
        },
    }


def _timeline_payload() -> dict:
    return {
        "metadata": {"matchId": "VN2_1"},
        "info": {
            "frames": [
                {
                    "timestamp": 0,
                    "participantFrames": {"1": {"participantId": 1}},
                    "events": [],
                },
                {
                    "timestamp": 60000,
                    "participantFrames": {"1": {"participantId": 1}},
                    "events": [
                        {
                            "timestamp": 65000,
                            "type": "ELITE_MONSTER_KILL",
                            "participantId": 1,
                            "killerId": 1,
                            "victimId": 2,
                            "teamId": 100,
                            "monsterType": "DRAGON",
                            "buildingType": "TOWER_BUILDING",
                            "laneType": "MID_LANE",
                        }
                    ],
                },
            ]
        },
    }


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
            },
            "bronze": {"output_partitions": 1},
            "silver": {"write_mode": "overwrite", "output_partitions": 1},
            "spark": {
                "app_name": "silver-transform-test",
                "master": "local[1]",
                "enable_delta": False,
            },
        },
    )


def test_transform_match_payload_to_silver_tables():
    result = transform_payload("matches", _match_payload())
    assert result["matches"][0]["match_id"] == "VN2_1"
    assert result["participants"][0]["participant_id"] == 1
    assert result["participants"][0]["kda"] == 12.0
    assert result["teams"][0]["champion_kills"] == 25


def test_transform_match_payload_filters_invalid_team_ids():
    payload = _match_payload()
    payload["info"]["teams"].append(
        {
            "teamId": 0,
            "win": False,
            "objectives": {
                "baron": {"kills": 0},
                "champion": {"kills": 0},
                "dragon": {"kills": 0},
                "riftHerald": {"kills": 0},
                "tower": {"kills": 0},
                "inhibitor": {"kills": 0},
            },
        }
    )

    rows = transform_payload("matches", payload)["teams"]

    assert [row["team_id"] for row in rows] == [100]


def test_ranked_mapping_keeps_summoner_id_and_optional_puuid():
    payload = {
        "leagueId": "league-1",
        "queue": "RANKED_SOLO_5x5",
        "tier": "DIAMOND",
        "entries": [
            {
                "summonerId": "summoner-1",
                "rank": "I",
                "leaguePoints": 90,
                "wins": 30,
                "losses": 10,
            }
        ],
    }
    row = transform_payload("ranked", payload)["ranked"][0]
    assert row["summoner_id"] == "summoner-1"
    assert row["puuid"] is None
    assert row["win_rate"] == 0.75


def test_timeline_events_use_event_column_names():
    row = transform_payload("timelines", _timeline_payload())["timeline_events"][0]
    assert row["event_type"] == "ELITE_MONSTER_KILL"
    assert row["event_timestamp"] == 65000
    assert row["lane_type"] == "MID_LANE"
    assert "type" not in row
    assert "timestamp" not in row


def test_zero_match_timestamps_fall_back_to_ingest_date():
    payload = _match_payload()
    payload["info"]["gameCreation"] = 0
    payload["info"]["gameStartTimestamp"] = 0
    record = {
        "dataset": "matches",
        "source_file": "raw/matches/VN2_1.json",
        "file_hash": "abc123",
        "ingest_ts": "2026-05-22T00:00:00Z",
        "ingest_date": "2026-05-22",
        "payload_json": json.dumps(payload),
    }

    assert derive_game_date_from_ms(0) is None
    assert transform_bronze_record(record)["matches"][0]["game_date"] == "2026-05-22"


def test_transform_bronze_record_enriches_lineage_metadata():
    record = {
        "dataset": "matches",
        "source_file": "raw/matches/VN2_1.json",
        "file_hash": "abc123",
        "ingest_ts": "2026-05-22T00:00:00Z",
        "ingest_date": "2026-05-22",
        "payload_json": json.dumps(_match_payload()),
    }
    transformed = transform_bronze_record(record)
    expected_game_date = derive_game_date_from_ms(1710000000000)

    for table in ["matches", "participants", "teams"]:
        row = transformed[table][0]
        assert row["dataset"] == "matches"
        assert row["source_file"] == "raw/matches/VN2_1.json"
        assert row["file_hash"] == "abc123"
        assert row["ingest_ts"] == "2026-05-22T00:00:00Z"
        assert row["ingest_date"] == "2026-05-22"
        assert row["game_date"] == expected_game_date


def test_selected_tables_rejects_unknown_table():
    with pytest.raises(ValueError, match="Unknown silver tables"):
        _selected_tables(["matches", "not_a_table"])


def test_run_silver_help():
    env = os.environ.copy()
    src_path = str(Path.cwd() / "src")
    env["PYTHONPATH"] = src_path + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-m", "lakehouse.jobs.run_silver", "--help"],
        cwd=Path.cwd(),
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0
    assert "--datasets" in result.stdout
    assert "--tables" in result.stdout


def test_run_silver_transform_writes_partitioned_domain_tables(tmp_path: Path):
    pytest.importorskip("pyspark")

    config = _config(tmp_path)
    records = [
        {
            "dataset": "matches",
            "source_file": "raw/matches/VN2_1.json",
            "file_hash": "match-hash",
            "ingest_ts": "2026-05-22T00:00:00Z",
            "ingest_date": "2026-05-22",
            "payload_json": json.dumps(_match_payload()),
        },
        {
            "dataset": "matches",
            "source_file": "raw/matches/VN2_1.json",
            "file_hash": "match-hash",
            "ingest_ts": "2026-05-22T00:00:00Z",
            "ingest_date": "2026-05-22",
            "payload_json": json.dumps(_match_payload()),
        },
        {
            "dataset": "summoners",
            "source_file": "raw/summoners/s1.json",
            "file_hash": "summoner-hash",
            "ingest_ts": "2026-05-22T00:01:00Z",
            "ingest_date": "2026-05-22",
            "payload_json": json.dumps(
                {
                    "puuid": "p1",
                    "id": "s1",
                    "accountId": "a1",
                    "profileIconId": 1,
                    "revisionDate": 1710000000000,
                    "summonerLevel": 100,
                }
            ),
        },
        {
            "dataset": "ranked",
            "source_file": "raw/ranked/league.json",
            "file_hash": "ranked-hash",
            "ingest_ts": "2026-05-22T00:02:00Z",
            "ingest_date": "2026-05-22",
            "payload_json": json.dumps(
                {
                    "leagueId": "league-1",
                    "queue": "RANKED_SOLO_5x5",
                    "tier": "DIAMOND",
                    "entries": [
                        {
                            "summonerId": "s1",
                            "puuid": "p1",
                            "rank": "I",
                            "leaguePoints": 90,
                            "wins": 30,
                            "losses": 10,
                            "hotStreak": False,
                            "veteran": True,
                            "freshBlood": False,
                            "inactive": False,
                        }
                    ],
                }
            ),
        },
        {
            "dataset": "timelines",
            "source_file": "raw/timelines/VN2_1.json",
            "file_hash": "timeline-hash",
            "ingest_ts": "2026-05-22T00:03:00Z",
            "ingest_date": "2026-05-22",
            "payload_json": json.dumps(_timeline_payload()),
        },
    ]

    spark = get_spark(config=config)
    try:
        bronze_path = config.layer_path("bronze", "raw_json")
        (
            spark.createDataFrame(records)
            .write.mode("append")
            .partitionBy("dataset", "ingest_date")
            .parquet(bronze_path.as_posix())
        )
    finally:
        spark.stop()

    counts = run_silver_transform(config)
    assert counts == {
        "matches": 1,
        "participants": 1,
        "teams": 1,
        "summoners": 1,
        "ranked": 1,
        "timeline_frames": 2,
        "timeline_events": 1,
    }

    for table in SILVER_TABLES:
        assert list(config.layer_path("silver", table).rglob("*.parquet"))

    expected_game_date = derive_game_date_from_ms(1710000000000)
    assert (
        config.layer_path("silver", "matches")
        / "dataset=matches"
        / f"game_date={expected_game_date}"
    ).exists()

    spark = get_spark(config=config)
    try:
        match = (
            spark.read.parquet(config.layer_path("silver", "matches").as_posix())
            .collect()[0]
            .asDict()
        )
        participant = (
            spark.read.parquet(config.layer_path("silver", "participants").as_posix())
            .collect()[0]
            .asDict()
        )
        assert match["dataset"] == "matches"
        assert match["game_date"] == expected_game_date
        assert match["source_file"] == "raw/matches/VN2_1.json"
        assert match["file_hash"] == "match-hash"
        assert match["ingest_ts"] == "2026-05-22T00:00:00Z"
        assert match["ingest_date"] == "2026-05-22"
        assert participant["dataset"] == "matches"
        assert participant["game_date"] == expected_game_date
        assert participant["kda"] == 12.0
    finally:
        spark.stop()
