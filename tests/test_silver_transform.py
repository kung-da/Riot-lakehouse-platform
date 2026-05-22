import json
from pathlib import Path

import pytest

from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.silver.silver_transformer import run_silver_transform, transform_payload


def test_transform_match_payload_to_silver_tables():
    payload = {
        "metadata": {"matchId": "VN2_1"},
        "info": {
            "gameId": 1,
            "participants": [{"puuid": "p1", "championId": 1, "win": True}],
            "teams": [{"teamId": 100, "win": True, "objectives": {}}],
        },
    }
    result = transform_payload("matches", payload)
    assert result["matches"][0]["match_id"] == "VN2_1"
    assert result["participants"][0]["puuid"] == "p1"
    assert result["teams"][0]["team_id"] == 100


def test_run_silver_transform_writes_domain_tables(tmp_path: Path):
    pytest.importorskip("pyspark")

    raw_match_dir = tmp_path / "raw" / "matches"
    raw_match_dir.mkdir(parents=True)
    payload = {
        "metadata": {"matchId": "VN2_1"},
        "info": {
            "gameId": 1,
            "gameCreation": 1710000000000,
            "gameDuration": 1800,
            "participants": [
                {
                    "puuid": "p1",
                    "summonerId": "s1",
                    "championId": 1,
                    "championName": "Annie",
                    "teamId": 100,
                    "teamPosition": "MIDDLE",
                    "win": True,
                    "kills": 5,
                    "deaths": 1,
                    "assists": 7,
                    "goldEarned": 12000,
                    "totalDamageDealtToChampions": 20000,
                    "visionScore": 18,
                    "totalMinionsKilled": 190,
                }
            ],
            "teams": [
                {
                    "teamId": 100,
                    "win": True,
                    "objectives": {
                        "baron": {"kills": 1},
                        "dragon": {"kills": 3},
                        "riftHerald": {"kills": 1},
                        "tower": {"kills": 8},
                        "inhibitor": {"kills": 1},
                    },
                }
            ],
        },
    }
    (raw_match_dir / "VN2_1.json").write_text(json.dumps(payload), encoding="utf-8")

    config = LakehouseConfig(
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

    assert run_bronze_ingestion(config, datasets=["matches"]) == 1
    counts = run_silver_transform(
        config,
        datasets=["matches"],
        tables=["matches", "participants", "teams"],
    )

    assert counts == {"matches": 1, "participants": 1, "teams": 1}
    assert list(config.layer_path("silver", "matches").rglob("*.parquet"))
    assert list(config.layer_path("silver", "participants").rglob("*.parquet"))
    assert list(config.layer_path("silver", "teams").rglob("*.parquet"))

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
        team = (
            spark.read.parquet(config.layer_path("silver", "teams").as_posix())
            .collect()[0]
            .asDict()
        )
        assert match["match_id"] == "VN2_1"
        assert participant["puuid"] == "p1"
        assert participant["gold_earned"] == 12000
        assert team["dragon_kills"] == 3
    finally:
        spark.stop()
