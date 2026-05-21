import json
from pathlib import Path

import pytest

from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark


def test_bronze_ingestion_writes_partitioned_parquet_once(tmp_path: Path):
    pytest.importorskip("pyspark")

    raw_match_dir = tmp_path / "raw" / "matches"
    raw_match_dir.mkdir(parents=True)
    raw_payload = json.dumps({"metadata": {"matchId": "VN2_1"}, "info": {"participants": []}})
    (raw_match_dir / "VN2_1.json").write_text(
        raw_payload,
        encoding="utf-8",
    )
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
            "partition_columns": {"bronze": ["dataset", "ingest_date"]},
            "spark": {"app_name": "bronze-ingestion-test", "master": "local[1]", "enable_delta": False},
        },
    )

    assert run_bronze_ingestion(config, datasets=["matches"]) == 1
    assert run_bronze_ingestion(config, datasets=["matches"]) == 0

    bronze_output = config.layer_path("bronze", "raw_json")
    assert (bronze_output / "dataset=matches").exists()
    assert list(bronze_output.rglob("*.parquet"))
    assert (config.checkpoint_root / "matches.json").exists()

    spark = get_spark(config=config)
    try:
        dataframe = spark.read.parquet(bronze_output.as_posix())
        assert {
            "dataset",
            "source_file",
            "file_hash",
            "ingest_ts",
            "ingest_date",
            "payload_json",
        } <= set(dataframe.columns)
        row = dataframe.collect()[0].asDict()
        assert row["dataset"] == "matches"
        assert row["source_file"] == "raw/matches/VN2_1.json"
        assert row["payload_json"] == raw_payload
    finally:
        spark.stop()
