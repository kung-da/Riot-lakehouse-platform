import json
from pathlib import Path

import pytest

from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.common.checkpoint import FileCheckpoint
from lakehouse.common.config import LakehouseConfig
from lakehouse.common.spark import get_spark
from lakehouse.raw.raw_to_bronze import iter_new_bronze_record_batches


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
            "bronze": {"output_partitions": 1},
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


def test_bronze_record_batches_limit_new_files(tmp_path: Path):
    raw_timeline_dir = tmp_path / "raw" / "timelines"
    raw_timeline_dir.mkdir(parents=True)
    for index in range(3):
        (raw_timeline_dir / f"VN2_{index}.json").write_text(
            json.dumps({"metadata": {"matchId": f"VN2_{index}"}, "info": {"frames": []}}),
            encoding="utf-8",
        )

    batches = list(
        iter_new_bronze_record_batches(
            raw_root=tmp_path / "raw",
            checkpoint_root=tmp_path / "metadata" / "checkpoints",
            datasets=["timelines"],
            max_records_per_batch=1,
            max_files=2,
        )
    )

    records = [record for batch in batches for record in batch.records]
    assert [len(batch) for batch in batches] == [1, 1]
    assert len(records) == 2
    assert {record["dataset"] for record in records} == {"timelines"}


def test_bronze_does_not_parse_json_when_dataset_folder_is_known(tmp_path: Path):
    raw_match_dir = tmp_path / "raw" / "matches"
    raw_match_dir.mkdir(parents=True)
    raw_payload = "{not-valid-json-but-still-raw-bronze}"
    (raw_match_dir / "VN2_invalid.json").write_text(raw_payload, encoding="utf-8")

    batches = list(
        iter_new_bronze_record_batches(
            raw_root=tmp_path / "raw",
            checkpoint_root=tmp_path / "metadata" / "checkpoints",
            datasets=["matches"],
        )
    )

    assert len(batches) == 1
    assert batches[0].records[0]["dataset"] == "matches"
    assert batches[0].records[0]["payload_json"] == raw_payload


def test_bronze_record_batches_limit_only_configured_dataset(tmp_path: Path):
    for dataset in ["matches", "timelines"]:
        raw_dir = tmp_path / "raw" / dataset
        raw_dir.mkdir(parents=True)
        for index in range(3):
            payload = {"metadata": {"matchId": f"VN2_{index}"}, "info": {"participants": []}}
            if dataset == "timelines":
                payload["info"] = {"frames": []}
            (raw_dir / f"VN2_{index}.json").write_text(json.dumps(payload), encoding="utf-8")

    batches = list(
        iter_new_bronze_record_batches(
            raw_root=tmp_path / "raw",
            checkpoint_root=tmp_path / "metadata" / "checkpoints",
            datasets=["matches", "timelines"],
            max_records_per_batch=2,
            dataset_max_files={"timelines": 2},
            dataset_batch_sizes={"timelines": 1},
        )
    )

    records = [record for batch in batches for record in batch.records]
    assert sum(record["dataset"] == "matches" for record in records) == 3
    assert sum(record["dataset"] == "timelines" for record in records) == 2
    assert [len(batch) for batch in batches if batch.records[0]["dataset"] == "timelines"] == [1, 1]


def test_dataset_limit_respects_existing_checkpoint(tmp_path: Path):
    raw_timeline_dir = tmp_path / "raw" / "timelines"
    raw_timeline_dir.mkdir(parents=True)
    for index in range(5):
        (raw_timeline_dir / f"VN2_{index}.json").write_text(
            json.dumps({"metadata": {"matchId": f"VN2_{index}"}, "info": {"frames": []}}),
            encoding="utf-8",
        )

    checkpoint_root = tmp_path / "metadata" / "checkpoints"
    checkpoint = FileCheckpoint(dataset="timelines")
    checkpoint.mark_processed("raw/timelines/VN2_0.json")
    checkpoint.mark_processed("raw/timelines/VN2_1.json")
    checkpoint.save(checkpoint_root)

    batches = list(
        iter_new_bronze_record_batches(
            raw_root=tmp_path / "raw",
            checkpoint_root=checkpoint_root,
            datasets=["timelines"],
            max_records_per_batch=10,
            dataset_max_files={"timelines": 3},
        )
    )

    records = [record for batch in batches for record in batch.records]
    assert len(records) == 1
    assert records[0]["source_file"] == "raw/timelines/VN2_2.json"


def test_bronze_record_batches_respect_byte_limit(tmp_path: Path):
    raw_match_dir = tmp_path / "raw" / "matches"
    raw_match_dir.mkdir(parents=True)
    for index in range(3):
        (raw_match_dir / f"VN2_{index}.json").write_text("x" * 8, encoding="utf-8")

    batches = list(
        iter_new_bronze_record_batches(
            raw_root=tmp_path / "raw",
            checkpoint_root=tmp_path / "metadata" / "checkpoints",
            datasets=["matches"],
            max_records_per_batch=10,
            max_bytes_per_batch=10,
        )
    )

    assert [len(batch) for batch in batches] == [1, 1, 1]
