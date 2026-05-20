import json
from pathlib import Path

from lakehouse.bronze.bronze_ingestion import run_bronze_ingestion
from lakehouse.common.config import LakehouseConfig


def test_bronze_ingestion_writes_new_records(tmp_path: Path):
    raw_match_dir = tmp_path / "raw" / "matches"
    raw_match_dir.mkdir(parents=True)
    (raw_match_dir / "VN2_1.json").write_text(
        json.dumps({"metadata": {"matchId": "VN2_1"}, "info": {"participants": []}}),
        encoding="utf-8",
    )
    config = LakehouseConfig(
        environment="test",
        project_root=tmp_path,
        raw_root=tmp_path / "raw",
        lakehouse_root=tmp_path / "data" / "lakehouse",
        checkpoint_root=tmp_path / "metadata" / "checkpoints",
        report_root=tmp_path / "reports",
        default_format="jsonl",
        write_mode="append",
        values={},
    )

    assert run_bronze_ingestion(config, datasets=["matches"]) == 1
    assert run_bronze_ingestion(config, datasets=["matches"]) == 0
