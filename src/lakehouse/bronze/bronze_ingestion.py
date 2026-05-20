from __future__ import annotations

from datetime import date

from lakehouse.common.config import LakehouseConfig
from lakehouse.raw.raw_to_bronze import collect_new_bronze_records
from lakehouse.bronze.bronze_writer import write_jsonl


def run_bronze_ingestion(config: LakehouseConfig, datasets: list[str] | None = None) -> int:
    records = collect_new_bronze_records(config.raw_root, config.checkpoint_root, datasets)
    if not records:
        return 0
    output = config.layer_path("bronze", "raw_json") / f"ingest_date={date.today().isoformat()}" / "data.jsonl"
    write_jsonl(records, output)
    return len(records)
