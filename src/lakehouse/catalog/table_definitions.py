from lakehouse.gold.schemas import GOLD_COLUMNS
from lakehouse.silver.silver_transformer import TABLE_FIELDS


def table_definitions() -> dict[str, list[str]]:
    silver_tables = {f"silver_{table}": fields for table, fields in TABLE_FIELDS.items()}
    gold_tables = {f"gold_{table}": fields for table, fields in GOLD_COLUMNS.items()}
    return {
        "bronze_raw_json": [
            "dataset",
            "source_file",
            "file_hash",
            "ingest_ts",
            "ingest_date",
            "payload_json",
        ],
        **silver_tables,
        **gold_tables,
    }
