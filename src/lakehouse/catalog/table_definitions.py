from lakehouse.silver.silver_transformer import TABLE_FIELDS


def table_definitions() -> dict[str, list[str]]:
    silver_tables = {f"silver_{table}": fields for table, fields in TABLE_FIELDS.items()}
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
    }
