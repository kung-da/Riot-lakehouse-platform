from lakehouse.catalog.table_definitions import table_definitions


def _lakehouse_location(root_location: str, table: str) -> str:
    root = root_location.rstrip("/")
    if table == "bronze_raw_json":
        return f"{root}/bronze/raw_json/"
    layer, _, name = table.partition("_")
    return f"{root}/{layer}/{name}/"


def generate_basic_ddl(
    database: str = "riot_lakehouse",
    root_location: str = "s3://<bucket>/<lakehouse-prefix>",
) -> str:
    statements = [f"CREATE DATABASE IF NOT EXISTS {database};"]
    for table, columns in table_definitions().items():
        location = _lakehouse_location(root_location, table)
        if table.startswith(("silver_", "gold_")):
            statements.append(
                f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table}\n"
                f"LOCATION '{location}'\n"
                "TBLPROPERTIES ('table_type' = 'DELTA');"
            )
            continue

        column_sql = ",\n  ".join(f"{column} string" for column in columns)
        statements.append(
            f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (\n"
            f"  {column_sql}\n"
            ")\n"
            "STORED AS PARQUET\n"
            f"LOCATION '{location}';"
        )
    return "\n\n".join(statements)
