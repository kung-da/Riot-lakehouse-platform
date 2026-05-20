from lakehouse.catalog.table_definitions import table_definitions


def generate_basic_ddl(database: str = "riot_lakehouse") -> str:
    statements = [f"CREATE DATABASE IF NOT EXISTS {database};"]
    for table, columns in table_definitions().items():
        column_sql = ",\n  ".join(f"{column} string" for column in columns)
        statements.append(f"CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table} (\n  {column_sql}\n)\nSTORED AS PARQUET;")
    return "\n\n".join(statements)
