from lakehouse.catalog.table_definitions import table_definitions


def planned_catalog_tables() -> list[str]:
    return sorted(table_definitions())
