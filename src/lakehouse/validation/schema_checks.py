def require_keys(record: dict, required_keys: list[str]) -> list[str]:
    return [key for key in required_keys if key not in record or record[key] is None]
