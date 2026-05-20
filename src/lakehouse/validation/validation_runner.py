from __future__ import annotations

from lakehouse.validation.data_quality_score import quality_score
from lakehouse.validation.schema_checks import require_keys


def validate_records(records: list[dict], required_keys: list[str]) -> dict:
    total_checks = len(records) * len(required_keys)
    failures = []
    for index, record in enumerate(records):
        missing = require_keys(record, required_keys)
        for key in missing:
            failures.append({"row": index, "field": key, "rule": "required"})
    passed = total_checks - len(failures)
    return {"total_checks": total_checks, "passed": passed, "failures": failures, "score": quality_score(passed, total_checks)}
