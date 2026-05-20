def quality_score(passed: int, total: int) -> float:
    if total == 0:
        return 1.0
    return round(passed / total, 4)
