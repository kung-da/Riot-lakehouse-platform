from lakehouse.platinum.feature_engineering import safe_divide
from lakehouse.platinum.platinum_builder import platinum_sql_registry


def test_safe_divide_handles_zero():
    assert safe_divide(1, 0) is None
    assert safe_divide(4, 2) == 2.0


def test_platinum_registry_contains_match_features():
    assert "match_win_features" in platinum_sql_registry()
