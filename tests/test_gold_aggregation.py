from lakehouse.gold.gold_builder import gold_sql_registry


def test_gold_registry_contains_expected_tables():
    registry = gold_sql_registry()
    assert "player_metrics" in registry
    assert "champion_metrics" in registry
