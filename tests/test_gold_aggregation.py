from lakehouse.gold.gold_builder import gold_sql_registry


def test_gold_registry_contains_expected_tables():
    registry = gold_sql_registry()
    assert "dim_summoner" in registry
    assert "fact_participant_performance" in registry
    assert "mart_champion_daily_performance" in registry
