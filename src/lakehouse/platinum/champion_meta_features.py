def champion_meta_features_sql() -> str:
    return (
        "select champion_id, champion_name, matches_played as picks "
        "from mart_champion_daily_performance"
    )
