def champion_meta_features_sql() -> str:
    return "select champion_id, champion_name, picks from champion_metrics"
