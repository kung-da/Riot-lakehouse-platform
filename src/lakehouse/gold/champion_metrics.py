def build_champion_metrics_sql() -> str:
    return "select champion_id, champion_name, count(*) as picks from participants group by champion_id, champion_name"
