def build_player_metrics_sql() -> str:
    return "select puuid, count(*) as matches, avg(kills) as avg_kills from participants group by puuid"
