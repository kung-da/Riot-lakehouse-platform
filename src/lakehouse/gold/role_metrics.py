def build_role_metrics_sql() -> str:
    return "select team_position, count(*) as matches from participants group by team_position"
