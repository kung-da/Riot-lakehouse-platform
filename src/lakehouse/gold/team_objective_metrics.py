def build_team_objective_metrics_sql() -> str:
    return "select team_id, avg(dragon_kills) as avg_dragons, avg(tower_kills) as avg_towers from teams group by team_id"
