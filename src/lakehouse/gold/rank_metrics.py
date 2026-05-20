def build_rank_metrics_sql() -> str:
    return "select tier, rank, count(*) as players from ranked group by tier, rank"
