from lakehouse.gold.champion_metrics import build_champion_metrics_sql
from lakehouse.gold.player_metrics import build_player_metrics_sql
from lakehouse.gold.rank_metrics import build_rank_metrics_sql
from lakehouse.gold.role_metrics import build_role_metrics_sql
from lakehouse.gold.team_objective_metrics import build_team_objective_metrics_sql


def gold_sql_registry() -> dict[str, str]:
    return {
        "player_metrics": build_player_metrics_sql(),
        "champion_metrics": build_champion_metrics_sql(),
        "role_metrics": build_role_metrics_sql(),
        "rank_metrics": build_rank_metrics_sql(),
        "team_objective_metrics": build_team_objective_metrics_sql(),
    }
