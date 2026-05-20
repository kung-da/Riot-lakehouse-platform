from lakehouse.platinum.champion_meta_features import champion_meta_features_sql
from lakehouse.platinum.match_win_features import match_win_features_sql
from lakehouse.platinum.player_performance_features import player_performance_features_sql


def platinum_sql_registry() -> dict[str, str]:
    return {
        "match_win_features": match_win_features_sql(),
        "player_performance_features": player_performance_features_sql(),
        "champion_meta_features": champion_meta_features_sql(),
    }
