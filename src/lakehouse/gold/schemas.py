from __future__ import annotations

from typing import Any


GOLD_TABLES = [
    "player_metrics",
    "champion_metrics",
    "role_metrics",
    "rank_metrics",
    "team_objective_metrics",
]

GOLD_COLUMNS = {
    "player_metrics": [
        "game_date",
        "puuid",
        "summoner_id",
        "summoner_name",
        "riot_id_game_name",
        "riot_id_tagline",
        "matches_played",
        "wins",
        "losses",
        "win_rate",
        "unique_champions",
        "total_kills",
        "total_deaths",
        "total_assists",
        "avg_kills",
        "avg_deaths",
        "avg_assists",
        "avg_kda",
        "avg_gold_earned",
        "avg_damage_dealt_to_champions",
        "avg_damage_taken",
        "avg_vision_score",
        "avg_cs",
    ],
    "champion_metrics": [
        "game_date",
        "champion_id",
        "champion_name",
        "matches_played",
        "unique_players",
        "wins",
        "losses",
        "win_rate",
        "total_kills",
        "total_deaths",
        "total_assists",
        "avg_kills",
        "avg_deaths",
        "avg_assists",
        "avg_kda",
        "avg_gold_earned",
        "avg_damage_dealt_to_champions",
        "avg_damage_taken",
        "avg_vision_score",
        "avg_cs",
    ],
    "role_metrics": [
        "game_date",
        "team_position",
        "matches_played",
        "unique_players",
        "unique_champions",
        "wins",
        "losses",
        "win_rate",
        "avg_kills",
        "avg_deaths",
        "avg_assists",
        "avg_kda",
        "avg_gold_earned",
        "avg_damage_dealt_to_champions",
        "avg_damage_taken",
        "avg_vision_score",
        "avg_cs",
    ],
    "rank_metrics": [
        "game_date",
        "queue",
        "tier",
        "rank",
        "players",
        "avg_league_points",
        "total_wins",
        "total_losses",
        "avg_win_rate",
        "hot_streak_players",
        "veteran_players",
        "fresh_blood_players",
        "inactive_players",
    ],
    "team_objective_metrics": [
        "game_date",
        "team_id",
        "games_played",
        "wins",
        "losses",
        "win_rate",
        "avg_baron_kills",
        "avg_dragon_kills",
        "avg_rift_herald_kills",
        "avg_tower_kills",
        "avg_inhibitor_kills",
        "avg_champion_kills",
    ],
}

STRING_COLUMNS = {
    "game_date",
    "puuid",
    "summoner_id",
    "summoner_name",
    "riot_id_game_name",
    "riot_id_tagline",
    "champion_name",
    "team_position",
    "queue",
    "tier",
    "rank",
}

LONG_COLUMNS = {
    "champion_id",
    "team_id",
    "matches_played",
    "games_played",
    "wins",
    "losses",
    "unique_champions",
    "unique_players",
    "players",
    "total_kills",
    "total_deaths",
    "total_assists",
    "total_wins",
    "total_losses",
    "hot_streak_players",
    "veteran_players",
    "fresh_blood_players",
    "inactive_players",
}

DOUBLE_COLUMNS = {
    "win_rate",
    "avg_kills",
    "avg_deaths",
    "avg_assists",
    "avg_kda",
    "avg_gold_earned",
    "avg_damage_dealt_to_champions",
    "avg_damage_taken",
    "avg_vision_score",
    "avg_cs",
    "avg_league_points",
    "avg_win_rate",
    "avg_baron_kills",
    "avg_dragon_kills",
    "avg_rift_herald_kills",
    "avg_tower_kills",
    "avg_inhibitor_kills",
    "avg_champion_kills",
}


def gold_schema(table: str) -> Any:
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    if table not in GOLD_COLUMNS:
        raise ValueError(f"Unknown gold table: {table}")

    fields = []
    for column in GOLD_COLUMNS[table]:
        if column in STRING_COLUMNS:
            data_type = StringType()
        elif column in LONG_COLUMNS:
            data_type = LongType()
        elif column in DOUBLE_COLUMNS:
            data_type = DoubleType()
        else:
            raise ValueError(f"No Gold schema type configured for column: {column}")
        fields.append(StructField(column, data_type, nullable=True))
    return StructType(fields)
