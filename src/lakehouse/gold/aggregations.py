from __future__ import annotations

from typing import Any, Callable

from lakehouse.gold.schemas import GOLD_COLUMNS


GoldAggregation = Callable[[dict[str, Any]], Any]

GOLD_TABLE_SOURCES = {
    "player_metrics": ["participants"],
    "champion_metrics": ["participants"],
    "role_metrics": ["participants"],
    "rank_metrics": ["ranked"],
    "team_objective_metrics": ["teams"],
}


def _functions() -> Any:
    from pyspark.sql import functions as F

    return F


def _true_count(column: str) -> Any:
    F = _functions()
    return F.sum(F.when(F.col(column) == F.lit(True), F.lit(1)).otherwise(F.lit(0))).cast("long")


def _sum_as_long(column: str) -> Any:
    F = _functions()
    return F.sum(F.coalesce(F.col(column), F.lit(0))).cast("long")


def _cs_column() -> Any:
    F = _functions()
    return (
        F.coalesce(F.col("total_minions_killed"), F.lit(0))
        + F.coalesce(F.col("neutral_minions_killed"), F.lit(0))
    ).cast("long")


def _valid_text(column: str) -> Any:
    F = _functions()
    return F.when(F.length(F.trim(F.col(column))) > 0, F.col(column))


def _add_loss_and_win_rate(dataframe: Any, count_column: str) -> Any:
    F = _functions()
    return dataframe.withColumn(
        "losses",
        (F.col(count_column) - F.col("wins")).cast("long"),
    ).withColumn(
        "win_rate",
        F.when(
            F.col(count_column) > 0,
            F.col("wins").cast("double") / F.col(count_column).cast("double"),
        ).otherwise(F.lit(None).cast("double")),
    )


def _participant_metrics(dataframe: Any, group_columns: list[str], include_totals: bool) -> Any:
    F = _functions()
    aggregations = [
        F.count("*").cast("long").alias("matches_played"),
        F.countDistinct("puuid").cast("long").alias("unique_players"),
        F.countDistinct("champion_id").cast("long").alias("unique_champions"),
        _true_count("win").alias("wins"),
        F.avg("kills").cast("double").alias("avg_kills"),
        F.avg("deaths").cast("double").alias("avg_deaths"),
        F.avg("assists").cast("double").alias("avg_assists"),
        F.avg("kda").cast("double").alias("avg_kda"),
        F.avg("gold_earned").cast("double").alias("avg_gold_earned"),
        F.avg("total_damage_dealt_to_champions")
        .cast("double")
        .alias("avg_damage_dealt_to_champions"),
        F.avg("total_damage_taken").cast("double").alias("avg_damage_taken"),
        F.avg("vision_score").cast("double").alias("avg_vision_score"),
        F.avg("_cs").cast("double").alias("avg_cs"),
    ]
    if include_totals:
        aggregations.extend(
            [
                _sum_as_long("kills").alias("total_kills"),
                _sum_as_long("deaths").alias("total_deaths"),
                _sum_as_long("assists").alias("total_assists"),
            ]
        )
    return dataframe.groupBy(*group_columns).agg(*aggregations)


def build_player_metrics(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    participants = (
        silver_tables["participants"]
        .where(F.col("puuid").isNotNull())
        .withColumn("_cs", _cs_column())
    )
    aggregated = participants.groupBy("game_date", "puuid").agg(
        F.first("summoner_id", True).alias("summoner_id"),
        F.first("summoner_name", True).alias("summoner_name"),
        F.first("riot_id_game_name", True).alias("riot_id_game_name"),
        F.first("riot_id_tagline", True).alias("riot_id_tagline"),
        F.countDistinct("match_id").cast("long").alias("matches_played"),
        _true_count("win").alias("wins"),
        F.countDistinct("champion_id").cast("long").alias("unique_champions"),
        _sum_as_long("kills").alias("total_kills"),
        _sum_as_long("deaths").alias("total_deaths"),
        _sum_as_long("assists").alias("total_assists"),
        F.avg("kills").cast("double").alias("avg_kills"),
        F.avg("deaths").cast("double").alias("avg_deaths"),
        F.avg("assists").cast("double").alias("avg_assists"),
        F.avg("kda").cast("double").alias("avg_kda"),
        F.avg("gold_earned").cast("double").alias("avg_gold_earned"),
        F.avg("total_damage_dealt_to_champions")
        .cast("double")
        .alias("avg_damage_dealt_to_champions"),
        F.avg("total_damage_taken").cast("double").alias("avg_damage_taken"),
        F.avg("vision_score").cast("double").alias("avg_vision_score"),
        F.avg("_cs").cast("double").alias("avg_cs"),
    )
    return _add_loss_and_win_rate(aggregated, "matches_played").select(
        *GOLD_COLUMNS["player_metrics"]
    )


def build_champion_metrics(silver_tables: dict[str, Any]) -> Any:
    participants = silver_tables["participants"].withColumn("_cs", _cs_column())
    aggregated = _participant_metrics(
        participants,
        group_columns=["game_date", "champion_id", "champion_name"],
        include_totals=True,
    )
    return _add_loss_and_win_rate(aggregated, "matches_played").select(
        *GOLD_COLUMNS["champion_metrics"]
    )


def build_role_metrics(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    participants = silver_tables["participants"].withColumn(
        "team_position",
        F.coalesce(
            _valid_text("team_position"),
            _valid_text("individual_position"),
            _valid_text("lane"),
            _valid_text("role"),
            F.lit("UNKNOWN"),
        ),
    ).withColumn("_cs", _cs_column())
    aggregated = _participant_metrics(
        participants,
        group_columns=["game_date", "team_position"],
        include_totals=False,
    )
    return _add_loss_and_win_rate(aggregated, "matches_played").select(
        *GOLD_COLUMNS["role_metrics"]
    )


def build_rank_metrics(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    ranked = silver_tables["ranked"]
    return ranked.groupBy("game_date", "queue", "tier", "rank").agg(
        F.countDistinct(F.coalesce(F.col("summoner_id"), F.col("puuid")))
        .cast("long")
        .alias("players"),
        F.avg("league_points").cast("double").alias("avg_league_points"),
        _sum_as_long("wins").alias("total_wins"),
        _sum_as_long("losses").alias("total_losses"),
        F.avg("win_rate").cast("double").alias("avg_win_rate"),
        _true_count("hot_streak").alias("hot_streak_players"),
        _true_count("veteran").alias("veteran_players"),
        _true_count("fresh_blood").alias("fresh_blood_players"),
        _true_count("inactive").alias("inactive_players"),
    ).select(*GOLD_COLUMNS["rank_metrics"])


def build_team_objective_metrics(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    teams = silver_tables["teams"]
    aggregated = teams.groupBy("game_date", "team_id").agg(
        F.countDistinct("match_id").cast("long").alias("games_played"),
        _true_count("win").alias("wins"),
        F.avg("baron_kills").cast("double").alias("avg_baron_kills"),
        F.avg("dragon_kills").cast("double").alias("avg_dragon_kills"),
        F.avg("rift_herald_kills").cast("double").alias("avg_rift_herald_kills"),
        F.avg("tower_kills").cast("double").alias("avg_tower_kills"),
        F.avg("inhibitor_kills").cast("double").alias("avg_inhibitor_kills"),
        F.avg("champion_kills").cast("double").alias("avg_champion_kills"),
    )
    return _add_loss_and_win_rate(aggregated, "games_played").select(
        *GOLD_COLUMNS["team_objective_metrics"]
    )


AGGREGATION_BUILDERS: dict[str, GoldAggregation] = {
    "player_metrics": build_player_metrics,
    "champion_metrics": build_champion_metrics,
    "role_metrics": build_role_metrics,
    "rank_metrics": build_rank_metrics,
    "team_objective_metrics": build_team_objective_metrics,
}
