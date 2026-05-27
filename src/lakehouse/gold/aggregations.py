from __future__ import annotations

from functools import reduce
from typing import Any, Callable

from lakehouse.gold.schemas import GOLD_COLUMNS


GoldAggregation = Callable[[dict[str, Any]], Any]

ROLE_BUCKETS = ("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY")

GOLD_TABLE_SOURCES = {
    "dim_date": ["participants"],
    "dim_match": ["matches"],
    "dim_summoner": ["participants", "summoners", "ranked"],
    "dim_champion": ["participants"],
    "dim_team": ["teams"],
    "dim_rank": ["ranked"],
    "fact_participant_performance": ["participants"],
    "fact_team_objectives": ["teams"],
    "fact_rank_snapshot": ["ranked"],
    "fact_timeline_frames": ["timeline_frames"],
    "fact_timeline_events": ["timeline_events"],
    "mart_player_daily_performance": ["participants"],
    "mart_champion_daily_performance": ["participants"],
    "mart_role_daily_performance": ["participants"],
    "mart_rank_daily_summary": ["ranked"],
    "mart_team_objective_daily_summary": ["teams"],
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


def _objective_score() -> Any:
    F = _functions()
    objective_columns = [
        "baron_kills",
        "dragon_kills",
        "rift_herald_kills",
        "tower_kills",
        "inhibitor_kills",
        "champion_kills",
    ]
    return reduce(
        lambda left, right: left + right,
        [F.coalesce(F.col(column), F.lit(0)) for column in objective_columns],
    ).cast("long")


def _team_side() -> Any:
    F = _functions()
    return (
        F.when(F.col("team_id") == F.lit(100), F.lit("BLUE"))
        .when(F.col("team_id") == F.lit(200), F.lit("RED"))
        .otherwise(F.lit("UNKNOWN"))
    )


def _role_bucket(column: str) -> Any:
    F = _functions()
    normalized = F.upper(F.trim(F.col(column)))
    return F.when(normalized.isin(*ROLE_BUCKETS), normalized)


def _normalized_role() -> Any:
    F = _functions()
    return F.coalesce(
        _role_bucket("team_position"),
        _role_bucket("individual_position"),
        _role_bucket("lane"),
        _role_bucket("role"),
        F.lit("UNKNOWN"),
    )


def _rank_order() -> Any:
    F = _functions()
    tier_order = {
        "IRON": 1,
        "BRONZE": 2,
        "SILVER": 3,
        "GOLD": 4,
        "PLATINUM": 5,
        "EMERALD": 6,
        "DIAMOND": 7,
        "MASTER": 8,
        "GRANDMASTER": 9,
        "CHALLENGER": 10,
    }
    division_order = {"IV": 1, "III": 2, "II": 3, "I": 4}
    tier_map = F.create_map(
        *[value for item in tier_order.items() for value in (F.lit(item[0]), F.lit(item[1]))]
    )
    division_map = F.create_map(
        *[value for item in division_order.items() for value in (F.lit(item[0]), F.lit(item[1]))]
    )
    return (
        F.coalesce(tier_map[F.upper(F.col("tier"))], F.lit(0)) * F.lit(10)
        + F.coalesce(division_map[F.upper(F.col("rank"))], F.lit(0))
    ).cast("long")


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


def _union_by_name(dataframes: list[Any]) -> Any:
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), dataframes)


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
        F.avg("cs").cast("double").alias("avg_cs"),
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


def build_dim_date(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    date_frames = [
        dataframe.select("game_date").where(F.col("game_date").isNotNull())
        for dataframe in silver_tables.values()
        if "game_date" in dataframe.columns
    ]
    dates = _union_by_name(date_frames).dropDuplicates(["game_date"])
    return (
        dates.withColumn("_date", F.to_date("game_date"))
        .withColumn("date_key", F.regexp_replace(F.col("game_date"), "-", ""))
        .withColumn("date_year", F.year("_date").cast("long"))
        .withColumn("date_month", F.month("_date").cast("long"))
        .withColumn("date_day", F.dayofmonth("_date").cast("long"))
        .withColumn("day_of_week", F.dayofweek("_date").cast("long"))
        .select(*GOLD_COLUMNS["dim_date"])
    )


def build_dim_match(silver_tables: dict[str, Any]) -> Any:
    return (
        silver_tables["matches"]
        .where("match_id is not null")
        .dropDuplicates(["match_id"])
        .select(*GOLD_COLUMNS["dim_match"])
    )


def build_dim_summoner(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    participants = silver_tables["participants"].select(
        "puuid",
        "summoner_id",
        F.lit(None).alias("account_id"),
        "summoner_name",
        "riot_id_game_name",
        "riot_id_tagline",
        F.lit(None).alias("profile_icon_id"),
        F.lit(None).alias("revision_date"),
        F.lit(None).alias("summoner_level"),
        F.col("game_date").alias("_seen_game_date"),
    )
    summoners = silver_tables["summoners"].select(
        "puuid",
        "summoner_id",
        "account_id",
        F.lit(None).alias("summoner_name"),
        F.lit(None).alias("riot_id_game_name"),
        F.lit(None).alias("riot_id_tagline"),
        "profile_icon_id",
        "revision_date",
        "summoner_level",
        F.col("game_date").alias("_seen_game_date"),
    )
    ranked = silver_tables["ranked"].select(
        "puuid",
        "summoner_id",
        F.lit(None).alias("account_id"),
        F.lit(None).alias("summoner_name"),
        F.lit(None).alias("riot_id_game_name"),
        F.lit(None).alias("riot_id_tagline"),
        F.lit(None).alias("profile_icon_id"),
        F.lit(None).alias("revision_date"),
        F.lit(None).alias("summoner_level"),
        F.col("game_date").alias("_seen_game_date"),
    )
    return (
        _union_by_name([participants, summoners, ranked])
        .where(F.col("puuid").isNotNull())
        .groupBy("puuid")
        .agg(
            F.first("summoner_id", True).alias("summoner_id"),
            F.first("account_id", True).alias("account_id"),
            F.first("summoner_name", True).alias("summoner_name"),
            F.first("riot_id_game_name", True).alias("riot_id_game_name"),
            F.first("riot_id_tagline", True).alias("riot_id_tagline"),
            F.max("profile_icon_id").alias("profile_icon_id"),
            F.max("revision_date").alias("revision_date"),
            F.max("summoner_level").alias("summoner_level"),
            F.min("_seen_game_date").alias("first_seen_game_date"),
            F.max("_seen_game_date").alias("last_seen_game_date"),
        )
        .select(*GOLD_COLUMNS["dim_summoner"])
    )


def build_dim_champion(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    return (
        silver_tables["participants"]
        .where(F.col("champion_id").isNotNull())
        .groupBy("champion_id")
        .agg(
            F.first("champion_name", True).alias("champion_name"),
            F.min("game_date").alias("first_seen_game_date"),
            F.max("game_date").alias("last_seen_game_date"),
        )
        .select(*GOLD_COLUMNS["dim_champion"])
    )


def build_dim_team(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    return (
        silver_tables["teams"]
        .where(F.col("team_id").isNotNull())
        .withColumn("team_side", _team_side())
        .groupBy("team_id", "team_side")
        .agg(
            F.min("game_date").alias("first_seen_game_date"),
            F.max("game_date").alias("last_seen_game_date"),
        )
        .select(*GOLD_COLUMNS["dim_team"])
    )


def build_dim_rank(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    return (
        silver_tables["ranked"]
        .where(F.col("queue").isNotNull() & F.col("tier").isNotNull() & F.col("rank").isNotNull())
        .select("queue", "tier", "rank")
        .dropDuplicates(["queue", "tier", "rank"])
        .withColumn("rank_order", _rank_order())
        .select(*GOLD_COLUMNS["dim_rank"])
    )


def build_fact_participant_performance(silver_tables: dict[str, Any]) -> Any:
    return (
        silver_tables["participants"]
        .withColumn("team_position", _normalized_role())
        .withColumn("cs", _cs_column())
        .select(*GOLD_COLUMNS["fact_participant_performance"])
    )


def build_fact_team_objectives(silver_tables: dict[str, Any]) -> Any:
    return (
        silver_tables["teams"]
        .withColumn("team_side", _team_side())
        .withColumn("objective_score", _objective_score())
        .select(*GOLD_COLUMNS["fact_team_objectives"])
    )


def build_fact_rank_snapshot(silver_tables: dict[str, Any]) -> Any:
    return silver_tables["ranked"].select(*GOLD_COLUMNS["fact_rank_snapshot"])


def build_fact_timeline_frames(silver_tables: dict[str, Any]) -> Any:
    return silver_tables["timeline_frames"].select(*GOLD_COLUMNS["fact_timeline_frames"])


def build_fact_timeline_events(silver_tables: dict[str, Any]) -> Any:
    return silver_tables["timeline_events"].select(*GOLD_COLUMNS["fact_timeline_events"])


def build_mart_player_daily_performance(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    participants = (
        silver_tables["participants"]
        .where(F.col("puuid").isNotNull())
        .withColumn("team_position", _normalized_role())
        .withColumn("cs", _cs_column())
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
        F.avg("cs").cast("double").alias("avg_cs"),
    )
    return _add_loss_and_win_rate(aggregated, "matches_played").select(
        *GOLD_COLUMNS["mart_player_daily_performance"]
    )


def build_mart_champion_daily_performance(silver_tables: dict[str, Any]) -> Any:
    participants = (
        silver_tables["participants"]
        .withColumn("team_position", _normalized_role())
        .withColumn("cs", _cs_column())
    )
    aggregated = _participant_metrics(
        participants,
        group_columns=["game_date", "champion_id", "champion_name"],
        include_totals=True,
    )
    return _add_loss_and_win_rate(aggregated, "matches_played").select(
        *GOLD_COLUMNS["mart_champion_daily_performance"]
    )


def build_mart_role_daily_performance(silver_tables: dict[str, Any]) -> Any:
    participants = (
        silver_tables["participants"]
        .withColumn("team_position", _normalized_role())
        .withColumn("cs", _cs_column())
    )
    aggregated = _participant_metrics(
        participants,
        group_columns=["game_date", "team_position"],
        include_totals=False,
    )
    return _add_loss_and_win_rate(aggregated, "matches_played").select(
        *GOLD_COLUMNS["mart_role_daily_performance"]
    )


def build_mart_rank_daily_summary(silver_tables: dict[str, Any]) -> Any:
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
    ).select(*GOLD_COLUMNS["mart_rank_daily_summary"])


def build_mart_team_objective_daily_summary(silver_tables: dict[str, Any]) -> Any:
    F = _functions()
    teams = silver_tables["teams"].withColumn("team_side", _team_side()).withColumn(
        "objective_score",
        _objective_score(),
    )
    aggregated = teams.groupBy("game_date", "team_id", "team_side").agg(
        F.countDistinct("match_id").cast("long").alias("games_played"),
        _true_count("win").alias("wins"),
        F.avg("baron_kills").cast("double").alias("avg_baron_kills"),
        F.avg("dragon_kills").cast("double").alias("avg_dragon_kills"),
        F.avg("rift_herald_kills").cast("double").alias("avg_rift_herald_kills"),
        F.avg("tower_kills").cast("double").alias("avg_tower_kills"),
        F.avg("inhibitor_kills").cast("double").alias("avg_inhibitor_kills"),
        F.avg("champion_kills").cast("double").alias("avg_champion_kills"),
        F.avg("objective_score").cast("double").alias("avg_objective_score"),
    )
    return _add_loss_and_win_rate(aggregated, "games_played").select(
        *GOLD_COLUMNS["mart_team_objective_daily_summary"]
    )


def build_player_metrics(silver_tables: dict[str, Any]) -> Any:
    return build_mart_player_daily_performance(silver_tables)


def build_champion_metrics(silver_tables: dict[str, Any]) -> Any:
    return build_mart_champion_daily_performance(silver_tables)


def build_role_metrics(silver_tables: dict[str, Any]) -> Any:
    return build_mart_role_daily_performance(silver_tables)


def build_rank_metrics(silver_tables: dict[str, Any]) -> Any:
    return build_mart_rank_daily_summary(silver_tables)


def build_team_objective_metrics(silver_tables: dict[str, Any]) -> Any:
    return build_mart_team_objective_daily_summary(silver_tables)


AGGREGATION_BUILDERS: dict[str, GoldAggregation] = {
    "dim_date": build_dim_date,
    "dim_match": build_dim_match,
    "dim_summoner": build_dim_summoner,
    "dim_champion": build_dim_champion,
    "dim_team": build_dim_team,
    "dim_rank": build_dim_rank,
    "fact_participant_performance": build_fact_participant_performance,
    "fact_team_objectives": build_fact_team_objectives,
    "fact_rank_snapshot": build_fact_rank_snapshot,
    "fact_timeline_frames": build_fact_timeline_frames,
    "fact_timeline_events": build_fact_timeline_events,
    "mart_player_daily_performance": build_mart_player_daily_performance,
    "mart_champion_daily_performance": build_mart_champion_daily_performance,
    "mart_role_daily_performance": build_mart_role_daily_performance,
    "mart_rank_daily_summary": build_mart_rank_daily_summary,
    "mart_team_objective_daily_summary": build_mart_team_objective_daily_summary,
}
