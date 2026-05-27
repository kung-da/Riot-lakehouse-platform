def gold_sql_registry() -> dict[str, str]:
    return {
        "dim_date": "select distinct game_date from participants where game_date is not null",
        "dim_match": "select distinct * from matches",
        "dim_summoner": (
            "select distinct puuid, summoner_id from participants where puuid is not null"
        ),
        "dim_champion": "select distinct champion_id, champion_name from participants",
        "dim_team": "select distinct team_id from teams",
        "dim_rank": "select distinct queue, tier, rank from ranked",
        "fact_participant_performance": "select * from participants",
        "fact_team_objectives": "select * from teams",
        "fact_rank_snapshot": "select * from ranked",
        "fact_timeline_frames": "select * from timeline_frames",
        "fact_timeline_events": "select * from timeline_events",
        "mart_player_daily_performance": (
            "select game_date, puuid, count(distinct match_id) as matches_played "
            "from participants group by game_date, puuid"
        ),
        "mart_champion_daily_performance": (
            "select game_date, champion_id, champion_name, count(*) as matches_played "
            "from participants group by game_date, champion_id, champion_name"
        ),
        "mart_role_daily_performance": (
            "select game_date, team_position, count(*) as matches_played "
            "from participants group by game_date, team_position"
        ),
        "mart_rank_daily_summary": (
            "select game_date, queue, tier, rank, count(*) as players "
            "from ranked group by game_date, queue, tier, rank"
        ),
        "mart_team_objective_daily_summary": (
            "select game_date, team_id, count(distinct match_id) as games_played "
            "from teams group by game_date, team_id"
        ),
    }
