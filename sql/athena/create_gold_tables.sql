CREATE DATABASE IF NOT EXISTS riot_lakehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_date (
  date_key string,
  game_date string,
  date_year bigint,
  date_month bigint,
  date_day bigint,
  day_of_week bigint
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_match (
  match_id string,
  game_id bigint,
  platform_id string,
  queue_id bigint,
  game_mode string,
  game_type string,
  game_version string,
  game_creation bigint,
  game_start_timestamp bigint,
  game_end_timestamp bigint,
  game_duration bigint,
  participant_count bigint,
  game_date string
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_summoner (
  puuid string,
  summoner_id string,
  account_id string,
  summoner_name string,
  riot_id_game_name string,
  riot_id_tagline string,
  profile_icon_id bigint,
  revision_date bigint,
  summoner_level bigint,
  first_seen_game_date string,
  last_seen_game_date string
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_champion (
  champion_id bigint,
  champion_name string,
  first_seen_game_date string,
  last_seen_game_date string
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_team (
  team_id bigint,
  team_side string,
  first_seen_game_date string,
  last_seen_game_date string
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_rank (
  queue string,
  tier string,
  rank string,
  rank_order bigint
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_participant_performance (
  game_date string,
  match_id string,
  participant_id bigint,
  puuid string,
  summoner_id string,
  champion_id bigint,
  team_id bigint,
  team_position string,
  win boolean,
  kills bigint,
  deaths bigint,
  assists bigint,
  kda double,
  gold_earned bigint,
  total_damage_dealt_to_champions bigint,
  total_damage_taken bigint,
  vision_score bigint,
  total_minions_killed bigint,
  neutral_minions_killed bigint,
  cs bigint
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_team_objectives (
  game_date string,
  match_id string,
  team_id bigint,
  team_side string,
  win boolean,
  baron_kills bigint,
  dragon_kills bigint,
  rift_herald_kills bigint,
  tower_kills bigint,
  inhibitor_kills bigint,
  champion_kills bigint,
  objective_score bigint
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_rank_snapshot (
  game_date string,
  puuid string,
  summoner_id string,
  queue string,
  tier string,
  rank string,
  league_points bigint,
  wins bigint,
  losses bigint,
  win_rate double,
  hot_streak boolean,
  veteran boolean,
  fresh_blood boolean,
  inactive boolean
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_timeline_frames (
  game_date string,
  match_id string,
  frame_index bigint,
  frame_timestamp bigint,
  participant_frame_count bigint,
  event_count bigint
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_timeline_events (
  game_date string,
  match_id string,
  frame_index bigint,
  event_index bigint,
  event_timestamp bigint,
  event_type string,
  participant_id bigint,
  killer_id bigint,
  victim_id bigint,
  team_id bigint,
  monster_type string,
  building_type string,
  lane_type string
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_player_daily_performance (
  game_date string,
  puuid string,
  summoner_id string,
  summoner_name string,
  riot_id_game_name string,
  riot_id_tagline string,
  matches_played bigint,
  wins bigint,
  losses bigint,
  win_rate double,
  unique_champions bigint,
  total_kills bigint,
  total_deaths bigint,
  total_assists bigint,
  avg_kills double,
  avg_deaths double,
  avg_assists double,
  avg_kda double,
  avg_gold_earned double,
  avg_damage_dealt_to_champions double,
  avg_damage_taken double,
  avg_vision_score double,
  avg_cs double
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_champion_daily_performance (
  game_date string,
  champion_id bigint,
  champion_name string,
  matches_played bigint,
  unique_players bigint,
  wins bigint,
  losses bigint,
  win_rate double,
  total_kills bigint,
  total_deaths bigint,
  total_assists bigint,
  avg_kills double,
  avg_deaths double,
  avg_assists double,
  avg_kda double,
  avg_gold_earned double,
  avg_damage_dealt_to_champions double,
  avg_damage_taken double,
  avg_vision_score double,
  avg_cs double
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_role_daily_performance (
  game_date string,
  team_position string,
  matches_played bigint,
  unique_players bigint,
  unique_champions bigint,
  wins bigint,
  losses bigint,
  win_rate double,
  avg_kills double,
  avg_deaths double,
  avg_assists double,
  avg_kda double,
  avg_gold_earned double,
  avg_damage_dealt_to_champions double,
  avg_damage_taken double,
  avg_vision_score double,
  avg_cs double
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_rank_daily_summary (
  game_date string,
  queue string,
  tier string,
  rank string,
  players bigint,
  avg_league_points double,
  total_wins bigint,
  total_losses bigint,
  avg_win_rate double,
  hot_streak_players bigint,
  veteran_players bigint,
  fresh_blood_players bigint,
  inactive_players bigint
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_team_objective_daily_summary (
  game_date string,
  team_id bigint,
  team_side string,
  games_played bigint,
  wins bigint,
  losses bigint,
  win_rate double,
  avg_baron_kills double,
  avg_dragon_kills double,
  avg_rift_herald_kills double,
  avg_tower_kills double,
  avg_inhibitor_kills double,
  avg_champion_kills double,
  avg_objective_score double
)
STORED AS PARQUET;
