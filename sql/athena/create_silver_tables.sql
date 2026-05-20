CREATE DATABASE IF NOT EXISTS riot_lakehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_matches (
  match_id string,
  game_id string,
  platform_id string,
  queue_id string,
  game_mode string,
  game_type string,
  game_version string,
  game_creation string,
  game_start_timestamp string,
  game_end_timestamp string,
  game_duration string
)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_participants (
  match_id string,
  puuid string,
  champion_id string,
  champion_name string,
  team_id string,
  team_position string,
  win string,
  kills string,
  deaths string,
  assists string
)
STORED AS PARQUET;
