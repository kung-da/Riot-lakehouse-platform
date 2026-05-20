CREATE DATABASE IF NOT EXISTS riot_lakehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.platinum_match_win_features (
  match_id string,
  team_id string,
  dragon_kills double,
  tower_kills double,
  label boolean
)
STORED AS PARQUET;
