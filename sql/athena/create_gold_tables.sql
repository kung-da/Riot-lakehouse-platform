CREATE DATABASE IF NOT EXISTS riot_lakehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_player_metrics (
  puuid string,
  matches bigint,
  avg_kills double
)
STORED AS PARQUET;
