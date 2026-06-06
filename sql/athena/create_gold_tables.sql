CREATE DATABASE IF NOT EXISTS riot_lakehouse;

-- Replace <bucket>/<lakehouse-prefix> with the S3 location used by configs/prod.yaml.
-- Athena infers Delta schemas and partitions from each table's _delta_log.

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_date
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/dim_date/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_match
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/dim_match/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_summoner
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/dim_summoner/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_champion
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/dim_champion/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_team
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/dim_team/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_dim_rank
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/dim_rank/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_participant_performance
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/fact_participant_performance/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_team_objectives
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/fact_team_objectives/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_rank_snapshot
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/fact_rank_snapshot/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_timeline_frames
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/fact_timeline_frames/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_fact_timeline_events
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/fact_timeline_events/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_player_daily_performance
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/mart_player_daily_performance/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_champion_daily_performance
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/mart_champion_daily_performance/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_role_daily_performance
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/mart_role_daily_performance/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_rank_daily_summary
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/mart_rank_daily_summary/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.gold_mart_team_objective_daily_summary
LOCATION 's3://<bucket>/<lakehouse-prefix>/gold/mart_team_objective_daily_summary/'
TBLPROPERTIES ('table_type' = 'DELTA');
