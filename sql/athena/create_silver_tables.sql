CREATE DATABASE IF NOT EXISTS riot_lakehouse;

-- Replace <bucket>/<lakehouse-prefix> with the S3 location used by configs/prod.yaml.
-- Athena infers Delta schemas and partitions from each table's _delta_log.

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_matches
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/matches/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_participants
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/participants/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_teams
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/teams/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_summoners
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/summoners/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_ranked
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/ranked/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_timeline_frames
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/timeline_frames/'
TBLPROPERTIES ('table_type' = 'DELTA');

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_timeline_events
LOCATION 's3://<bucket>/<lakehouse-prefix>/silver/timeline_events/'
TBLPROPERTIES ('table_type' = 'DELTA');
