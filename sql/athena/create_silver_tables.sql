CREATE DATABASE IF NOT EXISTS riot_lakehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_matches (
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
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_participants (
  match_id string,
  participant_id bigint,
  puuid string,
  summoner_id string,
  riot_id_game_name string,
  riot_id_tagline string,
  summoner_name string,
  champion_id bigint,
  champion_name string,
  team_id bigint,
  team_position string,
  individual_position string,
  lane string,
  role string,
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
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_teams (
  match_id string,
  team_id bigint,
  win boolean,
  baron_kills bigint,
  dragon_kills bigint,
  rift_herald_kills bigint,
  tower_kills bigint,
  inhibitor_kills bigint,
  champion_kills bigint,
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_summoners (
  puuid string,
  summoner_id string,
  account_id string,
  profile_icon_id bigint,
  revision_date bigint,
  summoner_level bigint,
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_ranked (
  league_id string,
  queue string,
  tier string,
  rank string,
  summoner_id string,
  puuid string,
  league_points bigint,
  wins bigint,
  losses bigint,
  win_rate double,
  hot_streak boolean,
  veteran boolean,
  fresh_blood boolean,
  inactive boolean,
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_timeline_frames (
  match_id string,
  frame_index bigint,
  frame_timestamp bigint,
  participant_frame_count bigint,
  event_count bigint,
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.silver_timeline_events (
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
  lane_type string,
  source_file string,
  file_hash string,
  ingest_ts string,
  ingest_date string
)
PARTITIONED BY (dataset string, game_date string)
STORED AS PARQUET;
