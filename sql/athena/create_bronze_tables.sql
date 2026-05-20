CREATE DATABASE IF NOT EXISTS riot_lakehouse;

CREATE EXTERNAL TABLE IF NOT EXISTS riot_lakehouse.bronze_raw_json (
  source_file string,
  file_hash string,
  ingest_ts string,
  payload_json string
)
PARTITIONED BY (dataset string, ingest_date string)
STORED AS PARQUET;
