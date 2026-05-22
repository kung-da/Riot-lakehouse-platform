from __future__ import annotations

import json
from typing import Any

from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark
from lakehouse.silver.clean_matches import clean_match
from lakehouse.silver.clean_participants import clean_participants
from lakehouse.silver.clean_ranked import clean_ranked
from lakehouse.silver.clean_summoners import clean_summoner
from lakehouse.silver.clean_teams import clean_teams
from lakehouse.silver.clean_timeline_events import clean_timeline_events
from lakehouse.silver.clean_timeline_frames import clean_timeline_frames


LOGGER = get_logger(__name__)

SILVER_TABLES = [
    "matches",
    "participants",
    "teams",
    "summoners",
    "ranked",
    "timeline_frames",
    "timeline_events",
]

TABLE_DATASETS = {
    "matches": ["matches"],
    "participants": ["matches"],
    "teams": ["matches"],
    "summoners": ["summoners"],
    "ranked": ["ranked"],
    "timeline_frames": ["timelines"],
    "timeline_events": ["timelines"],
}


def transform_payload(dataset: str, payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    if dataset == "matches":
        return {
            "matches": [clean_match(payload)],
            "participants": clean_participants(payload),
            "teams": clean_teams(payload),
        }
    if dataset == "summoners":
        return {"summoners": [clean_summoner(payload)]}
    if dataset == "ranked":
        return {"ranked": clean_ranked(payload)}
    if dataset == "timelines":
        return {
            "timeline_frames": clean_timeline_frames(payload),
            "timeline_events": clean_timeline_events(payload),
        }
    return {}


def _table_fields() -> dict[str, list[str]]:
    return {
        "matches": [
            "match_id",
            "game_id",
            "platform_id",
            "queue_id",
            "game_mode",
            "game_type",
            "game_version",
            "game_creation",
            "game_start_timestamp",
            "game_end_timestamp",
            "game_duration",
            "participant_count",
        ],
        "participants": [
            "match_id",
            "puuid",
            "summoner_id",
            "riot_id_game_name",
            "champion_id",
            "champion_name",
            "team_id",
            "team_position",
            "lane",
            "role",
            "win",
            "kills",
            "deaths",
            "assists",
            "gold_earned",
            "total_damage_dealt_to_champions",
            "vision_score",
            "total_minions_killed",
        ],
        "teams": [
            "match_id",
            "team_id",
            "win",
            "baron_kills",
            "dragon_kills",
            "rift_herald_kills",
            "tower_kills",
            "inhibitor_kills",
        ],
        "summoners": [
            "puuid",
            "profile_icon_id",
            "revision_date",
            "summoner_level",
        ],
        "ranked": [
            "league_id",
            "queue",
            "tier",
            "rank",
            "puuid",
            "league_points",
            "wins",
            "losses",
            "hot_streak",
            "veteran",
            "fresh_blood",
            "inactive",
        ],
        "timeline_frames": [
            "match_id",
            "frame_index",
            "timestamp",
            "participant_frame_count",
            "event_count",
        ],
        "timeline_events": [
            "match_id",
            "frame_index",
            "event_index",
            "timestamp",
            "type",
            "participant_id",
            "killer_id",
            "victim_id",
            "team_id",
            "monster_type",
            "building_type",
        ],
    }


def _silver_schema(table: str) -> Any:
    from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

    boolean_columns = {"win", "hot_streak", "veteran", "fresh_blood", "inactive"}
    string_columns = {
        "match_id",
        "platform_id",
        "game_mode",
        "game_type",
        "game_version",
        "puuid",
        "summoner_id",
        "riot_id_game_name",
        "champion_name",
        "team_position",
        "lane",
        "role",
        "league_id",
        "queue",
        "tier",
        "rank",
        "type",
        "monster_type",
        "building_type",
    }
    fields = []
    for field in _table_fields()[table]:
        if field in boolean_columns:
            field_type = BooleanType()
        elif field in string_columns:
            field_type = StringType()
        else:
            field_type = LongType()
        fields.append(StructField(field, field_type, nullable=True))
    return StructType(fields)


def _spark_path(path: Any) -> str:
    return path.as_posix()


def _has_parquet_files(path: Any) -> bool:
    path_text = path.as_posix()
    if path_text.startswith("s3:/"):
        return True
    return path.exists() and any(path.rglob("*.parquet"))


def _selected_tables(tables: list[str] | None) -> list[str]:
    selected = tables or SILVER_TABLES
    unknown = sorted(set(selected) - set(SILVER_TABLES))
    if unknown:
        raise ValueError(f"Unknown silver tables: {', '.join(unknown)}")
    return selected


def _selected_datasets(datasets: list[str] | None) -> list[str] | None:
    if datasets is None:
        return None
    return sorted(set(datasets))


def _rows_for_table(table: str, dataset_filter: list[str] | None = None) -> Any:
    fields = _table_fields()[table]
    table_datasets = set(TABLE_DATASETS[table])
    allowed_datasets = table_datasets
    if dataset_filter is not None:
        allowed_datasets = table_datasets.intersection(dataset_filter)

    def parse_partition(records: Any) -> Any:
        for record in records:
            dataset = record["dataset"]
            if dataset not in allowed_datasets:
                continue
            try:
                payload = json.loads(record["payload_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                source_file = getattr(record, "source_file", "<unknown>")
                LOGGER.warning("Skipping invalid Bronze payload %s: %s", source_file, exc)
                continue
            if not isinstance(payload, dict):
                continue
            for row in transform_payload(dataset, payload).get(table, []):
                yield tuple(row.get(field) for field in fields)

    return parse_partition


def _silver_write_mode(config: Any, write_mode: str | None) -> str:
    if write_mode:
        return write_mode
    silver_config = config.values.get("silver", {}) if hasattr(config, "values") else {}
    return str(silver_config.get("write_mode", "overwrite"))


def _silver_output_partitions(config: Any) -> int:
    silver_config = config.values.get("silver", {}) if hasattr(config, "values") else {}
    return int(silver_config.get("output_partitions", 1))


def _silver_partition_columns(config: Any) -> list[str]:
    partition_config = config.values.get("partition_columns", {}) if hasattr(config, "values") else {}
    columns = partition_config.get("silver", [])
    return [str(column) for column in columns]


def _write_table(
    dataframe: Any,
    output_path: Any,
    mode: str,
    partition_columns: list[str],
    output_partitions: int,
) -> None:
    if output_partitions < 1:
        raise ValueError("silver.output_partitions must be greater than zero")

    dataframe = dataframe.coalesce(output_partitions)
    writer = dataframe.write.mode(mode).option("compression", "snappy")
    available_partitions = [column for column in partition_columns if column in dataframe.columns]
    if available_partitions:
        writer = writer.partitionBy(*available_partitions)
    writer.parquet(_spark_path(output_path))


def run_silver_transform(
    config: Any,
    datasets: list[str] | None = None,
    tables: list[str] | None = None,
    write_mode: str | None = None,
) -> dict[str, int]:
    bronze_path = config.layer_path("bronze", "raw_json")
    selected_tables = _selected_tables(tables)
    selected_datasets = _selected_datasets(datasets)
    counts = {table: 0 for table in selected_tables}

    if not _has_parquet_files(bronze_path):
        LOGGER.warning("Bronze input path has no Parquet files: %s", bronze_path)
        return counts

    spark = get_spark(config=config)
    mode = _silver_write_mode(config, write_mode)
    partition_columns = _silver_partition_columns(config)
    output_partitions = _silver_output_partitions(config)
    try:
        bronze = spark.read.parquet(_spark_path(bronze_path)).select(
            "dataset",
            "source_file",
            "payload_json",
        )
        if selected_datasets is not None:
            bronze = bronze.where(bronze.dataset.isin(selected_datasets))

        for table in selected_tables:
            table_datasets = TABLE_DATASETS[table]
            if selected_datasets is not None and not set(table_datasets).intersection(selected_datasets):
                continue

            table_bronze = bronze.where(bronze.dataset.isin(table_datasets))
            rows = table_bronze.rdd.mapPartitions(_rows_for_table(table, selected_datasets))
            dataframe = spark.createDataFrame(rows, schema=_silver_schema(table)).cache()
            row_count = dataframe.count()
            output_path = config.layer_path("silver", table)
            try:
                _write_table(
                    dataframe=dataframe,
                    output_path=output_path,
                    mode=mode,
                    partition_columns=partition_columns,
                    output_partitions=output_partitions,
                )
            finally:
                dataframe.unpersist()
            counts[table] = row_count
            LOGGER.info("Silver table %s wrote %s rows to %s", table, row_count, output_path)
        return counts
    finally:
        spark.stop()
