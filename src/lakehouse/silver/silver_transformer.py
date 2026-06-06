from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from lakehouse.common.logging import get_logger
from lakehouse.common.spark import get_spark
from lakehouse.common.storage import (
    has_files,
    is_s3_path,
    layer_table_format,
    to_spark_path,
    write_table_dataset,
)
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

SILVER_DEDUP_KEYS = {
    "matches": ["match_id"],
    "participants": ["match_id", "participant_id"],
    "teams": ["match_id", "team_id"],
    "summoners": ["puuid"],
    "ranked": ["queue", "tier", "rank", "summoner_id", "puuid"],
    "timeline_frames": ["match_id", "frame_index"],
    "timeline_events": ["match_id", "frame_index", "event_index"],
}

VALID_DATASETS = sorted({dataset for datasets in TABLE_DATASETS.values() for dataset in datasets})

LINEAGE_FIELDS = [
    "source_file",
    "file_hash",
    "ingest_ts",
    "ingest_date",
    "dataset",
    "game_date",
]

TABLE_FIELDS = {
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
        *LINEAGE_FIELDS,
    ],
    "participants": [
        "match_id",
        "participant_id",
        "puuid",
        "summoner_id",
        "riot_id_game_name",
        "riot_id_tagline",
        "summoner_name",
        "champion_id",
        "champion_name",
        "team_id",
        "team_position",
        "individual_position",
        "lane",
        "role",
        "win",
        "kills",
        "deaths",
        "assists",
        "kda",
        "gold_earned",
        "total_damage_dealt_to_champions",
        "total_damage_taken",
        "vision_score",
        "total_minions_killed",
        "neutral_minions_killed",
        *LINEAGE_FIELDS,
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
        "champion_kills",
        *LINEAGE_FIELDS,
    ],
    "summoners": [
        "puuid",
        "summoner_id",
        "account_id",
        "profile_icon_id",
        "revision_date",
        "summoner_level",
        *LINEAGE_FIELDS,
    ],
    "ranked": [
        "league_id",
        "queue",
        "tier",
        "rank",
        "summoner_id",
        "puuid",
        "league_points",
        "wins",
        "losses",
        "win_rate",
        "hot_streak",
        "veteran",
        "fresh_blood",
        "inactive",
        *LINEAGE_FIELDS,
    ],
    "timeline_frames": [
        "match_id",
        "frame_index",
        "frame_timestamp",
        "participant_frame_count",
        "event_count",
        *LINEAGE_FIELDS,
    ],
    "timeline_events": [
        "match_id",
        "frame_index",
        "event_index",
        "event_timestamp",
        "event_type",
        "participant_id",
        "killer_id",
        "victim_id",
        "team_id",
        "monster_type",
        "building_type",
        "lane_type",
        *LINEAGE_FIELDS,
    ],
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
    return TABLE_FIELDS


def _silver_schema(table: str) -> Any:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    boolean_columns = {"win", "hot_streak", "veteran", "fresh_blood", "inactive"}
    double_columns = {"kda", "win_rate"}
    string_columns = {
        "match_id",
        "platform_id",
        "game_mode",
        "game_type",
        "game_version",
        "puuid",
        "summoner_id",
        "account_id",
        "riot_id_game_name",
        "riot_id_tagline",
        "summoner_name",
        "champion_name",
        "team_position",
        "individual_position",
        "lane",
        "role",
        "league_id",
        "queue",
        "tier",
        "rank",
        "event_type",
        "monster_type",
        "building_type",
        "lane_type",
        "source_file",
        "file_hash",
        "ingest_ts",
        "ingest_date",
        "dataset",
        "game_date",
    }
    fields = []
    for field in _table_fields()[table]:
        if field in boolean_columns:
            field_type = BooleanType()
        elif field in double_columns:
            field_type = DoubleType()
        elif field in string_columns:
            field_type = StringType()
        else:
            field_type = LongType()
        fields.append(StructField(field, field_type, nullable=True))
    return StructType(fields)


def _spark_path(path: Any) -> str:
    return to_spark_path(path)


def _has_parquet_files(path: Any) -> bool:
    return has_files(path, "*.parquet")


def _has_raw_json_files(path: Any, datasets: list[str] | None = None) -> bool:
    selected = datasets or VALID_DATASETS
    return any(has_files(path / dataset, "*.json") for dataset in selected)


def _selected_tables(tables: list[str] | None) -> list[str]:
    selected = tables or SILVER_TABLES
    unknown = sorted(set(selected) - set(SILVER_TABLES))
    if unknown:
        raise ValueError(f"Unknown silver tables: {', '.join(unknown)}")
    return selected


def _selected_datasets(datasets: list[str] | None) -> list[str] | None:
    if datasets is None:
        return None
    selected = sorted(set(datasets))
    return selected or None


def _record_value(record: Mapping[str, Any] | Any, field: str) -> Any:
    if isinstance(record, Mapping):
        return record.get(field)
    try:
        return record[field]
    except (KeyError, TypeError, ValueError):
        return getattr(record, field, None)


def _to_string(value: Any) -> str | None:
    return None if value is None else str(value)


def derive_game_date_from_ms(timestamp_ms: Any) -> str | None:
    if timestamp_ms is None:
        return None
    try:
        timestamp = int(timestamp_ms)
        if timestamp <= 0:
            return None
        return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).date().isoformat()
    except (OSError, OverflowError, TypeError, ValueError):
        return None


def _game_date_for_payload(
    dataset: str, payload: dict[str, Any], ingest_date: str | None
) -> str | None:
    if dataset in {"summoners", "ranked"}:
        return ingest_date

    info = payload.get("info") or {}
    if dataset == "matches":
        return (
            derive_game_date_from_ms(info.get("gameCreation"))
            or derive_game_date_from_ms(info.get("gameStartTimestamp"))
            or ingest_date
        )
    if dataset == "timelines":
        return (
            derive_game_date_from_ms(info.get("gameCreation"))
            or derive_game_date_from_ms(info.get("gameStartTimestamp"))
            or ingest_date
        )
    return ingest_date


def _lineage_metadata(
    record: Mapping[str, Any] | Any,
    payload: dict[str, Any],
    dataset: str,
) -> dict[str, Any]:
    ingest_date = _to_string(_record_value(record, "ingest_date"))
    return {
        "source_file": _to_string(_record_value(record, "source_file")),
        "file_hash": _to_string(_record_value(record, "file_hash")),
        "ingest_ts": _to_string(_record_value(record, "ingest_ts")),
        "ingest_date": ingest_date,
        "dataset": _to_string(dataset),
        "game_date": _game_date_for_payload(dataset, payload, ingest_date),
    }


def transform_bronze_record(record: Mapping[str, Any] | Any) -> dict[str, list[dict[str, Any]]]:
    dataset = _record_value(record, "dataset")
    if dataset not in VALID_DATASETS:
        return {}

    try:
        payload = json.loads(_record_value(record, "payload_json"))
    except (TypeError, json.JSONDecodeError) as exc:
        source_file = _record_value(record, "source_file") or "<unknown>"
        LOGGER.warning("Skipping invalid Bronze payload %s: %s", source_file, exc)
        return {}
    if not isinstance(payload, dict):
        return {}

    metadata = _lineage_metadata(record, payload, dataset)
    transformed = transform_payload(dataset, payload)
    return {
        table: [{**row, **metadata} for row in rows]
        for table, rows in transformed.items()
        if table in SILVER_TABLES
    }


def _rows_for_table(table: str, dataset_filter: list[str] | None = None) -> Any:
    fields = _table_fields()[table]
    table_datasets = set(TABLE_DATASETS[table])
    allowed_datasets = table_datasets
    if dataset_filter is not None:
        allowed_datasets = table_datasets.intersection(dataset_filter)

    def parse_partition(records: Any) -> Any:
        for record in records:
            dataset = _record_value(record, "dataset")
            if dataset not in allowed_datasets:
                continue
            for row in transform_bronze_record(record).get(table, []):
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


def _silver_cache_bronze(config: Any) -> bool:
    silver_config = config.values.get("silver", {}) if hasattr(config, "values") else {}
    return bool(silver_config.get("cache_bronze", True))


def _silver_cache_storage_level(config: Any) -> str:
    silver_config = config.values.get("silver", {}) if hasattr(config, "values") else {}
    return str(silver_config.get("cache_storage_level", "MEMORY_AND_DISK")).upper()


def _silver_source(config: Any) -> str:
    silver_config = config.values.get("silver", {}) if hasattr(config, "values") else {}
    return str(silver_config.get("source", "bronze")).lower()


def _silver_raw_input_partitions(config: Any) -> int:
    silver_config = config.values.get("silver", {}) if hasattr(config, "values") else {}
    return int(silver_config.get("raw_input_partitions", 256))


def _silver_partition_columns(config: Any) -> list[str]:
    partition_config = (
        config.values.get("partition_columns", {}) if hasattr(config, "values") else {}
    )
    columns = partition_config.get("silver", [])
    return [str(column) for column in columns]


def _write_table(
    dataframe: Any,
    output_path: Any,
    mode: str,
    partition_columns: list[str],
    output_partitions: int,
    table_format: str,
) -> None:
    if output_partitions < 1:
        raise ValueError("silver.output_partitions must be greater than zero")

    available_partitions = [column for column in partition_columns if column in dataframe.columns]
    write_table_dataset(
        dataframe=dataframe,
        output_path=output_path,
        mode=mode,
        partition_columns=available_partitions,
        output_partitions=output_partitions,
        table_format=table_format,
    )


def _dedupe_table(dataframe: Any, table: str) -> Any:
    key_columns = SILVER_DEDUP_KEYS.get(table)
    if not key_columns:
        return dataframe
    available_keys = [column for column in key_columns if column in dataframe.columns]
    if len(available_keys) != len(key_columns):
        return dataframe
    return dataframe.dropDuplicates(available_keys)


def _plain_file_path(path: str) -> str:
    parsed = urlsplit(path)
    if parsed.scheme == "file":
        return unquote(parsed.path)
    return path


def _source_file_for_raw_path(raw_root: Any, path: str) -> str:
    plain_path = _plain_file_path(path).replace("\\", "/")
    raw_root_text = str(raw_root.as_posix() if hasattr(raw_root, "as_posix") else raw_root)
    raw_parent = os.path.dirname(raw_root_text.replace("\\", "/").rstrip("/"))
    if raw_parent and plain_path.startswith(raw_parent.rstrip("/") + "/"):
        return plain_path.removeprefix(raw_parent.rstrip("/") + "/")
    return plain_path


def _raw_ingest_time(path: str) -> datetime:
    plain_path = _plain_file_path(path)
    try:
        return datetime.fromtimestamp(os.path.getmtime(plain_path), tz=timezone.utc)
    except OSError:
        return datetime.now(timezone.utc)


def _raw_pair_to_bronze_row(
    item: tuple[str, str],
    dataset: str,
    raw_root: Any,
) -> tuple[str, str, str, str, str, str]:
    path, payload_json = item
    payload_bytes = payload_json.encode("utf-8")
    ingest_time = _raw_ingest_time(path)
    return (
        dataset,
        _source_file_for_raw_path(raw_root, path),
        hashlib.sha256(payload_bytes).hexdigest(),
        ingest_time.isoformat(),
        ingest_time.date().isoformat(),
        payload_json,
    )


def _local_raw_path_to_bronze_row(
    item: tuple[str, str, str],
) -> tuple[str, str, str, str, str, str] | None:
    dataset, path_text, raw_root_text = item
    path = Path(path_text)
    try:
        raw_bytes = path.read_bytes()
        payload_json = raw_bytes.decode("utf-8")
        ingest_time = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except (OSError, UnicodeDecodeError) as exc:
        LOGGER.warning("Skipping invalid raw JSON file %s: %s", path, exc)
        return None
    return (
        dataset,
        _source_file_for_raw_path(raw_root_text, path.as_posix()),
        hashlib.sha256(raw_bytes).hexdigest(),
        ingest_time.isoformat(),
        ingest_time.date().isoformat(),
        payload_json,
    )


def _bronze_schema() -> Any:
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            StructField("dataset", StringType(), nullable=True),
            StructField("source_file", StringType(), nullable=True),
            StructField("file_hash", StringType(), nullable=True),
            StructField("ingest_ts", StringType(), nullable=True),
            StructField("ingest_date", StringType(), nullable=True),
            StructField("payload_json", StringType(), nullable=True),
        ]
    )


def _read_raw_as_bronze(
    spark: Any,
    config: Any,
    selected_datasets: list[str] | None,
) -> Any:
    datasets = selected_datasets or VALID_DATASETS
    raw_root = config.raw_root
    min_partitions = _silver_raw_input_partitions(config)
    if not is_s3_path(raw_root):
        raw_root_path = Path(raw_root)
        raw_items: list[tuple[str, str, str]] = []
        for dataset in datasets:
            raw_dataset_path = raw_root_path / dataset
            if raw_dataset_path.exists():
                raw_items.extend(
                    (dataset, path.as_posix(), raw_root_path.as_posix())
                    for path in raw_dataset_path.rglob("*.json")
                )
        LOGGER.info("Reading %s local raw JSON files for Silver", len(raw_items))
        raw_rdd = spark.sparkContext.parallelize(raw_items, min_partitions)
        rows = raw_rdd.map(_local_raw_path_to_bronze_row).filter(lambda row: row is not None)
        return spark.createDataFrame(rows, schema=_bronze_schema())

    raw_rdds = []
    for dataset in datasets:
        raw_dataset_path = raw_root / dataset
        if not has_files(raw_dataset_path, "*.json"):
            continue
        path = _spark_path(raw_dataset_path / "*.json")
        raw_rdds.append(
            spark.sparkContext.wholeTextFiles(path, min_partitions).map(
                lambda item, dataset=dataset, raw_root=raw_root: _raw_pair_to_bronze_row(
                    item,
                    dataset,
                    raw_root,
                )
            )
        )
    if not raw_rdds:
        return spark.createDataFrame([], schema=_bronze_schema())
    return spark.createDataFrame(spark.sparkContext.union(raw_rdds), schema=_bronze_schema())


def _storage_level_from_name(name: str) -> Any:
    from pyspark import StorageLevel

    levels = {
        "DISK_ONLY": StorageLevel.DISK_ONLY,
        "MEMORY_ONLY": StorageLevel.MEMORY_ONLY,
        "MEMORY_AND_DISK": StorageLevel.MEMORY_AND_DISK,
        "MEMORY_AND_DISK_DESER": StorageLevel.MEMORY_AND_DISK_DESER,
    }
    try:
        return levels[name]
    except KeyError as exc:
        valid = ", ".join(sorted(levels))
        raise ValueError(f"Unknown silver.cache_storage_level {name!r}. Use one of: {valid}") from exc


def run_silver_transform(
    config: Any,
    datasets: list[str] | None = None,
    tables: list[str] | None = None,
    write_mode: str | None = None,
) -> dict[str, int]:
    bronze_path = config.layer_path("bronze", "raw_json")
    selected_tables = _selected_tables(tables)
    selected_datasets = _selected_datasets(datasets)
    source = _silver_source(config)
    counts = {table: 0 for table in selected_tables}

    if source not in {"bronze", "raw"}:
        raise ValueError("silver.source must be 'bronze' or 'raw'")
    if source == "bronze" and not _has_parquet_files(bronze_path):
        LOGGER.warning("Bronze input path has no Parquet files: %s", bronze_path)
        return counts
    if source == "raw" and not _has_raw_json_files(config.raw_root, selected_datasets):
        LOGGER.warning("Raw input path has no JSON files: %s", config.raw_root)
        return counts

    table_format = layer_table_format(config, "silver")
    spark = get_spark(config=config, enable_delta=table_format == "delta")
    mode = _silver_write_mode(config, write_mode)
    partition_columns = _silver_partition_columns(config)
    output_partitions = _silver_output_partitions(config)
    cache_bronze = _silver_cache_bronze(config)
    storage_level = _storage_level_from_name(_silver_cache_storage_level(config))
    bronze = None
    bronze_cached = False
    try:
        if source == "raw":
            if cache_bronze:
                bronze = _read_raw_as_bronze(spark, config, selected_datasets)
        else:
            bronze = spark.read.parquet(_spark_path(bronze_path)).select(
                "dataset",
                "source_file",
                "file_hash",
                "ingest_ts",
                "ingest_date",
                "payload_json",
            )
        if bronze is not None and selected_datasets is not None:
            bronze = bronze.where(bronze.dataset.isin(*selected_datasets))
        if bronze is not None and cache_bronze:
            bronze = bronze.persist(storage_level)
            bronze.count()
            bronze_cached = True

        for table in selected_tables:
            table_datasets = TABLE_DATASETS[table]
            if selected_datasets is not None and not set(table_datasets).intersection(
                selected_datasets
            ):
                continue

            if source == "raw" and bronze is None:
                table_bronze = _read_raw_as_bronze(spark, config, table_datasets)
            else:
                table_bronze = bronze.where(bronze.dataset.isin(*table_datasets))
            rows = table_bronze.rdd.mapPartitions(_rows_for_table(table, selected_datasets))
            dataframe = _dedupe_table(
                spark.createDataFrame(rows, schema=_silver_schema(table)),
                table,
            ).persist(storage_level)
            row_count = dataframe.count()
            output_path = config.layer_path("silver", table)
            try:
                if row_count > 0:
                    _write_table(
                        dataframe=dataframe,
                        output_path=output_path,
                        mode=mode,
                        partition_columns=partition_columns,
                        output_partitions=output_partitions,
                        table_format=table_format,
                    )
            finally:
                dataframe.unpersist()
            counts[table] = row_count
            LOGGER.info(
                "Silver table %s wrote %s rows as %s to %s",
                table,
                row_count,
                table_format,
                output_path,
            )
        return counts
    finally:
        if bronze_cached and bronze is not None:
            bronze.unpersist()
        try:
            spark.stop()
        except Exception as exc:
            LOGGER.warning("Spark stop failed after Silver transform: %s", exc)
