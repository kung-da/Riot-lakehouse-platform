def table_definitions() -> dict[str, list[str]]:
    return {
        "bronze_raw_json": ["dataset", "source_file", "file_hash", "ingest_ts", "payload_json"],
        "silver_matches": ["match_id", "game_id", "platform_id", "queue_id", "game_mode"],
        "silver_participants": ["match_id", "puuid", "champion_id", "team_id", "win"],
    }
