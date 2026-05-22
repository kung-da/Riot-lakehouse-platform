from __future__ import annotations

from typing import Any


def clean_match(payload: dict[str, Any]) -> dict[str, Any]:
    info = payload.get("info") or {}
    metadata = payload.get("metadata") or {}
    participants = info.get("participants") or []
    return {
        "match_id": metadata.get("matchId"),
        "game_id": info.get("gameId"),
        "platform_id": info.get("platformId"),
        "queue_id": info.get("queueId"),
        "game_mode": info.get("gameMode"),
        "game_type": info.get("gameType"),
        "game_version": info.get("gameVersion"),
        "game_creation": info.get("gameCreation"),
        "game_start_timestamp": info.get("gameStartTimestamp"),
        "game_end_timestamp": info.get("gameEndTimestamp"),
        "game_duration": info.get("gameDuration"),
        "participant_count": len(participants),
    }
