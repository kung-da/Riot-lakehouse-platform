from __future__ import annotations

from typing import Any


def clean_timeline_events(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = payload.get("metadata", {}).get("matchId")
    rows = []
    for frame_index, frame in enumerate(payload.get("info", {}).get("frames", [])):
        for event_index, event in enumerate(frame.get("events", [])):
            rows.append(
                {
                    "match_id": match_id,
                    "frame_index": frame_index,
                    "event_index": event_index,
                    "timestamp": event.get("timestamp"),
                    "type": event.get("type"),
                    "participant_id": event.get("participantId"),
                    "killer_id": event.get("killerId"),
                    "victim_id": event.get("victimId"),
                    "team_id": event.get("teamId"),
                    "monster_type": event.get("monsterType"),
                    "building_type": event.get("buildingType"),
                }
            )
    return rows
