from __future__ import annotations

from typing import Any


def clean_timeline_frames(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = (payload.get("metadata") or {}).get("matchId")
    frames = (payload.get("info") or {}).get("frames") or []
    rows = []
    for index, frame in enumerate(frames):
        frame = frame or {}
        participant_frames = frame.get("participantFrames") or {}
        events = frame.get("events") or []
        rows.append(
            {
                "match_id": match_id,
                "frame_index": index,
                "frame_timestamp": frame.get("timestamp"),
                "participant_frame_count": len(participant_frames),
                "event_count": len(events),
            }
        )
    return rows
