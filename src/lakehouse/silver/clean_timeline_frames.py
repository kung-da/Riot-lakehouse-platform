from __future__ import annotations

from typing import Any


def clean_timeline_frames(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = payload.get("metadata", {}).get("matchId")
    rows = []
    for index, frame in enumerate(payload.get("info", {}).get("frames", [])):
        rows.append(
            {
                "match_id": match_id,
                "frame_index": index,
                "timestamp": frame.get("timestamp"),
                "participant_frame_count": len(frame.get("participantFrames", {})),
                "event_count": len(frame.get("events", [])),
            }
        )
    return rows
