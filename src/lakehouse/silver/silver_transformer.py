from __future__ import annotations

from typing import Any

from lakehouse.silver.clean_matches import clean_match
from lakehouse.silver.clean_participants import clean_participants
from lakehouse.silver.clean_ranked import clean_ranked
from lakehouse.silver.clean_summoners import clean_summoner
from lakehouse.silver.clean_teams import clean_teams
from lakehouse.silver.clean_timeline_events import clean_timeline_events
from lakehouse.silver.clean_timeline_frames import clean_timeline_frames


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
