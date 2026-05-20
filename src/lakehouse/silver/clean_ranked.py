from __future__ import annotations

from typing import Any


def clean_ranked(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for entry in payload.get("entries", []):
        rows.append(
            {
                "league_id": payload.get("leagueId"),
                "queue": payload.get("queue"),
                "tier": payload.get("tier"),
                "rank": entry.get("rank"),
                "puuid": entry.get("puuid"),
                "league_points": entry.get("leaguePoints"),
                "wins": entry.get("wins"),
                "losses": entry.get("losses"),
                "hot_streak": entry.get("hotStreak"),
                "veteran": entry.get("veteran"),
                "fresh_blood": entry.get("freshBlood"),
                "inactive": entry.get("inactive"),
            }
        )
    return rows
