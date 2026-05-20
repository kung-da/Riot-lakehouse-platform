from __future__ import annotations

from typing import Any


def clean_teams(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = payload.get("metadata", {}).get("matchId")
    rows = []
    for team in payload.get("info", {}).get("teams", []):
        objectives = team.get("objectives", {})
        rows.append(
            {
                "match_id": match_id,
                "team_id": team.get("teamId"),
                "win": team.get("win"),
                "baron_kills": objectives.get("baron", {}).get("kills"),
                "dragon_kills": objectives.get("dragon", {}).get("kills"),
                "rift_herald_kills": objectives.get("riftHerald", {}).get("kills"),
                "tower_kills": objectives.get("tower", {}).get("kills"),
                "inhibitor_kills": objectives.get("inhibitor", {}).get("kills"),
            }
        )
    return rows
