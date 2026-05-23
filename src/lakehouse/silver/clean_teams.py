from __future__ import annotations

from typing import Any

STANDARD_TEAM_IDS = {100, 200}


def _objective_kills(objectives: dict[str, Any], objective_name: str) -> Any:
    objective = objectives.get(objective_name) or {}
    return objective.get("kills")


def _standard_team_id(value: Any) -> int | None:
    try:
        team_id = int(value)
    except (TypeError, ValueError):
        return None
    return team_id if team_id in STANDARD_TEAM_IDS else None


def clean_teams(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = (payload.get("metadata") or {}).get("matchId")
    teams = (payload.get("info") or {}).get("teams") or []
    rows = []
    for team in teams:
        team = team or {}
        team_id = _standard_team_id(team.get("teamId"))
        if team_id is None:
            continue
        objectives = team.get("objectives") or {}
        rows.append(
            {
                "match_id": match_id,
                "team_id": team_id,
                "win": team.get("win"),
                "baron_kills": _objective_kills(objectives, "baron"),
                "dragon_kills": _objective_kills(objectives, "dragon"),
                "rift_herald_kills": _objective_kills(objectives, "riftHerald"),
                "tower_kills": _objective_kills(objectives, "tower"),
                "inhibitor_kills": _objective_kills(objectives, "inhibitor"),
                "champion_kills": _objective_kills(objectives, "champion"),
            }
        )
    return rows
