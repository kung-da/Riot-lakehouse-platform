from __future__ import annotations

from typing import Any


def _safe_win_rate(wins: Any, losses: Any) -> float | None:
    if wins is None or losses is None:
        return None
    try:
        win_count = float(wins)
        loss_count = float(losses)
    except (TypeError, ValueError):
        return None
    total_games = win_count + loss_count
    if total_games == 0:
        return None
    return win_count / total_games


def clean_ranked(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for entry in payload.get("entries") or []:
        entry = entry or {}
        wins = entry.get("wins")
        losses = entry.get("losses")
        rows.append(
            {
                "league_id": payload.get("leagueId"),
                "queue": payload.get("queue"),
                "tier": payload.get("tier"),
                "rank": entry.get("rank"),
                "summoner_id": entry.get("summonerId"),
                "puuid": entry.get("puuid"),
                "league_points": entry.get("leaguePoints"),
                "wins": wins,
                "losses": losses,
                "win_rate": _safe_win_rate(wins, losses),
                "hot_streak": entry.get("hotStreak"),
                "veteran": entry.get("veteran"),
                "fresh_blood": entry.get("freshBlood"),
                "inactive": entry.get("inactive"),
            }
        )
    return rows
