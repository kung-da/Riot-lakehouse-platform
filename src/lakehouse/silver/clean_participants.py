from __future__ import annotations

from typing import Any


def clean_participants(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = payload.get("metadata", {}).get("matchId")
    rows = []
    for participant in payload.get("info", {}).get("participants", []):
        rows.append(
            {
                "match_id": match_id,
                "puuid": participant.get("puuid"),
                "summoner_id": participant.get("summonerId"),
                "riot_id_game_name": participant.get("riotIdGameName"),
                "champion_id": participant.get("championId"),
                "champion_name": participant.get("championName"),
                "team_id": participant.get("teamId"),
                "team_position": participant.get("teamPosition"),
                "lane": participant.get("lane"),
                "role": participant.get("role"),
                "win": participant.get("win"),
                "kills": participant.get("kills"),
                "deaths": participant.get("deaths"),
                "assists": participant.get("assists"),
                "gold_earned": participant.get("goldEarned"),
                "total_damage_dealt_to_champions": participant.get("totalDamageDealtToChampions"),
                "vision_score": participant.get("visionScore"),
                "total_minions_killed": participant.get("totalMinionsKilled"),
            }
        )
    return rows
