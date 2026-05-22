from __future__ import annotations

from typing import Any


def _safe_kda(kills: Any, deaths: Any, assists: Any) -> float | None:
    if kills is None or deaths is None or assists is None:
        return None
    try:
        kill_assist_total = float(kills) + float(assists)
        death_count = float(deaths)
    except (TypeError, ValueError):
        return None
    if death_count == 0:
        return kill_assist_total
    return kill_assist_total / death_count


def clean_participants(payload: dict[str, Any]) -> list[dict[str, Any]]:
    match_id = (payload.get("metadata") or {}).get("matchId")
    participants = (payload.get("info") or {}).get("participants") or []
    rows = []
    for participant in participants:
        participant = participant or {}
        kills = participant.get("kills")
        deaths = participant.get("deaths")
        assists = participant.get("assists")
        rows.append(
            {
                "match_id": match_id,
                "participant_id": participant.get("participantId"),
                "puuid": participant.get("puuid"),
                "summoner_id": participant.get("summonerId"),
                "riot_id_game_name": participant.get("riotIdGameName"),
                "riot_id_tagline": participant.get("riotIdTagline"),
                "summoner_name": participant.get("summonerName"),
                "champion_id": participant.get("championId"),
                "champion_name": participant.get("championName"),
                "team_id": participant.get("teamId"),
                "team_position": participant.get("teamPosition"),
                "individual_position": participant.get("individualPosition"),
                "lane": participant.get("lane"),
                "role": participant.get("role"),
                "win": participant.get("win"),
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "kda": _safe_kda(kills, deaths, assists),
                "gold_earned": participant.get("goldEarned"),
                "total_damage_dealt_to_champions": participant.get("totalDamageDealtToChampions"),
                "total_damage_taken": participant.get("totalDamageTaken"),
                "vision_score": participant.get("visionScore"),
                "total_minions_killed": participant.get("totalMinionsKilled"),
                "neutral_minions_killed": participant.get("neutralMinionsKilled"),
            }
        )
    return rows
