from __future__ import annotations

from typing import Any


def clean_summoner(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "puuid": payload.get("puuid"),
        "summoner_id": payload.get("id"),
        "account_id": payload.get("accountId"),
        "profile_icon_id": payload.get("profileIconId"),
        "revision_date": payload.get("revisionDate"),
        "summoner_level": payload.get("summonerLevel"),
    }
