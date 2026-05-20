from lakehouse.silver.silver_transformer import transform_payload


def test_transform_match_payload_to_silver_tables():
    payload = {
        "metadata": {"matchId": "VN2_1"},
        "info": {
            "gameId": 1,
            "participants": [{"puuid": "p1", "championId": 1, "win": True}],
            "teams": [{"teamId": 100, "win": True, "objectives": {}}],
        },
    }
    result = transform_payload("matches", payload)
    assert result["matches"][0]["match_id"] == "VN2_1"
    assert result["participants"][0]["puuid"] == "p1"
    assert result["teams"][0]["team_id"] == 100
