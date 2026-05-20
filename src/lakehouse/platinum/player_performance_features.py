def player_performance_features_sql() -> str:
    return "select match_id, puuid, kills, deaths, assists, gold_earned, vision_score, win as label from participants"
