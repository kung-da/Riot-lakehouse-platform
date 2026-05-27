def match_win_features_sql() -> str:
    return (
        "select match_id, team_id, dragon_kills, tower_kills, win as label "
        "from fact_team_objectives"
    )
