let
    fnApplyGoldTypes = (Input as table, TableName as text) as table =>
        let
            Options = [Culture = "en-US", MissingField = MissingField.Ignore],
            Typed =
                if TableName = "dim_date" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"date_key", type text},
                            {"game_date", type date},
                            {"date_year", Int64.Type},
                            {"date_month", Int64.Type},
                            {"date_day", Int64.Type},
                            {"day_of_week", Int64.Type}
                        },
                        Options
                    )
                else if TableName = "dim_match" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"match_id", type text},
                            {"game_id", Int64.Type},
                            {"platform_id", type text},
                            {"queue_id", Int64.Type},
                            {"game_mode", type text},
                            {"game_type", type text},
                            {"game_version", type text},
                            {"game_creation", Int64.Type},
                            {"game_start_timestamp", Int64.Type},
                            {"game_end_timestamp", Int64.Type},
                            {"game_duration", Int64.Type},
                            {"participant_count", Int64.Type},
                            {"game_date", type date}
                        },
                        Options
                    )
                else if TableName = "dim_summoner" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"puuid", type text},
                            {"summoner_id", type text},
                            {"account_id", type text},
                            {"summoner_name", type text},
                            {"riot_id_game_name", type text},
                            {"riot_id_tagline", type text},
                            {"profile_icon_id", Int64.Type},
                            {"revision_date", Int64.Type},
                            {"summoner_level", Int64.Type},
                            {"first_seen_game_date", type date},
                            {"last_seen_game_date", type date}
                        },
                        Options
                    )
                else if TableName = "dim_champion" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"champion_id", Int64.Type},
                            {"champion_name", type text},
                            {"first_seen_game_date", type date},
                            {"last_seen_game_date", type date}
                        },
                        Options
                    )
                else if TableName = "dim_team" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"team_id", Int64.Type},
                            {"team_side", type text},
                            {"first_seen_game_date", type date},
                            {"last_seen_game_date", type date}
                        },
                        Options
                    )
                else if TableName = "dim_rank" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"queue", type text},
                            {"tier", type text},
                            {"rank", type text},
                            {"rank_order", Int64.Type}
                        },
                        Options
                    )
                else if TableName = "fact_participant_performance" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"match_id", type text},
                            {"participant_id", Int64.Type},
                            {"puuid", type text},
                            {"summoner_id", type text},
                            {"champion_id", Int64.Type},
                            {"team_id", Int64.Type},
                            {"team_position", type text},
                            {"win", type logical},
                            {"kills", Int64.Type},
                            {"deaths", Int64.Type},
                            {"assists", Int64.Type},
                            {"kda", type number},
                            {"gold_earned", Int64.Type},
                            {"total_damage_dealt_to_champions", Int64.Type},
                            {"total_damage_taken", Int64.Type},
                            {"vision_score", Int64.Type},
                            {"total_minions_killed", Int64.Type},
                            {"neutral_minions_killed", Int64.Type},
                            {"cs", Int64.Type}
                        },
                        Options
                    )
                else if TableName = "fact_team_objectives" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"match_id", type text},
                            {"team_id", Int64.Type},
                            {"team_side", type text},
                            {"win", type logical},
                            {"baron_kills", Int64.Type},
                            {"dragon_kills", Int64.Type},
                            {"rift_herald_kills", Int64.Type},
                            {"tower_kills", Int64.Type},
                            {"inhibitor_kills", Int64.Type},
                            {"champion_kills", Int64.Type},
                            {"objective_score", Int64.Type}
                        },
                        Options
                    )
                else if TableName = "fact_rank_snapshot" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"puuid", type text},
                            {"summoner_id", type text},
                            {"queue", type text},
                            {"tier", type text},
                            {"rank", type text},
                            {"league_points", Int64.Type},
                            {"wins", Int64.Type},
                            {"losses", Int64.Type},
                            {"win_rate", type number},
                            {"hot_streak", type logical},
                            {"veteran", type logical},
                            {"fresh_blood", type logical},
                            {"inactive", type logical}
                        },
                        Options
                    )
                else if TableName = "fact_timeline_frames" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"match_id", type text},
                            {"frame_index", Int64.Type},
                            {"frame_timestamp", Int64.Type},
                            {"participant_frame_count", Int64.Type},
                            {"event_count", Int64.Type}
                        },
                        Options
                    )
                else if TableName = "fact_timeline_events" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"match_id", type text},
                            {"frame_index", Int64.Type},
                            {"event_index", Int64.Type},
                            {"event_timestamp", Int64.Type},
                            {"event_type", type text},
                            {"participant_id", Int64.Type},
                            {"killer_id", Int64.Type},
                            {"victim_id", Int64.Type},
                            {"team_id", Int64.Type},
                            {"monster_type", type text},
                            {"building_type", type text},
                            {"lane_type", type text}
                        },
                        Options
                    )
                else if TableName = "mart_player_daily_performance" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"puuid", type text},
                            {"summoner_id", type text},
                            {"summoner_name", type text},
                            {"riot_id_game_name", type text},
                            {"riot_id_tagline", type text},
                            {"matches_played", Int64.Type},
                            {"wins", Int64.Type},
                            {"losses", Int64.Type},
                            {"win_rate", type number},
                            {"unique_champions", Int64.Type},
                            {"total_kills", Int64.Type},
                            {"total_deaths", Int64.Type},
                            {"total_assists", Int64.Type},
                            {"avg_kills", type number},
                            {"avg_deaths", type number},
                            {"avg_assists", type number},
                            {"avg_kda", type number},
                            {"avg_gold_earned", type number},
                            {"avg_damage_dealt_to_champions", type number},
                            {"avg_damage_taken", type number},
                            {"avg_vision_score", type number},
                            {"avg_cs", type number}
                        },
                        Options
                    )
                else if TableName = "mart_champion_daily_performance" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"champion_id", Int64.Type},
                            {"champion_name", type text},
                            {"matches_played", Int64.Type},
                            {"unique_players", Int64.Type},
                            {"wins", Int64.Type},
                            {"losses", Int64.Type},
                            {"win_rate", type number},
                            {"total_kills", Int64.Type},
                            {"total_deaths", Int64.Type},
                            {"total_assists", Int64.Type},
                            {"avg_kills", type number},
                            {"avg_deaths", type number},
                            {"avg_assists", type number},
                            {"avg_kda", type number},
                            {"avg_gold_earned", type number},
                            {"avg_damage_dealt_to_champions", type number},
                            {"avg_damage_taken", type number},
                            {"avg_vision_score", type number},
                            {"avg_cs", type number}
                        },
                        Options
                    )
                else if TableName = "mart_role_daily_performance" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"team_position", type text},
                            {"matches_played", Int64.Type},
                            {"unique_players", Int64.Type},
                            {"unique_champions", Int64.Type},
                            {"wins", Int64.Type},
                            {"losses", Int64.Type},
                            {"win_rate", type number},
                            {"avg_kills", type number},
                            {"avg_deaths", type number},
                            {"avg_assists", type number},
                            {"avg_kda", type number},
                            {"avg_gold_earned", type number},
                            {"avg_damage_dealt_to_champions", type number},
                            {"avg_damage_taken", type number},
                            {"avg_vision_score", type number},
                            {"avg_cs", type number}
                        },
                        Options
                    )
                else if TableName = "mart_rank_daily_summary" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"queue", type text},
                            {"tier", type text},
                            {"rank", type text},
                            {"players", Int64.Type},
                            {"avg_league_points", type number},
                            {"total_wins", Int64.Type},
                            {"total_losses", Int64.Type},
                            {"avg_win_rate", type number},
                            {"hot_streak_players", Int64.Type},
                            {"veteran_players", Int64.Type},
                            {"fresh_blood_players", Int64.Type},
                            {"inactive_players", Int64.Type}
                        },
                        Options
                    )
                else if TableName = "mart_team_objective_daily_summary" then
                    Table.TransformColumnTypes(
                        Input,
                        {
                            {"game_date", type date},
                            {"team_id", Int64.Type},
                            {"team_side", type text},
                            {"games_played", Int64.Type},
                            {"wins", Int64.Type},
                            {"losses", Int64.Type},
                            {"win_rate", type number},
                            {"avg_baron_kills", type number},
                            {"avg_dragon_kills", type number},
                            {"avg_rift_herald_kills", type number},
                            {"avg_tower_kills", type number},
                            {"avg_inhibitor_kills", type number},
                            {"avg_champion_kills", type number},
                            {"avg_objective_score", type number}
                        },
                        Options
                    )
                else
                    Input
        in
            Typed
in
    fnApplyGoldTypes
