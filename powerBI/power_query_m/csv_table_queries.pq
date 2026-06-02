// Use this only if Gold tables have been exported as CSV folders.
// Create a Power BI text parameter named GoldRoot first.
// Create fnLoadGoldCsvFolder and fnApplyGoldTypes before these table queries.

// Query name: dim_date
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "dim_date", "game_date"),
    Typed = fnApplyGoldTypes(Source, "dim_date")
in
    Typed

// Query name: dim_match
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "dim_match", "game_date"),
    Typed = fnApplyGoldTypes(Source, "dim_match")
in
    Typed

// Query name: dim_summoner
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "dim_summoner", null),
    Typed = fnApplyGoldTypes(Source, "dim_summoner")
in
    Typed

// Query name: dim_champion
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "dim_champion", null),
    Typed = fnApplyGoldTypes(Source, "dim_champion")
in
    Typed

// Query name: dim_team
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "dim_team", null),
    Typed = fnApplyGoldTypes(Source, "dim_team")
in
    Typed

// Query name: dim_rank
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "dim_rank", null),
    Typed = fnApplyGoldTypes(Source, "dim_rank")
in
    Typed

// Query name: fact_participant_performance
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "fact_participant_performance", "game_date"),
    Typed = fnApplyGoldTypes(Source, "fact_participant_performance")
in
    Typed

// Query name: fact_team_objectives
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "fact_team_objectives", "game_date"),
    Typed = fnApplyGoldTypes(Source, "fact_team_objectives")
in
    Typed

// Query name: fact_rank_snapshot
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "fact_rank_snapshot", "game_date"),
    Typed = fnApplyGoldTypes(Source, "fact_rank_snapshot")
in
    Typed

// Query name: fact_timeline_frames
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "fact_timeline_frames", "game_date"),
    Typed = fnApplyGoldTypes(Source, "fact_timeline_frames")
in
    Typed

// Query name: fact_timeline_events
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "fact_timeline_events", "game_date"),
    Typed = fnApplyGoldTypes(Source, "fact_timeline_events")
in
    Typed

// Query name: mart_player_daily_performance
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "mart_player_daily_performance", "game_date"),
    Typed = fnApplyGoldTypes(Source, "mart_player_daily_performance")
in
    Typed

// Query name: mart_champion_daily_performance
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "mart_champion_daily_performance", "game_date"),
    Typed = fnApplyGoldTypes(Source, "mart_champion_daily_performance")
in
    Typed

// Query name: mart_role_daily_performance
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "mart_role_daily_performance", "game_date"),
    Typed = fnApplyGoldTypes(Source, "mart_role_daily_performance")
in
    Typed

// Query name: mart_rank_daily_summary
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "mart_rank_daily_summary", "game_date"),
    Typed = fnApplyGoldTypes(Source, "mart_rank_daily_summary")
in
    Typed

// Query name: mart_team_objective_daily_summary
let
    Source = fnLoadGoldCsvFolder(GoldRoot, "mart_team_objective_daily_summary", "game_date"),
    Typed = fnApplyGoldTypes(Source, "mart_team_objective_daily_summary")
in
    Typed
