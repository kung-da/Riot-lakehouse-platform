// Use this only if Gold tables have been mirrored to PostgreSQL.
// Create these Power BI text parameters first:
// PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix
// Example PostgreSqlTablePrefix: gold_
// Create fnLoadGoldPostgreSqlTable and fnApplyGoldTypes before these table queries.

// Query name: dim_date
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "dim_date")
in
    Source

// Query name: dim_match
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "dim_match")
in
    Source

// Query name: dim_summoner
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "dim_summoner")
in
    Source

// Query name: dim_champion
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "dim_champion")
in
    Source

// Query name: dim_team
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "dim_team")
in
    Source

// Query name: dim_rank
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "dim_rank")
in
    Source

// Query name: fact_participant_performance
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "fact_participant_performance")
in
    Source

// Query name: fact_team_objectives
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "fact_team_objectives")
in
    Source

// Query name: fact_rank_snapshot
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "fact_rank_snapshot")
in
    Source

// Query name: fact_timeline_frames
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "fact_timeline_frames")
in
    Source

// Query name: fact_timeline_events
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "fact_timeline_events")
in
    Source

// Query name: mart_player_daily_performance
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "mart_player_daily_performance")
in
    Source

// Query name: mart_champion_daily_performance
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "mart_champion_daily_performance")
in
    Source

// Query name: mart_role_daily_performance
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "mart_role_daily_performance")
in
    Source

// Query name: mart_rank_daily_summary
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "mart_rank_daily_summary")
in
    Source

// Query name: mart_team_objective_daily_summary
let
    Source = fnLoadGoldPostgreSqlTable(PostgreSqlServer, PostgreSqlDatabase, PostgreSqlSchema, PostgreSqlTablePrefix, "mart_team_objective_daily_summary")
in
    Source
