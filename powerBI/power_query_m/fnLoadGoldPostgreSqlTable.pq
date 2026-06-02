let
    fnLoadGoldPostgreSqlTable =
        (
            Server as text,
            Database as text,
            SchemaName as text,
            TablePrefix as text,
            TableName as text
        ) as table =>
        let
            Source = PostgreSQL.Database(Server, Database, [CreateNavigationProperties = false]),
            RawTable = Source{[Schema = SchemaName, Item = TablePrefix & TableName]}[Data],
            Typed = fnApplyGoldTypes(RawTable, TableName)
        in
            Typed
in
    fnLoadGoldPostgreSqlTable
