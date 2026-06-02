let
    fnLoadGoldCsvFolder =
        (GoldRoot as text, TableName as text, optional DatePartitionColumn as nullable text) as table =>
        let
            Separator = if Text.Contains(GoldRoot, "/") and not Text.Contains(GoldRoot, "\") then "/" else "\",
            TablePath =
                if Text.EndsWith(GoldRoot, "\") or Text.EndsWith(GoldRoot, "/") then
                    GoldRoot & TableName
                else
                    GoldRoot & Separator & TableName,
            Source = Folder.Files(TablePath),
            CsvFiles =
                Table.SelectRows(
                    Source,
                    each Text.Lower([Extension]) = ".csv"
                        and not Text.StartsWith([Name], ".")
                        and not Text.StartsWith([Name], "_")
                ),
            WithData =
                Table.AddColumn(
                    CsvFiles,
                    "Data",
                    each
                        Table.PromoteHeaders(
                            Csv.Document(
                                [Content],
                                [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
                            ),
                            [PromoteAllScalars = true]
                        ),
                    type table
                ),
            DataColumns =
                if Table.RowCount(WithData) = 0 then
                    {}
                else
                    List.Union(List.Transform(WithData[Data], each Table.ColumnNames(_))),
            NeedsPartitionColumn =
                DatePartitionColumn <> null
                    and DatePartitionColumn <> ""
                    and not List.Contains(DataColumns, DatePartitionColumn),
            WithPartition =
                if NeedsPartitionColumn then
                    Table.AddColumn(
                        WithData,
                        DatePartitionColumn,
                        each
                            let
                                Marker = DatePartitionColumn & "=",
                                FolderPath = [Folder Path],
                                AfterMarker =
                                    if Text.Contains(FolderPath, Marker) then
                                        Text.AfterDelimiter(FolderPath, Marker, {0, RelativePosition.FromEnd})
                                    else
                                        null,
                                Parts = if AfterMarker = null then {} else Text.SplitAny(AfterMarker, "\/"),
                                Value = if List.Count(Parts) = 0 then null else Parts{0}
                            in
                                Value,
                        type text
                    )
                else
                    WithData,
            ColumnsToKeep =
                if NeedsPartitionColumn then
                    {"Data", DatePartitionColumn}
                else
                    {"Data"},
            ForExpand = Table.SelectColumns(WithPartition, ColumnsToKeep),
            Expanded =
                if List.Count(DataColumns) = 0 then
                    #table({}, {})
                else
                    Table.ExpandTableColumn(ForExpand, "Data", DataColumns, DataColumns),
            Result =
                if NeedsPartitionColumn then
                    Table.ReorderColumns(Expanded, List.Combine({{DatePartitionColumn}, DataColumns}), MissingField.Ignore)
                else
                    Expanded
        in
            Result
in
    fnLoadGoldCsvFolder
