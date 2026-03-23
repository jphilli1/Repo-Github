let
    // Read the data source path from the named range in _config sheet
    DataPath = Excel.CurrentWorkbook(){[Name="DataSourcePath"]}[Content]{0}[Column1],

    // Load CSV with UTF-8 encoding
    Source = Csv.Document(
        File.Contents(DataPath),
        [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]
    ),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),

    // Define expected column types — columns not in the CSV are silently skipped
    ExpectedTypes = {
        {"TL_FACILITY_DIGITS12", type text},
        {"FACILITY_ID", type text},
        {"ACCOUNT_NUMBER", type text},
        {"KEY_ACCT", type text},
        {"BORROWER", type text},
        {"NAME", type text},
        {"SUB_PRODUCT_NORM", type text},
        {"PRODUCT_BUCKET", type text},
        {"IS_LAL_NFP", type logical},
        {"FOCUS_LIST", type text},
        {"TXT_MSTR_FACIL_COLLATERAL_DESC", type text},
        {"SBL_PERC", type number},
        {"BOOK_DATE", type datetime},
        {"EFFECTIVE_DATE", type datetime},
        {"BALANCE", type number},
        {"CREDIT_LIMIT", type number},
        {"AMT_ORIGINAL_COMT", type number},
        {"CREDIT_LII", type number},
        {"NEW_CAMP_YN", type text},
        {"NEW_CAMP_REASON", type text},
        {"NEW_COMMITMENT_REASON", type text},
        {"NEW_COMMITMENT_AMOUNT", Int64.Type},
        {"IS_NTC", type logical},
        {"IS_OFFICE", type logical},
        {"HAS_CREDIT_POLICY_EXCEPTION", type logical},
        {"WMLC_FLAGS", type text},
        {"WMLC_FLAG_COUNT", Int64.Type},
        {"WMLC_QUALIFIED", type logical},
        {"CREDIT_LII_COMMITMENT_BUCKET", type text},
        {"CREDIT_LII_COMMITMENT_FLOOR", Int64.Type}
    },

    // Only apply types for columns that actually exist in the CSV
    ExistingColumns = Table.ColumnNames(PromotedHeaders),
    ApplicableTypes = List.Select(ExpectedTypes, each List.Contains(ExistingColumns, _{0})),
    #"Changed Type" = Table.TransformColumnTypes(PromotedHeaders, ApplicableTypes)
in
    #"Changed Type"
