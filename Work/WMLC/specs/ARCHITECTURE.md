# Architecture Specification: WMLC Dashboard Pipeline

## Two-Part Codebase Design

```
┌─────────────────────────────────────────────────────────────┐
│  PART 1: Corp Environment (runs on corporate machine)       │
│  Purpose: ETL — read extracts, tag loans, output tagged CSV │
│  Dependencies: Python 3.x, pandas, openpyxl (installed)     │
│  Creativity level: LOW — keep it simple and auditable       │
├─────────────────────────────────────────────────────────────┤
│  PART 2: Local Environment (runs on M4 Mac Mini)            │
│  Purpose: Build proxy data, Excel dashboard, VBA, PQ, test  │
│  Path: CR_Refactored/CR_PEERS_JP                            │
│  Engine: Claude Code with agent teams (4 sub-agents)        │
│  Creativity level: HIGH — full dashboard build               │
└─────────────────────────────────────────────────────────────┘
```

---

## Part 1: Corp ETL (`/corp_etl/`)

### Directory Structure

```
corp_etl/
├── config.yaml                  # All file paths, skip rows, column mappings
├── main.py                      # Entry point — orchestrates pipeline
├── readers/
│   ├── __init__.py
│   ├── base_reader.py           # Reads loan extract (base table)
│   ├── lal_credit_reader.py     # Reads LAL_Credit (skip 3 rows, pad account)
│   ├── loan_reserve_reader.py   # Reads Loan_Reserve_Report (skip 6 rows)
│   └── dar_tracker_reader.py    # Reads DAR_Tracker (no skip, pad facility)
├── taggers/
│   ├── __init__.py
│   ├── intermediate_tags.py     # Computes is_ntc, is_office, has_credit_policy_exception
│   └── wmlc_tagger.py           # Evaluates all 15 WMLC flags per WMLC_LOGIC.md
├── output/
│   └── writer.py                # Writes tagged CSV (or .xlsx) to output path
└── tests/
    └── test_wmlc_flags.py       # Unit tests for flag logic with known-answer cases
```

### config.yaml Schema

```yaml
# All paths are relative to config location or absolute
input_files:
  base_extract:
    path: "./data/loan_extract.csv"
    # No skip rows — standard CSV
  lal_credit:
    path: "./data/LAL_Credit.xlsx"
    skip_rows: 3
    key_column: "Account Number"
    key_format: "strip_dash_pad12"
  loan_reserve_report:
    path: "./data/Loan_Reserve_Report.xlsx"
    skip_rows: 6
    key_column: "Facility Account Number"
    key_format: "pad12"
  dar_tracker:
    path: "./data/DAR_Tracker.xlsx"
    skip_rows: 0
    key_column: "Facility ID"
    key_format: "pad12"

output:
  tagged_file: "./output/loan_extract_tagged.csv"

# Column name suggestions with fallback matching
column_hints:
  # Each entry: [preferred_name, ...alternates]
  product_bucket: ["product_bucket", "Product_Bucket", "PRODUCT_BUCKET"]
  credit_lii: ["credit_lii", "Credit_LII", "CREDIT_LII", "commitment"]
  account_number: ["account_number", "Account_Number", "ACCOUNT_NUMBER", "acct_num"]
  facility_id: ["facility_id", "Facility_ID", "FACILITY_ID", "tl_facility_digits12"]
```

### Key Design Principles (Corp)

1. **No hard failures on column name mismatch.** Use fuzzy column matching: check
   `column_hints` first, then case-insensitive match, then warn-and-skip.
2. **Config-driven.** Zero hardcoded paths. All file locations and skip-row counts
   come from `config.yaml`.
3. **Auditable.** Log every tagging decision. Output includes `wmlc_flags` as a
   pipe-delimited string so auditors can see exactly which rules fired.
4. **Minimal dependencies.** `pandas`, `openpyxl`, `pyyaml` only. No exotic packages.

---

## Part 2: Local Dashboard Build (`/local_dashboard/`)

### Directory Structure

```
local_dashboard/
├── CLAUDE.md                    # Master instructions for Claude Code orchestrator
├── skills/
│   ├── PROXY_DATA_SKILL.md      # Skill: generate realistic proxy data
│   ├── EXCEL_VBA_SKILL.md       # Skill: build Excel workbook with VBA
│   ├── POWER_QUERY_SKILL.md     # Skill: write M code for Power Query
│   └── DASHBOARD_FORMAT_SKILL.md # Skill: formatting, conditional formatting, layout
├── agents/
│   ├── orchestrator.md          # Orchestrator agent instructions
│   ├── data_gen_agent.md        # Sub-agent 1: proxy data generation
│   ├── etl_logic_agent.md       # Sub-agent 2: ETL/tagging logic
│   ├── excel_vba_agent.md       # Sub-agent 3: Excel + VBA + Power Query
│   └── testing_agent.md         # Sub-agent 4: validation & QA
├── scripts/
│   ├── generate_proxy_data.py   # Creates all proxy input files
│   ├── run_corp_etl.py          # Runs corp ETL against proxy data
│   ├── build_dashboard.py       # Builds the .xlsm workbook
│   └── inject_vba.py            # Injects VBA modules into workbook
├── templates/
│   ├── vba_modules/
│   │   ├── mod_Navigation.bas   # Dashboard ↔ loan_detail navigation
│   │   ├── mod_ViewToggle.bas   # 8-view toggle (4 metrics × WMLC on/off)
│   │   ├── mod_FilterReset.bas  # Reset all filters
│   │   └── mod_CellClick.bas    # Worksheet_SelectionChange event handler
│   └── power_query/
│       └── load_tagged_data.m   # M code for Power Query import
├── output/
│   └── WMLC_Dashboard.xlsm     # Final output workbook
└── tests/
    ├── test_proxy_data.py       # Validates proxy data completeness
    ├── test_dashboard_structure.py  # Validates sheet names, ranges, buttons
    └── test_vba_injection.py    # Validates VBA modules present in .xlsm
```

---

## Excel Workbook Specification

### Sheet: `Dashboard`

**Layout:** Matrix grid matching the screenshot.
- **Rows:** `credit_lii_commitment_bucket` ladder (descending from $1B to $1)
- **Columns:** All 14 `product_bucket` values + row labels + totals
- **Active view indicator:** Cell or merged range at top showing current view name

**Toggle controls (VBA buttons):**

| Button | Action |
|--------|--------|
| `Summary Count` | Show count of loans per cell |
| `Summary Commitment` | Show sum of `credit_lii` per cell |
| `Summary Count NEW` | Show count where `NEW_CAMP_YN == "Y"` |
| `Summary Commitment NEW` | Show sum of `credit_lii` where `NEW_CAMP_YN == "Y"` |
| `WMLC Filter ON` | Restrict all views to `wmlc_qualified == True` |
| `WMLC Filter OFF` | Show full population |
| `Reset Filters` | Return to default view (Summary Count, WMLC OFF) |

**Cell click behavior:**
- User clicks any data cell in the matrix
- VBA reads the row (bucket) and column (product_bucket) of the clicked cell
- Jumps to `loan_detail` sheet, auto-filtered to that population
- Respects current WMLC toggle state

### Sheet: `loan_detail`

**Layout:** Flat table with ALL columns from the tagged extract.
- Auto-filter enabled on all columns
- Navigation buttons:
  - `← Back to Dashboard` — clears filters, jumps to Dashboard sheet
  - `Reset Filters` — clears all auto-filters on this sheet

### Sheet: `_data` (hidden)

**Purpose:** Power Query landing zone. Tagged CSV is loaded here via PQ connection.
The dashboard and loan_detail sheets reference this data via structured table references.

---

## Power Query Design

### M Code Approach

```
// Resilient column loading — suggests expected names, does not hard-fail
let
    Source = Csv.Document(File.Contents(data_path), [Delimiter=",", Encoding=65001]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    // Type assignments use try...otherwise for resilience
    Typed = Table.TransformColumnTypes(PromotedHeaders, {
        try {"credit_lii", type number} otherwise {"credit_lii", type text},
        try {"balance", type number} otherwise {"balance", type text},
        ...
    })
in
    Typed
```

> Power Query connection string will point to the tagged CSV output path.
> On first open, user may need to click "Refresh" to load data.

---

## Data Flow

```
[Corp Machine]
  loan_extract.csv ─────┐
  LAL_Credit.xlsx ───────┤
  Loan_Reserve_Report.xlsx─┤──→ corp_etl/main.py ──→ loan_extract_tagged.csv
  DAR_Tracker.xlsx ──────┘

[Local Machine / Excel]
  loan_extract_tagged.csv ──→ Power Query ──→ _data sheet
                                                  │
                                    ┌─────────────┴──────────────┐
                                    ▼                            ▼
                              Dashboard sheet            loan_detail sheet
                           (matrix + buttons)         (flat table + nav)
```
