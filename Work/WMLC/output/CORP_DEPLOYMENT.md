# WMLC Corp ETL — Deployment Guide

## Files Needed

```
corp_etl/
├── main.py                      # ETL entry point
├── config.yaml                  # Edit this — all file paths
├── column_matcher.py
├── file_utils.py
├── tracker_matcher.py           # Step 4: fuzzy match against WMLC Tracker
├── build_tracker_analytics.py   # Step 3: rebuild Tracker Analytics sheet
├── __init__.py
├── readers/
│   ├── __init__.py
│   ├── base_reader.py
│   ├── lal_credit_reader.py
│   ├── loan_reserve_reader.py
│   └── dar_tracker_reader.py
└── taggers/
    ├── __init__.py
    ├── intermediate_tags.py
    └── wmlc_tagger.py
```

## Prerequisites

- Python 3.9+ (pre-installed on most corp machines)
- `pip install pandas openpyxl pyyaml rapidfuzz`

## Workflow

### Step 1: Run ETL (required)

```cmd
cd C:\path\to\WMLC
python corp_etl\main.py
```

Produces: `output/loan_extract_tagged.csv`

### Step 2: Open Dashboard and Load Data (required)

1. Open `WMLC_Dashboard.xlsm`, enable macros
2. If first time: set up Power Query (see POWER_QUERY_SETUP sheet inside the workbook)
   - Load destination: Loan Detail sheet, cell A1
   - Query name: tbl_LoanData
3. Click "Refresh Data" on Dashboard
4. All views recompute automatically

### Step 3: Rebuild Tracker Analytics (optional, when you have a new tracker file)

1. Update `config.yaml` with path to your WMLC Tracker File:
```yaml
input_files:
  wmlc_tracker:
    path: "S:/CreditRisk/Data/WMLC_Tracker.xlsx"
    skip_rows: 0
```
2. Update `config.yaml` with dashboard path:
```yaml
output:
  dashboard: "./WMLC_Dashboard.xlsm"
```
3. Run:
```cmd
python corp_etl\build_tracker_analytics.py
```
4. Reopen the dashboard — Tracker Analytics sheet is updated with real data

### Step 4: Run Tracker Matcher (optional)

```cmd
python corp_etl\tracker_matcher.py
```

Produces: `output/wmlc_tracker_matched.xlsx` with 20 new columns (5 suggestions x 4 fields)

Dependencies: `pip install rapidfuzz` (or `pip install fuzzywuzzy python-Levenshtein` as fallback)

## Configuration

Edit `corp_etl/config.yaml` before running. Update the file paths:

```yaml
input_files:
  base_extract:
    path: "C:/path/to/your/loan_extract.csv"    # <-- change this
  lal_credit:
    path: "C:/path/to/your/LAL_Credit.xlsx"      # <-- change this
    skip_rows: 3                                   # usually 3
  loan_reserve_report:
    path: "C:/path/to/your/Loan_Reserve_Report.xlsx"  # <-- change this
    skip_rows: 6                                       # usually 6
  dar_tracker:
    path: "C:/path/to/your/DAR_Tracker.xlsx"     # <-- change this
    skip_rows: 0
  wmlc_tracker:
    path: "C:/path/to/your/WMLC_Tracker.xlsx"   # <-- optional
    skip_rows: 0

output:
  tagged_file: "./output/loan_extract_tagged.csv"
  tracker_matched: "./output/wmlc_tracker_matched.xlsx"
  dashboard: "./WMLC_Dashboard.xlsm"
```

Use forward slashes in paths even on Windows, or escape backslashes.

## Output

| File | Description |
|------|-------------|
| `output/loan_extract_tagged.csv` | Tagged loan data with WMLC flags |
| `output/etl_run_YYYYMMDD_HHMMSS.log` | Full debug log for troubleshooting |
| `output/wmlc_tracker_matched.xlsx` | Fuzzy match results (Step 4) |

The tagged CSV contains all original columns plus 6 new ones:

| Column | Type | Description |
|--------|------|-------------|
| `is_ntc` | bool | Non-Traditional Client |
| `is_office` | bool | CRE Office property (from DAR Tracker) |
| `has_credit_policy_exception` | bool | Credit policy exception (from LAL Credit) |
| `wmlc_flags` | string | Pipe-delimited flag names (e.g. `"TL-CRE >$75MM\|NTC > $50MM"`) |
| `wmlc_flag_count` | int | Number of WMLC flags |
| `wmlc_qualified` | bool | True if any WMLC flag matched |

## Troubleshooting

### Check the log file first

Every run creates a timestamped log file in `output/`. The log contains:
- DEBUG: Full data samples, column names, config dump
- INFO: Row counts, match summaries, flag counts
- WARNING: Missing columns, fuzzy matches, empty inputs
- ERROR: File read failures, missing columns (with full traceback)

```bash
# Find the latest log
dir output\etl_run_*.log /O-D
# Open it
notepad output\etl_run_20260323_143000.log
```

### Common Issues

| Symptom | Check in log | Fix |
|---------|-------------|-----|
| `FILE NOT FOUND` | Wrong path in config.yaml | Update config.yaml paths |
| `Column 'X' not found` | Column name mismatch | The ETL tries fuzzy matching automatically — check the WARNING lines |
| 0 WMLC-qualified loans | No flags triggered | Verify base extract has correct column names and data |
| Missing NTC tags | LAL_Credit or Loan_Reserve_Report failed | Check those files exist and have correct skip_rows |
| Missing is_office tags | DAR_Tracker failed | Check DAR_Tracker path and Property_Type column |

### Graceful degradation

The ETL will **never crash** on missing or malformed external files. If LAL_Credit, Loan_Reserve_Report, or DAR_Tracker cannot be read, the pipeline continues with warnings. The corresponding intermediate tags will be False for all rows, and any WMLC flags that depend on those tags will not fire. The log will clearly state which files were skipped.

## Support

If the log file does not explain the issue, send the full `.log` file to the developer. It contains all diagnostic information needed for remote troubleshooting.
