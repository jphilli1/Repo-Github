# SKILL: Corp ETL Development

## Objective
Build a config-driven, auditable Python ETL that reads loan extracts + lookup files,
tags loans with intermediate flags and WMLC classifications, and outputs a tagged CSV.

## Tools
- Python 3.x, pandas, openpyxl, pyyaml
- No exotic dependencies — this runs in a locked-down corp environment

## Key Principles

### Config-Driven Everything
```yaml
# config.yaml — ALL paths and column hints live here
input_files:
  base_extract:
    path: "./proxy_data/loan_extract.csv"
  lal_credit:
    path: "./proxy_data/LAL_Credit.xlsx"
    skip_rows: 3
    key_column: "Account Number"
    key_format: "strip_dash_pad12"
  loan_reserve_report:
    path: "./proxy_data/Loan_Reserve_Report.xlsx"
    skip_rows: 6
    key_column: "Facility Account Number"
    key_format: "pad12"
  dar_tracker:
    path: "./proxy_data/DAR_Tracker.xlsx"
    skip_rows: 0
    key_column: "Facility ID"
    key_format: "pad12"
output:
  tagged_file: "./output/loan_extract_tagged.csv"
```

### Fuzzy Column Matching
Never hard-fail on column names. Implement a matcher:

```python
def find_column(df, preferred, alternates=None):
    """Find column by exact match, then case-insensitive, then warn."""
    all_names = [preferred] + (alternates or [])
    # Try exact
    for name in all_names:
        if name in df.columns:
            return name
    # Try case-insensitive
    col_map = {c.lower().replace(" ", "_"): c for c in df.columns}
    for name in all_names:
        normalized = name.lower().replace(" ", "_")
        if normalized in col_map:
            return col_map[normalized]
    # Warn and return None
    print(f"WARNING: Could not find column '{preferred}' or alternates in {list(df.columns)}")
    return None
```

### Key Format Handlers
```python
def strip_dash_pad12(val):
    """LAL_Credit format: '001-234567' → '000001234567'"""
    cleaned = str(val).replace("-", "").strip()
    return cleaned.zfill(12)

def pad12(val):
    """Zero-pad to 12 digits"""
    return str(val).strip().zfill(12)
```

### Reader Pattern
Each reader follows the same pattern:
1. Read file with skip_rows from config
2. Normalize key column using format handler
3. Return a clean DataFrame with standardized key column name

### Intermediate Tags (implement in taggers/intermediate_tags.py)
See specs/WMLC_LOGIC.md for exact logic. Three tags:
- `is_ntc`: Different logic for LAL vs TL product_buckets
- `is_office`: Join to DAR_Tracker on facility key, check Property_Type
- `has_credit_policy_exception`: Join to LAL_Credit, check 4 exception columns

### WMLC Tagger (implement in taggers/wmlc_tagger.py)
See specs/WMLC_LOGIC.md for all 15 flag definitions.
- Evaluate ALL flags independently for each row
- Store results as pipe-delimited string in `wmlc_flags`
- Compute `wmlc_flag_count` and `wmlc_qualified` from flags

### Logging
Print a summary after tagging:
```
=== WMLC Flag Summary ===
NTC > $50MM:                  12 loans
Non-Pass Originations >$0MM:   5 loans
TL-CRE >$75MM:                8 loans
...
Total WMLC qualified:         67 loans (13.4%)
```

## Anti-Patterns to Avoid
- Do NOT hardcode file paths anywhere except config.yaml
- Do NOT use `assert` for data validation — use `if/else` with warnings
- Do NOT merge external files into base in a way that drops rows (use left join)
- Do NOT modify the base extract columns — only ADD new columns

## Output Checklist
- [ ] corp_etl/main.py runs cleanly with `python corp_etl/main.py`
- [ ] Output CSV has all original columns plus 6 new ones
- [ ] wmlc_flags contains only valid flag names from WMLC_LOGIC.md
- [ ] Summary printout shows non-zero counts for multiple flag types
- [ ] config.yaml is the ONLY place file paths appear
