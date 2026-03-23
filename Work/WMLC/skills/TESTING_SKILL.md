# SKILL: Testing & Validation

## Objective
Validate all pipeline artifacts — proxy data, ETL output, and Excel workbook —
and produce a structured test report with pass/fail results.

## Tools
- Python 3.x, pandas, openpyxl, zipfile, os
- No external test frameworks needed — simple assert-based checks with try/except

## Test Architecture

Write a single `scripts/run_tests.py` that executes all tests and writes results
to `output/test_report.md`.

```python
class TestResult:
    def __init__(self, name, passed, details=""):
        self.name = name
        self.passed = passed
        self.details = details

results = []

# Run tests, collect results
results.append(test_proxy_data_exists())
results.append(test_proxy_data_columns())
# ... etc

# Write report
write_report(results, "output/test_report.md")
```

## Test Categories

### 1. Proxy Data Validation

```python
def test_proxy_data_exists():
    files = [
        "proxy_data/loan_extract.csv",
        "proxy_data/LAL_Credit.xlsx",
        "proxy_data/Loan_Reserve_Report.xlsx",
        "proxy_data/DAR_Tracker.xlsx"
    ]
    for f in files:
        assert os.path.exists(f), f"Missing: {f}"

def test_proxy_row_counts():
    df = pd.read_csv("proxy_data/loan_extract.csv")
    assert len(df) >= 400, f"loan_extract has only {len(df)} rows"

def test_product_bucket_coverage():
    df = pd.read_csv("proxy_data/loan_extract.csv")
    expected = {
        "LAL Diversified", "LAL Highly Conc.", "LAL NFPs", "RESI",
        "TL Aircraft", "TL CRE", "TL Life Insurance", "TL Multicollateral",
        "TL Other Secured", "TL PHA", "TL SBL Diversified",
        "TL SBL Highly Conc.", "TL Unsecured"
    }
    actual = set(df["product_bucket"].unique())
    missing = expected - actual
    assert len(missing) == 0, f"Missing product_buckets: {missing}"

def test_account_number_format():
    df = pd.read_csv("proxy_data/loan_extract.csv", dtype=str)
    bad = df[df["account_number"].str.len() != 12]
    assert len(bad) == 0, f"{len(bad)} rows have non-12-digit account_number"

def test_credit_lii_range():
    df = pd.read_csv("proxy_data/loan_extract.csv")
    assert df["credit_lii"].min() < 10_000_000, "No small loans"
    assert df["credit_lii"].max() > 100_000_000, "No large loans"

def test_bucket_consistency():
    """Verify credit_lii_commitment_bucket matches credit_lii value"""
    df = pd.read_csv("proxy_data/loan_extract.csv")
    # Spot-check: loans with credit_lii >= 300M should be in $300M+ bucket
    big = df[df["credit_lii"] >= 300_000_000]
    valid_buckets = {"$300,000,000", "$350,000,000", "$400,000,000",
                     "$500,000,000", "$600,000,000", "$700,000,000",
                     "$750,000,000", "$1,000,000,000"}
    bad = big[~big["credit_lii_commitment_bucket"].isin(valid_buckets)]
    assert len(bad) == 0, f"{len(bad)} large loans in wrong bucket"
```

### 2. ETL Validation

```python
def test_etl_runs():
    """Run corp ETL and verify exit code 0"""
    import subprocess
    result = subprocess.run(["python", "corp_etl/main.py"], capture_output=True, text=True)
    assert result.returncode == 0, f"ETL failed:\n{result.stderr}"

def test_tagged_columns():
    df = pd.read_csv("output/loan_extract_tagged.csv")
    required = ["is_ntc", "is_office", "has_credit_policy_exception",
                "wmlc_flags", "wmlc_flag_count", "wmlc_qualified"]
    missing = [c for c in required if c not in df.columns]
    assert len(missing) == 0, f"Missing columns: {missing}"

def test_wmlc_flag_validity():
    """All flag names must be from the approved list"""
    valid_flags = {
        "NTC > $50MM", "Non-Pass Originations >$0MM",
        "TL-CRE >$75MM", "TL-CRE Office >$10MM",
        "TL-SBL-D >$300MM", "TL-SBL-C >$100MM", "TL-LIC >$100MM",
        "TL-Alts HF/PE >$35MM", "TL-Alts Private Shares >$35MM",
        "TL-Alts Unsecured >$35MM", "TL-Alts PAF >$50MM",
        "TL-Alts Fine Art >$50MM", "TL-Alts Other Secured >$50MM",
        "LAL-D >$300MM", "LAL-C >$100MM"
    }
    df = pd.read_csv("output/loan_extract_tagged.csv")
    flagged = df[df["wmlc_flags"].notna() & (df["wmlc_flags"] != "")]
    for _, row in flagged.iterrows():
        flags = set(row["wmlc_flags"].split("|"))
        invalid = flags - valid_flags
        assert len(invalid) == 0, f"Invalid flags: {invalid} on row {row.name}"

def test_wmlc_consistency():
    """flag_count matches pipe count, qualified matches count > 0"""
    df = pd.read_csv("output/loan_extract_tagged.csv")
    for _, row in df.iterrows():
        flags = row["wmlc_flags"]
        count = row["wmlc_flag_count"]
        qualified = row["wmlc_qualified"]

        if pd.isna(flags) or flags == "":
            assert count == 0, f"Row {row.name}: empty flags but count={count}"
            assert not qualified, f"Row {row.name}: empty flags but qualified=True"
        else:
            expected_count = len(flags.split("|"))
            assert count == expected_count, \
                f"Row {row.name}: flag_count={count} but has {expected_count} flags"
            assert qualified, f"Row {row.name}: has flags but qualified=False"

def test_wmlc_coverage():
    """At least 8 distinct flag types should appear"""
    df = pd.read_csv("output/loan_extract_tagged.csv")
    all_flags = set()
    for flags in df["wmlc_flags"].dropna():
        if flags:
            all_flags.update(flags.split("|"))
    assert len(all_flags) >= 8, f"Only {len(all_flags)} distinct flags: {all_flags}"
```

### 3. Excel Structural Validation

```python
def test_excel_exists():
    # Check for either .xlsm or .xlsx
    assert os.path.exists("output/WMLC_Dashboard.xlsm") or \
           os.path.exists("output/WMLC_Dashboard.xlsx"), "Dashboard file missing"

def test_excel_valid_zip():
    import zipfile
    path = "output/WMLC_Dashboard.xlsm" if os.path.exists("output/WMLC_Dashboard.xlsm") \
           else "output/WMLC_Dashboard.xlsx"
    assert zipfile.is_zipfile(path), "File is not a valid ZIP/XLSX"

def test_excel_sheets():
    path = "output/WMLC_Dashboard.xlsm" if os.path.exists("output/WMLC_Dashboard.xlsm") \
           else "output/WMLC_Dashboard.xlsx"
    wb = openpyxl.load_workbook(path, read_only=True)
    required = {"Dashboard", "loan_detail", "_data"}
    actual = set(wb.sheetnames)
    missing = required - actual
    assert len(missing) == 0, f"Missing sheets: {missing}"
    wb.close()

def test_dashboard_dimensions():
    path = "output/WMLC_Dashboard.xlsm" if os.path.exists("output/WMLC_Dashboard.xlsm") \
           else "output/WMLC_Dashboard.xlsx"
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb["Dashboard"]
    # Should have 28+ rows (3 header + 24 data + 1 total)
    assert ws.max_row >= 27, f"Dashboard has only {ws.max_row} rows"
    # Should have 16 columns (A through P)
    assert ws.max_column >= 16, f"Dashboard has only {ws.max_column} columns"
    wb.close()

def test_view_sheets_exist():
    path = "output/WMLC_Dashboard.xlsm" if os.path.exists("output/WMLC_Dashboard.xlsm") \
           else "output/WMLC_Dashboard.xlsx"
    wb = openpyxl.load_workbook(path, read_only=True)
    for i in range(1, 9):
        sheet_name = f"_view{i}"
        assert sheet_name in wb.sheetnames, f"Missing hidden sheet: {sheet_name}"
    wb.close()

def test_vba_modules_exist():
    """Check that .bas files were generated for manual import"""
    required = [
        "templates/vba_modules/mod_ViewToggle.bas",
        "templates/vba_modules/mod_CellClick.bas",
        "templates/vba_modules/mod_Navigation.bas",
    ]
    missing = [f for f in required if not os.path.exists(f)]
    assert len(missing) == 0, f"Missing VBA files: {missing}"
```

### 4. Manual Test Checklist (appended to report)

Always include this section in the test report even if all automated tests pass:

```markdown
## Manual Test Checklist (Human Verification Required)

### Setup
- [ ] Open WMLC_Dashboard.xlsm (or .xlsx + import VBA modules)
- [ ] Enable macros when prompted
- [ ] If .xlsx: import all .bas files from templates/vba_modules/

### Dashboard View Toggles
- [ ] Click "Summary Count" → values change to integer counts
- [ ] Click "Summary Commitment" → values change to currency amounts
- [ ] Click "Summary Count NEW" → counts reflect only NEW_CAMP_YN="Y"
- [ ] Click "Summary Commitment NEW" → amounts reflect only NEW loans
- [ ] Verify view indicator text (Row 2) updates with each toggle
- [ ] Verify total row/column recalculate correctly

### WMLC Toggle
- [ ] Click "WMLC ON" → matrix values decrease (fewer qualifying loans)
- [ ] Click "WMLC OFF" → matrix returns to full population
- [ ] Verify WMLC state persists across view toggles

### Cell Click Drill-Down
- [ ] Click a non-zero cell in the Dashboard matrix
- [ ] Verify: jumps to loan_detail sheet
- [ ] Verify: loan_detail is filtered to correct product_bucket and bucket
- [ ] Verify: if WMLC ON, only wmlc_qualified loans shown
- [ ] Verify: if NEW view active, only NEW_CAMP_YN="Y" loans shown
- [ ] Click a zero-value cell → verify no data rows shown (correct empty filter)

### Navigation
- [ ] On loan_detail: click "Back to Dashboard" → returns to Dashboard
- [ ] Verify filters are cleared after returning
- [ ] On loan_detail: click "Reset Filters" → all filters cleared
- [ ] Click "Reset" on Dashboard → returns to Summary Commitment, WMLC OFF

### Data Integrity
- [ ] Spot-check 3 cells: manually count/sum loans in _data matching the bucket
- [ ] Verify loan_detail has ALL columns from the tagged CSV
- [ ] Verify _data sheet row count matches loan_extract_tagged.csv
```

## Report Format

```markdown
# WMLC Pipeline Test Report
Generated: {timestamp}

## Automated Test Results

| # | Test | Result | Details |
|---|------|--------|---------|
| 1 | Proxy data exists | ✅ PASS | |
| 2 | Proxy row counts | ✅ PASS | 523 rows |
| 3 | Product bucket coverage | ✅ PASS | 14/14 buckets |
| 4 | ETL runs cleanly | ❌ FAIL | KeyError: 'product_bucket' |
...

## Summary
Passed: X / Y
Failed: Z

## Manual Test Checklist
(as above)
```
