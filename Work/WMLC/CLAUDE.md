# WMLC Dashboard Pipeline — Orchestrator Instructions

## Project Overview

Build a Python-to-Excel pipeline that:
1. Generates realistic proxy data for a wealth management loan portfolio
2. Runs a corp-deployable ETL that tags loans with WMLC (Wealth Management Lending Committee) flags
3. Produces a fully formatted Excel dashboard (.xlsm) with VBA macros, Power Query, and interactive drill-down

**You are the ORCHESTRATOR.** You plan, coordinate, and validate. You do NOT write large code blocks yourself. You dispatch sub-agents for all implementation work and validate their outputs before proceeding.

---

## Critical Files — Read These First

Before doing ANYTHING, read these spec files in order:

1. `specs/WMLC_LOGIC.md` — Authoritative WMLC flag definitions (15 flags, intermediate tags, external file specs)
2. `specs/ARCHITECTURE.md` — Two-part codebase design (corp ETL + local dashboard), Excel workbook spec, Power Query design
3. `specs/AGENT_PLAN.md` — Sub-agent roles, handoff contracts, test suite

---

## Agent Dispatch Protocol

You have 4 logical sub-agents. With `AGENT_TEAMS=2`, dispatch ONE sub-agent at a time. After each completes, validate its handoff artifacts before dispatching the next.

### Phase 1: DATA GEN (Sub-Agent 1)

**Dispatch instructions for sub-agent:**
```
Read specs/WMLC_LOGIC.md and specs/AGENT_PLAN.md (Sub-Agent 1 section).
Read skills/PROXY_DATA_SKILL.md for guidance.

Generate all proxy data files:
- proxy_data/loan_extract.csv (500+ rows, all columns from schema below)
- proxy_data/LAL_Credit.xlsx (80+ rows, 3 skip rows of filler before header)
- proxy_data/Loan_Reserve_Report.xlsx (100+ rows, 6 skip rows before header)
- proxy_data/DAR_Tracker.xlsx (60+ rows, no skip rows)

Schema for loan_extract.csv — include ALL of these columns:
  tl_facility_digits12 (string, 12-digit zero-padded)
  facility_id (string)
  account_number (string, 12-digit zero-padded)
  key_acct (string, format "###-######")
  borrower (string, realistic names)
  name (string, guarantor names)
  sub_product_norm (string)
  product_bucket (string — must use EXACTLY these 14 values evenly distributed):
    LAL Diversified, LAL Highly Conc., LAL NFPs, RESI,
    TL Aircraft, TL CRE, TL Life Insurance, TL Multicollateral,
    TL Other Secured, TL PHA, TL SBL Diversified, TL SBL Highly Conc., TL Unsecured
  is_lal_nfp (boolean)
  focus_list (string — mostly blank, ~5% "Non-Pass")
  txt_mstr_facil_collateral_desc (string — realistic collateral text, MUST include
    instances of: "Hedge Fund", "Privately Held", "Aircraft", "Fine Art",
    "Unsecured", "Other Secured" scattered across appropriate product_buckets)
  SBL_PERC (float, 0-100)
  book_date (datetime)
  effective_date (datetime)
  origination_date (datetime)
  balance (float)
  credit_limit (float)
  amt_original_comt (float)
  credit_lii (float — THIS IS THE KEY COMMITMENT FIGURE. Distribute across full
    bucket ladder from $1 to $1B+. Ensure many rows cross WMLC thresholds:
    $10MM, $35MM, $50MM, $75MM, $100MM, $300MM)
  NEW_CAMP_YN (string, "Y" or "N" — at least 30% should be "Y")
  NEW_CAMP_REASON (string — "New" or "Increase Facility" if Y, blank if N)
  base_commit (float)
  latest_commit (float)
  commit_delta (float)
  new_commitment_amount (float)
  new_commitment_reason (string)
  credit_lii_commitment_bucket (string — human-readable bucket label matching credit_lii)
  credit_lii_commitment_floor (int — numeric floor for sort order)

Bucket ladder (credit_lii_commitment_bucket values, map from credit_lii):
  "$1,000,000,000"          floor=1000000000   (credit_lii >= 1,000,000,000)
  "$750,000,000"            floor=750000000
  "$700,000,000"            floor=700000000
  "$600,000,000"            floor=600000000
  "$500,000,000"            floor=500000000
  "$400,000,000"            floor=400000000
  "$350,000,000"            floor=350000000
  "$300,000,000"            floor=300000000
  "$250,000,000"            floor=250000000
  "$200,000,000"            floor=200000000
  "$175,000,000"            floor=175000000
  "$150,000,000"            floor=150000000
  "$125,000,000"            floor=125000000
  "$100,000,000"            floor=100000000
  "$75,000,000"             floor=75000000
  "$50,000,000"             floor=50000000
  "$40,000,000"             floor=40000000
  "$35,000,000"             floor=35000000
  "$30,000,000"             floor=30000000
  "$25,000,000"             floor=25000000
  "$20,000,000"             floor=20000000
  "$15,000,000"             floor=15000000
  "$10,000,001"             floor=10000001
  "$1"                      floor=1           (credit_lii >= 1 and < 10,000,001)

CRITICAL DATA REQUIREMENTS:
- At least 15% of loans must qualify for at least one WMLC flag
- Include 5+ loans that qualify for MULTIPLE flags
- Include NTC-qualifying entities in LAL_Credit and Loan_Reserve_Report
- Include "Office" properties in DAR_Tracker
- Include Non-Pass + NEW_CAMP_YN="Y" combinations
- Collateral descriptions must be realistic multi-word strings
```

**Validation before proceeding:**
- [ ] All 4 proxy files exist
- [ ] loan_extract.csv has 500+ rows and all listed columns
- [ ] All 14 product_buckets present (verify with value_counts)
- [ ] credit_lii spans from <$10M to >$300M
- [ ] External files have correct skip-row structure

---

### Phase 2: ETL LOGIC (Sub-Agent 2)

**Dispatch instructions for sub-agent:**
```
Read specs/WMLC_LOGIC.md (AUTHORITATIVE — implement exactly as written).
Read specs/ARCHITECTURE.md (corp_etl section).
Read skills/ETL_SKILL.md for guidance.

Build the corp_etl/ package:
  corp_etl/
  ├── __init__.py
  ├── main.py              # Entry point: load config, read files, tag, output
  ├── config.yaml          # Points to proxy_data/ files
  ├── column_matcher.py    # Fuzzy column matching utility
  ├── readers/
  │   ├── __init__.py
  │   ├── base_reader.py
  │   ├── lal_credit_reader.py
  │   ├── loan_reserve_reader.py
  │   └── dar_tracker_reader.py
  ├── taggers/
  │   ├── __init__.py
  │   ├── intermediate_tags.py   # is_ntc, is_office, has_credit_policy_exception
  │   └── wmlc_tagger.py         # All 15 WMLC flags
  └── tests/
      └── test_wmlc_flags.py

Key requirements:
- config.yaml drives ALL file paths and column name hints
- Column matching: try exact match → case-insensitive → warn+skip. NEVER hard fail.
- All readers must handle the skip_rows and key_format (strip dash, pad to 12) per file
- wmlc_tagger.py: evaluate all 15 flags independently, store as pipe-delimited string
- Output: loan_extract_tagged.csv in output/ with new columns:
    is_ntc, is_office, has_credit_policy_exception,
    wmlc_flags, wmlc_flag_count, wmlc_qualified
- Run main.py against proxy_data/ and verify output has correct columns and flag counts
- Print summary: how many loans matched each flag

After building, RUN the ETL: python corp_etl/main.py
Verify the output file exists and has the expected columns.
```

**Validation before proceeding:**
- [ ] `output/loan_extract_tagged.csv` exists
- [ ] Has all 6 new columns (is_ntc, is_office, has_credit_policy_exception, wmlc_flags, wmlc_flag_count, wmlc_qualified)
- [ ] `wmlc_flag_count` > 0 for at least 50 rows
- [ ] At least 8 distinct WMLC flag types appear in `wmlc_flags` column
- [ ] No Python errors during execution

---

### Phase 3: EXCEL/VBA (Sub-Agent 3)

**Dispatch instructions for sub-agent:**
```
Read specs/ARCHITECTURE.md (Excel Workbook Specification section — this is your blueprint).
Read specs/WMLC_LOGIC.md (for flag names used in WMLC filter).
Read skills/EXCEL_VBA_SKILL.md for guidance.

Build scripts/build_dashboard.py that:
1. Reads output/loan_extract_tagged.csv
2. Creates WMLC_Dashboard.xlsm using openpyxl with these sheets:

SHEET: _data (hidden)
- Load full tagged dataset as a table named "tbl_LoanData"
- All columns from the tagged CSV
- This sheet will be the Power Query landing zone (PQ code stored separately)

SHEET: Dashboard
- Row 1: "Current Portfolio Snapshot, $" header merged across columns
- Row 2: View indicator text (updates via VBA)
- Row 3: Column headers — "Gross Amount", "Defined Range", then all 14 product_bucket names, then "Total"
- Rows 4+: One row per commitment bucket (descending from $1,000,000,000 to $1)
- Last data row: "Total" row with column sums
- Cell values: initially populated with Summary Commitment view
  (SUMPRODUCT formulas or VBA-computed values referencing _data)
- Formatting:
  - Bold headers, light green fill on header row (match screenshot aesthetic)
  - Currency format ($#,##0) for commitment views, #,##0 for count views
  - Thin black borders on all data cells
  - Column A (Gross Amount) bold, right-aligned
  - Column B (Defined Range) italic
  - Freeze panes at row 3, column C
  - Column widths auto-fit but minimum 14 for data columns

SHEET: loan_detail
- Row 1: Title "Loan Detail" merged
- Row 2: Navigation buttons placeholder (VBA will create actual buttons)
- Row 3: Column headers from tagged CSV
- Row 4+: All loan data
- AutoFilter enabled on row 3
- Freeze panes at row 3

VBA MODULES (inject using openpyxl vbaProject or write .bas content):

mod_ViewToggle:
- Stores state in named ranges: "ViewState" (1-4) and "WMLCState" (0=off, 1=on)
- ViewState: 1=Summary Count, 2=Summary Commitment, 3=Summary Count NEW, 4=Summary Commitment NEW
- 6 button handlers: ShowSummaryCount(), ShowSummaryCommitment(),
  ShowSummaryCountNew(), ShowSummaryCommitmentNew(), WMLCOn(), WMLCOff()
- Each handler updates ViewState/WMLCState then calls RefreshDashboard()
- RefreshDashboard():
  - Reads _data sheet tbl_LoanData
  - For each cell in matrix: COUNTIFS/SUMIFS based on product_bucket + bucket range
  - If WMLC ON: adds wmlc_qualified = TRUE condition
  - If NEW view: adds NEW_CAMP_YN = "Y" condition
  - Updates view indicator text in row 2
  - Applies number format (currency for commitment, number for count)

mod_CellClick:
- Worksheet_SelectionChange event on Dashboard sheet
- Detect if click is in data area (row >= 4, col >= 3, col <= 16)
- Read bucket from column A of clicked row
- Read product_bucket from row 3 of clicked column
- Switch to loan_detail sheet
- Clear existing AutoFilter
- Apply AutoFilter on credit_lii_commitment_bucket = bucket
  AND product_bucket = product_bucket
  AND (if WMLC ON) wmlc_qualified = TRUE
  AND (if NEW view) NEW_CAMP_YN = "Y"

mod_Navigation:
- BackToDashboard(): clear loan_detail filters, activate Dashboard sheet
- ResetFilters(): clear AutoFilter on active sheet
- ResetDashboard(): set ViewState=2 (commitment), WMLCState=0, call RefreshDashboard()

BUTTONS (Form Controls or Shapes with macro assignment):
Dashboard sheet:
- 4 view toggle buttons in a row above the matrix (top-right area)
- 2 WMLC buttons (ON/OFF) next to view buttons
- 1 Reset button

loan_detail sheet:
- "← Back to Dashboard" button (top-left)
- "Reset Filters" button (next to it)

POWER QUERY:
- Create M code in templates/power_query/load_tagged_data.m
- The M code should load from a CSV path stored in a named range "DataSourcePath"
- Use try...otherwise for column type assignments
- Embed the connection in the workbook if possible via openpyxl,
  OR create the M code file and document that user must import it manually

After building, RUN: python scripts/build_dashboard.py
Verify the .xlsm file is created in output/
```

**Validation before proceeding:**
- [ ] `output/WMLC_Dashboard.xlsm` exists and is > 50KB
- [ ] File is a valid ZIP (xlsm format)
- [ ] Contains sheets: Dashboard, loan_detail, _data
- [ ] VBA modules are injected (check vbaProject.bin exists)
- [ ] Dashboard has the correct number of rows and columns

---

### Phase 4: TESTING (Sub-Agent 4)

**Dispatch instructions for sub-agent:**
```
Read specs/AGENT_PLAN.md (Sub-Agent 4 Testing section).
Read skills/TESTING_SKILL.md for guidance.

Run ALL automated tests and produce output/test_report.md.

Test categories:

1. DATA GEN VALIDATION
   - Verify all proxy files exist with minimum row counts
   - Verify all 14 product_buckets present in loan_extract.csv
   - Verify credit_lii spans expected range
   - Verify account_number format (12-digit, zero-padded)
   - Verify bucket labels match credit_lii values

2. ETL VALIDATION
   - Run corp_etl/main.py (should succeed with exit code 0)
   - Verify output CSV has all required new columns
   - Verify wmlc_flags only contains valid flag names from WMLC_LOGIC.md
   - Verify wmlc_flag_count == count of pipes in wmlc_flags + 1 (when non-empty)
   - Verify wmlc_qualified == True iff wmlc_flag_count > 0
   - Create 5 known-answer test loans, run through tagger, verify exact flag output

3. EXCEL STRUCTURAL VALIDATION
   - Verify .xlsm is valid ZIP
   - Verify sheet names present
   - Verify Dashboard row/column counts match expected
   - Verify _data sheet has correct number of data rows
   - Verify loan_detail has all columns from tagged CSV
   - Check for vbaProject.bin in ZIP contents

4. PRODUCE MANUAL TEST CHECKLIST
   - Write a checklist section in test_report.md for the human to verify:
     VBA button functionality, cell-click drill-down, filter behavior, WMLC toggle

Write results to output/test_report.md with pass/fail checkboxes.
If any tests fail, document the failure clearly with expected vs actual values.
```

**After testing:** Review test_report.md. If failures exist, re-dispatch the responsible sub-agent with the specific failure details.

---

## Handoff Validation Script

After EACH phase, run this validation before proceeding:

```python
# Quick validation — orchestrator runs this mentally or via bash
import os
import pandas as pd

# Phase 1 check
assert os.path.exists("proxy_data/loan_extract.csv")
df = pd.read_csv("proxy_data/loan_extract.csv")
assert len(df) >= 400
assert len(df["product_bucket"].unique()) >= 14

# Phase 2 check
assert os.path.exists("output/loan_extract_tagged.csv")
tagged = pd.read_csv("output/loan_extract_tagged.csv")
assert "wmlc_flags" in tagged.columns
assert tagged["wmlc_qualified"].sum() > 0

# Phase 3 check
assert os.path.exists("output/WMLC_Dashboard.xlsm")
assert os.path.getsize("output/WMLC_Dashboard.xlsm") > 50000
```

---

## File Locations (repo root = this directory)

```
CLAUDE.md                          ← YOU ARE HERE (orchestrator reads this)
specs/
  WMLC_LOGIC.md                    ← Flag definitions (source of truth)
  ARCHITECTURE.md                  ← System design
  AGENT_PLAN.md                    ← Agent roles + handoffs
skills/
  PROXY_DATA_SKILL.md              ← Guidance for data generation
  ETL_SKILL.md                     ← Guidance for ETL development
  EXCEL_VBA_SKILL.md               ← Guidance for Excel/VBA build
  TESTING_SKILL.md                 ← Guidance for test suite
corp_etl/                          ← Corp-deployable ETL package
proxy_data/                        ← Generated test data
scripts/                           ← Local build scripts
templates/                         ← VBA and PQ templates
output/                            ← Final artifacts land here
```

---

## Rules for the Orchestrator

1. **Read all 3 spec files before dispatching any agent.**
2. **Dispatch one agent at a time.** Wait for completion and validate before next.
3. **Never write more than 20 lines of code yourself.** Delegate to sub-agents.
4. **If a phase fails, diagnose which sub-agent's output is wrong and re-dispatch ONLY that agent** with specific instructions about what to fix.
5. **After Phase 4 (testing), if test_report.md shows failures, loop back** to the failing phase's agent. Maximum 3 retry loops per phase.
6. **Final deliverable check:** Before declaring done, verify these files exist:
   - `output/WMLC_Dashboard.xlsm` (the dashboard)
   - `output/loan_extract_tagged.csv` (tagged data)
   - `output/test_report.md` (test results)
   - `corp_etl/main.py` (deployable ETL)
   - `corp_etl/config.yaml` (corp config template)
