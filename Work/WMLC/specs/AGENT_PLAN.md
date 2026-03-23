# Agent Team Plan: WMLC Dashboard Pipeline

## Agent Architecture

```
                    ┌──────────────────────┐
                    │    ORCHESTRATOR       │
                    │  (plans, coordinates, │
                    │   validates handoffs) │
                    └──────────┬───────────┘
                               │
            ┌──────────────────┼──────────────────┐
            │                  │                   │
   ┌────────▼────────┐ ┌──────▼──────┐  ┌────────▼────────┐
   │  DATA GEN       │ │  ETL LOGIC  │  │  EXCEL/VBA      │
   │  Sub-agent 1    │ │  Sub-agent 2│  │  Sub-agent 3    │
   │                 │ │             │  │                  │
   │ proxy data,     │ │ readers,    │  │ openpyxl build,  │
   │ schema fidelity │ │ taggers,    │  │ VBA injection,   │
   │                 │ │ WMLC logic  │  │ Power Query M,   │
   │                 │ │             │  │ formatting       │
   └────────┬────────┘ └──────┬──────┘  └────────┬────────┘
            │                  │                   │
            └──────────────────┼───────────────────┘
                               │
                      ┌────────▼────────┐
                      │    TESTING       │
                      │   Sub-agent 4    │
                      │                  │
                      │ validates data,  │
                      │ checks VBA,      │
                      │ runs assertions  │
                      └─────────────────┘
```

---

## Orchestrator (`CLAUDE.md`)

### Role
- **Does NOT write code directly** (except minor config edits)
- Plans execution order
- Defines handoff contracts (what each agent produces and where)
- Validates that handoff artifacts exist and pass basic checks before next agent runs
- Coordinates re-runs when testing agent finds issues

### Execution Sequence

```
Phase 1: DATA GEN
  → Produces: proxy CSV files matching all schemas
  → Handoff artifact: /local_dashboard/proxy_data/*.csv
  → Validation: orchestrator checks file existence, row counts, column names

Phase 2: ETL LOGIC
  → Consumes: proxy data from Phase 1
  → Produces: corp_etl/ Python package + loan_extract_tagged.csv
  → Handoff artifact: /local_dashboard/output/loan_extract_tagged.csv
  → Validation: orchestrator checks WMLC columns exist, flag counts > 0

Phase 3: EXCEL/VBA
  → Consumes: tagged CSV from Phase 2
  → Produces: WMLC_Dashboard.xlsm with all sheets, VBA, PQ
  → Handoff artifact: /local_dashboard/output/WMLC_Dashboard.xlsm
  → Validation: orchestrator checks file size > 0, sheet names present

Phase 4: TESTING
  → Consumes: all artifacts from Phases 1-3
  → Produces: test_report.md with pass/fail for each check
  → Handoff artifact: /local_dashboard/output/test_report.md
  → Validation: orchestrator reads report, re-dispatches failed phases
```

---

## Sub-Agent 1: Data Gen

### Inputs
- `schema.txt` (base table schema)
- `file_names_to_import.txt` (external file schemas)
- `WMLC_LOGIC.md` (to ensure proxy data triggers flags)

### Outputs
- `proxy_data/loan_extract.csv` — ~500 rows, all schema columns, realistic distributions
- `proxy_data/LAL_Credit.xlsx` — ~80 rows, proper header + 3 skip rows
- `proxy_data/Loan_Reserve_Report.xlsx` — ~100 rows, proper header + 6 skip rows
- `proxy_data/DAR_Tracker.xlsx` — ~60 rows, includes "Office" property types
- `proxy_data/config.yaml` — pre-configured to point at proxy files

### Key Requirements
- Product_bucket distribution must cover all 14 buckets
- At least 30% of rows should have `NEW_CAMP_YN == "Y"`
- At least 15% should qualify for at least one WMLC flag
- Include edge cases: loans at exact thresholds, multi-flag loans, NTC+size combos
- `credit_lii_commitment_bucket` and `credit_lii_commitment_floor` must be consistent with `credit_lii`
- Account numbers must be 12-digit zero-padded strings
- Collateral descriptions must include realistic text for Hedge, Privately Held, Aircraft, Fine Art, Unsecured, Other

### Handoff Contract
```yaml
handoff:
  produces:
    - path: proxy_data/loan_extract.csv
      min_rows: 400
      required_columns: [tl_facility_digits12, account_number, product_bucket, credit_lii, NEW_CAMP_YN, wmlc_qualified]
    - path: proxy_data/LAL_Credit.xlsx
      min_rows: 50
    - path: proxy_data/Loan_Reserve_Report.xlsx
      min_rows: 50
    - path: proxy_data/DAR_Tracker.xlsx
      min_rows: 30
    - path: proxy_data/config.yaml
```

---

## Sub-Agent 2: ETL Logic

### Inputs
- `WMLC_LOGIC.md` (authoritative flag definitions)
- `ARCHITECTURE.md` (corp_etl structure)
- Proxy data from Sub-Agent 1

### Outputs
- Complete `corp_etl/` package (main.py, readers/, taggers/, config.yaml)
- `output/loan_extract_tagged.csv` — base + all intermediate tags + WMLC columns

### Key Requirements
- Fuzzy column matching (case-insensitive, hint-based)
- All 15 WMLC flags evaluated independently
- `wmlc_flags` column is pipe-delimited string of all matched flag names
- `wmlc_qualified` is boolean (True if any flag matched)
- Logging: print which flags fired and how many loans matched each
- Unit tests in `tests/test_wmlc_flags.py` with at least 3 known-answer rows per flag

### Handoff Contract
```yaml
handoff:
  consumes:
    - path: proxy_data/loan_extract.csv
    - path: proxy_data/LAL_Credit.xlsx
    - path: proxy_data/Loan_Reserve_Report.xlsx
    - path: proxy_data/DAR_Tracker.xlsx
  produces:
    - path: output/loan_extract_tagged.csv
      required_new_columns: [is_ntc, is_office, has_credit_policy_exception, wmlc_flags, wmlc_flag_count, wmlc_qualified]
    - path: corp_etl/main.py
    - path: corp_etl/config.yaml
```

---

## Sub-Agent 3: Excel/VBA

### Inputs
- `output/loan_extract_tagged.csv` from Sub-Agent 2
- `ARCHITECTURE.md` (workbook spec)
- Dashboard screenshot (matrix layout reference)

### Outputs
- `output/WMLC_Dashboard.xlsm` with:
  - `Dashboard` sheet (formatted matrix, buttons, click handlers)
  - `loan_detail` sheet (flat table, nav buttons)
  - `_data` sheet (hidden, PQ landing zone)
  - Embedded VBA modules
  - Power Query M connection

### Key Requirements

**Dashboard Sheet:**
- Row labels: `credit_lii_commitment_bucket` ladder (match screenshot exactly)
- Column headers: all 14 product_buckets + "Gross Amount" + "Defined Range" + Total row
- Cells contain formulas or values computed from `_data` sheet
- Conditional formatting: non-zero cells highlighted
- Header: "Current Portfolio Snapshot" with view name indicator

**VBA Modules:**

1. `mod_ViewToggle` — 6 buttons (4 views + WMLC ON/OFF)
   - Stores current view state in a hidden named range
   - Recalculates all matrix cells based on active view + WMLC state
   - Updates header text to reflect active view

2. `mod_CellClick` — `Worksheet_SelectionChange` on Dashboard
   - Detects click in data area (not headers/labels)
   - Reads row → bucket, column → product_bucket
   - Applies AutoFilter on `loan_detail` sheet
   - If WMLC ON, also filters `wmlc_qualified == True`
   - Activates `loan_detail` sheet

3. `mod_Navigation`
   - `BackToDashboard()` — clears `loan_detail` filters, activates Dashboard
   - `ResetFilters()` — clears all filters on active sheet
   - `ResetDashboard()` — sets view to "Summary Count", WMLC OFF

4. `mod_DataRefresh`
   - Refreshes Power Query connection
   - Rebuilds matrix after refresh

**Power Query:**
- Resilient M code — uses `try...otherwise` for column types
- Loads from CSV path specified in a named range (changeable)
- Lands in `_data` sheet as a Table named `tbl_LoanData`

**Formatting:**
- Dashboard matches screenshot aesthetic: bold headers, gridlines, currency format
- Buttons styled consistently (ActiveX or Form Controls)
- Named ranges for all key references

### Handoff Contract
```yaml
handoff:
  consumes:
    - path: output/loan_extract_tagged.csv
  produces:
    - path: output/WMLC_Dashboard.xlsm
      required_sheets: [Dashboard, loan_detail, _data]
      required_vba_modules: [mod_ViewToggle, mod_CellClick, mod_Navigation, mod_DataRefresh]
```

---

## Sub-Agent 4: Testing

### Inputs
- All artifacts from Agents 1-3
- `WMLC_LOGIC.md` (ground truth for flag validation)
- `ARCHITECTURE.md` (structural requirements)

### Outputs
- `output/test_report.md` — structured pass/fail report

### Test Suite

**Data Gen Tests:**
- [ ] All proxy files exist and have minimum row counts
- [ ] Column names match schema expectations
- [ ] At least 14 distinct product_buckets present
- [ ] Account numbers are 12-digit zero-padded
- [ ] credit_lii values span the full bucket ladder range
- [ ] NEW_CAMP_YN has both Y and N values

**ETL Tests:**
- [ ] Tagged CSV has all required new columns
- [ ] `wmlc_flags` contains only valid flag names from WMLC_LOGIC.md
- [ ] `wmlc_flag_count` matches pipe-count of `wmlc_flags`
- [ ] `wmlc_qualified` is True iff `wmlc_flag_count > 0`
- [ ] Known-answer rows: manually construct 5 loans with predictable flag outcomes
- [ ] NTC tagging: verify LAL vs TL path produces correct `is_ntc`
- [ ] is_office: verify DAR Tracker join works (case-insensitive "Office")

**Excel Tests (structural, no runtime VBA):**
- [ ] .xlsm file opens without corruption (zipfile validation)
- [ ] Sheet names: Dashboard, loan_detail, _data all present
- [ ] VBA project contains expected module names
- [ ] Dashboard has expected number of rows/columns
- [ ] Named ranges exist for view state and data path
- [ ] Power Query M code is embedded in workbook

**Manual Test Checklist (for human):**
- [ ] Open .xlsm, enable macros
- [ ] Click each of the 6 toggle buttons — view updates correctly
- [ ] Click a non-zero data cell — jumps to loan_detail filtered
- [ ] Verify loan_detail shows correct population
- [ ] Click "Back to Dashboard" — returns and clears filters
- [ ] Click "Reset Filters" on loan_detail — clears filters
- [ ] Toggle WMLC ON — matrix shows only wmlc_qualified loans
- [ ] Refresh Power Query — data reloads

### Handoff Contract
```yaml
handoff:
  consumes:
    - path: proxy_data/*.csv
    - path: proxy_data/*.xlsx
    - path: output/loan_extract_tagged.csv
    - path: output/WMLC_Dashboard.xlsm
    - path: corp_etl/main.py
  produces:
    - path: output/test_report.md
      format: markdown with checkboxes
```

---

## Claude Code Invocation

```bash
cd C:\Users\jmsph\Documents\Repo-Github\Work\CR_Refactored\CR_PEERS_JP
set CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=2 && claude --dangerously-skip-permissions
```

### CLAUDE.md (Orchestrator Instructions — placed at repo root)

The `CLAUDE.md` file will contain:
1. Project overview and objectives
2. Agent dispatch order (Data Gen → ETL → Excel/VBA → Testing)
3. Handoff validation checks between each phase
4. Re-dispatch rules (if testing fails, which agent to re-run)
5. File location conventions
6. Links to all skill files and spec markdowns
