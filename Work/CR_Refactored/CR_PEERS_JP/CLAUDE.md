# CR_PEERS_JP — Credit Risk Performance Reporting Engine

## 1. Project Overview

This repository is an **automated Credit Risk Performance reporting engine** for MSPBNA (Morgan Stanley Private Bank, National Association). The pipeline:

1. **Fetches** raw call-report data from the FDIC API and macroeconomic time-series from the FRED API.
2. **Processes** the data into standard and normalized credit-quality metrics, computes peer-group composites, and builds rolling 8-quarter averages.
3. **Outputs** a consolidated Excel dashboard (`Bank_Performance_Dashboard_*.xlsx`) containing multiple sheets (FDIC_Data, Averages_8Q, FRED_Data, FRED_Descriptions, FDIC_Metric_Descriptions).
4. **Generates reports**: PNG credit-deterioration charts, PNG scatter plots, and HTML comparison tables — all routed to structured subdirectories under `output/Peers/`.

The two core scripts are:

| Script | Role |
|---|---|
| `MSPBNA_CR_Normalized.py` | Data fetch, processing, normalization, and Excel dashboard creation |
| `report_generator.py` | Reads the dashboard Excel and produces charts, scatters, and HTML tables |
| `metric_registry.py` | Derived metric specs, validation engine, dependency graph |
| `fred_series_registry.py` | Central FRED series registry (SBL, Resi, CRE, Case-Shiller) |
| `fred_case_shiller_discovery.py` | Async Case-Shiller release-table discovery |
| `fred_transforms.py` | Transforms, spreads, z-scores, regime flags |
| `fred_ingestion_engine.py` | Async FRED fetcher, validation, sheet routing, Excel output |

---

## 2. Build & Execution Commands

Run the pipeline **sequentially** in a terminal (PowerShell on Windows):

```powershell
# Activate the virtual environment
.\venv\Scripts\Activate

# Install / update dependencies
pip install -r requirements.txt

# Step 1 — Data Fetch & Processing (produces the Excel dashboard)
python MSPBNA_CR_Normalized.py

# Step 2 — Report Generation (produces charts, scatters, HTML tables)
python report_generator.py
```

### Required Environment Variables

| Variable | Purpose | Example |
|---|---|---|
| `FRED_API_KEY` | FRED API authentication (required by Step 1) | `export FRED_API_KEY='abc123'` |
| `MSPBNA_CERT` | Subject bank CERT number (dynamic, no hardcoding) | `19977` |
| `MSBNA_CERT` | Secondary bank CERT number | `19977` |
| `SUBJECT_BANK_CERT` | Used by `MSPBNA_CR_Normalized.py` | `34221` |
| `MS_COMBINED_CERT` | MS Combined Entity CERT (default `88888`) | `88888` |
| `REPORT_VIEW` | Controls table filtering logic | `ALL_BANKS` or `MSPBNA_WEALTH_NORM` |

These can be set in a `.env` file in the project root or exported in the shell.

### Key Dependencies

`aiohttp`, `matplotlib`, `numpy`, `openpyxl`, `pandas`, `python-dotenv`, `requests`, `scipy`, `seaborn`, `tqdm`

---

## 3. Project Architecture & Output Routing

### Data Flow

```
FDIC API + FRED API
        │
        ▼
MSPBNA_CR_Normalized.py
        │
        ▼
output/Bank_Performance_Dashboard_YYYYMMDD_HHMMSS.xlsx
        │
        ▼
report_generator.py
        │
        ├──► output/Peers/charts/   (Standard + Normalized deterioration charts,
        │                           Portfolio Mix, Segment Attribution,
        │                           Reserve Allocation, Migration Ladder,
        │                           Years-of-Reserves, Growth vs Deterioration,
        │                           Risk-Adjusted Return, Concentration vs Capital,
        │                           Liquidity Overlay, Macro Overlay PNGs)
        ├──► output/Peers/scatter/  (Standard + Normalized scatter plot PNGs)
        └──► output/Peers/tables/   (Standard + Normalized HTML tables, FRED macro table)
```

### Excel Sheet Layout

| Sheet Name | Contents |
|---|---|
| `FDIC_Data` | Quarterly metrics for subject bank + all peer CERTs |
| `Averages_8Q*` | Rolling 8-quarter averages used for scatter plots |
| `FRED_Data` | Macroeconomic time-series (Fed Funds, VIX, Unemployment, etc.) |
| `FRED_Descriptions` | Series ID ↔ Short Name mapping for FRED data |
| `FDIC_Metric_Descriptions` | Human-readable descriptions of FDIC metrics |
| `FRED_SBL_Backdrop` | SBL proxy series + spreads / regime flags |
| `FRED_Residential_Jumbo` | Jumbo rates, SLOOS, resi balances, delinquency, charge-offs |
| `FRED_CRE` | CRE balances, CLD, SLOOS, delinquency, charge-offs, prices |
| `FRED_CaseShiller_Master` | Full Case-Shiller registry (all discovered series) |
| `FRED_CaseShiller_Selected` | Curated Case-Shiller subset for dashboard |
| `FRED_Expansion_Registry` | Full metadata registry audit sheet |

### Output File Naming

All output files include the source Excel stem and a datestamp:
- `{stem}_standard_credit_chart_YYYYMMDD.png`
- `{stem}_normalized_credit_chart_YYYYMMDD.png`
- `{stem}_portfolio_mix_YYYYMMDD.png`
- `{stem}_problem_asset_attribution_YYYYMMDD.png`
- `{stem}_reserve_risk_allocation_YYYYMMDD.png`
- `{stem}_migration_ladder_YYYYMMDD.png`
- `{stem}_scatter_nco_vs_npl_YYYYMMDD.png`
- `{stem}_scatter_pd_vs_npl_YYYYMMDD.png`
- `{stem}_scatter_norm_nco_vs_nonaccrual_YYYYMMDD.png`
- `{stem}_standard_table_YYYYMMDD.html`
- `{stem}_normalized_table_YYYYMMDD.html`
- `{stem}_fred_table_YYYYMMDD.html`
- `{stem}_years_of_reserves_YYYYMMDD.png`
- `{stem}_growth_vs_deterioration_YYYYMMDD.png`
- `{stem}_risk_adjusted_return_YYYYMMDD.png`
- `{stem}_concentration_vs_capital_YYYYMMDD.png`
- `{stem}_liquidity_overlay_YYYYMMDD.png`
- `{stem}_macro_overlay_YYYYMMDD.png`

---

## 4. Strict Coding Conventions & Rules

These are **non-negotiable** for any agent editing this codebase:

### ALWAYS UPDATE CLAUDE.md

- Whenever you make architectural changes, add new charts, or change data pipelines, you **must** update this `CLAUDE.md` file to review restrictions, track to-do items, identify potential gaps, and inform future coding agents.

### NO HARDCODING

- Subject bank CERTs **must** be loaded dynamically via `int(os.getenv("MSPBNA_CERT", "19977"))`.
- Never hardcode `19977` or any CERT number directly. Always use the config/env pattern.
- The FRED API key must come from `os.getenv('FRED_API_KEY')` or `.env`.

### ALIASING

- When plotting, use the `build_plot_df_with_alias(df, alias_map)` helper to rename composite CERTs (e.g., `{99999: 90006, 99998: 90004}`) rather than modifying the core dataframe permanently.
- The alias map creates temporary copies — the original dataframe must remain unmodified.

### PEER GROUPINGS

HTML tables must always reflect **3 peer columns** per table type:

| Table Type | Peer 1 | Peer 2 | Peer 3 |
|---|---|---|---|
| **Standard** | 90001 — Core PB | 90002 — MSPBNA+Wealth | 90003 — All Peers |
| **Normalized** | 90004 — Core PB Norm | 90005 — MSPBNA+Wealth Norm | 90006 — All Peers Norm |

### MS COMBINED ENTITY

- CERT `88888` (MS Combined Entity) must be **filtered out** of HTML table listings when `REPORT_VIEW == "MSPBNA_WEALTH_NORM"`.
- Load via `MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))`.

### CHARTING STYLES

All charts must use this color palette consistently:

| Entity | Hex Color | Description |
|---|---|---|
| **MSPBNA (Subject Bank)** | `#F7A81B` | Gold |
| **All Peers** | `#5B9BD5` or `#4C78A8` | Blue |
| **Core PB / Peers (Ex. F&V)** | `#70AD47` or `#9C6FB6` | Green or Purple |

**Do not alter** the internal matplotlib/seaborn plotting logic in `create_credit_deterioration_chart_ppt` or `plot_scatter_dynamic` without explicit permission from the project owner.

### CSS CLASS NAMING

- Use `mspbna-row` and `mspbna-value` (not `idb-row` / `idb-value`).
- All user-facing labels, HTML headers, and filenames must reference **MSPBNA**, never **IDB**.

### CHART METRICS

**Standard chart:** Bar = `TTM_NCO_Rate`, Line = `NPL_to_Gross_Loans_Rate`
**Normalized chart:** Bar = `Norm_NCO_Rate`, Line = `Norm_Nonaccrual_Rate` (fallback: `Norm_NPL_to_Gross_Loans_Rate`)

Both standard and normalized credit-deterioration charts are generated.

### TABLE COLUMN ORDERING

- HTML tables comparing individual peers **must** always place the Subject Bank (MSPBNA) and MSBNA on the extreme left, followed by individual peers, followed by the peer average.

### TABLE COMPLETENESS

- Segment Focus tables must never truncate metrics. Both standard and normalized variants must explicitly map all 15 core segment series.

### TABLE DUPLICATION

- The Detailed Peer Analysis, Core PB Analysis, CRE Segment, and Resi Segment tables **must** ALWAYS output both a Standard and a Normalized version.

### FORMATTING RULES

- **Diffs are Percentage-Point Deltas**: All "Diff vs Peer" columns MUST use simple subtraction (`v_subject - v_peer`), never relative percentage change (`(v_subject - v_peer) / v_peer`). Use `_fmt_percent_diff(diff, ref_value)` to format; it uses the reference value's scale to decide whether to multiply by 100.
- **Dollar Amounts = $B**: FDIC data stores dollar amounts in thousands ($K). Use `_fmt_money_billions()` which converts: `v/1e6` → `$X.XB`, `v/1e3` → `$X.XM`. Never divide by `1e9` (that produces values 1000x too small).
- **Multiplier metrics**: Metrics labeled `(x)` (e.g., CRE NPL Coverage) must format as `X.XXx`, not as `%`.
- **Dead metric suppression**: If a metric is entirely 0/NaN across all displayed entities for the latest quarter, the row is automatically suppressed from HTML tables.

---

## 5. Common Errors & Troubleshooting

### DNS / Proxy Error: `[Errno 11001] getaddrinfo failed`

**Symptom:** Step 1 (`MSPBNA_CR_Normalized.py`) fails when trying to reach `banks.data.fdic.gov` or `api.stlouisfed.org`.

**Cause:** The corporate VPN or firewall is blocking outbound HTTPS requests to the FDIC and FRED APIs.

**Fix (PowerShell):**

```powershell
# Set proxy environment variables before running Step 1
$env:HTTP_PROXY  = "http://your-corporate-proxy:port"
$env:HTTPS_PROXY = "http://your-corporate-proxy:port"

# Then run the pipeline
python MSPBNA_CR_Normalized.py
```

Alternatively, disconnect from the VPN before running Step 1, then reconnect afterward.

### Missing FRED_API_KEY

**Symptom:** `ValueError: FRED_API_KEY not found`

**Fix:** Create a `.env` file in the project root:

```
FRED_API_KEY=your_key_here
SUBJECT_BANK_CERT=34221
```

Or export it directly: `export FRED_API_KEY='your_key_here'`

### No Excel File Found

**Symptom:** `report_generator.py` prints `ERROR: No Excel files found in output/`

**Fix:** Run Step 1 first (`python MSPBNA_CR_Normalized.py`). The report generator reads the latest `Bank_Performance_Dashboard_*.xlsx` from the `output/` directory.

### Missing Sheet in Excel

**Symptom:** `FileNotFoundError: 8Q average sheet not found`

**Fix:** Ensure `MSPBNA_CR_Normalized.py` completed successfully and the output Excel contains the `Averages_8Q*` sheet. Re-run Step 1 if needed.

---

## 6. Changelog / Recent Fixes

### 2026-03-10 — Data Mapping, Math, and Formatting Bug Fixes

1. **Duplicate Peer Composites (MSPBNA_CR_Normalized.py)**: Fixed `_create_peer_composite()` so normalized composites (90004/90005/90006) are mathematically distinct from standard composites (90001/90002/90003). Standard composites now NaN-out `Norm_*` columns; normalized composites NaN-out standard rate columns. Previously, both sets produced identical averages because they shared the same cert lists and averaged all numeric columns indiscriminately.

2. **Diff Math — Percentage-Point Deltas (report_generator.py)**: Created `_fmt_percent_diff(diff, ref_value)` to fix inflated diff values (e.g., ROE 14.69% vs 13.94% showing +75.00% instead of +0.75%). The bug was in `_fmt_percent()` being used for both display and diffs — its `abs(v) < 1.0` heuristic incorrectly multiplied small ppt diffs by 100 when source values were in pct-point scale (>=1.0). Fixed in `generate_credit_metrics_email_table`, `generate_detailed_peer_table`, and `generate_segment_focus_table`.

3. **Ratio Components Column Mapping (report_generator.py)**: Fixed ~12 phantom column names in `generate_ratio_components_table` that didn't match upstream DataFrame columns. Key fixes: `Nonaccrual_Total` → `Total_Nonaccrual`, `Risk_Adj_Gross_Loans` → computed inline, `Total_Past_Due` → computed from `TopHouse_PD30 + TopHouse_PD90`, `RIC_Resi_Balance` → `RIC_Resi_Cost`, `RIC_CRE_NCO` → `RIC_CRE_NCO_TTM`, `RIC_CRE_Delinq`/`RIC_Resi_Delinq` → computed from PD30+PD90, `RIC_Fund_Finance_Loan_Share` → `Fund_Finance_Composition`.

4. **Unit Formatting ($B display) (report_generator.py)**: Fixed `_fmt_money_billions()` and inline `fmt_val()` functions. FDIC stores dollar amounts in $K (thousands), so dividing by `1e6` gives billions. The inline formatters were incorrectly dividing by `1e9` (producing values like `$0.3B` instead of `$254.7B`). Also fixed label mismatches (`$254.7M` → `$254.7B`).

5. **Dead Metric Suppression (report_generator.py)**: Added filtering in all 4 HTML table generators (`generate_credit_metrics_email_table`, `generate_ratio_components_table`, `generate_segment_focus_table`, `generate_detailed_peer_table`) to skip metric rows where all displayed entity values are 0 or NaN. Prevents misleading `0.00%` rows for metrics like `Norm_Loan_Yield` and `Norm_Provision_Rate`.

### 2026-03-10 — FRED Expansion Layer (Registry-Driven Macro/Market Context)

1. **New module `fred_series_registry.py`**: Registry-driven design with `FREDSeriesSpec` dataclass. 80+ series across 4 modules: SBL proxy (14), Residential/Jumbo (19), CRE (23), Case-Shiller seed (24+). Each spec carries category, priority, transforms, chart routing, and sheet assignment.

2. **New module `fred_case_shiller_discovery.py`**: Async release-table discovery for standard HPI (release 199159), tiered HPI (release 345173), and sales-pair counts. Classifies each discovered series by tier, metro, SA/NSA. Merges with static seed, deduplicating by series_id.

3. **New module `fred_transforms.py`**: Full transformation pipeline: pct_chg (MoM/QoQ/YoY), z-scores (5Y/10Y), rolling averages (3M/12M), 4 named spreads (jumbo-conforming, CRE demand-standards, resi growth-delinquency, high-tier vs national HPI), and 4 regime flags.

4. **New module `fred_ingestion_engine.py`**: Async fetcher (`FREDExpansionFetcher`) compatible with upstream pattern, validation engine (duplicates, discontinued, stale, missing metadata), sheet-level output routing, and Excel writer.

5. **First-wave charts in `report_generator.py`**: 5 new chart functions: `plot_sbl_backdrop`, `plot_jumbo_conditions`, `plot_resi_credit_cycle`, `plot_cre_cycle`, `plot_cs_collateral_panel`. Integrated into `generate_reports()` with graceful fallback when expansion sheets are absent.

6. **CLAUDE.md Section 9**: Full documentation of FRED expansion architecture, registry design, transforms, spreads, regime flags, output sheets, and validation checks.

### 2026-03-10 — HTML Table Overhaul (Standard + Normalized Split)

1. **Detailed Peer Table refactored**: `generate_detailed_peer_table` now accepts `is_normalized` parameter. Standard version uses headline metrics (TTM_NCO_Rate, etc.); Normalized version uses Norm_ metrics. Column ordering changed: MSPBNA and MSBNA placed on the extreme left.

2. **New Core PB Table**: Added `generate_core_pb_peer_table` that dynamically identifies Goldman/UBS CERTs and uses Core PB Avg (90001 standard, 90004 normalized).

3. **Shared HTML builder**: Added `_build_dynamic_peer_html` helper to eliminate code duplication between detailed peer and core PB tables.

4. **Segment Focus Table refactored**: `generate_segment_focus_table` now accepts `is_normalized` parameter. Normalized variant uses Norm_ common metrics and Norm_ segment-specific ACL share/coverage columns. Both variants maintain full 15-metric lists.

5. **generate_reports() unified loop**: All detailed peer, core PB, ratio components, CRE segment, and Resi segment tables now generate both Standard and Normalized versions via `for is_norm in [False, True]` loop.

6. **CLAUDE.md conventions added**: Table Column Ordering, Table Completeness, and Table Duplication rules added to Section 4.

---

## 7. To-Do / Known Issues

### Upstream Data Gaps (MSPBNA_CR_Normalized.py)

| Metric | Status | Root Cause |
|---|---|---|
| `Norm_Loan_Yield` | Flatlines at 0.00% | Computed as `Int_Inc_Loans_TTM / Norm_Gross_Loans`. The upstream `Int_Inc_Loans_TTM` column is likely not being populated because the FDIC API series for interest income on loans (`ILNLS`) may not be fetched, or the TTM rolling sum is failing silently. Check that `ILNLS` (or `Int_Inc_Loans_Raw`) is in the fetch list and that the TTM computation in `_calculate_ttm_metrics()` handles it. |
| `Norm_Provision_Rate` | Flatlines at 0.00% | Computed as `Provision_Exp_TTM / Norm_Gross_Loans`. Same pattern — `Provision_Exp_TTM` is derived from `ELNATR` (Provision for Loan Losses). Verify that `ELNATR` is fetched and that the TTM rolling sum is correctly computed. |
| `Norm_Loss_Adj_Yield` | Flatlines at 0.00% | Derived as `Norm_Loan_Yield - Norm_NCO_Rate`. Flatlines because `Norm_Loan_Yield` is zero. Will auto-resolve when `Norm_Loan_Yield` is fixed. |

**Action for future agents**: Search `MSPBNA_CR_Normalized.py` for the FDIC field list (around line 120-230) and confirm `ILNLS` and `ELNATR` are included. Then trace the TTM computation path to ensure `Int_Inc_Loans_TTM` and `Provision_Exp_TTM` are populated before the normalization step uses them.

---

## 8. Metric Registry & Validation Architecture

### Overview

Derived metrics are now formally registered in `metric_registry.py` using a `MetricSpec` dataclass. Each spec declares:

| Field | Purpose |
|---|---|
| `code` | Column name in the DataFrame (e.g., `Risk_Adj_Allowance_Coverage`) |
| `dependencies` | List of upstream columns required to compute this metric |
| `compute` | A lambda that recomputes the metric from raw columns (used for validation) |
| `unit` | Semantic type: `fraction`, `dollars`, `count`, `multiple`, `years` |
| `min_value` / `max_value` | Optional sanity bounds |
| `allow_negative` | Whether negative values are valid (e.g., NCO rates can go negative) |
| `consumers` | List of downstream report artifacts (charts/tables) that depend on this metric |
| `severity` | `high`, `medium`, or `low` — controls alerting priority |

### Report Consumer Map

The `REPORT_CONSUMER_MAP` dict (auto-built from specs) maps each metric code to its downstream consumers. This lets `report_generator.py` know which charts/tables are affected when a metric fails validation.

### Validation Engine

`run_upstream_validation_suite(df)` is called during `MSPBNA_CR_Normalized.py` right before writing the Excel output. For each registered metric, it:

1. **Recomputes** the metric from its declared formula (`spec.compute(df)`)
2. **Compares** the recomputed value against the stored column value
3. **Checks bounds** (min/max) and sign constraints
4. **Tags** each row with `CERT` and `REPDTE` for row-level tracing

Results are exported to the `Metric_Validation_Audit` sheet in the Excel dashboard.

### Dependency Graph

`build_reverse_dependency_map()` returns `{upstream_col: [derived_metrics]}`, enabling impact analysis: if an upstream column (e.g., `Gross_Loans`) is missing or corrupt, the graph shows exactly which derived metrics and downstream reports are affected.

---

## 9. FRED Expansion Layer — Registry-Driven Macro / Market Context

### Architecture

The FRED expansion is implemented as four new modules:

| Module | Purpose |
|---|---|
| `fred_series_registry.py` | Central registry of all expanded FRED series (`FREDSeriesSpec` dataclass) |
| `fred_case_shiller_discovery.py` | Async release-table discovery for all Case-Shiller series |
| `fred_transforms.py` | Transforms (pct_chg, z-scores, rolling avgs), named spreads, regime flags |
| `fred_ingestion_engine.py` | Async fetcher, validation, sheet routing, Excel output |

### Registry Design

Each series is registered as a `FREDSeriesSpec` with fields: `category`, `subcategory`, `series_id`, `display_name`, `freq`, `units`, `seasonal_adjustment`, `priority`, `use_case`, `chart_group`, `transformations`, `is_active`, `notes`, `sheet`, `discovery_source`.

**Priority levels:**
- **P1** — Series directly used in dashboard charts
- **P2** — Series used for overlays / regimes / alerts
- **P3** — Full discovered registry (stored but not shown by default)

### Modules

**Module A — SBL / Market-Collateral Proxy** (14 series, `FRED_SBL_Backdrop` sheet):
- Bank-system securities inventory (INVEST, SBCLCBM027SBOG family)
- Broker-dealer leverage / margin proxies (BOGZ1 family)
- Label as "market proxy" or "system-level SBL backdrop" — NOT as MSPBNA SBL comps

**Module B — Residential / Jumbo Mortgage** (19 series, `FRED_Residential_Jumbo` sheet):
- Jumbo rate (OBMMIJUMBO30YF), jumbo SLOOS demand/standards
- Large-bank residential balances (RRELCBM, H8B1221, CRLLCB, RHELCB families)
- Top-100-bank delinquency / charge-off (DRSFRMT100, CORSFRMT100)

**Module C — CRE Lending / Underwriting / Credit** (23 series, `FRED_CRE` sheet):
- CRE balance / growth (CREACB, CRELCB, H8B3219, CLDLCB families)
- CRE SLOOS standards/demand (9 series covering nonfarm, multifamily, C&LD)
- CRE credit quality (DRCRELEXF, CORCREXF, COMREPUSQ)

**Module D — Case-Shiller Collateral** (seed: 24 series, discovered: ~140+):
- Standard HPI: national, composites, key metros (SA + NSA)
- Tiered HPI: high/middle/low tiers for 16 markets (discovery via release API)
- Sales-pair counts: market liquidity context
- Discovery uses `release_id=199159` (standard) and `release_id=345173` (tiered)

### Transforms

| Transform | Description |
|---|---|
| `pct_chg_mom` | Month-over-month percent change |
| `pct_chg_qoq_annualized` | QoQ change, annualized |
| `pct_chg_yoy` | Year-over-year percent change |
| `z_score_5y` / `z_score_10y` | Rolling z-score over trailing window |
| `rolling_3m_avg` / `rolling_12m_avg` | Rolling mean |
| `spread_vs_*` | Spread against a reference series |

### Named Spreads

| Spread | Components |
|---|---|
| Jumbo vs Conforming | `OBMMIJUMBO30YF - MORTGAGE30US` |
| CRE Demand vs Standards | `SUBLPDRCDNLGNQ - SUBLPDRCSNLGNQ` |
| Resi Growth vs Delinquency | `H8B1221NLGCQG - DRSFRMT100S` |
| High-Tier vs National HPI | YoY growth differential |

### Regime Flags

| Flag | Trigger |
|---|---|
| `REGIME__CRE_Tightening` | CRE standards > 0 AND demand < 0 |
| `REGIME__Jumbo_Tightening` | Jumbo standards > 0 AND demand < 0 |
| `REGIME__Resi_Stress` | Resi delinquency rising AND high-tier HPI decelerating |
| `REGIME__SBL_Deleveraging` | Securities-in-bank-credit falling AND broker-dealer margin weakening |

### Excel Output Sheets

| Sheet | Contents |
|---|---|
| `FRED_SBL_Backdrop` | SBL proxy series + derived spreads / regime flags |
| `FRED_Residential_Jumbo` | Jumbo rates, SLOOS, residential balances, delinquency, charge-offs |
| `FRED_CRE` | CRE balances, CLD, SLOOS, delinquency, charge-offs, prices |
| `FRED_CaseShiller_Master` | Full discovered Case-Shiller registry |
| `FRED_CaseShiller_Selected` | Curated subset for dashboard visuals |
| `FRED_Expansion_Registry` | Full metadata registry (audit sheet) |
| `FRED_Expansion_Validation` | Validation results |

### First-Wave Charts (report_generator.py)

| Chart | Function | Key Series |
|---|---|---|
| SBL Backdrop | `plot_sbl_backdrop()` | SBCLCBM027SBOG + BOGZ1FU663067005A |
| Jumbo Conditions | `plot_jumbo_conditions()` | OBMMIJUMBO30YF + SUBLPDHMDJLGNQ + SUBLPDHMSKLGNQ |
| Resi Credit Cycle | `plot_resi_credit_cycle()` | DRSFRMT100S + CORSFRMT100S + H8B1221NLGCQG |
| CRE Cycle | `plot_cre_cycle()` | H8B3219NLGCMG + SUBLPDRCSNLGNQ + DRCRELEXFT100S |
| CS Collateral Panel | `plot_cs_collateral_panel()` | High-tier metros + national + sales-pair counts |

### Validation Checks

The ingestion engine validates:
- **Duplicates**: No series_id appears more than once in the registry
- **Discontinued**: Excludes series with observation_end before 2024
- **Missing metadata**: Flags specs missing display_name, freq, units, or use_case
- **Stale releases**: Warns if last valid observation is > 6 months old
- **Orphan columns**: Data columns not present in registry
