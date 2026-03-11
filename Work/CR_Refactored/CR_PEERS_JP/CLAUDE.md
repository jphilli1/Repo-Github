# CR_PEERS_JP — Credit Risk Performance Reporting Engine

## 1. Project Overview

This repository is an **automated Credit Risk Performance reporting engine** for MSPBNA (Morgan Stanley Private Bank, National Association). The pipeline:

1. **Fetches** raw call-report data from the FDIC API and macroeconomic time-series from the FRED API.
2. **Processes** the data into standard and normalized credit-quality metrics, computes peer-group composites, and builds rolling 8-quarter averages.
3. **Outputs** a consolidated Excel dashboard (`Bank_Performance_Dashboard_*.xlsx`) containing multiple sheets (Summary_Dashboard, Normalized_Comparison, Latest_Peer_Snapshot, Averages_8Q_All_Metrics, FDIC_Metric_Descriptions, Macro_Analysis, FDIC_Data, FRED_Data, FRED_Metadata, FRED_Descriptions, Data_Validation_Report, Normalization_Diagnostics, Peer_Group_Definitions, Exclusion_Component_Audit, Composite_Coverage_Audit, Metric_Validation_Audit, and optional Case-Shiller ZIP sheets).
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
| `test_regression.py` | Regression tests: scatter integrity, peer groups, over-exclusion, validation |
| `case_shiller_zip_mapper.py` | HUD USPS ZIP Crosswalk enrichment for Case-Shiller metros |

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
| `MSPBNA_CERT` | Subject bank CERT number (dynamic, no hardcoding) | `34221` |
| `MSBNA_CERT` | Secondary bank CERT number | `32992` |
| `SUBJECT_BANK_CERT` | Used by `MSPBNA_CR_Normalized.py` | `34221` |
| `MS_COMBINED_CERT` | MS Combined Entity CERT (default `88888`) | `88888` |
| `REPORT_VIEW` | Controls table filtering logic | `ALL_BANKS` or `MSPBNA_WEALTH_NORM` |
| `HUD_USER_TOKEN` | HUD USPS Crosswalk API bearer token (required for ZIP enrichment) | `export HUD_USER_TOKEN='eyJ...'` |
| `HUD_CROSSWALK_YEAR` | Optional crosswalk vintage year | `2025` |
| `HUD_CROSSWALK_QUARTER` | Optional crosswalk vintage quarter (1-4) | `4` |
| `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` | Enable/disable ZIP enrichment (default `true`) | `true` or `false` |

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
| `Normalization_Diagnostics` | Over-exclusion flags, residuals, severity per CERT/quarter |
| `Peer_Group_Definitions` | 4 peer group definitions with member CERTs and metadata |
| `Resi_Normalized_Audit` | Residential metric values, mapping/label status, latest quarter |
| `Exclusion_Component_Audit` | Per-bank/quarter excluded category balances, NCO, balance-gating flags, dominant category |
| `Composite_Coverage_Audit` | Per-group/metric contributor counts, coverage %, NaN-out decisions for normalized composites |
| `CaseShiller_Zip_Coverage` | One row per (Case-Shiller region, ZIP code) with HUD crosswalk ratios |
| `CaseShiller_Zip_Summary` | One row per metro with aggregate ZIP counts and mapping metadata |
| `CaseShiller_Metro_Map_Audit` | 20-entry metro → CBSA/CBSA-Div mapping table with judgment call notes |

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

### FDIC API Variables & FFIEC Waterfall

- Always prefer FDIC top-level text aliases (e.g., `ASSET`, `NTCI`) over raw MDRM codes (`RCFD...`, `RIAD...`). When raw codes are necessary, you **MUST** fetch both the `RCFD` (Consolidated) and `RCON` (Domestic) codes. You must then coalesce them (`RCFD.fillna(RCON)`) to ensure apples-to-apples comparisons between international G-SIBs (FFIEC 031) and domestic-only banks (FFIEC 041/051). Never force internationally active banks to strictly use `RCON`, as this strips out their foreign office balances. Synthetic local variables must never be included in the raw API fetch request.

### NO HARDCODING

- Subject bank CERTs **must** be loaded dynamically via `int(os.getenv("MSPBNA_CERT", "34221"))`.
- Never hardcode CERT numbers directly. Always use the config/env pattern. Production defaults: `MSPBNA_CERT=34221`, `MSBNA_CERT=32992`.
- The FRED API key must come from `os.getenv('FRED_API_KEY')` or `.env`.

### SCATTER & CHART COMPOSITE HANDLING

- Pass true composite CERTs directly: `peer_avg_cert_primary=90006, peer_avg_cert_alt=90004` for normalized, `peer_avg_cert_primary=90003, peer_avg_cert_alt=90001` for standard.
- `plot_scatter_dynamic()` has a `composite_certs` parameter (default: `{90001..90006, 99998, 99999, 88888}`) that excludes ALL composites from appearing as blue peer dots.
- The former `build_plot_df_with_alias()` function has been removed — it appended duplicate rows that contaminated scatter plots. Do not re-introduce it.

### PEER GROUPINGS

There are **4 peer groups** (2 standard + 2 normalized). Composite CERTs are assigned via `base_dummy_cert + display_order`:

| Table Type | Peer 1 | Peer 2 |
|---|---|---|
| **Standard** | 90001 — Core PB | 90003 — All Peers |
| **Normalized** | 90004 — Core PB Norm | 90006 — All Peers Norm |

The former MSPBNA+Wealth groups (90002/90005) were removed as duplicate cert membership. `validate_peer_group_uniqueness()` enforces that no two groups sharing the same `use_normalized` flag may have identical sorted cert lists.

**Cross-mode duplication by design**: 90001/90004 and 90003/90006 share identical member CERTs. The distinction is `use_normalized`: standard composites NaN-out `Norm_*` columns; normalized composites NaN-out standard rate columns. Hard validation (`validate_peer_group_uniqueness()`) ensures no two groups within the SAME `use_normalized` mode share identical cert lists.

**Peer_Group_Definitions sheet**: A new Excel sheet documents all 4 peer group definitions with member CERTs, use cases, and display order.

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

### FRED Deduplication

- `series_ids` must always be deduplicated before async fetching to prevent `ValueError: cannot reindex on an axis with duplicate labels`.
- Use `list(dict.fromkeys(series_ids))` to preserve order while removing duplicates.
- The guard must exist both at construction time (when building `series_ids_to_fetch`) and at the entry point of `fetch_all_series_async()`.

### FRED Series Validation

- The FRED API returns HTTP 400 Bad Request for discontinued or mistyped series IDs. Always verify IDs against the FRED website before adding them to `FRED_SERIES_TO_FETCH`.
- Known corrections: `CORCCACBS` (not `CORCCLACBS`), `RHVRUSQ156N` (not `RCVRUSQ156N`).
- Discontinued series must be removed, not left in the fetch list (e.g., `GOLDAMGBD228NLBM` was discontinued by FRED).
- Redundant series should be removed if covered by another ID (e.g., `DEPALL` removed in favor of `DPSACBW027SBOG`).

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

## 6. Normalization Conventions

### Top-Down Normalization with Over-Exclusion Detection

Normalized metrics use `calc_normalized_residual(total, excluded, label, tolerance_pct=0.05)` which returns:
- `final_value`: residual (total - excluded), with minor over-exclusions (<=5%) clipped to 0 and material ones set to NaN
- `severity`: one of `ok`, `minor_clip`, `material_nan`
- 15 diagnostics columns are written to the `Normalization_Diagnostics` sheet

### Normalized Ratio Components

`generate_ratio_components_table(is_normalized=True)` uses:
- **Delinquency numerator**: `_Norm_Total_Past_Due` (synthesized as `Norm_PD30 + Norm_PD90`)
- **ACL numerator**: `Norm_ACL_Balance` (not `Total_ACL`)
- **Risk-adjusted denominator**: `Norm_Risk_Adj_Gross_Loans` (= `Norm_Gross_Loans - SBL_Balance`)
- **Resi ACL Coverage**: `RIC_Resi_ACL / Wealth_Resi_Balance`

### Balance-Gating for Excluded NCO Categories

Four excluded NCO categories (Auto, Ag, ADC, OO CRE) are balance-gated: if the excluded balance for a category is zero, the excluded NCO is forced to zero regardless of MDRM field values. This prevents misclassification propagation (e.g., MSPBNA showing $27K Auto NCO with zero Auto balance). Each gating decision produces a `_*_NCO_Gated` flag column. The `Exclusion_Component_Audit` sheet documents per-bank/quarter gating decisions, dominant exclusion categories, and flags where balance is zero but NCO was nonzero.

| Category | Balance Column | NCO Column | Flag Column |
|---|---|---|---|
| Auto | `Excl_Auto_Balance` | `Excl_Auto_NCO_YTD` | `_Auto_NCO_Gated` |
| Ag | `Excl_Ag_Balance` | `Excl_Ag_NCO_YTD` | `_Ag_NCO_Gated` |
| ADC | `Excl_ADC_Balance` | `Excl_ADC_NCO_YTD` | `_ADC_NCO_Gated` |
| OO CRE | `Excl_OO_CRE_Balance` | `Excl_OO_CRE_NCO_YTD` | `_OO_CRE_NCO_Gated` |

### Normalized Composite Minimum Coverage

Normalized composites (90004/90006) must have ≥50% non-NaN contributor share per critical metric, otherwise the composite metric is NaN'd out. This prevents misleading composites when only 2 of 8 banks have usable normalized data. The `Composite_Coverage_Audit` sheet documents per-group/metric contributor counts, coverage percentages, and NaN-out decisions.

**Critical normalized metrics for coverage checks:**
- `Norm_NCO_Rate`, `Norm_Nonaccrual_Rate`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Gross_Loans`

**Important**: Normalized NCO is NOT fully solved — many G-SIBs still produce `material_nan` severity because their excluded categories exceed totals by >5%. The composite coverage threshold mitigates misleading averages but does not fix the upstream data quality issue.

### Case-Shiller ZIP Enrichment

Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` env var (default `true`). When disabled, `build_case_shiller_zip_sheets()` returns a single audit row with status `SKIPPED`. Maps 5-digit ZIP codes to 20 regional Case-Shiller metros.

### IDB Label Convention

Dictionary keys in `master_data_dictionary.py` must **never** use the `IDB_` prefix. All former `IDB_*` keys have been renamed (e.g., `IDB_CRE_Growth_TTM` → `CRE_Growth_TTM`). User-facing labels, CSS classes, and HTML headers must reference **MSPBNA**, never **IDB**.

### Curated Presentation Tabs

`Summary_Dashboard` and `Normalized_Comparison` use curated metric allowlists (`SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS` in `MSPBNA_CR_Normalized.py`). Only approved KPIs appear in these presentation-facing tabs. Raw MDRM fields and internal pipeline columns are excluded. The full dataset remains available in the `FDIC_Data` sheet.

### Display Label Policy

All presentation tabs use `_get_metric_short_name(code)` to resolve display names from `FDIC_Metric_Descriptions` (via `MasterDataDictionary`). Columns: `Metric Code` (technical field name) + `Metric Name` (display label). Falls back to the code itself when no display label exists.

### Metric Role Classification

Metrics are classified as **evaluative** (risk/return/coverage — receives performance flags) or **descriptive** (size/balance/composition — no evaluative flags). `DESCRIPTIVE_METRICS` frozenset in `MSPBNA_CR_Normalized.py` lists all descriptive metrics. `_get_performance_flag()` returns blank for descriptive metrics to prevent misleading "Top Quartile" / "Bottom Quartile" flags on non-evaluative fields like ASSET or LNLS.

### Norm_Provision_Rate Treatment

`Norm_Provision_Rate` is intentionally set to NaN in the pipeline — provision expense (`ELNATR`) is not segment-specific in call reports, so a normalized rate would be semantically misleading. It is excluded from `NORMALIZED_COMPARISON_METRICS` (presentation tab), and dead-metric suppression in HTML tables catches any remaining instances. It should never be presented as a normal KPI or as a silent zero.

---

## 7. Changelog / Recent Fixes

### 2026-03-10 — Balance-Gating, Composite Coverage, Preflight Scoping

**Root causes from workbook review:**
- Excluded NCO over-exclusion: MSPBNA 2024-12-31 showed Total_NCO_TTM=27K but Excluded_NCO_TTM=54K. Root cause: Auto/Ag NCO MDRM fields (RIADK205/K206, RIAD4635/4645) were nonzero despite zero excluded balances for those categories.
- Normalized composites misleading: Only 2 of 8 banks had non-NaN normalized NCO, yet composites showed usable averages.
- Preflight over-blocking: Historical material_nan rows caused blocking even when the latest plotted period was clean.
- Stale load_config defaults were 19977; corrected to production CERTs 34221/32992.

**Fixes applied:**
1. **Balance-gating for excluded NCO (MSPBNA_CR_Normalized.py)**: Added balance-gating for 4 excluded NCO categories (Auto, Ag, ADC, OO CRE). If excluded balance is zero, force excluded NCO to zero and set `_*_NCO_Gated` flag. Added `Exclusion_Component_Audit` sheet with per-bank/quarter gating decisions, dominant exclusion categories, and zero-balance/nonzero-NCO flags.

2. **Normalized composite minimum coverage (MSPBNA_CR_Normalized.py)**: Added 50% minimum contributor coverage threshold for normalized composites (90004/90006). Metrics below threshold are NaN'd out. Added `Composite_Coverage_Audit` sheet documenting per-group/metric contributor counts, coverage %, and NaN-out decisions. Critical metrics: `Norm_NCO_Rate`, `Norm_Nonaccrual_Rate`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Gross_Loans`.

3. **Preflight period-scoping (report_generator.py)**: Severity checks now determine `latest_repdte` and only block on material failures at the latest plotted period. Historical material_nan failures become informational warnings.

4. **load_config defaults fixed (report_generator.py)**: Changed defaults from 19977 to `MSPBNA_CERT=34221`, `MSBNA_CERT=32992`.

5. **Regression tests (test_regression.py)**: Added 7 function-based tests + 7 unittest classes covering balance-gating (Ag, Auto, ADC, OO CRE), composite coverage (below/above/at threshold), preflight period-scoping (blocks at latest, ignores historical), load_config defaults, and audit sheet inclusion.

6. **Case-Shiller ZIP sheet persistence (MSPBNA_CR_Normalized.py)**: Wrapped `build_case_shiller_zip_sheets()` call in try/except so HUD API failures do not crash the pipeline. Added logging for which ZIP sheets are written. The `**cs_kwargs` unpack in `write_excel_output()` persists non-empty sheets (CaseShiller_Zip_Coverage, CaseShiller_Zip_Summary, CaseShiller_Metro_Map_Audit). When enrichment is disabled or HUD token is missing, only the audit sheet (always non-empty) is written. Added regression tests for resilience and audit sheet presence.

**New Excel sheets:** `Exclusion_Component_Audit`, `Composite_Coverage_Audit`, `Metric_Validation_Audit`

### 2026-03-11 — Presentation Curation, Metric Roles, Chart Resilience

1. **Curated presentation tabs (MSPBNA_CR_Normalized.py)**: `Summary_Dashboard` now uses `SUMMARY_DASHBOARD_METRICS` allowlist (22 curated KPIs) instead of dumping all numeric columns. `Normalized_Comparison` uses `NORMALIZED_COMPARISON_METRICS` (14 curated normalized KPIs). Raw MDRM fields and internal pipeline columns are excluded from presentation tabs.

2. **Metric-role classification (MSPBNA_CR_Normalized.py)**: Added `DESCRIPTIVE_METRICS` frozenset containing size/balance/composition metrics (ASSET, LNLS, Gross_Loans, SBL_Composition, etc.). `_get_performance_flag()` returns blank for descriptive metrics — prevents misleading "Top Quartile" / "Bottom Quartile" flags on non-evaluative fields.

3. **Norm_Provision_Rate suppressed from presentation**: Excluded from `NORMALIZED_COMPARISON_METRICS` since it is intentionally NaN (provision expense is not segment-specific). Dead-metric suppression in HTML tables also catches it.

4. **Display labels**: Both `create_peer_comparison` and `create_normalized_comparison` use `_get_metric_short_name(metric)` which resolves display names from `FDIC_Metric_Descriptions` (via `MasterDataDictionary.lookup_metric`). Falls back to the metric code itself when no display label exists.

5. **Chart resilience (report_generator.py)**: `create_credit_deterioration_chart_ppt` filters out CERTs missing from data before plotting. Uses `colors.get(c, fallback)` instead of `colors[c]`. Skips all-NaN peer lines with warning. Removed `fig.tight_layout()` call that conflicted with `twinx()` + `GridSpec`.

6. **Ratio components table resilience (report_generator.py)**: `generate_ratio_components_table` no longer aborts when peer composite is missing. Subject bank is required; peer defaults to `pd.Series()` (renders as N/A).

7. **Regression tests (test_regression.py)**: Added 6 function-based tests + 8 unittest methods covering curated allowlists, metric-role policy, display labels, preflight logic, tight_layout removal, and ratio components safe lookup.

### 2026-03-10 — Consistency Pass: TTM Fix, Validation Wiring, Doc Cleanup

1. **Norm_Loan_Yield / Norm_Loss_Adj_Yield TTM fix (MSPBNA_CR_Normalized.py)**: Root cause was a column-name mismatch in the TTM rolling map. `col.replace('_YTD', '_Q')` produces `Int_Inc_Loans_Q` but the TTM map key was `Int_Inc_Loans_YTD_Q`. Same bug for `Provision_Exp_Q` and `Total_Int_Exp_Q`. Corrected all three TTM map keys. Also fixed `prov_q` reference and changed fallback from 0 to NaN so missing TTM columns produce NaN instead of silent zeros.

2. **Norm_Provision_Rate intentionally NaN**: Confirmed by-design — provision expense is not segment-specific in call reports. Documented in Section 8.

3. **Validation wiring (MSPBNA_CR_Normalized.py)**: Wired `run_upstream_validation_suite(proc_df_with_peers)` from `metric_registry.py` into Step 1, just before Excel output. Results go to `Metric_Validation_Audit` sheet. Wrapped in try/except for resilience.

4. **CLAUDE.md cleanup**: Removed stale `19977` default examples (replaced with `34221`/`32992`). Removed false claim that validation suite was already wired. Updated Section 8 (known issues) to reflect actual metric status. Updated Section 9 (validation engine) to accurately describe wiring.

5. **Regression tests**: Added TTM column name tests, validation wiring source check, no-stale-19977-defaults test.

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

### 2026-03-10 — 7-Part Defect Fix (Normalized Metrics, Scatter, Validation)

**Root Causes:**
- Scatter contamination: `build_plot_df_with_alias` appended alias rows instead of replacing, causing composite CERTs (90004, 90006) to appear as blue peer dots in normalized scatter plots.
- Silent NCO zeroing: `.clip(lower=0)` at 6 locations masked over-exclusion errors where Excluded_* exceeded Total_*, producing misleading zero values instead of NaN.
- Duplicate peer groups: 90001/90004, 90002/90005, 90003/90006 share identical cert lists — distinction is only `use_normalized` flag.

**Fixes applied:**
1. **Scatter integrity (report_generator.py)**: Removed all `build_plot_df_with_alias` calls and later deleted the function definition entirely. `plot_scatter_dynamic()` now has `composite_certs` parameter (default: all 9 synthetic CERTs) that excludes composites from peer dots. Standard/normalized scatters pass true composite CERTs (`90003`/`90006`) directly via `peer_avg_cert_primary`.

2. **Diagnostics-first normalization (MSPBNA_CR_Normalized.py)**: Replaced `.clip(lower=0)` with tolerance-aware logic: minor negatives (<5% of total) clip to 0, material negatives become NaN. Added diagnostic columns: `_Norm_NCO_Residual`, `_Norm_NA_Residual`, `_Norm_Loans_Residual`, `_Flag_*_OverExclusion`, `_*_OverExclusion_Pct`, `_Norm_*_Severity` (ok/minor_clip/material_nan).

3. **Peer group validation (MSPBNA_CR_Normalized.py)**: Added `validate_peer_group_uniqueness()` — hard validation that no two groups within the same `use_normalized` mode share identical cert lists. Added `build_peer_group_definitions_df()` for Excel output.

4. **Semantic validation rules (metric_registry.py)**: Added `ValidationRule` dataclass and 5 rules: (A) over-exclusion check, (B) flatline anomaly detection, (C) duplicate composite detection, (D) output contamination check, (E) consumer linkage audit. `run_semantic_validation()` and `run_full_validation_suite()` entry points.

5. **Preflight validation (report_generator.py)**: Added `validate_output_inputs()` — runs before report generation, checks for missing subject bank, all-NaN/all-zero normalized metrics, material over-exclusion severity, missing real peers, and semantic validation. Aborts on blocking errors, prints warnings.

6. **Regression tests (test_regression.py)**: 11 executable assertions covering scatter integrity, peer group uniqueness, over-exclusion detection, workbook sanity, consumer trace, and semantic validation.

**New Excel sheets:** `Normalization_Diagnostics`, `Peer_Group_Definitions`

### 2026-03-10 — Normalized Residential QA (Part 4B)

**Defects found:**
1. **Naming collision**: `Norm_Resi_Composition` referenced in detailed peer table and segment focus table but never computed upstream. Canonical name is `Norm_Wealth_Resi_Composition`.
2. **Casing inconsistency**: Upstream created `Norm_RESI_ACL_Coverage`, report_generator expected `Norm_Resi_ACL_Coverage`.
3. **Mislabeled metadata**: `Norm_RESI_ACL_Coverage` described as "Resi Reserve % of Norm ACL" — actually `RIC_Resi_ACL / Wealth_Resi_Balance` (coverage, not share). Same for `Norm_CRE_ACL_Coverage` and `Norm_Comm_ACL_Coverage`.
4. **Phantom column refs**: `RIC_Resi_Balance` and `RIC_CRE_Balance` in segment focus tables — these don't exist (should be `RIC_Resi_Cost` / `RIC_CRE_Cost`).

**Fixes applied:**
1. All `Norm_Resi_Composition` refs → `Norm_Wealth_Resi_Composition` in report_generator.py
2. `Norm_RESI_ACL_Coverage` → `Norm_Resi_ACL_Coverage` in upstream
3. Metadata display labels corrected to reflect actual numerator/denominator
4. `RIC_Resi_Balance` → `RIC_Resi_Cost`, `RIC_CRE_Balance` → `RIC_CRE_Cost`
5. Added `Norm_Wealth_Resi_Composition`, `Norm_Resi_ACL_Share`, `Norm_Resi_ACL_Coverage` to metric registry
6. Added `Resi_Normalized_Audit` Excel sheet
7. Added 4 regression tests for resi naming, coverage/share, and canonical references

**New Excel sheet:** `Resi_Normalized_Audit`

### 2026-03-10 — HUD USPS ZIP Crosswalk Enrichment for Case-Shiller Metros

1. **New module `case_shiller_zip_mapper.py`**: Maps 20 regional Case-Shiller metros to ZIP codes via HUD USPS Crosswalk API. Contains 20-entry `CASE_SHILLER_METRO_MAP` with CBSA/CBSA-Div codes, HUD API client with retry logic, coverage builder, summary aggregator, and 7-check validation.

2. **Metro mapping judgment calls**: 10 metros use CBSA Division-level mapping (type=9) where S&P CoreLogic tracks a subdivision: New York (Div 35614), Los Angeles (31084), Chicago (16974), Miami (33124), Washington (47894), Detroit (19804), Seattle (42644), Boston (14454), Dallas (19124), San Francisco (41884). Remaining 10 metros use CBSA-level (type=8).

3. **Integration into `fred_ingestion_engine.py`**: ZIP enrichment runs automatically after FRED data fetch in `run_expansion_pipeline_async()`. Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` env var (default: `true`). Graceful fallback if HUD token is missing or API fails.

4. **3 new Excel sheets**: `CaseShiller_Zip_Coverage` (one row per region/ZIP), `CaseShiller_Zip_Summary` (aggregate per metro), `CaseShiller_Metro_Map_Audit` (mapping table with judgment call notes).

5. **Excluded indexes**: U.S. National, Composite-10, Composite-20 are explicitly excluded (no geographic footprint).

### 2026-03-10 — Resi Series Expansion + Top-Down Normalization Fix

**Root cause**: A "OVERRIDE: Normalized Performance (Wealth Segments Only)" block was reconstructing `Norm_Total_NCO`, `Norm_Total_Nonaccrual`, `Norm_PD30`, and `Norm_PD90` bottom-up from `wealth_resi_nco_pure + sum_cols(...)`. This accidentally excluded SBL and any unmapped RESI lines from normalized totals.

**Fixes applied:**
1. **Expanded residential balance definitions**: `compute_wealth_resi_bal()` and `resi_sum` (for `RIC_Resi_Cost`) now include First Liens (1797) + Junior Liens (5367) + HELOC/Open-End (5368, 1799) with `best_of()` fallback. Both definitions are now consistent.
2. **Deleted bottom-up override**: Removed the 4 bottom-up assignments (`Norm_Total_NCO`, `Norm_Total_Nonaccrual`, `Norm_PD30`, `Norm_PD90`) from the override block.
3. **Restored top-down math**: `Norm_Total_NCO` and `Norm_Total_Nonaccrual` remain computed top-down via diagnostics-first logic. Added top-down `Norm_PD30 = TopHouse_PD30 - Excluded_PD30` and `Norm_PD90 = TopHouse_PD90 - Excluded_PD90`. Persisted `Excluded_PD30`/`Excluded_PD90` columns for this purpose.
4. **Retained segment metrics**: `Wealth_Resi_TTM_NCO_Rate`, `Wealth_Resi_NA_Rate`, `Wealth_Resi_Delinquency_Rate` remain as segment-level rates for HTML tables.
5. **CLAUDE.md**: Added "Normalization Methodology" convention mandating top-down-only approach.

### 2026-03-10 — Production-Safety Fix Pass

1. **Metadata sheet contract**: Renamed `FDIC_Metadata` kwarg to `FDIC_Metric_Descriptions` in `write_excel_output()` so workbook sheet name matches what `report_generator.py` reads.
2. **IDB label cleanup**: Removed `IDB_` prefix from all 17 keys in `master_data_dictionary.py` (`LOCAL_DERIVED_METRICS`) and updated 2 references in `MSPBNA_CR_Normalized.py`.
3. **Peer group docs**: Updated CLAUDE.md to reflect 4 groups (not 6); documented removal of MSPBNA+Wealth duplicates.
4. **Normalization conventions**: Added Section 6 documenting `_Norm_Total_Past_Due`, `Norm_ACL_Balance`, `Norm_Risk_Adj_Gross_Loans`, IDB label ban, and Case-Shiller toggle.

### 2026-03-10 — FRED Series Fix + metric_registry + Dead Code Cleanup

1. **FRED series typos fixed (MSPBNA_CR_Normalized.py)**: `CORCCLACBS` → `CORCCACBS`, `RCVRUSQ156N` → `RHVRUSQ156N`.
2. **Discontinued FRED series removed**: `GOLDAMGBD228NLBM` (discontinued), `DEPALL` (redundant with `DPSACBW027SBOG`).
3. **metric_registry.py — Norm_ACL_Coverage fix**: Changed dependencies from `Total_ACL` to `Norm_ACL_Balance`. Added `Norm_Risk_Adj_Allowance_Coverage` spec (`Norm_ACL_Balance / (Norm_Gross_Loans - SBL_Balance)`).
4. **Removed `build_plot_df_with_alias()` (report_generator.py)**: Function was dead code — defined but never called. Removed to eliminate confusion with SCATTER & CHART COMPOSITE HANDLING convention.
5. **CLAUDE.md**: Added FRED Series Validation convention, updated scatter handling docs, documented all changes.

**Remaining risks:**
- `Norm_Provision_Rate` is intentionally NaN — provision expense is not segment-specific in call reports, so a normalized rate would be semantically misleading.
- `run_upstream_validation_suite()` is now wired into MSPBNA_CR_Normalized.py (Step 1) and writes `Metric_Validation_Audit` sheet.

### 2026-03-10 — Targeted Cleanup Pass (ZIP Toggle, Metro Map, Tests, Preflight)

1. **case_shiller_zip_mapper.py — consolidated dual implementations**: Removed the second concatenated simple-version implementation (lines 767-897). Kept the comprehensive HUD API version as the single authoritative implementation. Moved `is_zip_enrichment_enabled()` to module-level. Added env toggle check at top of `build_case_shiller_zip_sheets()` — when disabled, returns empty Coverage/Summary DataFrames with audit noting `SKIPPED: disabled by env flag`. Preserved `CASE_SHILLER_METROS` dict and `map_zip_to_metro()` as backward-compatible helpers.

2. **fred_case_shiller_discovery.py — METRO_MAP fixes**: Fixed duplicate key `"CH"` (was mapped to both Chicago and Charlotte — Charlotte's correct FRED prefix is `"CR"`). Fixed `"Washington DC"` → `"Washington"` to match case_shiller_zip_mapper.py. Added `validate_metro_map()` assertion helper that checks entry count, duplicate values, and Washington naming.

3. **test_regression.py — stale peer group test rewrite**: Rewrote `test_peer_group_no_intra_mode_duplicates()` to import real `PEER_GROUPS` from `MSPBNA_CR_Normalized.py` (4 groups, not former 6). Added assertion for exactly 4 groups. Updated `test_zip_toggle_disables_output` to validate HUD version output (empty Coverage/Summary, SKIPPED in audit). Replaced old simple-version tests (`test_zip_codes_are_5_char_strings`, `test_summary_zip_count_matches_detail`) with unit tests for `_normalize_zip()` and `map_zip_to_metro()`. Added `test_discovery_metro_map_no_duplicates` and `test_discovery_washington_no_dc_suffix`.

4. **report_generator.py — preflight blocks on material normalization**: Updated `validate_output_inputs()` so material normalization severity (`material_nan`) on the **subject bank** is now a **blocking error** (added to `errors` list, suppresses affected normalized charts). Peer-only material failures remain warnings.

5. **Verified**: `IDB_` prefix absent from `master_data_dictionary.py`. `FDIC_Metric_Descriptions` used consistently across all files.

### 2026-03-10 — Preflight Hardening, Peer-Average Blocking, Test Expansion

1. **report_generator.py — preflight now blocks on plotted peer-average failures**: `validate_output_inputs()` now checks material normalization severity (`material_nan`) on plotted peer-average composites (90004, 90006) in addition to the subject bank. Any material failure on these CERTs is a blocking error that suppresses normalized charts/scatter.

2. **report_generator.py — composite existence validation**: Preflight now validates that all required composite CERTs exist in data before plotting. Standard flow requires 90003 + 90001; normalized flow requires 90006 + 90004. Missing composites produce blocking errors.

3. **report_generator.py — readable artifact names**: `suppressed_charts` now uses human-readable names (`normalized_credit_chart`, `normalized_scatter_nco_vs_nonaccrual`, `standard_scatter_nco_vs_npl`, `standard_scatter_pd_vs_npl`) instead of severity-column-derived names.

4. **test_regression.py — 7 new tests added**:
   - `test_no_idb_keys_in_data_dictionary` — asserts `LOCAL_DERIVED_METRICS` has no `IDB_` keys
   - `test_preflight_blocks_peer_avg_material_nan` — 90006 with `material_nan` blocks
   - `test_preflight_blocks_missing_normalized_composite` — missing 90004/90006 blocks
   - `test_claude_md_no_conflict_markers` — no git merge conflict markers in CLAUDE.md
   - `TestIDBCleanup.test_no_idb_keys_in_local_derived_metrics` (unittest)
   - `TestPreflightValidation` class (3 tests: peer-avg blocking, missing composite, healthy pass)
   - `TestClaudeMDIntegrity.test_no_merge_conflict_markers` (unittest)

5. **CLAUDE.md — preflight docs updated**: Section 9 (Preflight Validation) now accurately documents peer-average blocking, composite existence checks, and readable artifact names.

6. **Verified**: No merge conflict markers in CLAUDE.md. No `IDB_` keys in `master_data_dictionary.py`.

---

## 8. To-Do / Known Issues

### Normalized Profitability Metrics (MSPBNA_CR_Normalized.py)

| Metric | Status | Notes |
|---|---|---|
| `Norm_Loan_Yield` | **Fixed** | `Int_Inc_Loans_TTM / Norm_Gross_Loans`. Root cause was a column-name mismatch in the TTM map: `col.replace('_YTD', '_Q')` produces `Int_Inc_Loans_Q` but the TTM map key was `Int_Inc_Loans_YTD_Q`. Corrected. Source: `ILNDOM + ILNFOR` (both fetched). |
| `Norm_Provision_Rate` | **Intentionally NaN** | Provision expense (`ELNATR`) is not segment-specific in call reports. A normalized rate denominated by `Norm_Gross_Loans` would be semantically misleading since provision flow includes C&I/Consumer. Set to NaN by design. |
| `Norm_Loss_Adj_Yield` | **Fixed** (cascading) | `Norm_Loan_Yield - Norm_NCO_Rate`. Auto-resolves now that `Norm_Loan_Yield` is populated. |

**TTM pipeline for income metrics**: `ILNDOM + ILNFOR → Int_Inc_Loans_YTD → replace('_YTD','_Q') → Int_Inc_Loans_Q → rolling(4).sum() → Int_Inc_Loans_TTM`. Same pattern for `Provision_Exp_YTD → Provision_Exp_Q → Provision_Exp_TTM`.

---

## 9. Metric Registry & Validation Architecture

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

`run_upstream_validation_suite(df)` is called in `MSPBNA_CR_Normalized.py` just before the Excel write (after normalization diagnostics and composite coverage audit). For each registered metric, it:

1. **Recomputes** the metric from its declared formula (`spec.compute(df)`)
2. **Compares** the recomputed value against the stored column value
3. **Checks bounds** (min/max) and sign constraints
4. **Tags** each row with `CERT` and `REPDTE` for row-level tracing

Results are exported to the `Metric_Validation_Audit` sheet in the Excel dashboard. The call is wrapped in try/except — if the suite fails, the pipeline continues with an empty audit sheet.

### Dependency Graph

`build_reverse_dependency_map()` returns `{upstream_col: [derived_metrics]}`, enabling impact analysis: if an upstream column (e.g., `Gross_Loans`) is missing or corrupt, the graph shows exactly which derived metrics and downstream reports are affected.

### Semantic Validation Rules (A–E)

Beyond formula recomputation, `metric_registry.py` now includes 5 semantic validation rules via `run_semantic_validation(df)`:

| Rule | ID | Description |
|---|---|---|
| Over-Exclusion | A | Flags rows where `Excluded_*` exceeds `Total_*` |
| Flatline Anomaly | B | Flags normalized metrics that are constant across all entities in latest quarter |
| Duplicate Composite | C | Flags standard/normalized composites producing identical metric values |
| Output Contamination | D | Flags composite CERTs with metric values they shouldn't have (e.g., std composite with non-null Norm_* rates) |
| Consumer Linkage | E | Flags metrics with no declared downstream consumers (orphans) |

`run_full_validation_suite(df)` returns both the formula validation report and the semantic validation report.

### Normalization Diagnostics

The normalization pipeline no longer uses silent `.clip(lower=0)`. Instead:

| Column | Purpose |
|---|---|
| `_Norm_NCO_Residual` / `_Norm_NA_Residual` / `_Norm_Loans_Residual` | Raw subtraction residual (can be negative) |
| `_Flag_NCO_OverExclusion` / `_Flag_NA_OverExclusion` / `_Flag_Loans_OverExclusion` | Boolean (1 = exclusion > total) |
| `_*_OverExclusion_Pct` | Residual as % of total |
| `_Norm_*_Severity` | `ok` / `minor_clip` (< 5% over, clipped to 0) / `material_nan` (>= 5% over, set to NaN) |

### Preflight Validation

`validate_output_inputs()` in `report_generator.py` runs before any chart/table generation:
1. Checks subject bank CERT exists in data
2. Flags all-NaN or all-zero normalized metrics
3. **Blocks** on material over-exclusion severity for subject bank AND plotted peer-average composites (90004, 90006) — **scoped to latest plotted period only**. Historical material failures are logged as informational warnings but do NOT block.
4. **Blocks** if required composite CERTs are missing from data:
   - Standard: 90003 (All Peers), 90001 (Core PB)
   - Normalized: 90006 (All Peers Norm), 90004 (Core PB Norm)
5. Validates real peers exist for scatter plots
6. Runs semantic validation if metric_registry is available

**Period scoping**: The preflight determines `latest_repdte = proc_df["REPDTE"].max()` and only evaluates blocking severity checks against that single period. This prevents historical data-quality issues (which may have been present before balance-gating or upstream fixes) from blocking current-period report generation.

Suppressed charts use readable artifact names: `normalized_credit_chart`, `normalized_scatter_nco_vs_nonaccrual`, `standard_scatter_nco_vs_npl`, `standard_scatter_pd_vs_npl`.

---

## 10. FRED Expansion Layer — Registry-Driven Macro / Market Context

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

---

## 11. HUD USPS ZIP Crosswalk — Case-Shiller Metro Enrichment

### Overview

`case_shiller_zip_mapper.py` maps the 20 regional Case-Shiller metro indexes to ZIP codes using the HUD USPS ZIP Code Crosswalk API. This is an **internal reference mapping layer** — it does not alter FRED data or FDIC metrics. Its purpose is to enable downstream loan-level geographic tagging against Case-Shiller regions.

**Explicitly excluded**: U.S. National, Composite-10, and Composite-20 indexes (these have no geographic footprint to map).

### HUD API Token Setup (One-Time)

1. Register at https://www.huduser.gov/hudapi/public/register
2. Receive your access token via email
3. Set the environment variable:
   ```bash
   export HUD_USER_TOKEN='eyJ...'
   ```
   Or add to `.env`: `HUD_USER_TOKEN=eyJ...`
4. The token is a Bearer token used in the `Authorization` header

### Integration

ZIP enrichment runs automatically within `run_expansion_pipeline_async()` in `fred_ingestion_engine.py`, after FRED data fetch and Case-Shiller discovery. It is controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` (default: `true`). If the HUD token is missing, the enrichment is skipped gracefully with a warning.

### Metro → CBSA Mapping Judgment Calls

7 of the 20 metros use CBSA Division-level mapping (type=9) instead of full CBSA (type=8) because the S&P CoreLogic index tracks a subdivision of the broader metro:

| Metro | CBSA | Division Used | Rationale |
|---|---|---|---|
| New York | 35620 | 35614 (NY-Jersey City-White Plains) | Core NY metro tracked by S&P |
| Los Angeles | 31080 | 31084 (LA-Long Beach-Glendale) | Excludes Orange County |
| Chicago | 16980 | 16974 (Chicago-Naperville-Evanston) | Core Chicago metro |
| Miami | 33100 | 33124 (Miami-Miami Beach-Kendall) | Miami-Dade focus |
| Washington | 47900 | 47894 (DC-VA-MD-WV core) | Core DC metro |
| Detroit | 19820 | 19804 (Detroit-Dearborn-Livonia) | Core Detroit metro |
| Seattle | 42660 | 42644 (Seattle-Bellevue-Kent) | Core Seattle area |
| Boston | 14460 | 14454 (Boston, MA) | Core Boston metro |
| Dallas | 19100 | 19124 (Dallas-Plano-Irving) | Dallas division |
| San Francisco | 41860 | 41884 (SF-San Mateo-Redwood City) | Core SF area |

The remaining 10 metros (Atlanta, Charlotte, Cleveland, Denver, Las Vegas, Minneapolis, Phoenix, Portland, San Diego, Tampa) use CBSA-level mapping only as they have no subdivisions.

### Output Sheets

| Sheet | Description |
|---|---|
| `CaseShiller_Zip_Coverage` | One row per (region, ZIP) with CBSA codes, HUD ratios (`tot_ratio`, `res_ratio`, `bus_ratio`, `oth_ratio`), crosswalk vintage |
| `CaseShiller_Zip_Summary` | One row per metro: ZIP count, CBSA/Div counts, mapping type used, vintage |
| `CaseShiller_Metro_Map_Audit` | Full 20-entry mapping table with judgment call comments and `included_in_zip_output` flag |

### Validation (7 Checks)

1. No non-metro regions leak into coverage
2. No blank ZIP codes
3. All ZIPs are 5-character zero-padded strings
4. Summary ZIP counts reconcile with detail rows
5. No duplicate region definitions in metro map
6. All 20 metros have at least one ZIP row
7. HUD ratio columns are not entirely null
