# 99 — Changelog

History only — active instructions live in topic files `01`–`10`.

---

## 7. Changelog / Recent Fixes

### 2026-03-14 — Reconciliation & Hardening Pass (MSA Macro Feature)

**Objective:** Make code, tests, and docs describe the same architecture. Remove all contradictions between the old "not-yet-produced" / "future artifact" state and the current workbook-driven implementation.

**Contradictions removed:**
1. `test_regression.py::test_rendering_mode_documents_msa_panel_future` — asserted `"NOT yet produced"` in rendering_mode.py, but Prompt 4 already replaced that text with `"Produced by"`. Fixed: test now asserts the current state.
2. `test_regression.py::test_msa_macro_panel_not_produced_by_report_generator` — asserted `build_msa_macro_panel` absent (still correct) but did not verify `plot_msa_macro_panel` present. Fixed: renamed to `test_msa_macro_panel_uses_workbook_not_corp_overlay`, asserts both.
3. `test_regression.py::test_docs_tests_imports_triad_consistent` — only checked corp_overlay isolation, not local_macro isolation. Fixed: now also asserts no `from local_macro import` in report_generator.py.
4. `CLAUDE.md` Section 12: said `local_macro.py` produces "three Excel sheets" — actual count is six. Fixed.
5. `CLAUDE.md` Section 13 overview: same "three" → "six" fix.
6. `CLAUDE.md` Known Limitations: said "Census and BEA enrichment hooks are stub implementations" and "API calls are hook points, not yet implemented" — stale since `local_macro.py` has real API implementations. Removed.
7. `CLAUDE.md` Architecture Reconciliation changelog: described the intermediate state ("not-yet-produced", "future artifact") as if it were the final state. Consolidated into a single entry documenting the full multi-step resolution.
8. `CLAUDE.md` Chart Package Expansion: said `select_top_msas`/`build_msa_macro_panel` added to corp_overlay without noting they are superseded. Added superseded note.

**Final architecture contract (single source of truth):**
- `corp_overlay.py` — standalone module, NOT imported by report_generator or MSPBNA
- `local_macro.py` — owns all local macro data (BEA/BLS/Census APIs, geography spine, crosswalk audit); called by MSPBNA_CR_Normalized.py (Step 1); produces 6 Excel sheets
- `report_generator.py` — consumes workbook sheets only (`Local_Macro_Latest`, `MSA_Crosswalk_Audit`); never imports local_macro or corp_overlay; `plot_msa_macro_panel()` skips cleanly when sheets absent

**Files changed:** `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Fix 3 Post-Fix Regressions (8 → 0 Failed Artifacts)

**Bug A — numpy array truth test in credit charts**: `if idx_to_label` on a numpy array raised `ValueError`. Fixed with `if len(idx_to_label) > 0`. Restored: `standard_credit_chart`, `normalized_credit_chart`.

**Bug B — `placer` never instantiated in scatter plots**: `plot_scatter_dynamic()` called `placer.place()` but never created the object. Reverted annotation calls to use the existing in-scope `pick_offset()`/`tag()` helpers. Restored: all 5 scatter plot artifacts.

**Bug C — `strftime("%q")` invalid on Windows**: `%q` is not a valid C strftime code. Replaced with direct f-string formatting (`f"{dt.year} Q{(dt.month - 1) // 3 + 1}"`). Restored: `macro_corr_heatmap_lag1`.

**Figure cleanup**: Added `plt.close(fig)` in `_produce_chart()` after saving to prevent "More than 20 figures" RuntimeWarning during long runs.

**Files changed:** `report_generator.py`, `CLAUDE.md`

### 2026-03-13 — Workbook-Driven MSA Macro Panel (Prompt 4)

**Objective:** Replace the synthetic msa_macro_panel path with workbook-driven reporting. Complete the final reporting integration for geography-aware macro context.

**Dependency chain (end-to-end):**
```
Step 1 (MSPBNA_CR_Normalized.py)
  └─ local_macro.py::run_local_macro_pipeline()
       └─ BEA/BLS/Census APIs → geography spine → derived metrics
       └─ Writes: Local_Macro_Latest, MSA_Board_Panel, MSA_Crosswalk_Audit (+ 3 others)
       └─ If no data: writes Local_Macro_Skip_Audit with explicit reason
            ▼
Dashboard workbook (Bank_Performance_Dashboard_*.xlsx)
            ▼
Step 2 (report_generator.py)
  └─ plot_msa_macro_panel()
       └─ Reads Local_Macro_Latest from workbook (pd.ExcelFile)
       └─ Reads MSA_Crosswalk_Audit for quality flags
       └─ Top MSA selection: portfolio_balance → real_gdp_level → first 10
       └─ Produces msa_macro_panel PNG chart
       └─ Skips cleanly if sheets absent (no synthetic fallback)
```

**Changes:**

1. **`report_generator.py`** — Added `plot_msa_macro_panel()` and `_load_local_macro_sheet()`. Replaced the "NOT produced here" comment (lines 2879-2883) with `_produce_chart()` call. Chart reads workbook sheets, selects top MSAs by portfolio_balance (data-driven), shows correct units (GDP=%, unemployment=pp, GDP/100k=$), adds mapping-quality warnings and macro_stress_flag summary. Does NOT import `local_macro` or `corp_overlay`.

2. **`rendering_mode.py`** — Updated `msa_macro_panel` comment block from "NOT yet produced" to "Produced by report_generator.py::plot_msa_macro_panel()". Updated description.

3. **`test_regression.py`** — `TestMSAMacroPanelReporting` class (12 tests): no synthetic random data, chart reads workbook sheets, data-driven MSA selection, missing sheets cause controlled skip, correct unit labels, mapping quality propagation, no corp_overlay/local_macro imports, rendering_mode documents production, _produce_chart call exists, stress flag in chart, CLAUDE.md documentation.

4. **`CLAUDE.md`** — Updated Section 12 production status, added full dependency chain diagram, changelog entry.

**Synthetic logic removed:** The original `build_msa_macro_panel()` / `select_top_msas()` functions in `corp_overlay.py` are superseded. `report_generator.py` has zero references to synthetic MSA data generation. All data flows through the workbook.

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Board-Ready Workbook Output Persistence (Prompt 3)

**Objective:** Write canonical local macro sheets into the dashboard workbook so reporting artifacts can consume real geography-aware macro context.

**Changes:**

1. **Board-ready sheet builders** (`local_macro.py`) — Three new functions:
   - `build_local_macro_latest()` — One row per CBSA with all 30 BOARD_COLUMNS, latest-period values, macro_stress_flag (OK/WATCH/STRESS), geo_level classification, mapping quality persistence
   - `build_msa_board_panel()` — Compact presentation panel sorted by GDP level descending, subset of key columns for executive consumption
   - `build_skip_audit()` — Single-row audit explaining why pipeline was skipped (reason + context + timestamp)

2. **Pipeline output expansion** (`local_macro.py`) — `run_local_macro_pipeline()` now returns 6 sheets: Local_Macro_Raw, Local_Macro_Derived, Local_Macro_Mapped, Local_Macro_Latest, MSA_Board_Panel, MSA_Crosswalk_Audit.

3. **Skip-audit persistence** (`MSPBNA_CR_Normalized.py`) — Updated local macro integration block to write `Local_Macro_Skip_Audit` sheet when:
   - Pipeline returns no data (API keys missing)
   - `local_macro` module not importable
   - Pipeline raises an exception
   This ensures downstream consumers see an explicit skip reason rather than silent omission.

4. **Regression tests** (`test_regression.py`) — `TestLocalMacroWorkbookOutput` class (12 tests): pipeline returns all 6 keys, board columns present, skip audit structure, mapping quality persists, source metadata persists, geo_level classification, macro_stress_flag values, existing sheets not broken, CLAUDE.md documentation.

5. **CLAUDE.md** — Updated Section 13 output sheets table (7 sheets), added BOARD_COLUMNS specification (30 columns), added skip-audit documentation.

**Files changed:** `local_macro.py`, `MSPBNA_CR_Normalized.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Math Correctness & Quarterly Alignment (Prompt 2)

**Objective:** Add mathematically correct per-capita/per-100k GDP normalization and metadata-driven quarterly transformation policy to the local macro pipeline.

**Changes:**

1. **Transformation policy registry** (`local_macro.py`) — Added `TransformPolicy` dataclass and `TRANSFORM_POLICIES` dict with per-family metadata: `transform_type`, `aggregation_rule`, `date_basis`, `quarter_offset`, `units`, `per_capita_eligible`. Four families registered: gdp (sum/level), unemployment (mean/rate), population (point_in_time/level), hpi (last/index).

2. **Per-capita math helpers** (`local_macro.py`) — Six new functions:
   - `validate_population()` — zero/negative hard-fails, NaN preserves
   - `compute_real_gdp_per_capita()` — GDP level / population
   - `compute_real_gdp_per_100k()` — per-capita × 100,000
   - `compute_yoy_from_level()` — `series / lag(series) - 1` (levels only)
   - `compute_unemployment_change_pp()` — arithmetic diff in pp (NOT pct change)
   - `aggregate_to_quarter()` — dispatches to mean/last/sum/point_in_time per policy

3. **Derived metrics builder** (`local_macro.py`) — `build_derived_metrics()` orchestrates per-CBSA: GDP per-capita, per-100k, per-100k YoY%; quarterly unemployment mean, unemployment change pp. Integrated into `run_local_macro_pipeline()` — new `Local_Macro_Derived` output sheet.

4. **Regression tests** (`test_regression.py`) — `TestLocalMacroMathAndAlignment` class (18 tests): constant-pop identity, flat-GDP-rising-pop decline, per-100k equivalence, pp-not-pct, zero/negative pop fail, NaN pop propagation, lag-4 quarterly YoY, aggregation rules per family, policy field completeness, units correctness, derived metrics output, CLAUDE.md documentation check.

5. **CLAUDE.md** — Updated Section 13 with transformation policy registry table, per-capita formulas, hard rules, identity checks, helper function reference.

**Files changed:** `local_macro.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Canonical Geography Spine & Local Macro Pipeline

**Objective:** Build a real local/state/MSA macro pipeline with canonical geography spine and explicit mapping audit trail, replacing the placeholder MSA macro panel path removed during architecture reconciliation.

**New module: `local_macro.py`**

1. **Geography spine** (`build_geography_spine()`) — Resolves inputs through a 4-tier mapping hierarchy:
   - **Tier 1 (direct CBSA):** Input CBSA code matched against canonical TOP_MSAS list → quality `high`
   - **Tier 2 (ZIP → CBSA):** HUD USPS Crosswalk API type=4 lookup → quality `medium`
   - **Tier 3 (county → CBSA):** Internal `_COUNTY_TO_CBSA` table (~130 county FIPS → CBSA mappings) → quality `low`
   - **Tier 4 (state fallback):** State-level aggregation when no MSA resolution possible → quality `low`
   - **Unmatched:** Geographies that cannot be resolved at any tier → quality `unmatched`

2. **Macro source fetchers:**
   - `fetch_bea_gdp_metro()` — BEA Regional Economic Accounts API (CAGDP2 table, metro GDP)
   - `fetch_bls_unemployment_metro()` — BLS LAUS API (series LAUMT{sfips}{cbsa}00000003)
   - `fetch_census_population_metro()` — Census Population Estimates Program API

3. **Pipeline orchestrator** (`run_local_macro_pipeline()`) — Returns dict with three canonical output sheets:
   - `Local_Macro_Raw` — Raw API responses with source metadata (source_dataset, source_series_id, source_frequency, data_vintage, load_timestamp)
   - `Local_Macro_Mapped` — Geography-resolved macro data joined to spine
   - `MSA_Crosswalk_Audit` — Full audit trail of every geography resolution attempt with mapping_method and quality flags

4. **Design principles:**
   - Case-Shiller ZIP mapper (`case_shiller_zip_mapper.py`) is NOT reused as generic spine — kept separate per distinct methodology
   - All API calls wrapped in try/except with graceful degradation — pipeline never crashes if APIs unavailable
   - Source provenance metadata on every row (source_dataset, source_series_id, source_frequency, data_vintage, load_timestamp)
   - No synthetic/placeholder data — empty DataFrames returned when APIs are unavailable

**Integration into `MSPBNA_CR_Normalized.py`:**
- Added local macro pipeline call block between Case-Shiller enrichment and diagnostic logging
- Uses same `**kwargs` pattern as Case-Shiller for `write_excel_output()` — `**local_macro_kwargs` alongside `**cs_kwargs`
- Graceful degradation: ImportError skips silently, other exceptions logged as warnings (non-fatal)
- API keys read from `DashboardConfig` attrs: `hud_user_token`, `bea_api_key`, `census_api_key`

**Tests added** (`test_regression.py` — `TestLocalMacroGeographySpine`, 16 tests):
- `test_direct_cbsa_mapping_works` — CBSA "35620" resolves to "New York"
- `test_state_fallback_is_flagged` — state fallback gets quality "low"
- `test_unmatched_cbsa_visible_in_audit` — unmatched geos appear in audit
- `test_case_shiller_mapper_not_reused_as_generic_spine` — no import from case_shiller_zip_mapper
- `test_msa_crosswalk_audit_always_produced` — audit always returned
- `test_no_synthetic_data_in_local_macro` — no RandomState/synth_df
- `test_county_fips_to_cbsa_mapping_works` — "06037" → "31080" (LA)
- `test_pipeline_integration_in_mspbna` — verifies run_local_macro_pipeline called
- `test_zip_without_hud_token_produces_audit_entries`
- `test_claude_md_documents_local_macro`

**Files created:** `local_macro.py`
**Files changed:** `MSPBNA_CR_Normalized.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Architecture Reconciliation (Corp Overlay / MSA Macro Panel)

**Objective:** Resolve architecture drift between `report_generator.py`, `corp_overlay.py`, `test_regression.py`, and `CLAUDE.md` regarding ownership of MSA macro panel logic and the corp_overlay standalone contract.

**Problem:** `report_generator.py` originally imported `corp_overlay` to produce `msa_macro_panel` with synthetic placeholder data, violating the standalone contract.

**Resolution (multi-step):**
1. Removed the synthetic import/placeholder path from `report_generator.py`.
2. Built `local_macro.py` as the canonical macro data source (BEA/BLS/Census APIs, geography spine, MSA crosswalk audit).
3. Integrated `local_macro.py` into Step 1 (`MSPBNA_CR_Normalized.py`) to write six macro sheets to the dashboard workbook.
4. Added `plot_msa_macro_panel()` to `report_generator.py` — reads `Local_Macro_Latest` and `MSA_Crosswalk_Audit` from the workbook (no `corp_overlay` or `local_macro` import).
5. Updated `rendering_mode.py` to document `msa_macro_panel` as produced (workbook-driven).
6. `TestArchitectureReconciliation` (9 tests) enforces: no corp_overlay import, no synthetic data, workbook-driven panel, triad consistency.

**Final contract:** `corp_overlay.py` standalone · `local_macro.py` owns macro data · `report_generator.py` reads workbook sheets only.

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Final Production Fixes & Pipeline Runner

**4-item change set (Prompt 6):**

1. **Capital risk metric computation** (`MSPBNA_CR_Normalized.py`): Added `CRE_Concentration_Capital_Risk` and `CI_to_Capital_Risk` computation in the derived-metrics section before `return df_final.copy()`. Uses `safe_div(RIC_CRE_Cost / RBCT1J)` and `safe_div(LNCI / RBCT1J)` respectively, guarded by column-existence checks. These metrics had `MetricSpec` and `MetricSemantic` entries but the actual computation was missing, causing the `concentration_vs_capital` chart to fail.

2. **CHART_COLORS unified with CHART_PALETTE** (`report_generator.py`): Replaced the legacy `CHART_COLORS` dict (which had divergent hex values for purple `#9C6FB6` and peer_cloud `#A8B8C8`) with a derived view that references `CHART_PALETTE` directly. Purple is now consistently `#7B2D8E` and peer_cloud is `#8FA8C8` everywhere.

3. **Dead code removed** (`report_generator.py`): Deleted unreachable `return manifest` after the `try/except/finally` block in `generate_reports()`. The `try` and `except` blocks both have their own `return manifest`; the `finally` block always executes; the line after `finally` was dead code.

4. **Pipeline runner** (`run_pipeline.py`): New unified CLI script that runs Step 1 (`MSPBNA_CR_Normalized.py`) and Step 2 (`report_generator.py`) sequentially via subprocess (not import, per CLAUDE.md Section 4 "IMPORT SAFETY"). Features: `--mode` (full_local/corp_safe), `--step` (1/2/both), `--force` (continue on Step 1 failure), `.env` auto-loading, combined timing summary.

**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `CLAUDE.md`
**Files created:** `run_pipeline.py`

### 2026-03-13 — FRED Series Audit & Fetch Resilience

**3-item change set (Prompt 4):**

1. **FRED series audit**: Audited 10 failed FRED series IDs. All are valid active series — failures were VPN/proxy connection blocks, not bad IDs. One discontinued series found and removed: **STLFSI2** (St. Louis Financial Stress Index v2, last observation 2022-01-07, replaced by STLFSI4). STLFSI4 was already in `FRED_SERIES_TO_FETCH`. USALOLITONOSTSAM is valid but stale (last obs January 2024) — flagged for monitoring.

2. **Macro chart series update**: Replaced STLFSI2 → STLFSI4 in `MACRO_CORR_FRED_SERIES` and `_FRED_DISPLAY` in `report_generator.py`. All 13 macro chart FRED series confirmed present and active in `FRED_SERIES_TO_FETCH`.

3. **FRED fetch resilience** (`FREDDataFetcher` in `MSPBNA_CR_Normalized.py`):
   - `_fetch_single_series` now classifies failures: `FAIL_INVALID_ID` (HTTP 400 — bad/discontinued ID, no retry), `FAIL_CONNECTION` (DNS/proxy/timeout — retried with exponential backoff up to 3 times at 2s/4s/8s), `FAIL_OTHER` (unexpected errors, no retry).
   - Returns 3-tuple `(series_id, DataFrame_or_None, failure_class_or_None)` instead of 2-tuple.
   - `fetch_all_series_async` logs a structured summary: total/fetched/failed counts, breakdown by failure type (invalid IDs, connection failures, other), and a list of any missing macro chart series (cross-referenced against `MACRO_CORR_FRED_SERIES` from `report_generator.py`).

**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Capital Concentration Ratios & Cumulative Growth Base-Period Fix

**3-item change set:**

1. **Capital concentration ratios** (`CRE_Concentration_Capital_Risk`, `CI_to_Capital_Risk`): Computed in the derived-metrics section of `MSPBNA_CR_Normalized.py` using `RBCT1J` (Tier 1 Capital). `CRE_Concentration_Capital_Risk = RIC_CRE_Cost / RBCT1J`, `CI_to_Capital_Risk = LNCI / RBCT1J`. Both guarded by column-existence checks. Added `MetricSpec` entries in `metric_registry.py` (unit=multiple, consumer=concentration_vs_capital). Added `MetricSemantic` entries in `metric_semantics.py` (Polarity.ADVERSE, DisplayFormat.MULTIPLE, group=Coverage).

2. **Cumulative growth base-period auto-advance** (`executive_charts.py`): When the default Q4 2015 anchor has zero CRE balance or zero CRE ACL (as happens for MSPBNA which had no CRE exposure at that date), `prepare_cumulative_growth_data()` now auto-advances to the first subsequent quarter where both Target Loans and Target ACL are nonzero. Previously this returned `None`, causing both `cumul_growth_loans_vs_acl_wealth` and `cumul_growth_loans_vs_acl_allpeers` charts to skip entirely.

3. **RBCT1J confirmed present** in `FDIC_FIELDS_TO_FETCH` — no action needed.

**Files changed:** `MSPBNA_CR_Normalized.py`, `metric_registry.py`, `metric_semantics.py`, `executive_charts.py`, `CLAUDE.md`

### 2026-03-13 — Cross-Regime NaN-Out for Peer Composites

**Problem**: Standard composites (90001, 90003) carried non-null `Norm_*` values and normalized composites (90004, 90006) carried non-null standard rate values, triggering 5 OutputContamination preflight warnings in `report_generator.py`.

**Fix**: Added cross-regime NaN-out in `_create_peer_composite()` (after setting `HQ_STATE`, before appending to composites list):
- Standard composites (`use_normalized=False`): all `Norm_*` columns set to NaN
- Normalized composites (`use_normalized=True`): 6 standard rate columns (`TTM_NCO_Rate`, `NPL_to_Gross_Loans_Rate`, `Nonaccrual_to_Gross_Loans_Rate`, `Past_Due_Rate`, `Allowance_to_Gross_Loans_Rate`, `Risk_Adj_Allowance_Coverage`) set to NaN
- `RIC_*` segment metrics and size/balance columns are preserved for both regimes

**Files changed:** `MSPBNA_CR_Normalized.py`, `CLAUDE.md`

### 2026-03-13 — Cumulative Growth Chart Family (Target Loans vs CRE ACL)

**Objective**: Add a new chart family tracking cumulative growth of CRE+RESI balances against CRE ACL growth, indexed to zero at Q4 2015, with endpoint CAGR annotations.

**2 new artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `cumul_growth_loans_vs_acl_wealth` | `{stem}_cumul_growth_loans_vs_acl_wealth.png` | FULL_LOCAL_ONLY | PNG |
| `cumul_growth_loans_vs_acl_allpeers` | `{stem}_cumul_growth_loans_vs_acl_allpeers.png` | FULL_LOCAL_ONLY | PNG |

**Implementation:**

1. **Data preparation** (`prepare_cumulative_growth_data()`): Computes cumulative % growth from Q4 2015 anchor for Target Loans (CRE + RESI balance) and Target ACL (CRE ACL). Also computes running CAGR at each quarter.

2. **Chart output** (`plot_cumulative_growth_loans_vs_acl()`): 4 lines — MSPBNA Loans (solid gold), MSPBNA ACL (dashed gold), Peer Loans (solid peer color), Peer ACL (dashed peer color). Chart 1: MSPBNA vs Wealth Peers. Chart 2: MSPBNA vs All Peers.

3. **CAGR annotations**: Endpoint labels with `(Final/Base)^(1/years) - 1`. Horizontal dashed reference lines match parent series color. Anti-overlap via `_nudge_cagr_labels()` ensures min 3pp gap between adjacent labels.

4. **Helper functions in `executive_charts.py`**: `_find_col()` (column resolution), `prepare_cumulative_growth_data()`, `_nudge_cagr_labels()`, `plot_cumulative_growth_loans_vs_acl()`.

5. **Integration**: 2 artifacts registered in `rendering_mode.py` (FULL_LOCAL_ONLY). Wired into `report_generator.py` Phase 8 via `_cumul_specs` loop using standard composite CERTs (90001 Wealth, 90003 All Peers).

6. **Tests**: 12 new tests in `TestCumulativeGrowthChart`: artifact registration, mode availability, function existence, missing-cert returns None, zero growth at start date, anti-overlap prevention, import verification, Phase 8 wiring, color constants, start date constant.

**Files changed:** `executive_charts.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Comprehensive Formatting, Bug-Fix, and Feature Expansion Sweep

**Objective**: Standardize HTML table styling, fix chart generation bugs, expand views to cover both Standard and Normalized cuts, and split heatmaps/sparklines by peer group.

**6-part implementation:**

1. **PART 1 — Python bug fixes**:
   - **YoY heatmap RESI math fix**: Composition/share metrics (containing "Composition", "Loan_Share", "ACL_Share" in code name) now use percent-change `(cur/prior - 1)` for YoY instead of simple subtraction. This fixes the false 10% shrinkage display for RESI share metrics. Added `is_composition` flag to heatmap data rows and conditional formatting in `render_yoy_heatmap_html()`.
   - **Citi label bug (`_cc`/`CC`)**: Exhaustive search found zero instances — the bug does not exist in the current codebase. Documented as already resolved.

2. **PART 2 — HTML table formatting**:
   - **Thousands separators**: Dollar amounts formatted with `,` separator (e.g., `$3,752.7B`) in `_build_dynamic_peer_html()`.
   - **Norm→asterisk convention**: `_normalize_display_name()` helper replaces "Norm " prefix with trailing asterisk `*`. Applied across `generate_credit_metrics_email_table`, `generate_ratio_components_table`, `_build_dynamic_peer_html`, and `generate_html_email_table_dynamic`.
   - **Normalized footnote**: All normalized tables include `<p style="font-size: 10px; color: #555;">* Normalized for comparison</p>`.
   - **MSPBNA column highlight**: Already existed via `.subject-value` CSS class with `background-color: #E6F3FF`.

3. **PART 3 — Dual views verified**: `generate_reports()` already loops `for is_norm in [False, True]` for all table types. No changes needed.

4. **PART 4 — Heatmap and sparkline peer group split** (4 outputs each):
   - **Heatmaps**: Split from 2 to 4 outputs — Standard/Normalized × Wealth Peers/All Peers. New artifact names: `yoy_heatmap_standard_wealth`, `yoy_heatmap_standard_allpeers`, `yoy_heatmap_normalized_wealth`, `yoy_heatmap_normalized_allpeers`. Added `peer_label` parameter to `generate_yoy_heatmap()` and `render_yoy_heatmap_html()`. Column headers dynamically show peer group name (e.g., "Wealth Peers Current" instead of generic "Peers Current").
   - **Sparklines**: Split from 1 to 4 outputs — Standard/Normalized × Wealth Peers/All Peers. New artifact names: `sparkline_standard_wealth`, `sparkline_standard_allpeers`, `sparkline_normalized_wealth`, `sparkline_normalized_allpeers`. Added `SPARKLINE_METRICS_STANDARD` (7 metrics) and `SPARKLINE_METRICS_NORMALIZED` (5 metrics) lists. Added `peer_label` parameter to `generate_sparkline_table()`. Title includes peer group name.
   - Old artifact registrations removed: `yoy_heatmap_standard`, `yoy_heatmap_normalized`, `sparkline_summary`.
   - Phase 8 in `generate_reports()` now uses `_heatmap_specs` and `_sparkline_specs` loops.

5. **PART 5 — Expanded scatter plots**:
   - **CRE family**: `scatter_cre_nco_vs_nonaccrual` — X: RIC_CRE_Nonaccrual_Rate, Y: RIC_CRE_NCO_Rate (8Q avg, standard composites).
   - **Normalized bankwide**: `scatter_norm_acl_vs_delinquency` — X: Norm_Delinquency_Rate, Y: Norm_ACL_Coverage (8Q avg, normalized composites).
   - Both registered in `rendering_mode.py` as FULL_LOCAL_ONLY scatter artifacts.
   - Uses existing `plot_scatter_dynamic()` with Wealth Peers marker.

6. **PART 6 — CLAUDE.md**: Updated Executive Chart Artifacts table (8→14), output filenames, sparkline metric lists, scatter plot families, and this changelog.

**Files changed:** `executive_charts.py`, `report_generator.py`, `rendering_mode.py`, `CLAUDE.md`

### 2026-03-13 — Chart Package Expansion (Bookwide, Football-Field, MSA Panel)

**Objective**: Expand the chart package for board-level use with bookwide growth analysis, true football-field KRI visuals with nested peer bands, unit-family separation, and dynamic MSA macro panels.

**5-part implementation:**

1. **Bookwide growth vs deterioration chart** (`plot_growth_vs_deterioration_bookwide`): New scatter chart using trailing-4Q total gross loan growth (X-axis) vs TTM NCO Rate or NPL-to-Gross-Loans (Y-axis). Complements the existing CRE-focused `growth_vs_deterioration` chart. Same visual style with MSPBNA diamond, Wealth Peers triangle, All Peers square, and median quadrant crosshairs. X-axis formatted as %. Registered as `growth_vs_deterioration_bookwide` artifact.

2. **Football-field KRI charts with nested bands**: `generate_kri_bullet_chart()` in `executive_charts.py` rewritten with two new parameters: `wealth_member_certs` and `all_peers_member_certs`. When provided, computes actual min/max/median ranges from individual bank data instead of using composite CERT values. Produces nested bands: outer lighter band (#D0D0D0) = All Peers range, inner darker band (#B8A0C8) = Wealth Peers range. Median markers at each group center. Edge labels show min/max peer tickers via `resolve_display_label()`. Falls back to composite CERTs when member lists not provided.

3. **Unit family separation**: Standard KRI metrics split into two charts: `kri_bullet_standard` (5 % rate metrics) and `kri_bullet_standard_coverage` (2 x-multiple metrics). This ensures incompatible units never share a shared axis. Normalized was already split (rates vs composition). Total: 4 KRI football-field artifacts produced via unified `_bullet_specs` loop.

4. **Dynamic MSA macro panel**: Added `select_top_msas()` and `build_msa_macro_panel()` to `corp_overlay.py` as utility functions. *(These are now superseded by `report_generator.py::plot_msa_macro_panel()` which reads workbook sheets produced by `local_macro.py`. The corp_overlay utilities are retained for backward compatibility only.)* Critical unit rules enforced: GDP in %, unemployment in percentage points (pp). Registered as `msa_macro_panel` artifact.

5. **Tests and documentation**: Added 3 test classes (29 tests): `TestBookwideGrowthChart` (8 tests), `TestFootballFieldKRI` (12 tests), `TestMSAMacroPanel` (9 tests). Updated `test_registry_covers_known_artifacts` with new artifacts. Updated CLAUDE.md with football-field design, unit family split, MSA panel documentation, output filenames, and changelog.

**New metric lists (executive_charts.py):**
- `BULLET_METRICS_STANDARD_RATES` — 5 % rate metrics (split from BULLET_METRICS_STANDARD)
- `BULLET_METRICS_STANDARD_COVERAGE` — 2 x-multiple metrics (split from BULLET_METRICS_STANDARD)

**New constants (report_generator.py):**
- `_WEALTH_MEMBER_CERTS = [33124, 57565]` — GS, UBS (for football-field range computation)
- `_ALL_PEERS_MEMBER_CERTS = [32992, 33124, 57565, 628, 3511, 7213, 3510]` — all individual peers

**Files changed:** `report_generator.py`, `executive_charts.py`, `rendering_mode.py`, `corp_overlay.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-13 — Centralized Chart Palette, Annotation Engine, and Wealth Peers Inclusion

**Objective**: Establish a professional, board-ready charting foundation by enforcing a single color system, building a robust label anti-overlap engine, and adding Wealth Peers to high-value charts.

**6-part implementation:**

1. **Centralized chart palette (`CHART_PALETTE`)**: Created a single dict in `report_generator.py` with 9 color mappings (mspbna=gold, wealth_peers=purple, all_peers=blue, peer_cloud=muted blue-gray, guide=slate-gray, range bands, text, grid). All chart functions now use `_C_*` convenience aliases instead of per-chart hardcoded hex colors. Affected functions: `plot_scatter_dynamic`, `create_credit_deterioration_chart_ppt`, `plot_portfolio_mix`, `plot_years_of_reserves`, `plot_growth_vs_deterioration`, `plot_risk_adjusted_return`, `plot_concentration_vs_capital`, `plot_migration_ladder`, `plot_reserve_risk_allocation`, `plot_liquidity_overlay`.

2. **Chart annotation helper (`ChartAnnotationHelper`)**: New class with priority-tiered label placement. Tier 1 (MSPBNA) always labeled, Tier 2 (Wealth Peers) always labeled when plotted, Tier 3 (All Peers) labeled only when explicit, Tier 4 (individuals) outlier/edge only. Includes `place_label()` with collision detection, `place_endpoint_label()` for time-series endpoints, and `register_fixed_rect()` for pre-placed elements.

3. **Wealth Peers added to high-value charts**: `plot_scatter_dynamic` now accepts `wealth_peer_cert` parameter — plots Wealth Peers as a distinct purple triangle marker. All 3 scatter call sites pass `wealth_peer_cert`. `plot_years_of_reserves` now shows MSPBNA, Wealth Peers, and All Peers side-by-side. `plot_risk_adjusted_return`, `plot_growth_vs_deterioration`, and `plot_concentration_vs_capital` all add Wealth Peers (triangle) and All Peers (square) markers alongside the MSPBNA diamond.

4. **Credit chart polish**: `create_credit_deterioration_chart_ppt` uses centralized palette for GOLD/BLUE/PURPLE. Low-coverage series suppression already in place. Strategic sparse labels (latest quarter, Q4 only) already enforced by `idx_to_label` logic.

5. **Years of reserves update**: Title now clearly shows "Years of Reserves by Segment". Both Wealth Peers (triangle, purple) and All Peers (diamond, blue) plotted alongside MSPBNA (circle, gold).

6. **Tests and documentation**: Added 3 test classes (24 tests): `TestCentralizedChartPalette` (11 tests: palette keys, color ranges, aliases, no hardcoded hex), `TestChartAnnotationHelper` (5 tests: class exists, methods, priority tiers), `TestWealthPeersInclusion` (8 tests: scatter param, call sites, years_of_reserves, risk_adjusted_return, growth_vs_deterioration, concentration_vs_capital). Updated CLAUDE.md with centralized palette table, annotation policy, and Wealth Peers inclusion policy.

**Functions updated:**
- `plot_scatter_dynamic` — new `wealth_peer_cert` param, centralized colors, Wealth Peers marker
- `create_credit_deterioration_chart_ppt` — centralized palette aliases
- `plot_portfolio_mix` — centralized palette
- `plot_years_of_reserves` — Wealth Peers + All Peers markers, centralized colors
- `plot_growth_vs_deterioration` — Wealth Peers + All Peers markers, centralized colors
- `plot_risk_adjusted_return` — Wealth Peers + All Peers markers, centralized colors
- `plot_concentration_vs_capital` — Wealth Peers + All Peers markers, centralized colors
- `plot_migration_ladder` — centralized metric colors
- `plot_reserve_risk_allocation` — centralized colors
- `plot_liquidity_overlay` — centralized colors

**Files changed:** `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Normalized KRI Bullet Chart Split (Rates vs Composition)

**Problem**: The single `kri_bullet_normalized` artifact mixed rate metrics (0.xx% scale) and composition metrics (xx% scale) on a shared x-axis, making the chart unreadable. Additionally, the comparator fallback collapsed the gray band to the subject value when both peer composites were NaN, which was visually misleading.

**3-part fix:**

1. **Artifact split**: Replaced `kri_bullet_normalized` with two new artifacts:
   - `kri_bullet_normalized_rates` — 5 rate metrics (Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage)
   - `kri_bullet_normalized_composition` — 5 composition metrics (Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share)
   - Each gets its own axes, avoiding the mixed-scale problem
   - Exact titles: "Key Risk Indicators — MSPBNA vs Peer Range (Normalized Rates)" and "Key Risk Indicators — MSPBNA vs Peer Range (Normalized Composition)"

2. **Comparator fallback fix** in `generate_kri_bullet_chart()`:
   - Both comparators available → gray band between min and max (unchanged)
   - Single comparator available → thin vertical reference marker (new behavior)
   - Neither comparator available → metric row skipped entirely (was: collapsed to subject value)
   - Legend includes "Single Comparator Ref." when applicable

3. **Integration update**: `report_generator.py` Phase 8 now produces 3 bullet artifacts (1 standard + 2 normalized) instead of 2. Each normalized artifact passes its own `metrics` list and `title_override`.

**Changes:**

1. **rendering_mode.py** — Replaced `kri_bullet_normalized` registration with `kri_bullet_normalized_rates` and `kri_bullet_normalized_composition` (both FULL_LOCAL_ONLY).

2. **executive_charts.py** — Added `BULLET_METRICS_NORMALIZED_RATES` (5 metrics) and `BULLET_METRICS_NORMALIZED_COMPOSITION` (5 metrics). Added `title_override` parameter to `generate_kri_bullet_chart()`. Refactored comparator fallback: `ref_markers` dict tracks single-comparator rows; thin vertical line drawn instead of zero-width gray band. Neither-available rows skipped entirely via `continue`.

3. **report_generator.py** — Phase 8 bullet chart section rewritten: standard bullet produced as single artifact; normalized produced via loop over `_norm_bullet_specs` list with separate metric lists and titles. Imports `BULLET_METRICS_NORMALIZED_RATES` and `BULLET_METRICS_NORMALIZED_COMPOSITION` from `executive_charts`.

4. **test_regression.py** — Updated 8 existing tests to reference new artifact names. Added 8 new tests: `test_no_obsolete_kri_bullet_normalized_artifact`, `test_normalized_rates_metric_list`, `test_normalized_composition_metric_list`, `test_no_overlap_between_rates_and_composition`, `test_bullet_chart_has_title_override_param`, `test_comparator_fallback_neither_skips_row`, `test_comparator_fallback_single_uses_ref_marker`, `test_exact_normalized_rates_title`, `test_exact_normalized_composition_title`.

5. **CLAUDE.md** — Updated executive artifacts table (5→6 artifacts), bullet metric lists, output filenames, comparator fallback documentation. Added changelog.

**Files changed:** `rendering_mode.py`, `executive_charts.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Rendering Architecture Reconciliation (Final Verification)

**Objective**: Verify that `report_generator.py` is fully aligned with the canonical rendering architecture in `rendering_mode.py`, and that executive/macro chart integration matches the documented target state.

**Verification results** (all items confirmed clean — no code changes needed in report_generator.py, rendering_mode.py, or executive_charts.py):

1. **No merge conflict markers** in `report_generator.py` — confirmed zero instances.
2. **No duplicate local rendering abstractions** — `ReportMode`, `ArtifactStatus`, `ArtifactSpec`, `ManifestEntry`, `ArtifactManifest`, local `ARTIFACT_REGISTRY`, local `should_produce()`, `resolve_report_mode_for_generator()` are all absent. All imported from `rendering_mode.py`.
3. **Executive chart integration correct** — 5 artifacts produced with correct loop patterns, composite CERTs (90001/90003 standard, 90004/90006 normalized), sparkline norm_peer_cert=90006. Obsolete `kri_bullet_chart` artifact name absent.
4. **Macro chart integration correct** — 3 deterministic artifacts with exact FRED series IDs. No heuristic fallback. `plot_macro_overlay()` deleted. Obsolete `macro_overlay` artifact name absent.
5. **`rendering_mode.py` registry** contains all 5 executive + 3 macro artifacts with correct mode declarations.

**Changes:**

1. **CLAUDE.md** — Added "Canonical Rendering Abstraction Rule" to Section 3a. Added "Executive Chart Artifacts" subsection documenting all 5 artifacts. Added "Deterministic Macro Chart Artifacts" subsection documenting all 3 artifacts. Added "Remaining Risks" subsection. Updated changelog.

2. **test_regression.py** — Added `TestRenderingReconciliation` class (22 tests): no merge conflicts, no duplicate abstractions (8 checks), no obsolete artifact names (2), canonical executive artifacts present, deterministic macro artifacts present, no heuristic fallback, rendering_mode.py is canonical source (7 checks), report_generator imports from rendering_mode.

**Files changed:** `CLAUDE.md`, `test_regression.py`

### 2026-03-12 — HUD HTTP Failure Diagnostics & Request Hardening

**Problem**: All HUD county crosswalk HTTP requests were failing (`FAILED_HTTP`), preventing `CaseShiller_Zip_Coverage` and `CaseShiller_Zip_Summary` sheets from being produced. The root cause was twofold: (1) `fetch_hud_crosswalk()` returned empty DataFrames on HTTP failure without propagating diagnostics, and (2) the orchestrator's `http_errors` list stayed empty even when all requests failed because the function never raised exceptions on HTTP errors, causing misclassification as `FAILED_EMPTY_RESPONSE` instead of `FAILED_HTTP`.

**9-part fix:**

1. **HTTP failure diagnostics (Part 1)**: `fetch_hud_crosswalk()` now returns `(DataFrame, diagnostics_dict)` tuple. Diagnostics include: `query`, `status`, `failure_class`, `status_code`, `url`, `params`, `response_preview`, `exception_info`, `retry_count`.

2. **Request hardening (Part 2)**: Switched from bare `requests.get()` to `requests.Session()` with persistent `Accept: application/json` and `User-Agent` headers. Module-level session is lazy-initialized via `_get_hud_session()`.

3. **Request validation (Part 3)**: Added `build_hud_crosswalk_request()` helper that validates query, crosswalk_type, year, quarter, and token before sending. First-request debug log shows masked token prefix and full session headers.

4. **Status code classification (Part 4)**: Added `_classify_http_status()` mapping HTTP codes to query-level failure constants: `QUERY_FAILED_TOKEN_AUTH` (401/403), `QUERY_FAILED_HTTP_NOT_FOUND` (404), `QUERY_FAILED_HTTP_BAD_REQUEST` (400), `QUERY_FAILED_HTTP_RATE_LIMIT` (429), `QUERY_FAILED_HTTP_SERVER` (5xx), `QUERY_FAILED_HTTP_EXCEPTION` (connection/timeout errors).

5. **County-level failure summary (Part 5)**: Orchestrator logs breakdown at end: total, success, failed_auth, failed_bad_request, failed_not_found, failed_rate_limit, failed_server, failed_exception, failed_parse, failed_empty.

6. **Misclassification fix (Part 6)**: Orchestrator now tracks `county_diagnostics` list and checks `any_http_failure` + `all_failed_http` guards. When all county requests fail HTTP, final status is `FAILED_HTTP` (not `FAILED_EMPTY_RESPONSE` / `SUCCESS_NO_MATCHES` / `SUCCESS_NO_ZIPS`).

7. **Smoke test helper (Part 7)**: `run_hud_smoke_test(fips_code, token)` runs a single HUD request for local debugging. Returns structured result dict with success, status_code, failure_class, row_count, columns.

8. **Regression tests (Part 8)**: 9 new tests in `TestHUDHTTPDiagnosticsAndHardening`: tuple return, diagnostics keys, session headers, request validation, status classification, constants, misclassification guard, smoke test, failure summary logging.

9. **Documentation (Part 9)**: Updated CLAUDE.md with HTTP failure blocker documentation and new diagnostics.

**Query-level failure constants:**

| Constant | HTTP Code | Meaning |
|---|---|---|
| `QUERY_FAILED_TOKEN_AUTH` | 401, 403 | Bearer token invalid or expired |
| `QUERY_FAILED_HTTP_NOT_FOUND` | 404 | Endpoint or resource not found |
| `QUERY_FAILED_HTTP_BAD_REQUEST` | 400 | Malformed request parameters |
| `QUERY_FAILED_HTTP_RATE_LIMIT` | 429 | Rate limited by HUD API |
| `QUERY_FAILED_HTTP_SERVER` | 5xx | Server-side error |
| `QUERY_FAILED_HTTP_EXCEPTION` | N/A | Connection timeout, DNS failure, etc. |
| `QUERY_FAILED_PARSE` | 200 | Response parsed but has wrapper-only columns |
| `QUERY_SUCCESS` | 200 | Successful fetch with usable data |

**Example diagnostics dict:**
```python
{
    "query": "06037",
    "status": "error",
    "failure_class": "FAILED_HTTP_SERVER",
    "status_code": 500,
    "url": "https://www.huduser.gov/hudapi/public/usps",
    "params": {"type": 7, "query": "06037"},
    "response_preview": "Internal Server Error",
    "exception_info": None,
    "retry_count": 2
}
```

**Example county-level failure summary log:**
```
HUD county-ZIP fetch summary: total=160, success=0, failed=160,
breakdown={ auth=0, bad_request=0, not_found=0, rate_limit=0,
server=160, exception=0, parse=0, empty=0 }
```

**Files changed:** `case_shiller_zip_mapper.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Corp-Safe Overlay Workflow (4 Artifacts, Separate Module)

**Objective**: Build a dedicated corp-safe overlay layer as a separate module/workflow. Joins local Bank_Performance_Dashboard output with internal loan-level extracts to produce 4 corp-safe artifacts. NOT integrated into report_generator.py or MSPBNA_CR_Normalized.py.

**New modules:**

| Module | Role |
|---|---|
| `corp_overlay.py` | Schema contracts, loan-file ingestion, dashboard ingestion, join logic, optional enrichment hooks, 4 artifact generators, orchestrator |
| `corp_overlay_runner.py` | Standalone CLI entrypoint with argparse (separate from report_generator.py) |

**4 Artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `loan_balance_by_product` | `corp_overlay_YYYYMMDD_loan_balance_by_product.png` | FULL_LOCAL_ONLY | PNG chart |
| `top10_geography_by_balance` | `corp_overlay_YYYYMMDD_top10_geography_by_balance.png` | FULL_LOCAL_ONLY | PNG chart |
| `internal_credit_flags_summary` | `corp_overlay_YYYYMMDD_internal_credit_flags_summary.html` | BOTH | HTML table |
| `peer_vs_internal_mix_bridge` | `corp_overlay_YYYYMMDD_peer_vs_internal_mix_bridge.html` | BOTH | HTML table |

**Input contracts:**

- Input A: `Bank_Performance_Dashboard_YYYYMMDD.xlsx` (auto-discovered from `output/`)
- Input B: Internal loan-level CSV or Excel with required columns:
  - `loan_id` — unique loan identifier (required)
  - `current_balance` — outstanding balance in $ (required)
  - `product_type` — loan product classification (required)
  - At least one geo field: `msa` (preferred), `zip_code`, or `county` (required)
  - Optional: `risk_rating`, `delinquency_status`, `nonaccrual_flag`, `segment`, `portfolio`, `collateral_type`

**Contract validation:** `validate_loan_file()` raises `LoanFileContractError` if required columns or all geo fields are missing. Geo priority: MSA > zip_code > county. Optional columns degrade gracefully — reduced output when absent.

**Optional enrichment hooks:**
- Census (`CENSUS_API_KEY`): population/income by geography — hook point, not yet implemented
- BEA (`BEA_API_KEY` → `BEA_USER_ID` fallback): GDP/employment by MSA/county — hook point, not yet implemented
- Case-Shiller: `map_zip_to_metro()` from existing `case_shiller_zip_mapper.py` for ZIP → metro tagging
- All enrichment is optional — workflow runs fully offline without any API keys

**Reduced-mode behavior:**
- No `risk_rating` → credit flags summary shows portfolio summary only, no rating distribution
- No `delinquency_status` → no delinquency section
- No `nonaccrual_flag` → no nonaccrual section
- No dashboard found → bridge table shows "not available" for peer composition
- corp_safe mode → PNG charts skipped, HTML tables produced

**Changes:**

1. **corp_overlay.py** (NEW) — Full module: `REQUIRED_COLUMNS`, `GEO_COLUMNS`, `OPTIONAL_COLUMNS` contracts; `LoanFileContractError`; `validate_loan_file()`; `find_latest_dashboard()`; `load_dashboard_composition()`; `load_loan_file()`; `enrich_geography()` with Census/BEA/Case-Shiller hooks; `generate_loan_balance_by_product()`; `generate_top10_geography()`; `generate_credit_flags_summary()`; `generate_peer_vs_internal_bridge()`; `run_corp_overlay()` orchestrator.

2. **corp_overlay_runner.py** (NEW) — CLI entrypoint with argparse: `loan_file` (required), `--dashboard`, `--output-dir`, `--mode`. Validates loan file existence before importing corp_overlay.

3. **rendering_mode.py** — Added 4 artifact registrations: `loan_balance_by_product` (FULL_LOCAL_ONLY), `top10_geography_by_balance` (FULL_LOCAL_ONLY), `internal_credit_flags_summary` (BOTH), `peer_vs_internal_mix_bridge` (BOTH).

4. **test_regression.py** — Added 6 test classes (31 tests): `TestCorpOverlayContractValidation` (10): required/geo/optional column validation, case-insensitive, priority order. `TestCorpOverlayArtifactRegistration` (5): all 4 registered, mode availability correct. `TestCorpOverlayOfflineOperation` (4): enrichment without keys, key resolution fallback. `TestCorpOverlayReducedMode` (5): HTML output with/without optional columns, bridge with/without dashboard. `TestCorpOverlayNoMergeConflicts` (3): no conflict markers in report_generator.py, corp_overlay not referenced in existing scripts. `TestCorpOverlayCLAUDEMDAccuracy` (4): documentation completeness.

5. **CLAUDE.md** — Added Section 12 (Corp-Safe Overlay Architecture), script table entries, changelog.

**Files changed:** `corp_overlay.py` (NEW), `corp_overlay_runner.py` (NEW), `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Macro Chart Tranche (3 Artifacts, Deterministic Series Selection)

**Objective**: Implement macro-to-credit correlation and overlay charts using precise named FRED series. Replace the old heuristic "pick first available" macro overlay with deterministic, documented series selection.

**3 Artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `macro_corr_heatmap_lag1` | `{stem}_macro_corr_heatmap_lag1.html` | BOTH | HTML table |
| `macro_overlay_credit_stress` | `{stem}_macro_overlay_credit_stress.png` | FULL_LOCAL_ONLY | PNG chart |
| `macro_overlay_rates_housing` | `{stem}_macro_overlay_rates_housing.png` | FULL_LOCAL_ONLY | PNG chart |

**Required FRED Series (13):**

| Category | Series ID | Short Name | Frequency |
|---|---|---|---|
| Rates/Curve | FEDFUNDS | Fed Funds Rate | Monthly |
| Rates/Curve | T10Y2Y | 10Y-2Y Spread | Daily |
| Credit/Stress | BAMLH0A0HYM2 | HY OAS | Daily |
| Credit/Stress | VIXCLS | VIX | Daily |
| Credit/Stress | NFCI | Chicago Fed NFCI | Weekly |
| Credit/Stress | STLFSI4 | St. Louis FSI | Weekly |
| Credit/Stress | DRTSCILM | C&I Standards (Large/Med) | Quarterly |
| Bank Health | DRALACBS | All Loans Delinquency | Quarterly |
| Bank Health | DRCRELEXFACBS | CRE Delinq (ex-farm) | Quarterly |
| Bank Health | DRSFRMACBS | 1-4 Resi Delinquency | Quarterly |
| Housing | MORTGAGE30US | 30Y Mortgage Rate | Weekly |
| Housing | HOUST | Housing Starts | Monthly |
| Housing | CSUSHPISA | Case-Shiller National (SA) | Monthly |

**Series availability:** All 13 series are in `FRED_SERIES_TO_FETCH` (legacy ingestion path). STLFSI2 was discontinued (last obs 2022-01-07) and replaced by STLFSI4 (already present). CSUSHPISA was added in a prior prompt. All 13 are valid and active.

**Artifact 1 — macro_corr_heatmap_lag1.html:**
- Internal metric rows (8): Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_NCO_Rate, RIC_CRE_ACL_Coverage
- Macro columns (13): All 13 FRED series above
- Pearson correlation, trailing 20Q window
- Macro series lagged +1 quarter vs internal metrics
- Insufficient overlap (< 4 quarters) → N/A cell
- Color scale: green (strong negative/counter-cyclical) → neutral → red (strong positive/comovement)
- All series quarterly-aligned via `_fred_to_quarterly()` (last-observation-per-quarter resampling)

**Artifact 2 — macro_overlay_credit_stress.png:**
- Left axis: MSPBNA Norm_NCO_Rate (gold, solid)
- Right axis: BAMLH0A0HYM2 (blue, dashed) + NFCI (red, dashed), both z-scored over plotted window
- Z-scoring: `(value - mean) / std` for readability when units differ
- Title: "Credit Stress Overlay: Norm NCO Rate vs HY OAS + NFCI"

**Artifact 3 — macro_overlay_rates_housing.png:**
- Left axis: RIC_Resi_Nonaccrual_Rate (preferred) or Norm_Nonaccrual_Rate (fallback)
- Right axis: FEDFUNDS (solid), MORTGAGE30US (dashed), CSUSHPISA YoY % (dash-dot)
- CSUSHPISA transformed: `pct_change(4) * 100` (quarterly YoY %)
- Title dynamically names the actual plotted series

**Critical change — heuristic fallback deleted:**
The old `plot_macro_overlay()` used: `target_names = ["Fed Funds", "Unemployment", "All Loans Delinquency Rate"]` with fallback to "any available series". This has been completely removed. Macro chart selection is now deterministic — each chart function specifies exact FRED series IDs. No fallback to random/first-available series. The old `macro_overlay` artifact is replaced by 3 deterministic artifacts.

**Changes:**

1. **MSPBNA_CR_Normalized.py** — Added CSUSHPISA to `FRED_SERIES_TO_FETCH` (STLFSI2 was also added here but later removed as discontinued — see 2026-03-13 FRED Series Audit).

2. **rendering_mode.py** — Replaced `macro_overlay` (FULL_LOCAL_ONLY) with 3 new registrations: `macro_corr_heatmap_lag1` (BOTH), `macro_overlay_credit_stress` (FULL_LOCAL_ONLY), `macro_overlay_rates_housing` (FULL_LOCAL_ONLY).

3. **report_generator.py** — Deleted old `plot_macro_overlay()` with its heuristic fallback. Added constants `MACRO_CORR_INTERNAL_METRICS` (8 metrics), `MACRO_CORR_FRED_SERIES` (13 series), `_FRED_DISPLAY` (human-readable names). Added `_fred_to_quarterly()` helper for quarterly alignment. Added 3 new generators: `generate_macro_corr_heatmap()`, `plot_macro_overlay_credit_stress()`, `plot_macro_overlay_rates_housing()`. Updated `generate_reports()` Phase 6 to call all 3 new artifacts instead of the old single macro_overlay.

4. **test_regression.py** — Added `TestMacroChartTranche` class (18 tests): all 3 artifacts registered, mode declarations correct, old macro_overlay removed, no heuristic fallback in source, all 13 FRED series in fetch list, exact internal metric list, exact FRED column list, deterministic series in credit stress, deterministic series in rates/housing, left-axis preference order, chart titles include series names, empty data handling, new artifacts referenced in report_generator, _fred_to_quarterly exists, z-scoring verified, STLFSI2 in fetch, CSUSHPISA in fetch. Updated `test_registry_covers_known_artifacts` to include new artifacts.

5. **CLAUDE.md** — Added artifact table, series table, transformations, quarterly alignment, and changelog.

**Files changed:** `MSPBNA_CR_Normalized.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Executive Chart Tranche (5 Artifacts)

**Objective**: Complete and harden the first executive chart tranche using existing workbook data only. No new external dependencies.

**5 Artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `yoy_heatmap_standard` | `{stem}_yoy_heatmap_standard.html` | BOTH | HTML table |
| `yoy_heatmap_normalized` | `{stem}_yoy_heatmap_normalized.html` | BOTH | HTML table |
| `kri_bullet_standard` | `{stem}_kri_bullet_standard.png` | FULL_LOCAL_ONLY | PNG chart |
| `kri_bullet_normalized_rates` | `{stem}_kri_bullet_normalized_rates.png` | FULL_LOCAL_ONLY | PNG chart |
| `kri_bullet_normalized_composition` | `{stem}_kri_bullet_normalized_composition.png` | FULL_LOCAL_ONLY | PNG chart |
| `sparkline_summary` | `{stem}_sparkline_summary.html` | BOTH | HTML table |

**Exact metric lists:**

| Artifact | Metrics |
|---|---|
| Standard heatmap (12) | TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Past_Due_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, SBL_Composition, RIC_CRE_Loan_Share, RIC_Resi_Loan_Share, RIC_CRE_ACL_Coverage, RIC_CRE_Risk_Adj_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_NCO_Rate |
| Normalized heatmap (10) | Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share |
| Standard bullet (7) | TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Past_Due_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, RIC_CRE_ACL_Coverage, RIC_CRE_Risk_Adj_Coverage |
| Normalized bullet — rates (5) | Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage |
| Normalized bullet — composition (5) | Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share |
| Sparkline (10) | TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, Past_Due_Rate, Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_ACL_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_ACL_Coverage |

**Comparator CERTs:**

| Artifact | Standard | Normalized |
|---|---|---|
| Heatmap | 90003 (All Peers) | 90006 (All Peers Norm) |
| Bullet | 90001 (Core PB) + 90003 (All Peers) | 90004 (Core PB Norm) + 90006 (All Peers Norm) |
| Sparkline | 90003 for standard rows | 90006 for Norm_ rows (per-metric selection) |

**Changes:**

1. **executive_charts.py** — Split `BULLET_METRICS` into `BULLET_METRICS_STANDARD` (7 metrics) and `BULLET_METRICS_NORMALIZED` (7 metrics). Added `is_normalized` parameter to `generate_kri_bullet_chart()` which selects correct metric list and chart title. Added `norm_peer_cert` parameter to `generate_sparkline_table()` — sparkline peer lookup uses 90006 for `Norm_*` metrics, 90003 for standard metrics. Backward-compatible `BULLET_METRICS` alias retained.

2. **rendering_mode.py** — Replaced single `kri_bullet_chart` registration with two: `kri_bullet_standard` (FULL_LOCAL_ONLY) and `kri_bullet_normalized` (FULL_LOCAL_ONLY). Heatmaps and sparkline remain BOTH.

3. **report_generator.py** — Phase 8 executive charts section now loops over `[False, True]` for bullet charts (same pattern as heatmaps), producing both `kri_bullet_standard` and `kri_bullet_normalized` with correct composite CERTs per variant. Sparkline call now passes `norm_peer_cert=ACTIVE_NORMALIZED_COMPOSITES["all_peers"]`.

4. **test_regression.py** — Updated 3 existing tests (`test_executive_chart_artifacts_registered`, `test_executive_charts_integrated_in_report_generator`, `test_corp_safe_skips_bullet_chart`) to reflect new artifact names. Updated `test_registry_covers_known_artifacts` to include both bullet variants. Added `TestExecutiveChartTranche` class (16 tests): all 5 artifacts registered, heatmap metric allowlists exact match, bullet metric lists exact match, sparkline metrics exact match, is_normalized parameter exists, norm_peer_cert parameter exists with default 90006, mode support declarations correct, missing metric resilience (heatmap and sparkline), ordered_metrics usage verified, both bullet variants in report_generator, sparkline norm peer logic verified, N/A cells explicit, old kri_bullet_chart removed, all metrics in semantic registry.

5. **CLAUDE.md** — Added artifact table, exact metric lists, comparator CERTs, and changelog.

**Verification:** Heatmaps already use `ordered_metrics()` (from `metric_semantics.py`) for GROUP_ORDER row sorting. All metrics in all lists are registered in `metric_semantics.py`. Missing metrics skip individually (not whole artifact). Missing values render as explicit "N/A". HTML artifacts are self-contained.

**Files changed:** `executive_charts.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Architecture Reconciliation (Merge Conflict Resolution)

**Mandatory cleanup**: Resolved all merge conflicts and made `rendering_mode.py` the single canonical home for render abstractions.

**Changes:**

1. **report_generator.py** — Resolved 7 merge conflicts (scatter plots, segment/roadmap charts, FRED expansion charts, executive charts, tables). Removed ~230 lines of duplicate local abstractions (`ReportMode`, `ArtifactStatus`, `ArtifactSpec`, `ManifestEntry`, `ArtifactManifest`, `ReportContext`, `resolve_report_mode_for_generator()`, local `ARTIFACT_REGISTRY`, `_ARTIFACT_BY_NAME`, `get_artifacts_for_mode()`, local `should_produce()`). All render types now imported from `rendering_mode.py`. Added `_ReportContext` (lightweight internal dataclass) and `_produce_table()`/`_produce_chart()` DRY helpers. Fixed manifest API from stale `manifest.record()` to canonical `record_generated()`/`record_skipped()`/`record_failed()`. Fixed FRED expansion preflight to use `is_artifact_available()` (side-effect-free) instead of `should_produce()`.

2. **rendering_mode.py** — Already canonical. Added `is_artifact_available()` for side-effect-free availability checks, `ArtifactCapability.filename_suffix` field with auto-generation in `_reg()`, and backward-compatible `REPORT_RENDER_MODE` alias in `select_mode()`.

3. **test_regression.py** — Resolved 1 merge conflict. Replaced broken `TestDualModeArchitecture` (imported removed local types) with 3 new test classes (25 tests): `TestCanonicalRenderingArchitecture` (no merge conflicts, no duplicate abstractions, canonical imports, correct manifest API), `TestCanonicalModeResolution` (REPORT_MODE resolution, REPORT_RENDER_MODE alias, explicit override, invalid raises, active composites), `TestArtifactRegistryCanonical` (registry coverage, mode availability, manifest API, should_produce records skips, is_artifact_available no side effects).

4. **CLAUDE.md** — Updated env var table (added REPORT_MODE, REPORT_RENDER_MODE, BEA_API_KEY, BEA_USER_ID, CENSUS_API_KEY). Updated mode selection priority (4-level: explicit → REPORT_MODE → REPORT_RENDER_MODE → default). Documented alias resolution rules. Added changelog.

**Deferred work:**
- Executive charts (`_produce_chart` DRY conversion) — currently use inline `should_produce` + manual manifest calls. Works correctly but not as DRY as scatter/segment/roadmap charts.
- `_produce_chart` does not yet handle all chart function signatures uniformly (some take `proc_df_with_peers, subject_bank_cert`, others take `fred_expansion_df`).

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Normalized Segment Taxonomy Alignment

Aligned the normalized segment taxonomy to Call Report research and removed
non-CR-consistent mappings. Changes:

1. **RESI denominator correction**: Removed `LNRERES + LNRELOC` fallback;
   denominator now uses `LNRERES` alone (already includes HELOC/open-end).
   Split numerators (NTRERES+NTRELOC, etc.) preserved.
2. **C&I exclusion**: Changed `Excl_CI_Balance` from `max(0, LNCI - SBL_Balance)`
   to `LNCI` directly. SBL is RC-C item 9, not item 4.
3. **NDFI (Fund Finance)**: Removed J458/J459/J460 from all credit-quality
   mappings (PD30, PD90, NA). J454 retained for balance. NDFI PD/NA set to 0.0
   with limitation flags.
4. **Ag past due**: Replaced RCON2746/RCFD2746 and RCON2747/RCFD2747 with
   P3AG/P9AG (actual agricultural past-due fields).
5. **CRE pure balance**: Tightened to `LNREMULT + LNRENROT` only; removed
   LNREOTH.
6. **Tailored lending**: Documented as unsupported in CR-only mode. No proxy
   math added.

Files changed: `MSPBNA_CR_Normalized.py`, `CLAUDE.md`

### 2026-03-12 — Hardened Normalized Exclusion Engine

Hardened the exclusion engine so balances, NCO, nonaccrual, PD30, and PD90 all
use internally consistent exclusion math with no unsupported numerator leakage.

1. **C&I NCO balance-gating**: `Excl_CI_NCO_YTD` now gated to 0.0 when
   `Excl_CI_Balance == 0`. Audit flag `_CI_NCO_Gated` added.
2. **Credit Card NCO balance-gating**: `Excl_CC_NCO_YTD` now gated to 0.0 when
   `Excl_CreditCard_Balance == 0`. Audit flag `_CC_NCO_Gated` added.
3. **NDFI NCO set to 0.0**: `Excl_NDFI_NCO_YTD` forced to 0.0 (no CR-only
   mapping exists). Audit flag `_audit_ndfi_nco_unsupported` added.
4. **Structured audit flags**: Added 4 boolean audit columns:
   - `_audit_unsupported_ndfi_pdna` — NDFI PD/NA unsupported in CR-only mode
   - `_audit_ndfi_nco_unsupported` — NDFI NCO unsupported in CR-only mode
   - `_audit_ag_pd_fallback_to_zero` — P3AG/P9AG both absent, Ag PD fell back to 0
   - `_audit_resi_balance_fallback_used` — RC-C components absent, LNRERES fallback used
5. **Metric registry**: Added `Norm_Delinquency_Rate` MetricSpec
   (`(Norm_PD30 + Norm_PD90) / Norm_Gross_Loans`). Added Rule F
   (`_check_unsupported_mappings`) to semantic validation.
6. **Exclusion audit sheet**: `excl_audit_cols` extended with NDFI balance/NCO,
   all gating flags, and all audit flags. `gate_cols` updated.
7. **CLAUDE.md**: Added Supported Exclusion Stack table, updated balance-gating
   documentation (6 categories), added Structured Audit Flags section.

Files changed: `MSPBNA_CR_Normalized.py`, `metric_registry.py`, `CLAUDE.md`

### 2026-03-12 — Dual-Mode Rendering Architecture (Preflight Refactor)

1. **New module `rendering_mode.py`**: Implements dual-mode architecture with `RenderMode` enum (`full_local`, `corp_safe`), `ArtifactCapability` declarations, `ArtifactManifest` outcome tracking, `ARTIFACT_REGISTRY` (31 artifacts), and `should_produce()` guard helper.

2. **Refactored `report_generator.py`**: `generate_reports()` now accepts `render_mode` parameter (default: `full_local`, preserves existing behaviour). Every artifact production block is guarded by `should_produce()`. Returns `ArtifactManifest` with per-artifact outcome tracking. CLI supports `python report_generator.py [full_local|corp_safe]`.

3. **Capability matrix**: All HTML tables declared `BOTH` (available in both modes). All matplotlib-based charts/scatters declared `FULL_LOCAL_ONLY`. In `corp_safe` mode, chart artifacts are skipped with clear `[SKIP]` log messages.

4. **Artifact manifest**: Every run produces a summary table printed to console: artifact name, mode, status (generated/skipped/failed), path or reason. Counts displayed at bottom.

5. **15 new regression tests** in `test_regression.py` covering: mode selection (default, explicit, env var, invalid), capability matrix (tables both, charts full_local only), skip reasons, manifest recording, `should_produce()` behaviour, preflight suppression integration, registry completeness, render_mode parameter presence, active composite preservation.

6. **Non-breaking**: Default execution path (`python report_generator.py` with no args) is identical to pre-refactor behaviour. No peers, segments, composite definitions, or chart semantics changed.

**New files**: `rendering_mode.py`
**Changed files**: `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — HUD Response Two-Pass Flattening Fix

**Problem**: Even after the initial parsing fix, runtime logs showed the combined HUD DataFrame still had only wrapper columns (`year`, `quarter`, `input`, `crosswalk_type`, `results`). The parser was stopping one layer too early.

**Root cause**: The HUD type=7 API returns `{"results": [wrapper, wrapper, ...]}` where each wrapper is `{"year":..., "quarter":..., "input":..., "crosswalk_type":..., "results": [actual_row, ...]}`. `extract_hud_result_rows()` correctly extracted the wrapper list (Shape B), but `pd.DataFrame(wrapper_rows)` produced a DataFrame with wrapper columns because each wrapper's `results` key contained the actual crosswalk rows as a nested list.

**Fix**: Added `flatten_hud_rows()` as a second-pass flattener that detects wrapper rows (keys ⊆ `_HUD_WRAPPER_KEYS` + nested `results` list) and explodes them into actual data rows, propagating parent metadata. The fetch pipeline is now: `extract_hud_result_rows()` → `flatten_hud_rows()` → `pd.json_normalize()` → `canonicalize_hud_columns()`.

**Changes:**
1. **case_shiller_zip_mapper.py**:
   - `flatten_hud_rows(rows)` — NEW: detects wrapper rows, explodes nested `results`, propagates parent metadata
   - `_HUD_WRAPPER_KEYS` — NEW: frozenset of known wrapper metadata keys
   - `fetch_hud_crosswalk()` — now calls `flatten_hud_rows()` between extraction and normalization; logs `rows_before_flatten` and `rows_after_flatten`
   - Wrapper-only detection uses `_HUD_WRAPPER_KEYS` constant instead of inline set
2. **test_regression.py** — 8 new tests:
   - `test_extract_hud_rows_wrapper_results_list`, `test_flatten_hud_rows_propagates_parent_metadata`, `test_flatten_hud_rows_already_flat`, `test_flatten_hud_rows_multiple_wrappers`, `test_fetch_hud_crosswalk_uses_flatten`, `test_wrapper_keys_constant_exists`, `test_failed_parse_not_misclassified_as_success_no_zips`, `test_end_to_end_nested_wrapper_to_canonical_columns`
3. **CLAUDE.md** — Updated "HUD Response Parsing & Flattening" to document two-pass architecture with example payload

**Example before/after parse shape:**
```
HUD API returns: {"results": [{"year":2025, "quarter":4, "input":"06037",
                               "crosswalk_type":"7",
                               "results": [{"zip":"90001","county":"06037",...}]}]}
Pass 1 (extract): [{"year":2025, "quarter":4, "input":"06037", "results":[...]}]  ← 1 wrapper row
Pass 2 (flatten): [{"year":2025, "quarter":4, "input":"06037", "zip":"90001", "county":"06037", ...}]  ← N data rows
```

**Test baseline**: 246 tests (previous 238 + 8 new).

### 2026-03-12 — HUD Response Parsing & Canonicalization Fix

**Problem**: HUD token was no longer the blocker. The mapper was reaching the HUD API and getting responses, but the returned payload was not being flattened correctly. The resulting DataFrame had only wrapper columns (`year`, `quarter`, `input`, `crosswalk_type`, `results`) and `build_case_shiller_zip_coverage()` failed because it could not find usable ZIP/FIPS columns.

**Root cause**: `fetch_hud_crosswalk()` extracted `data["results"]` but if that was still a wrapper dict (not a flat list of row dicts), `pd.DataFrame(records)` produced a single-row DataFrame with wrapper columns instead of the actual crosswalk rows.

**Changes by file:**

1. **case_shiller_zip_mapper.py** — 6 new/refactored functions:
   - `extract_hud_result_rows(payload)` — Robust row-list extractor handling 5 documented HUD response shapes (A: flat list, B: `results` list, C: `results.rows`, D: `results.data`, E: `data` key)
   - `canonicalize_hud_columns(df)` — Renames HUD column variants to canonical names (`zip`, `county_fips`, `res_ratio`, etc.) and zero-pads ZIP/FIPS to 5 chars
   - `_describe_payload(payload)` — Log-safe payload shape diagnostics (type, keys, nested types, sample keys)
   - `_HUD_COLUMN_MAP` — Canonical column name mapping dict (14 variant → 6 canonical)
   - `fetch_hud_crosswalk()` — Now uses `extract_hud_result_rows()` + `pd.json_normalize()` + `canonicalize_hud_columns()`. Detects wrapper-only columns and rejects them.
   - `build_case_shiller_zip_coverage()` — Added structured diagnostics (total HUD rows, distinct FIPS counts, matched/unmatched counts, FIPS samples on mismatch)
   - `build_case_shiller_zip_sheets()` — New `FAILED_PARSE` status when columns are wrapper-only. New `SUCCESS_NO_MATCHES` status when HUD rows exist but none match S&P FIPS. Downstream write validation logs `zip_coverage_rows`, `zip_summary_rows`, `metro_count` before returning.

2. **test_regression.py** — 14 new tests in `TestHUDResponseParsing`:
   - `test_extract_hud_rows_shape_a_list` through `test_extract_hud_rows_shape_e_data_key` (5 shape tests)
   - `test_extract_hud_rows_empty_payload`
   - `test_canonicalize_hud_columns_maps_zip_and_county`
   - `test_canonicalize_zeropad_zip`
   - `test_build_case_shiller_zip_coverage_missing_ratios`
   - `test_failed_parse_status_distinct_from_no_token`
   - `test_success_no_matches_status_exists`
   - `test_describe_payload_returns_shape_info`
   - `test_fetch_hud_crosswalk_uses_extract_and_canonicalize`
   - `test_metric_format_type_maintenance_comment`
   - Updated 3 existing tests for new status codes (`FAILED_PARSE`, `SUCCESS_NO_MATCHES`)

3. **CLAUDE.md** — Updated Section 11:
   - Added "HUD Response Parsing & Flattening" subsection with 5-shape table
   - Added "Canonical HUD Crosswalk Fields" subsection with variant mapping
   - Expanded enrichment status table from 8 to 10 codes
   - Noted that token is no longer the main blocker

**Enrichment status transitions:**
```
Token missing     → SKIPPED_NO_TOKEN
Token found + 401 → FAILED_TOKEN_AUTH
Token found + 5xx → FAILED_HTTP
Token found + 200 + wrapper-only columns → FAILED_PARSE
Token found + 200 + empty results        → FAILED_EMPTY_RESPONSE
Token found + 200 + rows but no FIPS match → SUCCESS_NO_MATCHES
Token found + 200 + rows + FIPS match + 0 ZIPs → SUCCESS_NO_ZIPS
Token found + 200 + rows + FIPS match + ZIPs   → SUCCESS_WITH_ZIPS
```

**Example normalized HUD columns after parsing:**
```
Before: year | quarter | input | crosswalk_type | results
After:  zip | county_fips | res_ratio | bus_ratio | oth_ratio | tot_ratio | year | quarter
```

**Test baseline**: 238 tests (previous 224 + 14 new).

### 2026-03-11 — Final Consistency Pass (HUD Token + ACL Semantics)

**Objective**: Verify code, tests, and CLAUDE.md all agree after the DashboardConfig HUD token fix and ACL coverage/share semantics fix.

**Gaps found and fixed:**
1. **`Norm_CRE_ACL_Coverage` missing from metric_registry.py** — Computed in MSPBNA_CR_Normalized.py (`RIC_CRE_ACL / CRE_Investment_Pure_Balance`) but had no `MetricSpec`. Added with correct exposure-base dependency.
2. **`RIC_Comm_Risk_Adj_Coverage` missing from `_METRIC_FORMAT_TYPE`** — C&I NPL coverage metric (ACL / Nonaccrual) was not registered for x-format. Added alongside the existing CRE and Resi NPL coverage entries.

**Verified clean (no changes needed):**
- No dict-style `config[...]` writes in MSPBNA_CR_Normalized.py
- No `self.config.get()` calls
- Single `resolve_hud_token()` call path (in `_validate_runtime_env()` only)
- All ratio-components labels match denominators (no "Coverage" on ACL-pool denominator)
- CLAUDE.md accurately documents `DashboardConfig.hud_user_token`, `_METRIC_FORMAT_TYPE`, coverage/share rule, enrichment status codes

**Changes:**
1. **metric_registry.py** — Added `Norm_CRE_ACL_Coverage` MetricSpec (deps: `RIC_CRE_ACL`, `CRE_Investment_Pure_Balance`)
2. **report_generator.py** — Added `RIC_Comm_Risk_Adj_Coverage: "x"` to `_METRIC_FORMAT_TYPE`
3. **test_regression.py** — Added `TestConsistencyPass` (10 tests): no dict config access, single token resolution path, all Risk_Adj_Coverage in x-format, Norm_CRE_ACL_Coverage in registry with exposure denominator, CLAUDE.md documents DashboardConfig token + _METRIC_FORMAT_TYPE + coverage/share rule, no stale dict references in docs, enrichment status codes match code and docs

**Test baseline**: 224 tests (previous 213 + 10 new + 1 pre-existing Norm_CRE_ACL_Coverage test now passes).

### 2026-03-11 — ACL Ratio Semantic Integrity & Metric Registry Completeness

**Problem**: Missing metric registry entries for `RIC_CRE_ACL_Share`, `RIC_Resi_ACL_Share`, and `Norm_CRE_ACL_Share`. The `_METRIC_FORMAT_TYPE` maintenance rule needed explicit exclusion examples.

**Coverage vs Share vs x-Multiple Rule (non-negotiable):**

| Denominator Type | Label Term | Format | Examples |
|---|---|---|---|
| Exposure/loan base | "Coverage" or "Ratio" | % | `RIC_CRE_ACL / RIC_CRE_Cost`, `Norm_ACL_Balance / Norm_Gross_Loans` |
| ACL pool | "Share" or "% of ACL" | % | `RIC_CRE_ACL / Total_ACL`, `RIC_CRE_ACL / Norm_ACL_Balance` |
| Nonaccrual/NPL base | "NPL Coverage" | x-multiple | `RIC_CRE_ACL / RIC_CRE_Nonaccrual` |

**`_METRIC_FORMAT_TYPE` Maintenance Rule**: Only NPL coverage metrics (denominator = nonaccrual/past-due) belong in the x-format registry. Any new NPL coverage metric MUST be added explicitly. Loan-coverage and share-of-ACL metrics must NEVER be added — they default to percent.

**Changes:**
1. **metric_registry.py** — Added 3 missing `MetricSpec` entries: `RIC_CRE_ACL_Share` (deps: `RIC_CRE_ACL`, `Total_ACL`), `RIC_Resi_ACL_Share` (deps: `RIC_Resi_ACL`, `Total_ACL`), `Norm_CRE_ACL_Share` (deps: `RIC_CRE_ACL`, `Norm_ACL_Balance`)
2. **report_generator.py** — Added explicit exclusion examples to `_METRIC_FORMAT_TYPE` comment listing coverage/share metrics that must NOT be added to x-format
3. **test_regression.py** — Added `TestACLRatioSemanticIntegrity` (9 tests): Total_ACL denominator never labeled Coverage, Norm_ACL_Balance denominator never labeled Coverage, exposure denominator labeled Coverage/Ratio, NPL metrics in x-format, coverage/share not in x-format, maintenance rule documented, registry has all share entries, share metrics depend on ACL denominators, Norm_CRE_ACL_Share depends on Norm_ACL_Balance

**Example corrected labels (all verified correct in current code):**
- `RIC_CRE_ACL / Total_ACL` → "CRE % of ACL" (Share) ✓
- `RIC_CRE_ACL / RIC_CRE_Cost` → "CRE ACL Coverage" (Coverage) ✓
- `RIC_CRE_ACL / Norm_ACL_Balance` → "Norm CRE % of ACL" (Share) ✓
- `RIC_Resi_ACL / Wealth_Resi_Balance` → "Norm Resi ACL Coverage" (Coverage) ✓
- `RIC_CRE_ACL / RIC_CRE_Nonaccrual` → "CRE NPL Coverage" (x-multiple) ✓

**Test baseline**: 213 tests (previous 204 + 9 new).

### 2026-03-11 — Fix DashboardConfig HUD Token TypeError

**Bug**: `main()` crashed with `TypeError: 'DashboardConfig' object does not support item assignment` at `config["_hud_user_token"] = _hud_token`. The `DashboardConfig` class is a plain Python class, not a dict, so bracket-style item assignment fails.

**Root cause**: Two dict-style accesses on `DashboardConfig`:
1. `config["_hud_user_token"] = _hud_token` in `main()` (item assignment)
2. `self.config.get("_hud_user_token")` in the pipeline (dict `.get()` call)

**Fix** (3 changes in `MSPBNA_CR_Normalized.py`):
1. Added `hud_user_token: Optional[str] = None` field to `DashboardConfig` class
2. Changed `config["_hud_user_token"] = _hud_token` → `config.hud_user_token = _hud_token` (attribute assignment)
3. Changed `self.config.get("_hud_user_token")` → `getattr(self.config, "hud_user_token", None)` (safe attribute access)

**Token flow (explicit)**: `_validate_runtime_env()` → returns resolved token → `main()` sets `config.hud_user_token` → pipeline reads via `getattr(self.config, "hud_user_token", None)` → passes `hud_user_token=_hud_tok` explicitly to `build_case_shiller_zip_sheets()`.

**Why the TypeError can no longer occur**: `DashboardConfig` now has a proper `hud_user_token` attribute. All access uses Python attribute syntax (dot notation), never dict bracket syntax. No code path attempts `config[...]` assignment.

**Tests added** (`TestHUDTokenDashboardConfigFix`, 8 tests):
- `test_dashboard_config_has_hud_user_token_field`
- `test_no_item_assignment_on_dashboard_config`
- `test_no_config_get_dict_style`
- `test_main_uses_attribute_assignment_for_token`
- `test_pipeline_uses_getattr_for_token`
- `test_hud_token_passed_explicitly_to_enrichment`
- `test_token_diagnostics_no_full_token_leak`
- `test_enrichment_status_codes_are_distinct`

**Test baseline**: 204 tests (previous 196 + 8 new).

### 2026-03-11 — Coverage/Share Semantics & HUD Token Discovery (Directive 7)

**8-part directive**: Fix coverage vs share label semantics, metric formatting, and HUD token discovery/diagnostics.

**Coverage vs Share Rule (non-negotiable):**
- **"Coverage"** = ACL or reserve divided by an **exposure base** (loans, cost basis, nonaccrual)
  - Examples: `Total_ACL / Gross_Loans`, `RIC_CRE_ACL / RIC_CRE_Cost`, `Norm_ACL_Balance / Norm_Gross_Loans`
- **"Share" or "% of ACL"** = segment reserve divided by **ACL pool**
  - Examples: `RIC_CRE_ACL / Total_ACL`, `RIC_CRE_ACL / Norm_ACL_Balance`
- If denominator is `Total_ACL` or `Norm_ACL_Balance`, the metric **must NOT** be labeled "Coverage"

**Metric Format Types:**
| Type | Display | Typical Range | Examples |
|---|---|---|---|
| Loan coverage | % | 0.5%-3% | ACL/Loans, Risk-Adj ACL, Norm ACL Coverage |
| NPL coverage | x-multiple | 0.5x-5x | CRE ACL/CRE NPL, Resi ACL/Resi NPL |
| Share/composition | % | 5%-50% | CRE % of ACL, Resi % of ACL |

Formatting is now driven by `_METRIC_FORMAT_TYPE` dict in `report_generator.py` (keyed by metric code), not fragile keyword matching. Only NPL coverage metrics (`RIC_CRE_Risk_Adj_Coverage`, `RIC_Resi_Risk_Adj_Coverage`) use x-format. Everything else defaults to percent.

**HUD Token Discovery:**

`resolve_hud_token(explicit_token, script_dir)` in `case_shiller_zip_mapper.py` is the single token resolver. Resolution order:
1. Explicit function argument (if provided)
2. `os.getenv("HUD_USER_TOKEN")`
3. `.env` in script directory
4. `.env` in current working directory

Returns `(token, diagnostics_dict)` where diagnostics includes: `token_found`, `source_used`, `token_length`, `token_prefix_masked` (first 6 chars only), `dotenv_available`, `paths_checked`, `current_working_directory`, `script_directory`, `process_executable`, `process_pid`. The full token is **never** logged or printed.

**Enrichment Status Codes:**
| Status | Meaning |
|---|---|
| `SKIPPED_DISABLED` | `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT=false` |
| `SKIPPED_NO_TOKEN` | Token not visible to current Python process |
| `SKIPPED_NO_REQUESTS` | `requests` library not installed |
| `FAILED_TOKEN_AUTH` | HTTP 401/403 from HUD API |
| `FAILED_HTTP` | Non-auth HTTP failures |
| `FAILED_EMPTY_RESPONSE` | API responded but all counties returned empty |
| `SUCCESS_NO_ZIPS` | Enrichment ran but produced zero ZIP rows |
| `SUCCESS_WITH_ZIPS` | Normal success |

**Changes:**
1. **MSPBNA_CR_Normalized.py** — `_validate_runtime_env()` uses `resolve_hud_token()` for multi-source discovery with full diagnostics; returns resolved token. `main()` passes token to pipeline via config. Enrichment call passes `hud_user_token=` explicitly. Fixed `Norm_CRE_ACL_Coverage` display label from "CRE Reserve % of Norm ACL" to "CRE ACL Coverage (% of CRE Loans)".
2. **case_shiller_zip_mapper.py** — Added `resolve_hud_token()` with 4-source resolution and diagnostics dict. Added 8 enrichment status constants. `build_case_shiller_zip_sheets()` now accepts `hud_user_token` parameter, returns `enrichment_status` and `token_diagnostics` keys, and distinguishes auth failure / HTTP failure / empty response / success.
3. **report_generator.py** — Added `_METRIC_FORMAT_TYPE` dict for semantic formatting. Replaced `_COVERAGE_KEYWORDS` substring matching with `_METRIC_FORMAT_TYPE.get(rat_col, "pct")` lookup. NPL coverage → x-multiples, all other metrics → percent.
4. **test_regression.py** — 16 new tests across 3 classes + 2 updated tests:
   - `TestCoverageVsShareSemantics` (6): share rows not labeled coverage, exposure coverage has correct denominator, Norm_ACL_Coverage uses Norm_Gross_Loans, NPL format is x, loan coverage format is %, display label corrected
   - `TestHUDTokenDiscovery` (6): resolver exists, explicit overrides, diagnostics keys, token never logged in full, env var resolution, missing token diagnostics
   - `TestEnrichmentStatusCodes` (4): status codes defined, build returns status/diagnostics, hud_user_token param, token passed explicitly from main
   - Updated `TestCoverageMetricFormatting` (2): _METRIC_FORMAT_TYPE replaces _COVERAGE_KEYWORDS

**Test baseline**: 196 tests — 184 passing, 0 failures, 10 pre-existing errors (missing matplotlib/aiohttp), 2 skipped.

### 2026-03-11 — Output Quality Fixes (Directive 6)

**8-part directive** covering FDIC history, chart quality, formatting, logging, and label accuracy.

**Changes:**

1. **FDIC history extended to 48 quarters (MSPBNA_CR_Normalized.py)**: `quarters_back` changed from 30 to 48 (12 years). Removed hardcoded `[:30]` slice in FFIEC healing so the full date range is retained.

2. **Scatter outlier labels use tickers (report_generator.py)**: `plot_scatter_dynamic()` outlier annotations now use `resolve_display_label(cert, name)` instead of raw bank names. This produces ticker-style labels (GS, UBS, JPM) consistent with table output.

3. **Comparative migration ladder (report_generator.py)**: `plot_migration_ladder()` rewritten as a comparative chart plotting MSPBNA (solid), Wealth Peers (dashed), and All Peers (dotted). Uses `ACTIVE_STANDARD_COMPOSITES` for peer CERTs. Title updated to "Early-Warning Migration Ladder — MSPBNA vs Peers".

4. **Low-coverage bar series suppression (report_generator.py)**: `create_credit_deterioration_chart_ppt()` now checks `vals.isna().all()` for each bar entity. All-NaN series are excluded from the chart and tracked in `suppressed_series`. A footnote ("Suppressed (low coverage): ...") is added when series are suppressed.

5. **Coverage metric x-format (report_generator.py)**: `generate_ratio_components_table()` now uses `_METRIC_FORMAT_TYPE` dict for semantic formatting — NPL coverage metrics (ACL/NPL) display as x-multiples (0.62x), loan coverage and share metrics display as percentages. Superseded the fragile `_COVERAGE_KEYWORDS` substring approach.

6. **Normalized CRE label fix (report_generator.py)**: "Norm CRE ACL Coverage" renamed to "Norm CRE % of ACL" — the metric is `Norm_CRE_ACL_Share` (share of ACL allocated to CRE), not a coverage ratio.

7. **CSV log severity classification (logging_utils.py)**: `TeeToLogger._classify_level()` rewritten with `_PROGRESS_PATTERNS` regex. Progress bars and tqdm output on stderr are now classified as INFO (not ERROR). Explicit error keywords → ERROR. Other stderr → WARNING.

8. **Duplicate-date artifact paths confirmed fixed**: Verified no `{stamp}` variable usage in `report_generator.py` — all artifact paths use the dashboard stem which already contains the date.

**Tests (test_regression.py)**: 15 new tests across 8 classes:
- `TestFDICHistoryHorizon` (2): quarters_back=48, no hardcoded [:30]
- `TestScatterTickerLabels` (2): resolve_display_label in scatter annotations
- `TestMigrationLadderComparative` (2): ACTIVE_STANDARD_COMPOSITES usage, title mentions Peers
- `TestLowCoverageChartSuppression` (2): suppressed_series tracking, footnote annotation
- `TestCoverageMetricFormatting` (3): _fmt_multiple usage, _METRIC_FORMAT_TYPE registry, explicit NPL policy
- `TestNormalizedRatioLabels` (1): CRE ACL share not labeled "Coverage"
- `TestCsvLogSeverityClassification` (3): _classify_level heuristics, no blind ERROR, progress pattern detection
- `TestNoDoubleDateArtifactPaths` (1): no {stamp} variable in report_generator.py

**Test baseline**: 180 tests — 168 passing, 0 failures, 10 pre-existing errors (missing matplotlib/aiohttp), 2 skipped.

### 2026-03-11 — FRED Frequency Inference Bugfix

**Bug**: `_infer_freq_from_index()` crashed with `TypeError: 'int' object is not subscriptable` during the last-resort fallback branch. The old code built a `pd.Series` of `(year, month)` tuples, then did `.groupby(lambda x: x[0])` — but `groupby` passes the **Series index label** (an integer) into the lambda, not the tuple value. So `x[0]` tried to subscript an `int`.

**Fix**: Replaced the `Series.groupby(lambda)` pattern with a `DataFrame.groupby("year")["month"].nunique()` approach that groups by actual column values, not index labels. The function now:
- Normalizes the index: coerce to `DatetimeIndex`, drop `NaT`, sort, deduplicate
- Tries `pd.infer_freq()` first (most reliable)
- Falls back to median-observations-per-year heuristic
- Last resort uses `DataFrame.groupby` on year/month columns (the fixed path)
- Wraps everything in `try/except` — returns `("quarterly", "Q")` on any unexpected failure

**Note**: Frequency inference is still heuristic when FRED metadata is missing or marked "Unknown". The helper is a best-effort fallback, not a guarantee. Series with highly irregular observation patterns may be classified conservatively as quarterly.

**Changes:**
1. **MSPBNA_CR_Normalized.py** — Extracted `infer_freq_from_index()` to module level for testability. Nested version now delegates to it. Added `NaT` handling, `try/except` safety wrapper.
2. **test_regression.py** — Added `TestInferFreqFromIndex` (9 tests): monthly, quarterly, daily, irregular, duplicates, NaT, empty, exact reproduction of old bug, module-level function existence.

**Test baseline**: 165 tests — 153 passing, 0 failures, 10 pre-existing errors (missing `matplotlib`/`aiohttp`), 2 skipped.

### 2026-03-11 — Final Cleanup & Testability

**Changes:**
1. **Deprecated `generate_normalized_comparison_table()`** — Function body replaced with `raise NotImplementedError`. Retained as a compatibility stub with deprecation docstring pointing to the separate standard/normalized table generators. Was already removed from `generate_reports()` in prior directive; this formalizes the deprecation.

2. **Restored `validate_composite_cert_regime()` as a proper function** — The implementation was orphaned as dead code after `resolve_display_label()`'s `return` statement. Extracted into a standalone top-level function with proper signature (`proc_df: pd.DataFrame`) and docstring. Returns structured dict with `valid`, `active_present`, `active_missing`, `legacy_present`, `warnings`, `errors`. Fixes the pre-existing `test_validate_composite_cert_regime_exists` test failure.

3. **Removed import-time side effects from `MSPBNA_CR_Normalized.py`** — Importing the module no longer requires environment variables, alters `cwd`, prints to stdout, or opens log files. Specifically:
   - `os.chdir(script_dir)` moved from module level into `main()`
   - HUD token print statements moved into `_validate_runtime_env()`
   - `ValueError` raise on missing MSPBNA_CERT/MSBNA_CERT moved into `_validate_runtime_env()`
   - `MSPBNA_CERT` and `MSBNA_CERT` now use `os.getenv()` with defaults (`34221`/`32992`) for import safety
   - `logger = setup_logging()` moved from module level into `main()`
   - `main()` now calls `os.chdir(script_dir)`, `_validate_runtime_env()`, `setup_logging()` at start

4. **Added 15 new regression tests** across 3 test classes:
   - `TestDeprecatedNormalizedComparison` (3 tests): stub exists, raises NotImplementedError, not called in generate_reports
   - `TestValidateCompositeCertRegime` (5 tests): function exists, top-level (not dead code), returns required keys, checks active/legacy composites
   - `TestImportSafety` (7 tests): no import-time ValueError, print, setup_logging, os.chdir; env vars have defaults; `_validate_runtime_env` exists; main calls runtime bootstrap

5. **Fixed `TestNoMixedRegimeArtifact` false positive** — Test regex was matching the `NotImplementedError` message string, not an actual function call. Updated to skip string literals.

**Test baseline**: 156 tests — 144 passing, 0 failures, 10 pre-existing errors (missing `matplotlib`/`aiohttp`), 2 skipped.

### 2026-03-11 — Presentation & Output Cleanup

**Fixes:**
1. **Removed mixed standard-vs-normalized comparison HTML artifact** — `generate_normalized_comparison_table()` produced a side-by-side table mixing standard and normalized columns. This violates the single-regime-per-artifact rule. Removed from `generate_reports()`. The function definition is retained but no longer called.
2. **Fixed double-date artifact filenames** — `{stem}` already contains the date from the Excel file (`Bank_Performance_Dashboard_YYYYMMDD`), so appending `_{stamp}` produced `..._20260311_..._20260311.html`. Removed the redundant `_{stamp}` suffix from all 28+ artifact paths.
3. **Fixed "Norm NCO Rate (TTM) (%)" label** — Normalized NCO is not TTM; changed to "Norm NCO Rate (%)" in `generate_core_pb_peer_table`, `generate_detailed_peer_table`, and `generate_segment_focus_table`.
4. **Updated CLAUDE.md** — Corrected artifact filename examples (no double date), updated table column examples to use tickers (GS, UBS not Goldman Sachs), removed stale normalized_comparison_table reference from naming docs.
5. **Added 5 regression tests** — no mixed-regime artifacts, no double-date filenames, correct normalized NCO label, CLAUDE.md ticker convention, CLAUDE.md no stale full-name output examples.

### 2026-03-11 — CSV Logger Lifecycle Safety Fix

**Problem**: `csv_log.close()` inside `dash.run()` closed the CSV file while `sys.stdout`/`sys.stderr` were still wrapped by `TeeToLogger`. Subsequent `print()` calls in `main()` raised `ValueError: I/O operation on closed file`. Same risk in `report_generator.py`.

**Root cause**: `CsvLogger.close()` did not restore streams, `TeeToLogger.write()` did not check if the logger was closed, and the close was called too early in the pipeline (inside `run()` before `main()` finished printing).

**Solution**: Made the entire logging lifecycle crash-proof.

**Changes by file:**

1. **logging_utils.py** — Safe lifecycle:
   - `CsvLogger._closed` flag; `is_closed` property
   - `CsvLogger.log()` — no-op after close, wrapped in try/except (never raises)
   - `CsvLogger.log_exception()` — wrapped in try/except (never raises)
   - `CsvLogger.restore_streams()` — puts `sys.stdout`/`sys.stderr` back to originals
   - `CsvLogger.close()` — idempotent: restores streams → flushes → closes file → sets `_closed = True`
   - `CsvLogger.shutdown()` — logs final CONFIG message, then calls `close()`; suppresses secondary exceptions
   - `TeeToLogger.write()` — checks `is_closed` before CSV logging; try/except around CSV call
   - `setup_csv_logging()` — unwraps existing `TeeToLogger` to prevent nested stacking
   - Docstring corrected: 15-column schema (was incorrectly labeled 16)

2. **MSPBNA_CR_Normalized.py** — Removed early close:
   - Removed `csv_log.close()` from `run()` method (was line ~6406)
   - Added `csv_log.shutdown()` in `main()`'s `finally` block — after ALL prints, verification, Power BI, and FRED series check

3. **report_generator.py** — Safe shutdown:
   - Replaced `csv_log.info("shutdown") + csv_log.close()` with `csv_log.shutdown()`
   - Wrapped `csv_log.log_exception()` in try/except to prevent logging errors masking real exceptions

4. **test_regression.py** — 7 new tests in `TestLoggerSafeLifecycle`:
   - `test_print_after_logger_close_does_not_crash`
   - `test_stderr_after_logger_close_does_not_crash`
   - `test_close_restores_streams`
   - `test_double_close_is_safe`
   - `test_setup_csv_logging_does_not_stack_nested_tees`
   - `test_mspbna_has_single_terminal_csv_close_path`
   - `test_report_generator_safe_shutdown`
   - Fixed docstring: "16 columns" → "15 columns"

5. **CLAUDE.md** — Added safe logging lifecycle documentation, corrected schema count, added changelog entry

### 2026-03-11 — Output Naming + CSV Logging Overhaul

**Problem**: Artifact filenames used HHMMSS timestamps causing unnecessary file proliferation. Logging used unstructured text files not suitable for LLM debugging. No stdout/stderr capture.

**Solution**: Date-only artifact naming (YYYYMMDD), per-script CSV structured logs with 15-column schema, stdout/stderr tee capture, centralized utilities.

**Changes by file:**

1. **logging_utils.py** (NEW) — Centralized logging & naming module:
   - `get_run_date_str()` — returns YYYYMMDD date string
   - `build_artifact_filename()` — date-stamped filenames
   - `CsvLogger` — 15-column CSV log writer (reset each run)
   - `TeeToLogger` — stdout/stderr stream wrapper
   - `setup_csv_logging()` — one-call setup
   - Event types: CONFIG, FILE_DISCOVERED, FILE_WRITTEN, DATAFRAME_SHAPE, VALIDATION_WARNING, VALIDATION_ERROR, EXCEPTION, STDOUT, STDERR, CHART_SKIPPED, TABLE_SKIPPED, METRIC_SUPPRESSED, PRECHECK_FAIL, PRECHECK_WARN

2. **MSPBNA_CR_Normalized.py** — Integrated CSV logging:
   - Dashboard filename: `build_artifact_filename()` (date-only, no HHMMSS)
   - `setup_logging()` now creates CSV log via `setup_csv_logging("MSPBNA_CR_Normalized")`
   - Structured log events at: startup (CONFIG), FDIC fetch (DATAFRAME_SHAPE), peer composite (DATAFRAME_SHAPE), 8Q averages (DATAFRAME_SHAPE), workbook write (FILE_WRITTEN), shutdown (CONFIG)
   - stdout/stderr tee'd into CSV log

3. **report_generator.py** — Integrated CSV logging:
   - Date stamp: `get_run_date_str()` (centralized, no inline datetime)
   - CSV log via `setup_csv_logging("report_generator")` at start of `generate_reports()`
   - Structured log events at: workbook discovery (FILE_DISCOVERED), sheet loads (DATAFRAME_SHAPE), preflight warnings (PRECHECK_WARN), preflight errors (PRECHECK_FAIL), chart/scatter/table writes (FILE_WRITTEN), exception (EXCEPTION), shutdown (CONFIG)
   - stdout/stderr tee'd into CSV log

4. **test_regression.py** — 19 new tests across 6 test classes:
   - `TestDateOnlyNaming` (6 tests): date string format, no HHMMSS, suffix/no-suffix, output dir, source checks
   - `TestCsvLogging` (5 tests): column count, required columns, script name in filename, reset per run, schema headers
   - `TestStdoutStderrCapture` (2 tests): stdout/stderr captured as STDOUT/STDERR events
   - `TestFileWriteEvents` (2 tests): FILE_WRITTEN logged in both scripts
   - `TestPreflightEvents` (2 tests): PRECHECK_WARN and PRECHECK_FAIL events
   - `TestClaudeMDLogging` (2 tests): CLAUDE.md documents CSV logging and YYYYMMDD naming

5. **CLAUDE.md** — Updated: date-only naming documentation, CSV log schema, event types, per-script log files, stdout/stderr mirroring, `logging_utils.py` in script table

### 2026-03-11 — County-Level Case-Shiller Geographic Mapping Refactor

**Problem**: Case-Shiller ZIP mapping used CBSA/CBSA-Division approximations (HUD type 8/9) which do not match the exact county-level definitions in the S&P CoreLogic methodology.

**Solution**: Replaced `CASE_SHILLER_METRO_MAP` (20-entry CBSA-based) with `CASE_SHILLER_COUNTY_MAP` (~160-entry FIPS-based) sourced from the S&P Case-Shiller "Index Geography" table. Switched HUD API from type 8/9 (CBSA/CBSADIV-to-ZIP) to type 7 (County-to-ZIP).

**Changes by file:**

1. **case_shiller_zip_mapper.py** — Complete refactor:
   - Removed `CASE_SHILLER_METRO_MAP` (CBSA-based), replaced with `CASE_SHILLER_COUNTY_MAP` (FIPS-based, ~160 entries across 20 metros)
   - Changed HUD API from type 8/9 to type 7 (`_HUD_TYPE_COUNTY_ZIP = 7`)
   - Removed `build_case_shiller_metro_map()`, added `build_case_shiller_county_map()`
   - Rewrote `build_case_shiller_zip_coverage()` — now takes single `county_xwalk` DataFrame, joins on 5-digit FIPS
   - Rewrote `summarize_case_shiller_zip_coverage()` — aggregates by region with unique county counts
   - Updated `validate_zip_coverage()` — validates FIPS code format instead of CBSA duplicates
   - Renamed output sheet `CaseShiller_Metro_Map_Audit` → `CaseShiller_County_Map_Audit`
   - Updated `build_case_shiller_zip_sheets()` orchestrator — fetches per-county HUD data
   - Retained: retry/backoff logic, env toggle, HUD token auth, ratio extraction, `_normalize_zip`, backward-compatible `CASE_SHILLER_METROS`/`map_zip_to_metro`

2. **MSPBNA_CR_Normalized.py** — Updated comment referencing `CaseShiller_County_Map_Audit`

3. **report_generator.py** — Fixed MSBNA label: `resolve_display_label` returns `"MSBNA"` (not `"MS"`)

4. **test_regression.py** — Updated `TestCaseShillerZIP`:
   - Changed audit sheet key assertions from `CaseShiller_Metro_Map_Audit` to `CaseShiller_County_Map_Audit`
   - Added 6 new tests: `test_county_map_has_20_regions`, `test_county_map_fips_are_5_digit`, `test_county_map_has_required_fields`, `test_hud_type_is_county_zip`, `test_no_cbsa_references_in_coverage_builder`
   - Fixed `test_msbna_label_is_msbna` (was `test_msbna_label_is_ms`)

5. **CLAUDE.md** — Rewrote Section 11 for county-level mapping. Updated sheet layout table. Added changelog.

### 2026-03-11 — Centralized Label Resolver (Ticker-Style Display Labels)

**Problem**: Bank/comparator labels were hardcoded as full names ("Goldman Sachs", "Core PB Avg") across 6+ table/chart builder functions, producing inconsistent output.

**Solution**: Created `resolve_display_label(cert, name)` — a single centralized resolver in `report_generator.py`.

**Changes by file:**

1. **report_generator.py** — Added `_TICKER_MAP`, `_COMPOSITE_LABELS`, `_SUBJECT_CERT`, `_MSBNA_CERT` constants and `resolve_display_label()` function. Applied resolver to:
   - `generate_credit_metrics_email_table()` — dynamic peer identification via resolver
   - `generate_core_pb_peer_table()` — col_mapping uses resolver
   - `generate_detailed_peer_table()` — col_mapping uses resolver; `_MSBNA_CERT` constant
   - `generate_segment_focus_table()` — col_certs built via resolver
   - `_build_dynamic_peer_html()` — uses `_MSBNA_CERT` constant
   - `create_credit_deterioration_chart_v3()` — entity_names via resolver
   - `create_credit_deterioration_chart_ppt()` — names dict via resolver
   - `generate_html_email_table_dynamic()` — CSS class check uses `"MS"` instead of `"MSBNA"`
   - Years-of-reserves scatter label uses resolver

2. **test_regression.py** — Added `TestLabelResolver` class (11 tests): resolver exists, ticker map entries, composite labels, no hardcoded "Goldman Sachs" in 6 builder functions, chart v3/PPT use resolver, detailed/segment tables use resolver, subject→MSPBNA, MSBNA→MS, _MSBNA_CERT constant usage. Updated 3 existing TestDirectiveC tests for resolver checks.

3. **CLAUDE.md** — Added ENTITY DISPLAY LABEL POLICY section with full label map. Updated table column examples from full names to tickers. Added naming convention documentation.

### 2026-03-11 — Wealth-Focused Tables, Data Integrity, Stock vs Flow

**Report Design — Wealth-Focused Executive Summary & Segment Tables:**

1. **Executive summary rewrite (report_generator.py)**: `generate_credit_metrics_email_table` now accepts `is_normalized` parameter. Columns changed from MSPBNA/MSBNA/Core PB/All Peers to **MSPBNA | GS | UBS | Wealth Peers | Delta MSPBNA vs Wealth Peers**. Wealth Peers = Core PB composite (90001 standard, 90004 normalized). Both standard and normalized versions generated as separate artifacts.

2. **Segment focus tables (report_generator.py)**: `generate_segment_focus_table` rewritten with same wealth-focused columns (MSPBNA | GS | UBS | Wealth Peers | Delta). Dropped MSBNA and All Peers. Uses Core PB composite as Wealth Peers. GS and UBS identified dynamically from bank NAME.

3. **Core PB peer table (report_generator.py)**: `generate_core_pb_peer_table` repurposed — dropped MSBNA column, composite labeled "Wealth Peers" instead of "Core PB Avg". Title changed to "Wealth Peer Analysis".

4. **Ratio components fix (report_generator.py)**: `_synth()` now synthesizes `Norm_Risk_Adj_Gross_Loans = Norm_Gross_Loans - SBL_Balance` for normalized mode. Fixes N/A denominator on Norm_Risk_Adj_Allowance_Coverage row.

5. **Detailed peer table unchanged**: `generate_detailed_peer_table` remains the only broad all-peer table using 90003/90006.

**Data Integrity — Stock vs Flow Math:**

1. **PLLL added to FDIC_FIELDS_TO_FETCH**: Provision for Loan & Lease Losses now fetched.

2. **Module-level `ytd_to_discrete()` (MSPBNA_CR_Normalized.py)**: New helper converts YTD cumulative series to discrete quarterly flows. Inner `compute_quarterly_from_ytd` now delegates to this. Groups by CERT, handles Q1 (flow = YTD), Q2-Q4 (flow = diff).

3. **Module-level `annualize_ytd()` (MSPBNA_CR_Normalized.py)**: New helper annualizes YTD flow variables: `YTD_value * (4.0 / quarter)`. Standard banking convention for income-statement ratios.

4. **Provision and Yield annualized**: `Provision_to_Loans_Rate` and `Loan_Yield_Proxy` (and `Norm_Loan_Yield`) now use `annualize_ytd()` instead of TTM rolling sums. Gives current-period-only view without stale prior-year quarters.

5. **NCO TTM verified correct**: All NCO calculations already use `ytd_to_discrete` → `rolling(4).sum()` path. No change needed.

6. **`TTM_Past_Due_Rate` → `Past_Due_Rate`**: Renamed globally across MSPBNA_CR_Normalized.py, report_generator.py, master_data_dictionary.py. Delinquency is a point-in-time stock variable — the TTM prefix was incorrect. Display label updated to "Past Due Rate (%)" without TTM.

**Tests (test_regression.py)**: Added `TestDirectiveC` class (15 tests): executive summary wealth columns, core_pb composite usage, is_normalized parameter, segment table wealth peers, core PB table no MSBNA, detailed table all_peers, ratio components norm denominator, Past_Due_Rate rename, PLLL fetch, ytd_to_discrete/annualize_ytd existence, provision/yield annualization, quarterly delegation, both executive versions, display label.

**CLAUDE.md**: Added Stock vs Flow Math convention, Wealth-Focused vs Detailed Table distinction, executive/segment column requirements.

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

### 2026-03-11 — Composite CERT Regime Cleanup (report_generator.py)

**Root cause**: Upstream peer-group construction was correct (4 groups, 7 peers, active composites 90001/90003/90004/90006 in workbook). But downstream `report_generator.py` was still littered with stale references to legacy composite CERTs from the old scheme (99998, 99999, 90002, 90005). This mismatch prevented many charts and tables from constructing correctly.

**Active composite regime (current):**

| Role | Standard | Normalized |
|---|---|---|
| All Peers | 90003 | 90006 |
| Core PB | 90001 | 90004 |

**Legacy/inactive (must never drive artifact selection):**
- 90002 (former MSPBNA+Wealth standard)
- 90005 (former MSPBNA+Wealth normalized)
- 99998 (former "Peers Ex. F&V")
- 99999 (former "All Peers" alias)

Legacy CERTs may still appear in `ALL_COMPOSITE_CERTS` for scatter-dot exclusion, but they are never used as peer-average selectors for charts or tables.

**Fixes applied:**

1. **Canonical constants (report_generator.py)**: Added `ACTIVE_STANDARD_COMPOSITES`, `ACTIVE_NORMALIZED_COMPOSITES`, `INACTIVE_LEGACY_COMPOSITES` at module top. `ALL_COMPOSITE_CERTS` is now derived from these.

2. **create_credit_deterioration_chart_v3**: Replaced 99999→90003 (All Peers), 99998→90001 (Core PB).

3. **create_credit_deterioration_chart_ppt**: Default entities changed from `[subject, 99999, 99998]` to `[subject, 90003, 90001]`. Names/colors dict updated for all 4 active composites.

4. **plot_scatter_dynamic**: Default args changed from `peer_avg_cert_primary=99999, peer_avg_cert_alt=99998` to `90003, 90001`.

5. **generate_detailed_peer_table**: Removed 99999 fallback — now uses `ACTIVE_NORMALIZED_COMPOSITES["all_peers"]` or `ACTIVE_STANDARD_COMPOSITES["all_peers"]`.

6. **generate_segment_focus_table**: Same — removed 99999 fallback.

7. **generate_ratio_components_table**: Same — uses canonical constants.

8. **generate_credit_metrics_email_table**: Changed from 90002 (removed "Wealth" composite) to `ACTIVE_STANDARD_COMPOSITES["all_peers"]` (90003). Column header changed from "MS+Wealth" to "All Peers".

9. **generate_flexible_html_table**: Default peer_certs changed from `[90001, 90002, 90003]` to `[90001, 90003]`.

10. **Scatter exclusion sets**: All 3 chart helpers (`build_growth_vs_deterioration_chart`, `build_risk_adjusted_return_chart`, `build_concentration_vs_capital_chart`) now use `ALL_COMPOSITE_CERTS` instead of inline sets.

11. **validate_composite_cert_regime helper**: New function confirms active composites present and flags legacy composites.

12. **Regression tests (test_regression.py)**: Added `TestCompositeRegimeCleanup` (11 tests): canonical constant definitions, no legacy CERTs in selection paths, correct standard/normalized peer avg CERTs, chart v3 uses current composites, scatter defaults current regime, no legacy fallback in table generators, validate helper exists.

### 2026-03-11 — End-to-End Workbook Output Fix, Display Labels, Matplotlib Suppression

**Root cause of workbook-level failure**: The presentation-layer code changes (curated allowlists, metric roles, display labels) were structurally correct in source, but the workbook output still appeared as a raw dump because:
- `LOCAL_DERIVED_METRICS` in `master_data_dictionary.py` was missing display labels for ~30 metrics used in the curated tabs (`Norm_*`, `RIC_*`, profitability, liquidity). `_get_metric_short_name()` fell back to raw codes, making `Metric Name` identical to `Metric Code`.
- `create_normalized_comparison` did not compute `Performance_Flag`, so the Normalized_Comparison tab lacked evaluative context.
- `generate_ratio_components_table` always selected 99999/90003 as the peer composite regardless of `is_normalized` mode, so normalized HTML tables used the wrong peer.
- No workbook-level regression tests existed to catch these end-to-end failures.

**Lesson**: Presentation-layer fixes are only considered complete once visible in the generated workbook. Source-code-level tests alone are insufficient — workbook-level validation is required.

**Fixes applied:**

1. **Display label coverage (master_data_dictionary.py)**: Added ~30 entries to `LOCAL_DERIVED_METRICS` covering all metrics in `SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS`: `Norm_Gross_Loans`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Nonaccrual_Rate`, `Norm_NCO_Rate`, `Norm_Delinquency_Rate`, `Norm_SBL_Composition`, `Norm_Fund_Finance_Composition`, `Norm_Wealth_Resi_Composition`, `Norm_CRE_Investment_Composition`, `Norm_Exclusion_Pct`, `Norm_Loan_Yield`, `Norm_Loss_Adj_Yield`, `Norm_Risk_Adj_Return`, `Norm_Provision_Rate`, `Norm_Total_NCO`, `Norm_Total_Nonaccrual`, `Norm_CRE_ACL_Share`, `Norm_Resi_ACL_Share`, `RIC_CRE_Loan_Share`, `RIC_Resi_Loan_Share`, `RIC_CRE_ACL_Share`, `RIC_CRE_ACL_Coverage`, `RIC_CRE_Risk_Adj_Coverage`, `RIC_CRE_NCO_Rate`, `Provision_to_Loans_Rate`, `Liquidity_Ratio`, `HQLA_Ratio`, `Loans_to_Deposits`. Debug logging added for any remaining fallback cases.

2. **Normalized comparison Performance_Flag (MSPBNA_CR_Normalized.py)**: `create_normalized_comparison` now computes `Performance_Flag` using Core PB Norm (90004) percentile, routed through `_get_performance_flag()` which returns blank for descriptive metrics.

3. **Diagnostic logging before workbook write (MSPBNA_CR_Normalized.py)**: Added explicit logging of row count and first 5 `Metric Code` values for both `Summary_Dashboard` and `Normalized_Comparison` immediately before `write_excel_output()`. Also adds a hard error log if `Norm_Provision_Rate` leaks into the normalized tab.

4. **Ratio components table is_normalized-aware peer (report_generator.py)**: `generate_ratio_components_table` now selects `peer_cert=90006` when `is_normalized=True` (previously always used 99999/90003). Re-pick after synthetic columns uses safe `peer_slice.empty` check instead of bare `.iloc[0]`.

5. **Matplotlib warning suppressed (report_generator.py)**: Added `import warnings` and `warnings.filterwarnings("ignore", message="This figure includes Axes that are not compatible with tight_layout", category=UserWarning)` at module top.

6. **Workbook-level regression tests (test_regression.py)**: Added 4 new test classes (14 test methods total):
   - `TestWorkbookLevelCuration` — validates curated allowlist sizes, excludes raw MDRM fields, excludes internal pipeline columns, excludes Norm_Provision_Rate, verifies display label coverage for all curated metrics
   - `TestDisplayLabelCoverage` — validates LOCAL_DERIVED_METRICS has entries for all Norm_, RIC_, and profitability/liquidity metrics
   - `TestHTMLTableResilience` — validates is_normalized-aware peer selection (90006), safe .empty fallback, matplotlib warning filter
   - `TestNormalizedComparisonFlags` — validates Performance_Flag column and _get_performance_flag usage in create_normalized_comparison

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
1. **Expanded residential balance definitions**: `compute_wealth_resi_bal()` and `resi_sum` (for `RIC_Resi_Cost`) now use Open-end/revolving (1797) + Closed-end first liens (5367) + Closed-end junior liens (5368). MDRM 1799 removed from itemization (was incorrectly included).
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

