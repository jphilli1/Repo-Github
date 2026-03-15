# 99 — Changelog

All dated entries documenting architectural changes, bug fixes, and feature additions.

---

## 2026-03-15 — Extract Reporting Subsystems (Monolith Reduction Phase 2)

Extracted shared chart configuration from `report_generator.py` (4947→~4830 lines, net -117 lines of inline definitions):

**`chart_config.py`** — Centralized chart configuration:
- `CHART_PALETTE` and color convenience aliases (`_C_MSPBNA`, etc.)
- Composite CERT regime (`ACTIVE_STANDARD_COMPOSITES`, `ACTIVE_NORMALIZED_COMPOSITES`, `ALL_COMPOSITE_CERTS`)
- Member CERTs for football-field computation
- Ticker/label maps (`_TICKER_MAP`, `_COMPOSITE_LABELS`)
- `resolve_display_label()` — canonical entity label resolver
- `CHART_COLORS` and `_build_cert_color_map()`

The monolith now imports all chart config from `chart_config.py`. Workbook-driven integration contract preserved: Step 2 reads from Excel, never imports Step 1 modules.

**Files created:** `src/reporting/chart_config.py`
**Files changed:** `src/reporting/report_generator.py`, `docs/claude/01-project-overview.md`

---

## 2026-03-15 — Extract Data Processing Subsystems (Monolith Reduction Phase 1)

Extracted two subsystems from `MSPBNA_CR_Normalized.py` (7184→~7050 lines, net -134 lines of inline definitions):

**`flow_math.py`** — Stateless flow-variable utilities:
- `retry_request()` — HTTP retry with exponential backoff (Timeouts, 5xx)
- `ytd_to_discrete()` — YTD cumulative → discrete quarterly flows
- `annualize_ytd()` — YTD → annualized projection (Q1×4, Q2×2, Q3×4/3, Q4×1)
- `infer_freq_from_index()` — FRED series frequency inference

**`peer_assembly.py`** — Peer group definitions and validation:
- `PeerGroupType` enum (6 values: 3 standard + 3 normalized)
- `PEER_GROUPS` dict (4 active groups with cert lists, display_order, use_normalized)
- `validate_peer_group_uniqueness()` — prevents duplicate cert membership within same normalization mode
- `get_all_peer_certs()` — union of all peer CERTs

The monolith now imports from these modules with backward-compatible re-exports. Math constraint preserved: income metrics are de-accumulated quarterly (`ytd_to_discrete`) before annualization (`annualize_ytd`).

**Files created:** `src/data_processing/flow_math.py`, `src/data_processing/peer_assembly.py`
**Files changed:** `src/data_processing/MSPBNA_CR_Normalized.py`, `tests/test_ytd_to_discrete.py`, `docs/claude/01-project-overview.md`

---

## 2026-03-15 — Fix Regime-Mixed Normalized Segment Tables

The normalized CRE and Resi segment analysis HTML tables mixed normalized whole-book rows (`Norm_*`) with standard segment rows (`RIC_CRE_*`, `RIC_Resi_*`), making the table appear fully normalized when only part of it was. Fix: (1) removed false `*` suffixes from standard `RIC_Resi_*` labels, (2) added a visual section separator row between the normalized whole-book section and the standard segment-detail section, (3) relabeled standard rows for clarity, (4) added a footnote explaining the `*` convention. The `RIC_*` segment metrics cannot be truly normalized because the normalization engine subtracts excluded categories from whole-book totals — Call Report has no normalized segment-level MDRM fields.

**Files changed:** `report_generator.py`

---

## 2026-03-15 — Schema & Input Validations (FRED + Metric Dependencies)

Added `_validate_fred_schema()` helper in `report_generator.py` — validates FRED DataFrame has required SeriesID/DATE/VALUE columns before charting. Called from `generate_macro_corr_heatmap()`, `plot_macro_overlay_credit_stress()`, and `plot_macro_overlay_rates_housing()`. Added `validate_metric_inputs()` static method on `CRProcessor` in `MSPBNA_CR_Normalized.py` — checks that dependency columns (NTLNLS_Q, TopHouse_PD30/PD90, LNLS) exist before TTM metric computation, logging warnings for missing deps. Note: `executive_charts.py` and `metric_registry.py` do not exist (referenced modules were never created; imports are guarded by try/except). `master_data_dictionary.py` has no `.drop()` or `.rename()` calls.

**Files changed:** `report_generator.py`, `MSPBNA_CR_Normalized.py`

---

## 2026-03-15 — Reporting Engine Pandas & I/O Guards (report_generator.py)

Added column existence checks to `create_credit_deterioration_chart_v3()` (returns None,None if CERT/REPDTE/TTM_NCO_Rate/NPL_to_Gross_Loans_Rate missing) and `plot_scatter_dynamic()` (raises ValueError if x_col/y_col/CERT missing). Replaced falsy-zero `(r.get(col) or 0)` patterns with explicit `pd.notna()` checks for CRE/Resi delinquency numerator synthesis. Wrapped macro correlation heatmap HTML file write in try/except OSError. Audited all 20 `.iloc[0]` usages — all already guarded.

**Files changed:** `report_generator.py`

---

## 2026-03-15 — API Retry & Backoff for FDIC, FFIEC, and FRED Metadata Fetches

Added `_retry_request()` synchronous helper with 3-attempt exponential backoff (2^n seconds) for Timeouts, ConnectionErrors, and HTTP 5xx responses. Applied to:
- `FDICDataFetcher.fetch_lnci_separately()` and `fetch_all_banks()` (synchronous `requests` calls)
- `FFIECBulkLoader` via retry loop in `fetch_quarter_data()` wrapping `_download_strict()` for retryable statuses (TIMEOUT, REQUEST_ERROR, INITIAL_GET_FAILED, DOWNLOAD_HTTP_ERROR, UNKNOWN_ERROR)
- `FREDDataFetcher._fetch_series_metadata()` (async aiohttp — catches `ClientConnectorError`, `ServerDisconnectedError`, `OSError`, `TimeoutError`, and HTTP 5xx)

Also added structured failure summary to `heal_dataset()`: tracks attempted/succeeded/skipped/failed quarters with per-failure reason logging.

**Files changed:** `MSPBNA_CR_Normalized.py`

---

## 2026-03-15 — Pandas Data Fragility Hardening (MSPBNA_CR_Normalized.py)

Replaced 6 bare `+` exclusion summations (Excluded_Balance, Excluded_NCO_YTD, Excluded_Nonaccrual, excluded_pd30, excluded_pd90, delinquency sums) with `np.nansum()` to prevent NaN propagation when any exclusion category column is NaN. Added 7 empty-DataFrame / column-existence guards: `compute_quarterly_from_ytd()` early return, TTM groupby warning, `Excluded_NCO_Q` column check before `.rolling()`, 8Q averages `available_metrics` filter, LNCI merge guard, locations merge guard, FRED metadata merge guard.

**Files changed:** `MSPBNA_CR_Normalized.py`

---

## 2026-03-15 — Fix HTML Diff-Coloring Precedence for Coverage Metrics

Reversed trend-class logic in `generate_html_email_table_dynamic()`: coverage/ratio semantics (`is_safe`) now checked before adverse keywords (`is_risk`). Previously "Risk-Adj ACL Ratio" triggered `is_risk` because it contains "Risk", coloring higher coverage deltas red (bad) when they should be green (safer). Also narrowed `'Ratio'` to `'ACL Ratio'` to prevent Leverage Ratio from being incorrectly treated as favorable.

**Files changed:** `report_generator.py`

---

## 2026-03-15 — Suppress Inflated Normalized Profitability Stack

Suppressed `Norm_Loan_Yield`, `Norm_Loss_Adj_Yield`, and `Norm_Risk_Adj_Return` from all presentation surfaces: executive summary metric map, `NORMALIZED_COMPARISON_METRICS` list, and `risk_adjusted_return` scatter chart. All three share the same numerator/denominator mismatch (full-book interest income / normalized loans). Metrics remain in the data layer for future use if segment-level interest income becomes available.

**Files changed:** `report_generator.py`, `MSPBNA_CR_Normalized.py`

---

## 2026-03-15 — Suppress Inflated Norm_Loan_Yield from HTML Tables (superseded by above)

Removed `Norm_Loan_Yield` and `Norm_Loss_Adj_Yield` from the normalized executive summary metric map in `report_generator.py`. These metrics have a numerator/denominator mismatch: full-book annualized interest income divided by normalized (ex-C&I/consumer) gross loans. Call Report does not provide segment-level interest income, so the numerator cannot be normalized. GS showed ~26% vs ~7% standard, clearly inflated. Metrics remain in the data layer for potential future use.

**Files changed:** `report_generator.py`

---

## 2026-03-15 — Fix Zero-Denominator NPL Coverage Leaking 0.00x

Changed `RIC_{seg}_Risk_Adj_Coverage` (ACL / Nonaccrual) to return NaN instead of 0 when nonaccrual is zero. Previously `safe_div` returned 0.0, which contaminated composite means (e.g., UBS with zero CRE nonaccrual contributed 0.00x, pulling 90001 composite from ~0.56x to ~0.37x). NaN values are now excluded from composite averages and display as "N/A" in HTML tables. Affects all 6 segment-level Risk_Adj_Coverage metrics.

**Files changed:** `MSPBNA_CR_Normalized.py`

---

## 2026-03-15 — Fix Self-Inclusive Wealth Peer Composite

Removed MSPBNA_CERT (34221) from `Core_Private_Bank` and `Core_Private_Bank_Norm` peer group member lists. Composites 90001/90004 previously averaged MSPBNA+GS+UBS, making "Delta vs Wealth Peers" self-inclusive and mechanically understating the delta. Now composites average GS+UBS only (external peers). All downstream HTML table deltas, KRI chart ranges, and scatter composite dots automatically corrected.

**Files changed:** `MSPBNA_CR_Normalized.py`

---

## 2026-03-14 — Final Hardening Pass (KRI Legend, Geography, Completeness)

**4 workstreams:**

**A. KRI chart legend refinement** (`executive_charts.py`):
- Added Wealth PB Avg marker (purple triangle-up, mean of wealth peer member values)
- Added All Peers Avg marker (blue square, mean of all peer member values)
- Both markers conditional on >= 2 members having data
- Updated `_range_for_group()` to compute `mean` alongside `median`
- Legend now shows up to 5 entries

**B. Expand geography to all-CBSA universe** (`local_macro.py`):
- Added `_resolve_cbsa()` — accepts any valid 5-digit CBSA code, not just curated TOP_MSAS
- County-to-CBSA quality flag changed from `medium` to `low`

**C. Completeness flags** (`local_macro.py`):
- Added `macro_data_completeness` and `missing_sources` fields to BOARD_COLUMNS (now 33 columns)
- Added API key missing warning

**D. Tests** (`test_regression.py` — 20 new tests)

**Files changed:** `executive_charts.py`, `local_macro.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-14 — Reconciliation & Hardening Pass (MSA Macro Feature)

Removed contradictions between old "not-yet-produced" state and current workbook-driven implementation. Updated 4 stale tests, fixed sheet count documentation (three → six), removed stale limitation notes.

**Final architecture contract:**
- `corp_overlay.py` — standalone, NOT imported by report_generator or MSPBNA
- `local_macro.py` — owns all local macro data; called by Step 1; produces 6 Excel sheets
- `report_generator.py` — consumes workbook sheets only; never imports local_macro or corp_overlay

**Files changed:** `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Fix 3 Post-Fix Regressions (8 → 0 Failed Artifacts)

**Bug A** — numpy array truth test in credit charts: `if idx_to_label` raised `ValueError`. Fixed with `if len(idx_to_label) > 0`.

**Bug B** — `placer` never instantiated in scatter plots. Reverted to existing `pick_offset()`/`tag()` helpers.

**Bug C** — `strftime("%q")` invalid on Windows. Replaced with f-string formatting.

Added `plt.close(fig)` in `_produce_chart()` after saving.

**Files changed:** `report_generator.py`, `CLAUDE.md`

---

## 2026-03-13 — Workbook-Driven MSA Macro Panel (Prompt 4)

Added `plot_msa_macro_panel()` to `report_generator.py` — reads `Local_Macro_Latest` from workbook. Data-driven MSA selection (portfolio_balance → real_gdp_level → first 10). Does NOT import `local_macro` or `corp_overlay`. Updated `rendering_mode.py` to document production. 12 new tests.

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Board-Ready Workbook Output Persistence (Prompt 3)

Added `build_local_macro_latest()`, `build_msa_board_panel()`, `build_skip_audit()` to `local_macro.py`. Pipeline now returns 6 sheets. Added `Local_Macro_Skip_Audit` persistence in `MSPBNA_CR_Normalized.py`. 12 new tests.

**Files changed:** `local_macro.py`, `MSPBNA_CR_Normalized.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Math Correctness & Quarterly Alignment (Prompt 2)

Added `TransformPolicy` registry, per-capita math helpers (6 functions), `build_derived_metrics()`, `Local_Macro_Derived` output sheet. 18 new tests.

**Files changed:** `local_macro.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Canonical Geography Spine & Local Macro Pipeline

New module `local_macro.py` with 4-tier geography spine, BEA/BLS/Census API fetchers, `run_local_macro_pipeline()` orchestrator. Integrated into `MSPBNA_CR_Normalized.py`. 16 new tests.

**Files created:** `local_macro.py`
**Files changed:** `MSPBNA_CR_Normalized.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Architecture Reconciliation (Corp Overlay / MSA Macro Panel)

Multi-step resolution: removed synthetic import from report_generator, built local_macro.py, integrated into Step 1, added workbook-driven plot_msa_macro_panel. 9 tests enforce: no corp_overlay import, no synthetic data, workbook-driven panel.

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Final Production Fixes & Pipeline Runner

1. Added `CRE_Concentration_Capital_Risk` and `CI_to_Capital_Risk` computation
2. Unified `CHART_COLORS` with `CHART_PALETTE`
3. Removed dead `return manifest` after try/except/finally
4. New `run_pipeline.py` — unified CLI for Step 1 + Step 2

**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `CLAUDE.md`
**Files created:** `run_pipeline.py`

---

## 2026-03-13 — FRED Series Audit & Fetch Resilience

Audited 10 failed FRED series (all VPN/proxy blocks, not bad IDs). Removed discontinued STLFSI2. Added retry with exponential backoff for connection errors. Structured fetch summary logging.

**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Capital Concentration Ratios & Cumulative Growth Base-Period Fix

Added `CRE_Concentration_Capital_Risk` and `CI_to_Capital_Risk` using RBCT1J. Auto-advance base period when Q4 2015 has zero CRE exposure.

**Files changed:** `MSPBNA_CR_Normalized.py`, `metric_registry.py`, `metric_semantics.py`, `executive_charts.py`, `CLAUDE.md`

---

## 2026-03-13 — Cross-Regime NaN-Out for Peer Composites

Standard composites (90001, 90003) now NaN-out `Norm_*` values; normalized composites (90004, 90006) NaN-out standard rate values. Eliminated 5 OutputContamination preflight warnings.

**Files changed:** `MSPBNA_CR_Normalized.py`, `CLAUDE.md`

---

## 2026-03-13 — Cumulative Growth Chart Family

2 new artifacts: `cumul_growth_loans_vs_acl_wealth`, `cumul_growth_loans_vs_acl_allpeers`. CRE+RESI vs CRE ACL growth indexed to Q4 2015 with CAGR annotations. 12 new tests.

**Files changed:** `executive_charts.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Comprehensive Formatting, Bug-Fix, and Feature Expansion Sweep

6 parts: YoY heatmap RESI math fix, HTML table formatting (thousands separators, Norm→asterisk), dual views verified, heatmap/sparkline peer group split (2→4 each), expanded scatter plots (CRE + normalized bankwide).

**Files changed:** `executive_charts.py`, `report_generator.py`, `rendering_mode.py`, `CLAUDE.md`

---

## 2026-03-13 — Chart Package Expansion (Bookwide, Football-Field, MSA Panel)

5 parts: bookwide growth vs deterioration chart, football-field KRI with nested bands, unit family separation, dynamic MSA macro panel, 29 new tests.

**Files changed:** `report_generator.py`, `executive_charts.py`, `rendering_mode.py`, `corp_overlay.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-13 — Centralized Chart Palette, Annotation Engine, and Wealth Peers Inclusion

Created `CHART_PALETTE`, `ChartAnnotationHelper`, added Wealth Peers to 7 high-value charts. 24 new tests.

**Files changed:** `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Normalized KRI Bullet Chart Split (Rates vs Composition)

Split `kri_bullet_normalized` into `kri_bullet_normalized_rates` (5 rate metrics) and `kri_bullet_normalized_composition` (5 composition metrics). Fixed comparator fallback (single → ref marker, neither → skip). 8 new tests.

**Files changed:** `rendering_mode.py`, `executive_charts.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Rendering Architecture Reconciliation (Final Verification)

Verified: no merge conflicts, no duplicate abstractions, correct executive/macro chart integration, canonical rendering_mode.py source. Added Canonical Rendering Abstraction Rule. 22 new tests.

**Files changed:** `CLAUDE.md`, `test_regression.py`

---

## 2026-03-12 — HUD HTTP Failure Diagnostics & Request Hardening

9-part fix: tuple return from `fetch_hud_crosswalk()`, `requests.Session()` with headers, request validation, HTTP status classification (6 failure constants), county-level failure summary, misclassification fix, smoke test helper. 9 new tests.

**Files changed:** `case_shiller_zip_mapper.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Corp-Safe Overlay Workflow (4 Artifacts)

New modules: `corp_overlay.py`, `corp_overlay_runner.py`. 4 artifacts (2 PNG + 2 HTML). Input contract with required/optional columns, geo priority, offline operation. 31 new tests.

**Files created:** `corp_overlay.py`, `corp_overlay_runner.py`
**Files changed:** `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Macro Chart Tranche (3 Artifacts, Deterministic Series Selection)

Replaced heuristic `plot_macro_overlay()` with 3 deterministic artifacts using 13 named FRED series. Deleted old heuristic fallback. 18 new tests.

**Files changed:** `MSPBNA_CR_Normalized.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Executive Chart Tranche (5 Artifacts)

YoY heatmaps (2), KRI bullets (standard + normalized rates + normalized composition), sparkline summary. Exact metric lists, comparator CERTs, is_normalized parameter. 16 new tests.

**Files changed:** `executive_charts.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Architecture Reconciliation (Merge Conflict Resolution)

Resolved 7 merge conflicts in `report_generator.py`. Removed ~230 lines of duplicate local rendering abstractions. All render types now imported from `rendering_mode.py`. Added `is_artifact_available()`, `ArtifactCapability.filename_suffix`. 25 new tests.

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — Normalized Segment Taxonomy Alignment

Aligned to Call Report RC-C: RESI denominator uses LNRERES alone, C&I exclusion uses LNCI directly, removed J458/J459/J460, replaced Ag PD with P3AG/P9AG, tightened CRE pure to LNREMULT+LNRENROT.

**Files changed:** `MSPBNA_CR_Normalized.py`, `CLAUDE.md`

---

## 2026-03-12 — Hardened Normalized Exclusion Engine

Balance-gating for 6 NCO categories, NDFI NCO set to 0.0, structured audit flags (4 boolean columns), `Norm_Delinquency_Rate` MetricSpec, expanded exclusion audit sheet.

**Files changed:** `MSPBNA_CR_Normalized.py`, `metric_registry.py`, `CLAUDE.md`

---

## 2026-03-12 — Dual-Mode Rendering Architecture (Preflight Refactor)

New module `rendering_mode.py`. `generate_reports()` accepts `render_mode` parameter. 31 artifacts registered. CLI supports `full_local`/`corp_safe`. 15 new tests.

**Files created:** `rendering_mode.py`
**Files changed:** `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — HUD Response Two-Pass Flattening Fix

Added `flatten_hud_rows()` as second-pass flattener for nested HUD wrapper objects. 8 new tests.

**Files changed:** `case_shiller_zip_mapper.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-12 — HUD Response Parsing & Canonicalization Fix

Added `extract_hud_result_rows()` (5 shapes), `canonicalize_hud_columns()`, `_HUD_COLUMN_MAP`. Added `FAILED_PARSE` and `SUCCESS_NO_MATCHES` status codes. 14 new tests.

**Files changed:** `case_shiller_zip_mapper.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-11 — Final Consistency Pass (HUD Token + ACL Semantics)

Added missing `Norm_CRE_ACL_Coverage` MetricSpec, added `RIC_Comm_Risk_Adj_Coverage` to `_METRIC_FORMAT_TYPE`. 10 new tests.

**Files changed:** `metric_registry.py`, `report_generator.py`, `test_regression.py`

---

## 2026-03-11 — ACL Ratio Semantic Integrity & Metric Registry Completeness

Added 3 missing MetricSpec entries (RIC_CRE_ACL_Share, RIC_Resi_ACL_Share, Norm_CRE_ACL_Share). 9 new tests.

**Files changed:** `metric_registry.py`, `report_generator.py`, `test_regression.py`

---

## 2026-03-11 — Fix DashboardConfig HUD Token TypeError

Added `hud_user_token` field to `DashboardConfig`. Changed dict-style access to attribute access. 8 new tests.

**Files changed:** `MSPBNA_CR_Normalized.py`, `test_regression.py`

---

## 2026-03-11 — Coverage/Share Semantics & HUD Token Discovery (Directive 7)

8-part directive: coverage vs share label rule, `_METRIC_FORMAT_TYPE` dict, `resolve_hud_token()` with multi-source discovery, enrichment status codes. 16 new tests.

**Files changed:** `MSPBNA_CR_Normalized.py`, `case_shiller_zip_mapper.py`, `report_generator.py`, `test_regression.py`

---

## 2026-03-11 — Output Quality Fixes (Directive 6)

8 parts: FDIC history extended to 48 quarters, scatter outlier labels use tickers, comparative migration ladder, low-coverage bar series suppression, coverage metric x-format, normalized CRE label fix, CSV log severity classification, duplicate-date paths confirmed fixed. 15 new tests.

**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `logging_utils.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-11 — FRED Frequency Inference Bugfix

Fixed `TypeError: 'int' object is not subscriptable` in `_infer_freq_from_index()`. Replaced `Series.groupby(lambda)` with `DataFrame.groupby()`. 9 new tests.

**Files changed:** `MSPBNA_CR_Normalized.py`, `test_regression.py`

---

## 2026-03-11 — Final Cleanup & Testability

Deprecated `generate_normalized_comparison_table()`, restored `validate_composite_cert_regime()` as proper function, removed import-time side effects from `MSPBNA_CR_Normalized.py`. 15 new tests.

**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `test_regression.py`

---

## 2026-03-11 — Presentation & Output Cleanup

Removed mixed standard-vs-normalized HTML artifact, fixed double-date filenames, fixed "Norm NCO Rate (TTM)" label. 5 new tests.

**Files changed:** `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-11 — CSV Logger Lifecycle Safety Fix

Made entire logging lifecycle crash-proof. `CsvLogger._closed` flag, idempotent `close()`/`shutdown()`, `TeeToLogger` checks `is_closed`. Removed early close from `run()`. 7 new tests.

**Files changed:** `logging_utils.py`, `MSPBNA_CR_Normalized.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

---

## 2026-03-11 — Output Naming + CSV Logging Overhaul

Date-only artifact naming (YYYYMMDD), 15-column CSV structured logs, stdout/stderr tee capture. New `logging_utils.py`. 19 new tests.

**Files created:** `logging_utils.py`
**Files changed:** `MSPBNA_CR_Normalized.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`
