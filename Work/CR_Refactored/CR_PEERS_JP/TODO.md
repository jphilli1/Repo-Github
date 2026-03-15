# Active Sprint Tasks

## High Priority: Bug Fixes & Setup

# Active Sprint Tasks: Hardening & Test Coverage

## Priority 1: Core Data Processing Resilience (`MSPBNA_CR_Normalized.py`)
- [ ] **Pandas Data Fragility:** In `src/data_processing/MSPBNA_CR_Normalized.py`, add strict column existence checks and empty-DataFrame guards before all `.groupby()`, `.fillna()`, `.rolling()`, and `.merge()` operations. Replace the normalization exclusion bare addition with `np.nansum()` to prevent NaN propagation. Run `pytest tests/` to verify no regressions.
- [ ] **API Retry & Backoff:** In `src/data_processing/MSPBNA_CR_Normalized.py`, add a 3-attempt exponential backoff retry loop (for Timeouts and 5xx errors) to `FDICDataFetcher` calls, the `FFIECBulkLoader` sequence, and the FRED metadata fetch. Add a structured failure summary to `heal_dataset()`. Run `pytest tests/`.

## Priority 2: Reporting Engine Resilience (`report_generator.py`)
- [ ] **Reporting Pandas & I/O Guards:** In `src/reporting/report_generator.py`, add column existence checks at the start of `create_credit_deterioration_chart_v3()` and `plot_scatter_dynamic()`. Guard all `.iloc[0]` accesses against empty DataFrames. Replace falsy-zero `(r.get(col) or 0)` patterns with explicit `pd.notna()` checks. Wrap Excel sheet reads and HTML file writes in `try/except` blocks. Run `pytest tests/`.

## Priority 3: Supporting Module Hardening
- [ ] **Schema & Input Validations:** Update three supporting modules: 1) In `src/reporting/executive_charts.py`, add a `_validate_fred_schema` helper before charting. 2) In `src/data_processing/metric_registry.py`, add a `validate_metric_inputs` wrapper to ensure lambda dependencies exist. 3) In `src/data_processing/master_data_dictionary.py`, guard `.drop()` and `.rename()` calls. Run `pytest tests/`.
- [ ] **Macro API Resilience:** In `src/local_macro/local_macro.py`, wrap the raw `requests` calls for BEA GDP, BLS Unemployment, and Census Population in 3-attempt retry blocks (explicitly catching BLS 504 Gateway Timeouts). Add a module-load log identifying which optional API keys are missing. Run `pytest tests/`.
- [ ] **Pipeline Entry Preflight:** In `run_pipeline.py`, add an early validation check before invoking Step 1. Fail fast with a clear error and `sys.exit(1)` if `FRED_API_KEY` or `HUD_USER_TOKEN` are missing when required. Run `python run_pipeline.py --help`.

## Priority 4: Critical Math Unit Tests
- [ ] **Unit Tests - Flow Math:** Create `tests/test_ytd_to_discrete.py`. Write isolated tests for `ytd_to_discrete()` and `annualize_ytd()` covering: standard YTD de-accumulation, missing prior quarters (fallback behavior), NaN passthrough, and Q4 annualization idempotency. Run `pytest tests/test_ytd_to_discrete.py -v`.
- [ ] **Unit Tests - Normalization:** Create `tests/test_normalization.py`. Write isolated tests for `calc_normalized_residual()` covering: standard exclusion, over-exclusion clamping (must not be negative, must flag severity), boundary zero values, NaN propagation, and multi-segment exclusion chains. Run `pytest tests/test_normalization.py -v`.

- [ ] Audit Logic Issue: The normalized CRE Segment Analysis and Resi Segment Analysis HTML artifacts are regime-mixed. They combine normalized whole-book rows (Norm_Gross_Loans, Norm_ACL_Coverage, Norm_NCO_Rate, etc.) with standard segment rows like RIC_CRE_Cost, RIC_CRE_Risk_Adj_Coverage, RIC_CRE_NCO_Rate, RIC_Resi_Cost, and RIC_Resi_Risk_Adj_Coverage. That makes the table look fully normalized when only part of it is. Use @.claude/skills/cr-math-audit/SKILL.md to trace the row map in Work/CR_Refactored/CR_PEERS_JP/src/reporting/report_generator.py and the upstream metric lineage in Work/CR_Refactored/CR_PEERS_JP/src/data_processing/MSPBNA_CR_Normalized.py, then either compute truly normalized segment rows or relabel/remove the standard RIC_* rows from the normalized HTML tables.
- [ ] **Feature: Dependency Management:** Create `requirements.txt` pinning: pandas, numpy, openpyxl, scipy, matplotlib, seaborn, requests, aiohttp, tqdm, python-dotenv. Validate by running `pip install -r requirements.txt`.
- [ ] **Feature: Early API Key Validation:** Add preflight env-var checks in `run_pipeline.py` before invoking Step 1. Fail fast with a clear error if `FRED_API_KEY` or `HUD_USER_TOKEN` are missing. Run `tests/test_regression.py` to verify.

## Medium Priority: Architecture Hardening
- [ ] **Feature: Centralize Composite CERTs:** Audit all modules for hardcoded composite CERT sets (90001–90006, 99998, 99999, 88888). Consolidate into a single canonical `COMPOSITE_CERTS` frozenset in a central registry module and update all imports. **Math constraint: these CERTs must be excluded from peer scatter dots but included for aggregate lines.** Run regression tests and append a summary to `docs/claude/99-changelog.md`.

## Low Priority / Epic: Monolith Reduction
- [ ] **Epic: Extract Data Processing Subsystems:** Identify and extract subsystems within `src/data_processing/MSPBNA_CR_Normalized.py` (FFIEC fetch, YTD de-accumulation, annualization, peer assembly). **Math constraint: income metrics must be de-accumulated quarterly before annualization.** Run regression tests after extraction, append to changelog, and update the module table in `docs/claude/01-project-overview.md`.
- [ ] **Epic: Extract Reporting Subsystems:** Identify and extract subsystems within `src/reporting/report_generator.py` (scatter generation, HTML table generation, chart orchestration). Ensure the workbook-driven integration contract between Step 1 and Step 2 is preserved. Run tests, append to changelog, and update `01-project-overview.md`.

## Backlog / Identified Risks
- [ ] *(Claude Code: Append newly discovered out-of-scope risks or bugs here formatted as actionable tasks)*