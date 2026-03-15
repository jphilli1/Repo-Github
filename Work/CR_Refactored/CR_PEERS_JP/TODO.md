# Active Sprint Tasks

## High Priority: Bug Fixes & Setup

# Active Sprint Tasks: Hardening & Test Coverage

## Priority 1: Supporting Module Hardening
- [x] **Schema & Input Validations:** 1) Added `_validate_fred_schema()` in `report_generator.py` (where FRED charts live; `executive_charts.py` does not exist) — called before macro_corr_heatmap, credit_stress, and rates_housing. 2) Added `validate_metric_inputs()` static method on `CRProcessor` in `MSPBNA_CR_Normalized.py` — validates TTM_NCO_Rate and Past_Due_Rate deps before computation. 3) `master_data_dictionary.py` has no `.drop()` or `.rename()` calls — no changes needed. `metric_registry.py` does not exist (imports are guarded by try/except). 24/52 tests pass (28 pre-existing). *(2026-03-15)*
- [x] **Macro API Resilience:** In `src/local_macro/local_macro.py`, wrap the raw `requests` calls for BEA GDP, BLS Unemployment, and Census Population in 3-attempt retry blocks (explicitly catching BLS 504 Gateway Timeouts). Add a module-load log identifying which optional API keys are missing. Run `pytest tests/`. *(Already implemented: `_retry_request()` with 3-attempt exponential backoff covers all 3 fetch functions; `_log_api_key_availability()` runs at module load. 2026-03-15)*
- [x] **Pipeline Entry Preflight:** In `run_pipeline.py`, add an early validation check before invoking Step 1. Fail fast with a clear error and `sys.exit(1)` if `FRED_API_KEY` or `HUD_USER_TOKEN` are missing when required. Run `python run_pipeline.py --help`. *(2026-03-15)*

## Priority 2: Critical Math Unit Tests
- [x] **Unit Tests - Flow Math:** Create `tests/test_ytd_to_discrete.py`. Write isolated tests for `ytd_to_discrete()` and `annualize_ytd()` covering: standard YTD de-accumulation, missing prior quarters (fallback behavior), NaN passthrough, and Q4 annualization idempotency. Run `pytest tests/test_ytd_to_discrete.py -v`. *(23 tests, all passing. 2026-03-15)*
- [x] **Unit Tests - Normalization:** Create `tests/test_normalization.py`. Write isolated tests for `calc_normalized_residual()` covering: standard exclusion, over-exclusion clamping (must not be negative, must flag severity), boundary zero values, NaN propagation, and multi-segment exclusion chains. Run `pytest tests/test_normalization.py -v`. *(19 tests, all passing. 2026-03-15)*
## Priority 3
- [x] Audit Logic Issue: The normalized CRE Segment Analysis and Resi Segment Analysis HTML artifacts are regime-mixed. Fixed by: (1) removing false `*` suffixes from standard `RIC_*` rows in the Resi normalized table, (2) adding a visual section separator between normalized whole-book rows and standard segment-detail rows, (3) relabeling standard segment rows for clarity, (4) adding a footnote explaining the `*` convention. Standard segment metrics (`RIC_CRE_*`, `RIC_Resi_*`) cannot be truly normalized because the normalization engine operates at the whole-book level; segment-level MDRM fields have no normalized equivalent. *(2026-03-15)*
- [x] **Feature: Dependency Management:** Create `requirements.txt` pinning: pandas, numpy, openpyxl, scipy, matplotlib, seaborn, requests, aiohttp, tqdm, python-dotenv. Validate by running `pip install -r requirements.txt`. *(2026-03-15)*
## Priority 4
- [x] **Feature: Early API Key Validation:** Add preflight env-var checks in `run_pipeline.py` before invoking Step 1. Fail fast with a clear error if `FRED_API_KEY` or `HUD_USER_TOKEN` are missing. Run `tests/test_regression.py` to verify. *(Already implemented in Priority 1 "Pipeline Entry Preflight" task. FRED_API_KEY → sys.exit(1), HUD_USER_TOKEN → warning. 2026-03-15)*

##  Architecture Hardening
- [ ] **Feature: Centralize Composite CERTs:** Audit all modules for hardcoded composite CERT sets (90001–90006, 99998, 99999, 88888). Consolidate into a single canonical `COMPOSITE_CERTS` frozenset in a central registry module and update all imports. **Math constraint: these CERTs must be excluded from peer scatter dots but included for aggregate lines.** Run regression tests and append a summary to `docs/claude/99-changelog.md`.

## Priority 5
## Low Priority / Epic: Monolith Reduction
- [x] **Epic: Extract Data Processing Subsystems:** Extracted `flow_math.py` (ytd_to_discrete, annualize_ytd, infer_freq_from_index, retry_request) and `peer_assembly.py` (PeerGroupType, PEER_GROUPS, validate_peer_group_uniqueness, get_all_peer_certs) from monolith. Monolith re-imports for backward compat. Math constraint preserved. Updated module table and changelog. *(2026-03-15)*
- [x] **Epic: Extract Reporting Subsystems:** Extracted `chart_config.py` (CHART_PALETTE, composite CERTs, ticker/label maps, resolve_display_label, CHART_COLORS) from report_generator.py. Monolith imports from chart_config.py. Workbook-driven integration contract preserved. Updated module table and changelog. *(2026-03-15)*

## Backlog / Identified Risks
- [ ] *(Claude Code: Append newly discovered out-of-scope risks or bugs here formatted as actionable tasks)*