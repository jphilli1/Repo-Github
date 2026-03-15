# Active Sprint Tasks

## Priority 1: Core Performance Optimization
- [ ] **Performance Fix:** In `src/data_processing/MSPBNA_CR_Normalized.py` (lines ~2734+), refactor the continuous `df['col'] = ...` assignments. Store newly derived metrics in a temporary dictionary and append them to the main DataFrame using `pd.concat(axis=1)` to resolve the `PerformanceWarning` fragmentation. Run `python run_pipeline.py` to verify warnings disappear.

## Priority 2: FRED API Resilience & Fallbacks
- [ ] **API Resiliency Fix:** In `src/data_processing/MSPBNA_CR_Normalized.py`, implement a 3-attempt exponential backoff mechanism in the FRED fetching function for macro series (DGS3, DGS1) to handle HTTP 429/400 errors.
- [ ] **Pipeline Stability Fix:** In `src/data_processing/MSPBNA_CR_Normalized.py` inside the FRED export function, check if the observations payload exists. If the fetch fails entirely, generate an empty fallback DataFrame with the expected schema (date, value) to guarantee "FRED expansion sheets" are created so downstream charts do not skip.

## Priority 3: Fail-Fast & I/O Guardrails
- [ ] **Env Var Preflight:** In `run_pipeline.py` (~line 43), add a preflight check before invoking Step 1. `sys.exit(1)` with a clear error if `FRED_API_KEY` or `HUD_USER_TOKEN` are missing. Add optional key load logs to `local_macro.py`.
- [ ] **File I/O Safety:** In `src/reporting/report_generator.py`, wrap `f.write(html)` inside `_produce_table()` and `pd.read_excel()` calls inside `generate_reports()` in `try/except` blocks so missing optional sheets or permission errors log a warning instead of aborting the pipeline.

## Priority 4: Data Fragility - Step 1 (`MSPBNA_CR_Normalized.py`)
- [ ] **Groupby & Merge Guards:** In `MSPBNA_CR_Normalized.py`, add `assert 'CERT' in df.columns` before the 6 `.groupby('CERT')` calls. Add pre-merge dtype alignment checks for the 5 `.merge()` operations.
- [ ] **Derivation Fallback Guards:** In `MSPBNA_CR_Normalized.py` `create_derived_metrics()`, wrap the RCFD/RCON fallback loop and the `.fillna(0)` loop with column existence checks. Default to `0.0` with a logged warning if missing.
- [ ] **Rolling & Exclusion Guards:** In `MSPBNA_CR_Normalized.py`, add column validations before `.rolling(window=4)` calls. Replace bare addition in normalization exclusions (~lines 3063) with axis-wise `np.nansum()` to stop NaN propagation.

## Priority 5: Data Fragility - Step 2 (`report_generator.py`)
- [ ] **Chart & Table Guards:** In `report_generator.py`, add column existence checks at the top of `create_credit_deterioration_chart_v3()` and `plot_scatter_dynamic()`. Guard PD30+PD90 column additions in `generate_normalized_comparison_table()`.
- [ ] **Empty Data Guards:** In `report_generator.py`, guard all `.iloc[0]` accesses (~lines 1173, 1431, etc.) with `if filtered_df.empty:` checks. Replace `(r.get(..., 0) or 0)` in `_synth()` with explicit `pd.notna()` checks.

## Priority 6: Core API Resilience 
- [ ] **FDIC Retry Loop:** In `MSPBNA_CR_Normalized.py`, add a 3-attempt exponential backoff (ConnectionError, Timeout, 5xx) to `FDICDataFetcher.fetch_lnci_separately()`, `fetch_all_banks()`, and `get_bank_locations()`.
- [ ] **FFIEC Retry & Logs:** In `MSPBNA_CR_Normalized.py`, add a 2-attempt retry to `FFIECBulkLoader.fetch_quarter_data()`. Add a structured failure summary (attempted, succeeded, failed) at the end of `heal_dataset()`.

## Priority 7: Supporting Module Resilience
- [ ] **Schema Validations:** In `src/reporting/executive_charts.py`, add a `_validate_fred_schema` helper. In `src/data_processing/metric_registry.py`, add `validate_metric_inputs` wrapper for lambda dependencies. Guard `.drop()`/`.rename()` in `master_data_dictionary.py`.
- [ ] **Macro API Retries:** In `src/local_macro/local_macro.py`, wrap raw `requests.get/post` calls for BEA GDP, BLS unemployment (explicitly catch 504), and Census in 3-attempt retry blocks.

## Priority 8: Critical Math Unit Tests
- [ ] **YTD Flow Tests:** Create `tests/test_ytd_to_discrete.py`. Write isolated tests for `ytd_to_discrete()` (standard, missing prior quarter, NaN input) and `annualize_ytd()` (Q1-Q4 scaling, Q4 idempotency). Run tests.
- [ ] **Normalization Tests:** Create `tests/test_normalization.py`. Write tests for `calc_normalized_residual()` covering standard exclusions, over-exclusion clamping, boundary zeros, NaN passthrough, and multi-segment chains. Run tests.

## Priority 9: Architecture Hardening
- [ ] **Centralize Composite CERTs:** Audit all modules for hardcoded composite CERT sets (90001–90006, 99998, 99999, 88888). Consolidate into a single canonical `COMPOSITE_CERTS` frozenset in a central registry module. Update all imports.

## Backlog / Identified Risks
- [ ] *(Claude Code: Append newly discovered out-of-scope risks or bugs here formatted as actionable tasks)*