# Active Sprint Tasks

## High Priority: Bug Fixes & Setup
- [ ] **Audit Math Issue (Template):** The output for [Insert Metric Name] for [Insert Peer Group] is calculating as [Insert Wrong Number], but it should be closer to [Insert Expected Number]. Use the `cr-math-audit` skill to trace this calculation in `src/data_processing/MSPBNA_CR_Normalized.py`, identify the failure point, fix the logic, and run the script to verify.
- [ ] **Feature: Dependency Management:** Create `requirements.txt` pinning: pandas, numpy, openpyxl, scipy, matplotlib, seaborn, requests, aiohttp, tqdm, python-dotenv. Validate by running `pip install -r requirements.txt`.
- [ ] **Feature: Early API Key Validation:** Add preflight env-var checks in `run_pipeline.py` before invoking Step 1. Fail fast with a clear error if `FRED_API_KEY` or `HUD_USER_TOKEN` are missing. Run `tests/test_regression.py` to verify.

## Medium Priority: Architecture Hardening
- [ ] **Feature: Centralize Composite CERTs:** Audit all modules for hardcoded composite CERT sets (90001–90006, 99998, 99999, 88888). Consolidate into a single canonical `COMPOSITE_CERTS` frozenset in a central registry module and update all imports. **Math constraint: these CERTs must be excluded from peer scatter dots but included for aggregate lines.** Run regression tests and append a summary to `docs/claude/99-changelog.md`.

## Low Priority / Epic: Monolith Reduction
- [ ] **Epic: Extract Data Processing Subsystems:** Identify and extract subsystems within `src/data_processing/MSPBNA_CR_Normalized.py` (FFIEC fetch, YTD de-accumulation, annualization, peer assembly). **Math constraint: income metrics must be de-accumulated quarterly before annualization.** Run regression tests after extraction, append to changelog, and update the module table in `docs/claude/01-project-overview.md`.
- [ ] **Epic: Extract Reporting Subsystems:** Identify and extract subsystems within `src/reporting/report_generator.py` (scatter generation, HTML table generation, chart orchestration). Ensure the workbook-driven integration contract between Step 1 and Step 2 is preserved. Run tests, append to changelog, and update `01-project-overview.md`.

## Backlog / Identified Risks
- [ ] *(Claude Code: Append newly discovered out-of-scope risks or bugs here formatted as actionable tasks)*