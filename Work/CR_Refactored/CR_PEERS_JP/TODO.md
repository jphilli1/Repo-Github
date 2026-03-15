- [ ] Feature: Execute Documentation Split

 Create docs/claude/01-project-overview.md from archived monolith Section 1 (lines 1–31), per SPLIT_PLAN.md mapping. Path: Work/CR_Refactored/CR_PEERS_JP/docs/claude/01-project-overview.md
 Create docs/claude/02-build-run-config.md from Section 2 (lines 33–88) + import safety/no-hardcoding subsets. Path: Work/CR_Refactored/CR_PEERS_JP/docs/claude/02-build-run-config.md
 Create remaining topic files (03 through 10, 99) per SPLIT_PLAN.md section-to-file mapping
 Verify root CLAUDE.md cross-references resolve to actual files
 Run python test_regression.py in Work/CR_Refactored/CR_PEERS_JP/ to confirm no regressions

- [ ] Feature: Add Dependency Management

 Create Work/CR_Refactored/CR_PEERS_JP/requirements.txt pinning: pandas, numpy, openpyxl, scipy, matplotlib, seaborn, requests, aiohttp, tqdm, python-dotenv
 Validate with pip install -r requirements.txt && python run_pipeline.py --help

- [ ] Feature: Early API Key Validation

 Add preflight env-var checks in run_pipeline.py before invoking Step 1 — fail fast if FRED_API_KEY or HUD_USER_TOKEN are missing when required
 Run python test_regression.py to verify

- [ ] Feature: Centralize Composite CERTs

 Audit all modules for hardcoded composite CERT sets (90001–90006, 99998, 99999, 88888)
 Consolidate into a single canonical definition (e.g., in metric_registry.py) and import elsewhere
 Run python test_regression.py to verify no behavioral changes
 Append summary to docs/claude/99-changelog.md

 - [ ] Leave for future additions to list, do not treat this item as a task.