# Test Subsystem Rules

Scoped rules for `test_regression.py`.

> `test_regression.py` lives in this directory.

## No Synthetic Production Data

- Tests must **never** generate synthetic data that could be mistaken for production reporting output.
- No `RandomState`, `synth_df`, or fake bank data in test fixtures.
- Tests verify code structure, contracts, and regression guards — not end-to-end data generation.

## Regression Test Expectations

- Every new chart/table artifact must have corresponding test coverage.
- Tests verify: artifact registration in `rendering_mode.py`, mode availability (BOTH vs FULL_LOCAL_ONLY), correct composite CERTs, metric list completeness.
- Architecture tests enforce: no corp_overlay import in report_generator, no local_macro import in report_generator, workbook-driven data flow.
- No obsolete artifact names should exist in the registry (tests verify removal).

## Doc-Update Expectations

- When architecture changes, tests should verify that `CLAUDE.md` / docs reflect the change.
- `test_claude_md_documents_*` pattern verifies documentation completeness.
- Tests should check for contradictions between code state and documentation.

## Test Organization

- Test classes are grouped by feature area (e.g., `TestFootballFieldKRI`, `TestLocalMacroMathAndAlignment`).
- Each test class documents the number of tests it contains.
- Pre-existing errors from missing optional dependencies (`matplotlib`, `aiohttp`) are expected and do not indicate regressions.
