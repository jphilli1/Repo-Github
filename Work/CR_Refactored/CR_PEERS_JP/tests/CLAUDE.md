# tests — Regression Test Subtree

## Key Rules

- All regression tests live in `test_regression.py`.
- Tests cover: scatter integrity, peer groups, over-exclusion detection, validation engine, rendering architecture, executive charts, macro charts, HUD parsing, local macro pipeline, corp overlay contracts.
- When adding new features, **always** add corresponding regression tests.
- Test names should clearly describe the contract being tested (e.g., `test_no_obsolete_kri_bullet_normalized_artifact`).
- Tests must not require network access or API keys — use mocks/stubs for external services.
- Pre-existing test errors for `matplotlib`/`aiohttp` missing are expected in minimal environments and do not indicate regressions.

## Test Conventions

- Use `unittest.TestCase` classes grouped by feature area.
- Source-inspection tests (checking function signatures, imports, source text) use `inspect` module.
- Regression tests for architecture rules import real constants from production modules.
- Each changelog entry in `docs/claude/99-changelog.md` lists the test classes added.

## Reference

@docs/claude/06-normalization-and-peer-groups.md
@docs/claude/10-coding-rules.md
