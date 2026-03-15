# Reporting Subsystem Rules

Scoped rules for `report_generator.py`, `rendering_mode.py`, `executive_charts.py`.

> `report_generator.py` lives in this directory. `rendering_mode.py` and `executive_charts.py` remain at repo root.

## Artifact Contracts

- Every artifact must be registered in `rendering_mode.py` via `_reg()` before use.
- Every artifact block in `generate_reports()` must be guarded by `should_produce()`.
- `rendering_mode.py` is the **single canonical source** for RenderMode, ArtifactManifest, ARTIFACT_REGISTRY, and should_produce(). Never duplicate these in report_generator.py.
- Artifact manifest must be printed at the end of every run.

## Unit-Label Rules

- Rates (NCO, delinquency, nonaccrual) → display as `%`
- NPL coverage (ACL / nonaccrual) → display as `x` multiple (e.g., `1.23x`)
- Composition / share of ACL → display as `%`, label as "Share" or "% of ACL", **never** "Coverage"
- Use `_METRIC_FORMAT_TYPE` dict for semantic formatting — no keyword matching.

## Chart Rules

- All charts use `CHART_PALETTE` — no arbitrary per-chart colors.
- Wealth Peers must appear as explicit styled markers in scatter, years-of-reserves, risk-adjusted-return, growth-vs-deterioration, concentration-vs-capital charts.
- Use ticker abbreviations (GS, UBS, JPM) via `resolve_display_label()` — never full bank names.
- KRI bullet charts must separate % rate metrics from x-multiple metrics (unit family separation).
- Football-field charts compute min/max from member CERTs, not composite values.

## Composite CERTs

- Standard: `core_pb=90001`, `all_peers=90003`
- Normalized: `core_pb=90004`, `all_peers=90006`
- Legacy (90002, 90005, 99998, 99999) — scatter-dot exclusion only, never as selectors.
