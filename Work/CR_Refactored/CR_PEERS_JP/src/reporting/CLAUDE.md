# src/reporting — Reporting Subtree

This directory contains report generation modules for the CR_PEERS_JP pipeline.

## Key Rules

- **Rendering abstractions** live in `rendering_mode.py` only. `report_generator.py` must import, never redefine.
- **All charts** must use `CHART_PALETTE` and `resolve_display_label()` — see `@docs/claude/10-coding-rules.md`.
- **Artifact guards**: every artifact production block must be wrapped in `should_produce()`.
- **Dual-mode**: `full_local` (all artifacts) vs `corp_safe` (HTML tables only) — see `@docs/claude/04-rendering-architecture.md`.
- **Executive charts** are in `executive_charts.py`, integrated in Phase 8 — see `@docs/claude/05-executive-and-macro-artifacts.md`.
- **Composite CERTs**: Standard 90001/90003, Normalized 90004/90006. Never use legacy 99998/99999/90002/90005.

## Reference

@docs/claude/04-rendering-architecture.md
@docs/claude/05-executive-and-macro-artifacts.md
@docs/claude/10-coding-rules.md
