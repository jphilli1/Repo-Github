# Migration Summary — CLAUDE.md Modularization (2026-03-14)

## Section → File Mapping

| Old Monolith Section | New File | Lines (approx) |
|---|---|---|
| §1 Project Overview (lines 1–30) | `01-project-overview.md` | Module table, pipeline summary |
| §2 Build & Execution Commands (lines 33–87) | `02-build-run-config.md` | Run commands, env vars, dependencies |
| §3 Project Architecture & Output Routing (lines 90–247) | `03-output-routing-and-logging.md` | Data flow, sheet layout, file naming, CSV logging |
| §3a Dual-Mode Rendering Architecture (lines 249–448) | `04-rendering-architecture.md` | Dual-mode, manifest, canonical rules, risks |
| §3a Executive Chart subsections + §10 FRED Expansion (lines 352–448, 2451–2555) | `05-executive-and-macro-artifacts.md` | Executive charts, KRI, macro overlays, FRED expansion registry |
| §6 Normalization + §4 Peer Groups + §9 Validation + §11 HUD (lines 726–866, 490–503, 2372–2448, 2558–2686) | `06-normalization-and-peer-groups.md` | Exclusion stack, composites, peer groups, validation engine, HUD enrichment |
| §13 Local Macro Pipeline (lines 2813–2983) | `07-local-macro.md` | Geography spine, BEA/BLS/Census APIs, per-capita math |
| §12 Corp-Safe Overlay (lines 2689–2810) | `08-corp-overlay.md` | Standalone contract, runner, limitations |
| §5 Common Errors + §8 To-Do (lines 678–724, 2358–2370) | `09-troubleshooting.md` | DNS fixes, missing keys, known issues |
| §4 Strict Coding Conventions (lines 450–675) | `10-coding-rules.md` | No-hardcoding, import safety, chart palette, label policy, table distinction |
| §7 Changelog (lines 868–2355) | `99-changelog.md` | All dated change entries (history only) |

## Duplicated Content Removed

| Content | Was In | Now In | Action |
|---|---|---|---|
| Peer group CERT table (90001/90003/90004/90006) | §4 and §6 | `06-normalization-and-peer-groups.md` only | Removed from `10-coding-rules.md`; `10` references composites via SCATTER & CHART COMPOSITE HANDLING but peer group definition is in `06` |
| Coverage vs Share label rule | §4 and §7 changelog entries | `10-coding-rules.md` (active rule) + `99-changelog.md` (history) | Changelog retains dated context; active rule is authoritative |
| Normalization diagnostics columns | §6, §7, and §9 | `06-normalization-and-peer-groups.md` | Consolidated validation + diagnostics in one file |
| FRED series list (13 macro chart series) | §3a and §10 | `05-executive-and-macro-artifacts.md` | Single location for both chart specs and series registry |

## Content Intentionally Kept in Multiple Places

| Content | Files | Reason |
|---|---|---|
| Composite CERT numbers (90001/90003/90004/90006) | `06` (peer group definitions), `10` (scatter/chart handling rules) | Different contexts: `06` defines the groups; `10` prescribes how charts must use them |
| `REPORT_MODE` / `REPORT_RENDER_MODE` env vars | `02` (env var table), `04` (mode selection priority) | `02` is the reference table; `04` is the rendering-specific context |
| `DashboardConfig` HUD token access rules | `10` (coding rule) | Single location — referenced from subtree files |

## New Files Created

- `docs/claude/_archive/CLAUDE.monolith.2026-03-14.md` — exact copy of pre-split monolith
- `docs/claude/01-project-overview.md` through `99-changelog.md` — 11 topic files
- `src/reporting/CLAUDE.md` — subtree memory for report generation
- `src/local_macro/CLAUDE.md` — subtree memory for local macro pipeline
- `tests/CLAUDE.md` — subtree memory for regression tests
- `CLAUDE.md` — thin index with `@path` imports (17 lines)
- `docs/claude/MIGRATION_SUMMARY.md` — this file

## Validation

- [x] All `@path` imports in root `CLAUDE.md` point to existing files
- [x] Root file is 17 lines (thin, non-duplicative)
- [x] Archive file is byte-identical to pre-split monolith (`diff` returns empty)
- [x] No material content from the monolith was lost — all sections mapped
- [x] No Python modules were moved (doc-only split)
