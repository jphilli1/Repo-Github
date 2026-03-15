# Memory Structure Audit — Post-Split Hardening

Audit date: 2026-03-14
Auditor: Claude (Step 3 of 3-step memory reorganization)
Source: `CLAUDE - OLD.md` (3,018-line monolith) → 11 topic files + 3 subtree files + thin root index

---

## File-by-File Purpose

| File | Lines | Purpose |
|---|---|---|
| `CLAUDE.md` (root) | 29 | Thin index: @path imports to all 11 topic files + 9 global rules |
| `docs/claude/01-project-overview.md` | 35 | Mission statement, 14-module table, cross-references |
| `docs/claude/02-build-run-config.md` | 71 | Build commands, 15 env vars, alias resolution, import safety, no-hardcoding rules |
| `docs/claude/03-output-routing-and-logging.md` | 161 | Data flow, 20+ Excel sheets, ~45 output filenames, CSV 15-column schema, logging lifecycle |
| `docs/claude/04-rendering-architecture.md` | 105 | RenderMode enum, mode selection, capability matrix, artifact manifest, adding artifacts, canonical rule |
| `docs/claude/05-executive-and-macro-artifacts.md` | 144 | 16 executive chart artifacts, KRI design spec, 3 macro chart artifacts, FRED expansion charts |
| `docs/claude/06-normalization-and-peer-groups.md` | 178 | 4 peer groups, composite CERTs, coverage/share label rule, exclusion stack, segment taxonomy |
| `docs/claude/07-local-macro.md` | 170 | Geography spine, 4 data sources, 7 output sheets, 33 BOARD_COLUMNS, transformation policy, per-capita math |
| `docs/claude/08-corp-overlay.md` | 100 | Standalone module, data flow, input contract, CLI, 4 artifacts, reduced-mode behavior |
| `docs/claude/09-troubleshooting.md` | 64 | 4 common errors, 6 remaining risks, known issues |
| `docs/claude/10-coding-rules.md` | 355 | FDIC/FFIEC, HUD token, palette, annotation, entity labels, FRED rules, metric registry, HUD crosswalk |
| `docs/claude/99-changelog.md` | 343 | 38 dated entries (2026-03-11 to 2026-03-14), history only |
| `src/reporting/CLAUDE.md` | 33 | Scoped: artifact contracts, unit-label rules, chart rules, composite CERTs |
| `src/local_macro/CLAUDE.md` | 37 | Scoped: geography spine, GDP math hard rules, transformation, completeness, integration |
| `tests/CLAUDE.md` | 31 | Scoped: no synthetic data, regression expectations, doc-update tests, test organization |

**Total topic file lines:** ~1,726 (vs 3,018 monolith — 43% reduction through changelog condensation and deduplication)

---

## Import Path Validation

All `@path` references verified:

| Source File | Reference | Resolves? |
|---|---|---|
| Root CLAUDE.md | 11 `@docs/claude/*.md` imports | Yes (all 11) |
| 01-project-overview.md | `@docs/claude/02-build-run-config.md` | Yes |
| 01-project-overview.md | `@docs/claude/03-output-routing-and-logging.md` | Yes |
| 01-project-overview.md | `@docs/claude/04-rendering-architecture.md` | Yes |
| 06-normalization-and-peer-groups.md | `@docs/claude/10-coding-rules.md` | Yes |
| 08-corp-overlay.md | `@docs/claude/07-local-macro.md` | Yes |
| 10-coding-rules.md | `@docs/claude/99-changelog.md` | Yes |

---

## Deduplication Analysis

### Intentional Repetitions (Kept)

These rules appear in multiple files by design — each occurrence serves a different scope:

| Rule | Locations | Rationale |
|---|---|---|
| `rendering_mode.py` is canonical source | Root (global rule), 04 (detail), `src/reporting/CLAUDE.md` (scoped reminder) | Safety-critical architecture constraint |
| Coverage vs Share label rule | Root (pointer), 06 (canonical rule), `src/reporting/CLAUDE.md` (scoped reminder) | Non-negotiable semantic rule |
| Never divide growth rate by population | 07 (detail), `src/local_macro/CLAUDE.md` (scoped reminder) | Safety-critical math rule |
| 4-tier geography spine | 07 (full detail), `src/local_macro/CLAUDE.md` (abbreviated) | Core architectural concept |
| Composite CERTs (90001/90003/90004/90006) | 06 (full detail), `src/reporting/CLAUDE.md` (quick reference) | Frequently needed by chart code |
| report_generator never imports local_macro | 07 (integration section), `src/local_macro/CLAUDE.md` (integration) | Integration contract |
| MSA_Crosswalk_Audit always produced | 07 (output sheets), `src/local_macro/CLAUDE.md` (reminder) | Output contract |
| No synthetic production data | Root (global rule), `tests/CLAUDE.md` (test enforcement) | Foundational safety rule |
| Always update docs/claude/ | Root (global rule), 10 (coding convention) | Workflow rule |

### Minor Overlap (Accepted)

| Overlap | Files | Decision |
|---|---|---|
| IDB → MSPBNA naming | 06 (dictionary keys + CSS), 10 (CSS + filenames) | Complementary — 06 covers dict keys, 10 covers CSS/filenames. Overlap is ~2 lines. |

### What Was Deduplicated (From Monolith)

- Changelog entries condensed from ~800 lines to 343 lines (key facts only, removed verbose diffs)
- Removed redundant section preambles and repeated cross-references
- Consolidated scattered env var mentions into single table in 02
- Unified Excel sheet documentation into single table in 03 (was spread across multiple monolith sections)

---

## Changelog Audit (99-changelog.md)

**Status: Clean — history only.**

All 38 entries are dated, past-tense summaries of what changed. No active operating rules, no instructions, no "always do X" directives found. The "Final architecture contract" entry (2026-03-14) documents a past decision, not a current instruction.

---

## Subtree File Scope Audit

| File | Scope | Sharp? | Notes |
|---|---|---|---|
| `src/reporting/CLAUDE.md` | report_generator.py, rendering_mode.py, executive_charts.py | Yes | Contains only rules an agent editing reporting code would need. Cross-references parent docs for detail. |
| `src/local_macro/CLAUDE.md` | local_macro.py | Yes | Contains only rules an agent editing local_macro code would need. Abbreviates 07's full detail appropriately. |
| `tests/CLAUDE.md` | test_regression.py | Yes | Contains only test authoring rules. No code architecture details. |

**Note:** All three subtree files note that the actual Python modules "currently live at the repo root." These notes should be removed when modules are moved to their target directories.

---

## Root CLAUDE.md Weight Check

**Status: Lightweight (29 lines).**

- 1-line description
- 11 @path imports
- 9 global rules (all one-liners)
- No detailed specifications, no tables, no code blocks

No re-accumulation risk.

---

## Contradictions Check

No contradictions found between any file pairs. Verified:

- Sheet counts (6 regular + 1 conditional) consistent between 07 and subtree
- Composite CERT numbers consistent between 06 and subtree
- Architecture contracts (no cross-imports) consistent across 07, 08, subtree files
- Unit family separation in 05 consistent with _METRIC_FORMAT_TYPE in 06
- Env var names consistent between 02 and 07

---

## Unresolved Ambiguities

1. **10-coding-rules.md is the largest file (355 lines).** It covers 8+ distinct technical domains (FDIC, HUD, FRED, metrics, charts, crosswalk). A future split into sub-files (e.g., `10a-fdic-fred-rules.md`, `10b-chart-design-rules.md`, `10c-hud-crosswalk.md`) could improve scoped loading, but is not urgent.

2. **metric_registry.py and metric_semantics.py** are documented in 10-coding-rules.md but have no subtree CLAUDE.md. If these modules move to `src/metrics/`, a scoped subtree file would be warranted.

3. **FRED expansion layer** documentation in 10 (lines 180-261) is substantial. If `fred_series_registry.py`, `fred_transforms.py`, `fred_ingestion_engine.py`, and `fred_case_shiller_discovery.py` move to `src/fred/`, a dedicated subtree file would help.

---

## Recommendations for Module Folder Reorganization

When moving Python modules to subdirectories:

1. **Remove "Note: This module currently lives at the repo root"** from all three subtree CLAUDE.md files.

2. **Proposed directory structure:**
   ```
   src/
     reporting/       ← report_generator.py, rendering_mode.py, executive_charts.py
     local_macro/     ← local_macro.py
     metrics/         ← metric_registry.py, metric_semantics.py
     fred/            ← fred_series_registry.py, fred_transforms.py, fred_ingestion_engine.py, fred_case_shiller_discovery.py
     hud/             ← case_shiller_zip_mapper.py
     overlay/         ← corp_overlay.py, corp_overlay_runner.py
     logging/         ← logging_utils.py
   tests/             ← test_regression.py
   ```

3. **New subtree CLAUDE.md files to create** when modules move:
   - `src/metrics/CLAUDE.md` — MetricSpec, MetricSemantic, validation engine rules
   - `src/fred/CLAUDE.md` — FRED dedup, frequency inference, series validation, expansion layer
   - `src/hud/CLAUDE.md` — HUD token access, response parsing, canonical fields, enrichment status codes

4. **Consider splitting 10-coding-rules.md** once modules are reorganized — the content naturally segments by module group.

5. **Update all @path references** if docs/claude/ files are renamed or restructured.

6. **Update README.md** memory file layout diagram to reflect new directory structure.

7. **Run test_regression.py** after any module moves to verify import paths.
