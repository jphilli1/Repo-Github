# CLAUDE.md Split Plan

**Date:** 2026-03-14
**Status:** Planning only — no split files created yet

---

## Pre-Split Findings

### File Inventory

| File | Location | Lines | Size | Status |
|---|---|---|---|---|
| `CLAUDE.md` | Project root | 23 | 819 B | Active — lightweight stub with `@docs/claude/` references |
| `CLAUDE - OLD.md` | Project root | 3,018 | 233 KB | Old monolith — full project memory with all sections and changelog |

### Archive Copies

| Archive File | Source | Status |
|---|---|---|
| `docs/claude/_archive/CLAUDE.current.pre-split.md` | `CLAUDE.md` (23-line stub) | Created |
| `docs/claude/_archive/CLAUDE.old.monolith.md` | `CLAUDE - OLD.md` (3,018-line monolith) | Created |

### Filename Issues

- Root file was already named `CLAUDE.md` (not bare `CLAUDE`). No rename needed.
- The current `CLAUDE.md` is already a lightweight stub pointing to `docs/claude/` — it references the target split files but those files do not yet exist.
- `CLAUDE - OLD.md` contains the **actual content** that needs to be split. This is the primary source material.

---

## Source Section Inventory (`CLAUDE - OLD.md` — 13 top-level sections)

| # | Section Header (H2) | Line Range | ~Lines | Topic |
|---|---|---|---|---|
| 1 | Project Overview | 1–31 | 31 | Mission, script/module table, data flow summary |
| 2 | Build & Execution Commands | 33–88 | 56 | venv, pip, CLI, env vars, deps |
| 3 | Project Architecture & Output Routing | 90–247 | 158 | Data flow diagram, Excel sheets, output naming, CSV logging |
| 3a | Dual-Mode Rendering Architecture | 249–448 | 200 | RenderMode, capability matrix, executive charts (16), macro charts (3), risks |
| 4 | Strict Coding Conventions & Rules | 450–676 | 227 | FDIC API, no hardcoding, import safety, composites, peer groups, palette, labels |
| 5 | Common Errors & Troubleshooting | 678–724 | 47 | DNS/proxy, missing keys, missing Excel |
| 6 | Normalization Conventions | 726–866 | 141 | Top-down normalization, exclusion stack, balance-gating, segments, audit flags |
| 7 | Changelog / Recent Fixes | 868–1997 | 1,130 | ~37% of file — 30+ changelog entries (2026-03-10 to 2026-03-14) |
| 8 | (within Section 7 tail / post-changelog) | ~1998–2370 | — | Output naming/logging overhaul continuation, To-Do items |
| 9 | Metric Registry & Validation Architecture | 2372–2449 | 78 | MetricSpec, validation engine, semantic rules |
| 10 | FRED Expansion Layer | 2451–2590 | 140 | Registry architecture, transforms, spreads, regime flags, sheets |
| 11 | HUD USPS ZIP Crosswalk | 2592–2720 | 129 | County FIPS mapping, HUD API, parsing, enrichment status codes |
| 12 | Corp-Safe Overlay Architecture | 2722–2844 | 123 | corp_overlay.py standalone, 4 artifacts, MSA panel (superseded) |
| 13 | Local Macro Pipeline Architecture | 2847–3017 | 171 | local_macro.py, geography spine, BEA/BLS/Census, board columns |

---

## Target File Structure & Migration Map

### `docs/claude/01-project-overview.md`

**Source sections:**
- Section 1: Project Overview (lines 1–31) — mission statement, script/module table
- Brief cross-references to 02 (build/config) and 03 (output routing)

**Content:** One-paragraph mission, the script-role table (MSPBNA_CR_Normalized.py through run_pipeline.py), and a pointer to detailed docs.

**Overlap:** The script table partially duplicates module descriptions in sections 10, 12, 13. **Decision: keep summary table in 01, detailed architecture in respective files.**

---

### `docs/claude/02-build-run-config.md`

**Source sections:**
- Section 2: Build & Execution Commands (lines 33–88) — full copy
  - PowerShell activation, pip install, CLI examples
  - Full env var table (17 variables)
  - Env var alias resolution priority
  - Key dependencies list
- Section 4 subsets:
  - `NO HARDCODING` (lines 462–467) — runtime config concern
  - `IMPORT SAFETY` (lines 468–476) — module-level safety rules

**Overlap:** Env var table was mentioned briefly in Section 1. **Decision: full table lives in 02 only; 01 cross-references.**

---

### `docs/claude/03-output-routing-and-logging.md`

**Source sections:**
- Section 3: Project Architecture & Output Routing (lines 90–247)
  - Data flow diagram (FDIC/FRED → MSPBNA → Excel → report_generator → charts/scatter/tables)
  - Excel sheet layout table (20+ sheets)
  - Output file naming convention (date-only YYYYMMDD, ~40 artifact filenames)
  - CSV structured logging (15-column schema, event types, TeeToLogger lifecycle)

**Overlap:** Excel sheet table overlaps with sections 10 (FRED sheets), 11 (Case-Shiller sheets), 13 (local macro sheets). **Decision: master sheet table stays in 03; domain-specific files document only their own sheets with cross-references back.**

---

### `docs/claude/04-rendering-architecture.md`

**Source sections:**
- Section 3a: Dual-Mode Rendering Architecture (lines 249–352 + selected content from 353–448)
  - RenderMode enum, mode selection priority (4-level)
  - Capability matrix (BOTH vs FULL_LOCAL_ONLY)
  - Artifact skip semantics (mode-based + preflight suppression)
  - ArtifactManifest tracking
  - CLI usage for mode selection
  - Adding new artifacts pattern
  - Canonical rendering abstraction rule (rendering_mode.py is single source)

**What moves OUT to 05:** Executive chart artifact tables, macro chart artifact tables, remaining risks.

**Overlap with 05:** Artifact tables appear in both source subsections. **Decision: 04 owns the rendering architecture (how artifacts are gated/manifested). 05 owns the artifact catalog (what each artifact produces, metrics, files).**

---

### `docs/claude/05-executive-and-macro-artifacts.md`

**Source sections:**
- Section 3a subset: Executive Chart Artifacts table (lines 353–420) — 16 artifacts
  - Heatmaps (4), KRI bullets (4), sparklines (4), bookwide growth (1), cumulative growth (2), growth_vs_deterioration_bookwide (1)
  - Football-field KRI design (nested bands, avg markers, legend)
  - Sparkline metric lists (standard 7, normalized 5)
  - Unit family separation (rates vs multiples vs composition)
  - Comparator fallback behavior
  - Cumulative growth chart family (Target Loans vs CRE ACL)
- Section 3a subset: Deterministic Macro Chart Artifacts (lines 422–448) — 3 artifacts
  - macro_corr_heatmap_lag1, macro_overlay_credit_stress, macro_overlay_rates_housing
  - 13 required FRED series with categories
  - Z-scoring, quarterly alignment
- Section 3a subset: Remaining Risks (lines 439–448)
- Section 10 subset: First-Wave Charts (lines 2571–2579) — FRED expansion chart specs

**Overlap with 04:** Artifact tables. **Decision: 05 is the artifact catalog; 04 keeps only the rendering gate pattern.**

---

### `docs/claude/06-normalization-and-peer-groups.md`

**Source sections:**
- Section 6: Normalization Conventions (lines 726–866)
  - Top-down normalization with over-exclusion detection
  - Normalized ratio components
  - Supported exclusion stack (7 categories table)
  - Balance-gating for excluded NCO (6 categories)
  - Structured audit flags (5 flags)
  - Normalized composite minimum coverage (50% threshold)
  - Case-Shiller ZIP enrichment (brief, cross-ref to HUD section)
  - IDB label convention
  - Curated presentation tabs
  - Display label policy
  - Metric role classification
  - Norm_Provision_Rate treatment
  - Normalized segment taxonomy (Wealth Resi, C&I, NDFI, Ag, ADC, CRE, Tailored)
  - Known limitations (segment taxonomy)
  - Segment support boundaries table
- Section 4 subsets:
  - PEER GROUPINGS (lines 490–506) — 4 peer groups, composite CERTs
  - MS COMBINED ENTITY (lines 508–511)
  - SCATTER & CHART COMPOSITE HANDLING (lines 478–492)
  - COVERAGE vs SHARE vs x-MULTIPLE LABEL RULE (lines 513–524)
  - `_METRIC_FORMAT_TYPE` MAINTENANCE RULE (lines 525–533)

**This is the analytical core** — normalization math, peer group definitions, composite handling, and segment taxonomy.

**Overlap:** Peer group CERTs referenced in 04 and 10. **Decision: 06 is the canonical source for peer group definitions. Others cross-reference.**

---

### `docs/claude/07-local-macro.md`

**Source sections:**
- Section 13: Local Macro Pipeline Architecture (lines 2847–3017)
  - Overview (local_macro.py as dedicated module)
  - Geography spine (4-tier hierarchy)
  - Data sources (BEA, BLS, Census, HUD)
  - Output sheets (7 sheets including skip audit)
  - Board columns (33-column schema — BOARD_COLUMNS)
  - Source metadata policy
  - Integration point (MSPBNA_CR_Normalized.py call pattern)
  - Transformation policy registry (gdp/unemployment/population/hpi)
  - Per-capita normalization formulas + hard rules
  - Helper functions table

**Self-contained** — local_macro.py has its own architecture.

---

### `docs/claude/08-corp-overlay.md`

**Source sections:**
- Section 12: Corp-Safe Overlay Architecture (lines 2722–2844)
  - Overview (standalone module, not integrated)
  - Data flow diagram
  - Input contract (loan file — required + optional columns)
  - Optional enrichment hooks (Census, BEA, Case-Shiller)
  - CLI usage (corp_overlay_runner.py)
  - Artifact details (4 artifacts)
  - MSA-level macro panel (superseded by workbook-driven path)
  - Known limitations

**Self-contained** — corp_overlay.py is standalone by design.

---

### `docs/claude/09-troubleshooting.md`

**Source sections:**
- Section 5: Common Errors & Troubleshooting (lines 678–724)
  - DNS/Proxy error (`getaddrinfo failed`)
  - Missing FRED_API_KEY
  - No Excel file found
  - Missing sheet in Excel
- Section 3a subset: Remaining Risks (lines 439–448) — duplicated intentionally from 05
- To-Do / Known Issues (post-changelog, ~lines 2358–2370)
  - Normalized profitability metrics status

**Overlap:** Remaining Risks also in 04/05. **Decision: duplicate intentionally — different audience (operators vs developers).**

---

### `docs/claude/10-coding-rules.md`

**Source sections:**
- Section 4: Strict Coding Conventions & Rules (lines 450–676) — most subsections
  - ALWAYS UPDATE CLAUDE.md
  - FDIC API Variables & FFIEC Waterfall
  - NO HARDCODING (cross-ref to 02)
  - IMPORT SAFETY (cross-ref to 02)
  - DEPRECATED FUNCTIONS
  - HUD TOKEN — ATTRIBUTE-ONLY ACCESS
  - CENTRALIZED CHART PALETTE (CHART_PALETTE) + color table
  - CHART ANNOTATION / ANTI-OVERLAP POLICY
  - WEALTH PEERS COMPARATOR INCLUSION POLICY
  - CSS CLASS NAMING
  - ENTITY DISPLAY LABEL POLICY + ticker map
  - CHART METRICS (standard/normalized bar/line assignments)
  - FRED Deduplication
  - FRED Frequency Inference
  - FRED Series Validation
  - STOCK vs FLOW MATH CONVENTION
  - WEALTH-FOCUSED vs DETAILED TABLE DISTINCTION
- Section 9: Metric Registry & Validation Architecture (lines 2372–2449)
  - MetricSpec dataclass, validation engine, 6 semantic rules, preflight integration
- Section 10: FRED Expansion Layer (lines 2451–2590)
  - FREDSeriesSpec registry, priority levels, 4 modules (SBL/Resi/CRE/Case-Shiller)
  - Transforms, named spreads, regime flags
  - Excel output sheets, validation checks
- Section 11: HUD USPS ZIP Crosswalk (lines 2592–2720)
  - County-level FIPS mapping, HUD API token setup
  - HUD response parsing (two-pass), canonical fields
  - Enrichment status codes (10 codes)
  - Request hardening, misclassification prevention

**Note:** This is the largest target file. FRED (Section 10) and HUD (Section 11) could be separate files, but they are reference material used primarily when editing those subsystems. **Decision: keep in 10 for now** to avoid excessive fragmentation.

---

### `docs/claude/99-changelog.md`

**Source sections:**
- Section 7: Changelog / Recent Fixes (lines 868–1997+) — **~1,130+ lines, ~37% of file**
  - 30+ dated changelog entries from 2026-03-10 to 2026-03-14
  - Includes: Final Hardening, Reconciliation, Post-Fix Regressions, MSA Macro Panel, Board-Ready Output, Math Correctness, Geography Spine, Architecture Reconciliation, Pipeline Runner, FRED Series Audit, Capital Concentration, Cross-Regime NaN-Out, Cumulative Growth, Formatting Sweep, Chart Package Expansion, Palette/Annotation/Wealth Peers, KRI Split, Rendering Reconciliation, HUD HTTP Diagnostics, Corp-Safe Overlay, Macro Chart Tranche, Executive Chart Tranche, Merge Conflict Resolution, Segment Taxonomy, Exclusion Engine, Dual-Mode Rendering, HUD Two-Pass Flatten, HUD Parsing, Consistency Pass, ACL Semantics, DashboardConfig Fix, Coverage/Share/HUD Token, Output Quality, FRED Frequency Inference, Final Cleanup, Presentation Cleanup, Logger Lifecycle, Output Naming/Logging

**Rationale:** The changelog is the single largest section and grows with every commit. Isolating it keeps the active docs lean.

---

## Sections That Should Remain Duplicated Intentionally

| Content | Primary Location | Also In | Reason |
|---|---|---|---|
| Peer group CERT table (90001/90003/90004/90006) | `06-normalization-and-peer-groups.md` | `10-coding-rules.md` (brief), `04-rendering-architecture.md` (brief) | CERTs are referenced in 3 contexts; cross-refs too indirect |
| Remaining Risks list | `05-executive-and-macro-artifacts.md` | `09-troubleshooting.md` | Different audiences: developers vs operators |
| "ALWAYS UPDATE CLAUDE.md" rule | `10-coding-rules.md` | Root `CLAUDE.md` stub | Must be visible in the root file agents read first |
| Env var table | `02-build-run-config.md` | (nowhere else — single source) | Was duplicated in overview; removing that |

---

## Sections That Overlap and Need Deduplication

| Overlapping Content | Locations in Source | Resolution |
|---|---|---|
| Script/module table | Section 1 (summary), Sections 10/12/13 (detailed) | Keep summary in 01, detailed in respective files |
| Env var table | Section 1 (brief mention), Section 2 (full table) | Full table only in 02; pointer from 01 |
| Excel sheet table | Section 3 (master), Sections 10/11/13 (domain sheets) | Master stays in 03; domain files document only their own sheets |
| Executive chart artifact specs | Section 3a (within rendering arch) | Move to 05; 04 keeps only the rendering pattern |
| Peer group CERTs | Sections 4, 6, 3a | Canonical in 06; others cross-reference |
| Composite handling rules | Section 4 (coding rules), Section 6 (normalization) | Canonical in 06; 10 has cross-ref |
| FRED series lists | Section 3a (macro charts), Section 10 (expansion layer) | Merge into 05 (artifact specs) and 10 (coding rules/architecture) |

---

## Ambiguous Content and Resolutions

| Content | Ambiguity | Resolution |
|---|---|---|
| Section 3a "Adding New Artifacts" (lines 327–340) | Coding rule or rendering architecture? | **04** — it's a rendering-specific workflow |
| Section 3a "Canonical Rendering Abstraction Rule" (lines 341–352) | Coding rule or architecture? | **04** — defines the import contract for rendering_mode.py |
| Section 4 "CHART METRICS" (lines 614–620) | Coding rule or artifact spec? | **10** — it's a coding convention for which metrics to use |
| Section 4 "WEALTH PEERS COMPARATOR INCLUSION POLICY" (lines 576–588) | Coding rule or artifact spec? | **10** — it's a policy for chart developers to follow |
| Section 6 "Case-Shiller ZIP Enrichment" (lines 794–797) | Normalization or HUD/Case-Shiller? | **06** (brief mention stays), **10** has the detailed HUD/CS architecture |
| To-Do / Known Issues (post-changelog) | Troubleshooting or changelog? | **09** — it's operational status |
| Section 12 "MSA-Level Macro Panel" (lines 2805–2834) | Corp overlay or local macro? | **08** — functions live in corp_overlay.py (superseded but retained) |

---

## Content Source Priority

Since `CLAUDE - OLD.md` (3,018 lines) contains all the actual project documentation and `CLAUDE.md` (23 lines) is already a stub, the split will:

1. **Use `CLAUDE - OLD.md` as the primary source** for all split file content
2. **Preserve the current `CLAUDE.md` stub structure** — the `@docs/claude/` reference pattern is already correct
3. **Add the "Global rules" section from current `CLAUDE.md`** into both the root stub and `10-coding-rules.md`

---

## Root `CLAUDE.md` Post-Split Stub (Planned)

The current root `CLAUDE.md` is already a lightweight stub with the correct structure. After splitting, it will be updated to:

1. Keep the one-line project title
2. Keep the `@docs/claude/` reference list (already present)
3. Keep the "Global rules" section (already present)
4. Add a note that split files contain the full documentation
5. Ensure "ALWAYS UPDATE docs/claude/ files" rule is prominent

**Estimated size:** ~30–50 lines. The current 23-line stub is close to the target already.

---

## Validation Checklist (Pre-Split)

- [x] `CLAUDE.md` exists at project root (23-line stub)
- [x] `CLAUDE - OLD.md` exists at project root (3,018-line monolith — primary source)
- [x] `docs/claude/_archive/CLAUDE.current.pre-split.md` exists (copy of 23-line stub)
- [x] `docs/claude/_archive/CLAUDE.old.monolith.md` exists (copy of 3,018-line monolith)
- [x] `docs/claude/SPLIT_PLAN.md` exists (this file)
- [x] No Python code moved
- [x] No split files created yet
- [x] No root `CLAUDE.md` replaced yet
