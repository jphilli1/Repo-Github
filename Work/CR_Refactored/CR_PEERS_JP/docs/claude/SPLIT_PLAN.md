# CLAUDE.md Split Plan

**Date:** 2026-03-14
**Source file:** `CLAUDE.md` (2,982 lines, ~227 KB)
**Status:** Planning only — no split files created yet

---

## Pre-Split Findings

### File Inventory

| File | Location | Status |
|---|---|---|
| `CLAUDE.md` | Project root (`Work/CR_Refactored/CR_PEERS_JP/`) | Active — 2,982 lines |
| `CLAUDE - OLD.md` | (expected) | **Not found** — does not exist in repo or git history |

**Resolution:** Only one source file exists. Both archive copies (`CLAUDE.current.pre-split.md` and `CLAUDE.old.monolith.md`) are identical snapshots of the current `CLAUDE.md`. When/if the old monolith surfaces later, it can be placed in `_archive/` manually.

### Filename Issues

- Root file was already named `CLAUDE.md` (not bare `CLAUDE`). No rename needed.
- No other `CLAUDE*` files exist anywhere in the repo.

---

## Source Section Inventory (13 top-level sections)

| # | Section Header (H2) | Line Range | ~Lines | Topic |
|---|---|---|---|---|
| 1 | Project Overview | 3–31 | 29 | Mission, script table, data flow summary |
| 2 | Build & Execution Commands | 33–88 | 56 | venv, pip, CLI, env vars, deps |
| 3 | Project Architecture & Output Routing | 90–247 | 158 | Data flow, Excel sheets, output naming, CSV logging |
| 3a | Dual-Mode Rendering Architecture | 249–448 | 200 | RenderMode, artifacts, capability matrix, executive charts, macro charts, risks |
| 4 | Strict Coding Conventions & Rules | 450–676 | 227 | FDIC API, no hardcoding, import safety, composites, peer groups, palette, labels |
| 5 | Common Errors & Troubleshooting | 678–724 | 47 | DNS/proxy, missing keys, missing Excel |
| 6 | Normalization Conventions | 726–866 | 141 | Top-down normalization, exclusion stack, balance-gating, audit flags, segments |
| 7 | Changelog / Recent Fixes | 868–2356 | 1,489 | **~50% of file** — 40+ changelog entries from 2026-03-10 to 2026-03-14 |
| 8 | To-Do / Known Issues | 2358–2370 | 13 | Norm profitability metrics status |
| 9 | Metric Registry & Validation Architecture | 2372–2449 | 78 | MetricSpec, validation engine, semantic rules, preflight |
| 10 | FRED Expansion Layer | 2451–2556 | 106 | Registry architecture, transforms, spreads, regime flags, sheets |
| 11 | HUD USPS ZIP Crosswalk | 2558–2687 | 130 | County FIPS mapping, HUD API, parsing, enrichment status codes |
| 12 | Corp-Safe Overlay Architecture | 2689–2811 | 123 | corp_overlay.py standalone workflow, 4 artifacts, MSA panel |
| 13 | Local Macro Pipeline Architecture | 2813–2982 | 170 | local_macro.py, geography spine, BEA/BLS/Census, board columns |

---

## Target File Structure & Migration Map

### `docs/claude/01-project-overview.md`

**Source sections:**
- Section 1: Project Overview (lines 1–31)
- Section 2: Build & Execution Commands (lines 33–88) — env vars, key deps, CLI

**Rationale:** Overview + how-to-run belong together as the entry point.

**Overlap/dedup:** The script table in Section 1 partially duplicates the module tables in sections 10, 12, 13. Keep both — Section 1 is a summary, others are detailed.

### `docs/claude/02-build-run-config.md`

**Source sections:**
- Section 2: Build & Execution Commands (lines 33–88) — full copy
- Section 4 subset: `IMPORT SAFETY` (lines 468–473), `NO HARDCODING` (lines 462–467)

**Rationale:** All build/run/config material grouped. Import safety and no-hardcoding rules are runtime configuration concerns.

**Overlap note:** Section 2 env var table also appears in `01-project-overview.md`. **Decision: keep env vars in 02 only.** 01 gets a brief "see 02-build-run-config.md for env vars" pointer. This avoids the primary source of future drift.

**Revised 01 content:** Section 1 (project overview + script table) only, with a cross-reference to 02.

### `docs/claude/03-output-routing-and-logging.md`

**Source sections:**
- Section 3: Project Architecture & Output Routing (lines 90–247)
  - Data Flow diagram
  - Excel Sheet Layout table
  - Output File Naming convention
  - CSV Structured Logging (schema, event types, lifecycle)

**Rationale:** All output routing, naming, sheet layout, and logging in one place.

**Overlap:** The Excel sheet table overlaps with section 10 (FRED sheets), section 11 (Case-Shiller sheets), section 13 (local macro sheets). **Decision: keep the master sheet table in 03, with cross-references to detailed docs in 10/11/13 equivalents.** Individual sheet details stay in their domain files.

### `docs/claude/04-rendering-architecture.md`

**Source sections:**
- Section 3a: Dual-Mode Rendering Architecture (lines 249–448)
  - RenderMode enum, mode selection priority
  - Capability matrix, artifact skip semantics
  - ArtifactManifest
  - CLI usage for modes
  - Adding new artifacts
  - Canonical rendering abstraction rule
  - Executive chart artifacts table (16 artifacts)
  - Deterministic macro chart artifacts (3 artifacts)
  - Remaining risks

**Rationale:** All rendering/artifact concerns are self-contained in 3a.

**Overlap:** Executive chart details also touch sections 9 (metric registry consumers) and the changelog. **Decision: keep artifact specs in 04.** Metric registry stays in its own file.

### `docs/claude/05-executive-and-macro-artifacts.md`

**Source sections:**
- Section 3a subset: Executive Chart Artifacts table (lines 353–420)
- Section 3a subset: Deterministic Macro Chart Artifacts (lines 422–447)
- Section 3a subset: Remaining Risks (lines 439–447)
- Section 10 subset: First-Wave Charts (lines 2537–2556) — FRED expansion chart specs

**Rationale:** All artifact specification (what charts exist, their metrics, their files) in one reference doc.

**Overlap with 04:** The artifact tables appear in both. **Decision: 04 keeps the rendering architecture (how artifacts are gated/manifested). 05 keeps the artifact catalog (what each artifact does, its metrics, its files).** The executive chart table moves from 04 to 05; 04 retains only the pattern for adding new artifacts and the capability matrix.

### `docs/claude/06-normalization-and-peer-groups.md`

**Source sections:**
- Section 6: Normalization Conventions (lines 726–866)
  - Top-down normalization, over-exclusion detection
  - Normalized ratio components
  - Supported exclusion stack (7 categories)
  - Balance-gating (6 categories)
  - Structured audit flags
  - Normalized composite minimum coverage
  - Case-Shiller ZIP enrichment (brief — detailed in 11)
  - IDB label convention
  - Curated presentation tabs
  - Display label policy
  - Metric role classification
  - Norm_Provision_Rate treatment
  - Normalized segment taxonomy
  - Known limitations (segment taxonomy)
  - Segment support boundaries
- Section 4 subsets:
  - PEER GROUPINGS (lines 490–504)
  - MS COMBINED ENTITY (lines 505–509)
  - SCATTER & CHART COMPOSITE HANDLING (lines 478–489)
  - COVERAGE vs SHARE vs x-MULTIPLE LABEL RULE (lines 510–521)
  - `_METRIC_FORMAT_TYPE` MAINTENANCE RULE (lines 522–530)

**Rationale:** All normalization math, peer group definitions, composite handling, and segment taxonomy belong together as the analytical core.

**Overlap:** Peer group CERTs also mentioned in 04 (artifact specs) and 10 (coding rules). **Decision: 06 is the canonical source for peer group definitions. Other files cross-reference.**

### `docs/claude/07-local-macro.md`

**Source sections:**
- Section 13: Local Macro Pipeline Architecture (lines 2813–2982)
  - Overview, geography spine, data sources
  - Output sheets (7 sheets)
  - Board columns (30-column spec)
  - Source metadata policy
  - Integration point (MSPBNA_CR_Normalized.py call)
  - Transformation policy registry
  - Per-capita normalization formulas
  - Helper functions

**Rationale:** `local_macro.py` is a self-contained module with its own architecture. Deserves its own doc.

### `docs/claude/08-corp-overlay.md`

**Source sections:**
- Section 12: Corp-Safe Overlay Architecture (lines 2689–2811)
  - Overview, data flow
  - Input contract (loan file)
  - Optional enrichment hooks
  - CLI usage
  - Artifact details
  - MSA-level macro panel (superseded note)
  - Known limitations

**Rationale:** `corp_overlay.py` is standalone by design. Its docs should be standalone too.

### `docs/claude/09-troubleshooting.md`

**Source sections:**
- Section 5: Common Errors & Troubleshooting (lines 678–724)
  - DNS/Proxy errors
  - Missing FRED_API_KEY
  - No Excel file found
  - Missing sheet in Excel
- Section 8: To-Do / Known Issues (lines 2358–2370)
  - Normalized profitability metrics status
- Section 3a subset: Remaining Risks (lines 439–447)

**Rationale:** All troubleshooting, known issues, and risks in one place for operators.

**Overlap:** "Remaining Risks" also in 04. **Decision: duplicate intentionally.** 04 keeps risks in context of rendering architecture; 09 collects all operational risks.

### `docs/claude/10-coding-rules.md`

**Source sections:**
- Section 4: Strict Coding Conventions & Rules (lines 450–676)
  - ALWAYS UPDATE CLAUDE.md
  - FDIC API Variables & FFIEC Waterfall
  - NO HARDCODING (cross-ref to 02)
  - IMPORT SAFETY (cross-ref to 02)
  - DEPRECATED FUNCTIONS
  - SCATTER & CHART COMPOSITE HANDLING (cross-ref to 06)
  - PEER GROUPINGS (cross-ref to 06)
  - MS COMBINED ENTITY (cross-ref to 06)
  - COVERAGE vs SHARE vs x-MULTIPLE LABEL RULE (cross-ref to 06)
  - `_METRIC_FORMAT_TYPE` MAINTENANCE RULE (cross-ref to 06)
  - HUD TOKEN — ATTRIBUTE-ONLY ACCESS
  - CENTRALIZED CHART PALETTE
  - CHART ANNOTATION / ANTI-OVERLAP POLICY
  - WEALTH PEERS COMPARATOR INCLUSION POLICY
  - CSS CLASS NAMING
  - ENTITY DISPLAY LABEL POLICY
  - CHART METRICS
  - FRED Deduplication
  - FRED Frequency Inference
  - FRED Series Validation
  - STOCK vs FLOW MATH CONVENTION
  - WEALTH-FOCUSED vs DETAILED TABLE DISTINCTION
- Section 9: Metric Registry & Validation Architecture (lines 2372–2449)
- Section 10: FRED Expansion Layer (lines 2451–2556)
- Section 11: HUD USPS ZIP Crosswalk (lines 2558–2687)

**Rationale:** All coding rules, conventions, and detailed technical architecture for FRED/HUD subsystems.

**Note:** This is the largest target file. Consider whether FRED (Section 10) and HUD (Section 11) should be separate files. **Decision: keep in 10 for now.** They are reference material used primarily when editing those subsystems, and splitting further would fragment the developer reference.

### `docs/claude/99-changelog.md`

**Source sections:**
- Section 7: Changelog / Recent Fixes (lines 868–2356) — **1,489 lines, ~50% of the file**

**Rationale:** The changelog is by far the largest section and grows with every commit. Isolating it prevents the root `CLAUDE.md` from growing unboundedly.

---

## Sections That Should Remain Duplicated Intentionally

| Content | Primary Location | Also In | Reason |
|---|---|---|---|
| Peer group CERT table (90001/90003/90004/90006) | `06-normalization-and-peer-groups.md` | `10-coding-rules.md` (brief), `04-rendering-architecture.md` (brief) | CERTs are referenced in 3 different contexts; cross-refs would be too indirect |
| Remaining Risks list | `04-rendering-architecture.md` | `09-troubleshooting.md` | Different audiences: developers vs operators |
| Env var table | `02-build-run-config.md` | (nowhere else — single source) | Was duplicated in Section 1; removing that duplication |
| "ALWAYS UPDATE CLAUDE.md" rule | `10-coding-rules.md` | Root `CLAUDE.md` stub | Must be visible in the root file that agents read first |

---

## Sections That Overlap and Need Deduplication

| Overlapping Content | Locations in Source | Resolution |
|---|---|---|
| Script/module table | Section 1 (summary), Sections 10/12/13 (detailed) | Keep summary in 01, detailed in respective files |
| Env var table | Section 1 (brief mention), Section 2 (full table) | Full table only in 02; pointer from 01 |
| Excel sheet table | Section 3 (master), Sections 10/11/13 (domain sheets) | Master stays in 03; domain files document only their own sheets |
| Executive chart artifact specs | Section 3a (within rendering arch) | Move to 05; 04 keeps only the rendering pattern |
| Peer group CERTs | Section 4, Section 6, Section 3a | Canonical in 06; others cross-reference |
| Composite handling rules | Section 4 (coding rules), Section 6 (normalization) | Canonical in 06; 10 has cross-ref |
| FRED series lists | Section 3a (macro charts), Section 10 (expansion layer) | Merge into 05 (artifact specs) and 10 (coding rules/architecture) |

---

## Ambiguous Content and Resolutions

| Content | Ambiguity | Resolution |
|---|---|---|
| Section 3a "Adding New Artifacts" (lines 327–340) | Is this a coding rule (→10) or rendering architecture (→04)? | **04** — it's a rendering-specific workflow |
| Section 3a "Canonical Rendering Abstraction Rule" (lines 341–352) | Coding rule or architecture? | **04** — defines the import contract for rendering_mode.py |
| Section 4 "CHART METRICS" (lines 614–620) | Coding rule or artifact spec? | **10** — it's a coding convention for which metrics to use |
| Section 4 "WEALTH PEERS COMPARATOR INCLUSION POLICY" (lines 576–588) | Coding rule or artifact spec? | **10** — it's a policy for chart developers to follow |
| Section 6 "Case-Shiller ZIP Enrichment" (lines 794–797) | Normalization or HUD/Case-Shiller? | **06** (brief mention stays), **10** has the detailed HUD/CS architecture |
| Section 8 "To-Do" (lines 2358–2370) | Troubleshooting or changelog? | **09** — it's operational status of known issues |
| Section 12 "MSA-Level Macro Panel" (lines 2771–2810) | Corp overlay or local macro? | **08** — the functions live in corp_overlay.py (superseded but retained) |

---

## Root `CLAUDE.md` Post-Split Stub (Planned)

After the split, the root `CLAUDE.md` will be replaced with a lightweight stub containing:

1. One-paragraph project description
2. "ALWAYS UPDATE docs/claude/ files" rule
3. Index of split files with one-line descriptions
4. Pointer to `docs/claude/99-changelog.md` for history

**Estimated size:** ~40–60 lines (vs current 2,982).

**This stub has NOT been created yet.** It will be created in the next phase after split files are populated.

---

## Validation Checklist (Pre-Split)

- [x] `CLAUDE.md` exists at project root
- [x] `docs/claude/_archive/CLAUDE.current.pre-split.md` exists (identical to current CLAUDE.md)
- [x] `docs/claude/_archive/CLAUDE.old.monolith.md` exists (identical — no separate old file found)
- [x] `docs/claude/SPLIT_PLAN.md` exists (this file)
- [x] No Python code moved
- [x] No split files created yet
- [x] No root `CLAUDE.md` replaced yet
