# CR_PEERS_JP — Credit Risk Performance Reporting Engine

Automated credit risk performance reporting for MSPBNA (Morgan Stanley Private Bank, National Association).

## Quick Start

```powershell
.\venv\Scripts\Activate
pip install -r requirements.txt
python run_pipeline.py          # Run full pipeline (Step 1 + Step 2)
```

See `docs/claude/02-build-run-config.md` for full environment setup and configuration.

## Pipeline

1. **Step 1** (`src/data_processing/MSPBNA_CR_Normalized.py`) — Fetches FDIC + FRED data, produces Excel dashboard
2. **Step 2** (`src/reporting/report_generator.py`) — Reads dashboard, produces charts/scatter/HTML tables

## Memory File Layout

Project documentation is organized as modular Claude memory files:

```
CLAUDE.md                              ← Thin root index (global rules + @imports)
docs/claude/
  ├── 01-project-overview.md           ← Mission, module table
  ├── 02-build-run-config.md           ← Build commands, env vars, dependencies
  ├── 03-output-routing-and-logging.md ← Data flow, Excel sheets, naming, CSV logging
  ├── 04-rendering-architecture.md     ← Dual-mode rendering (full_local / corp_safe)
  ├── 05-executive-and-macro-artifacts.md ← Chart/artifact catalog (16 exec + 3 macro)
  ├── 06-normalization-and-peer-groups.md ← Normalization math, peer groups, segments
  ├── 07-local-macro.md                ← Geography spine, BEA/BLS/Census pipeline
  ├── 08-corp-overlay.md               ← Standalone corp-safe overlay workflow
  ├── 09-troubleshooting.md            ← Common errors, known issues, risks
  ├── 10-coding-rules.md               ← Coding conventions, FRED/HUD architecture
  ├── 99-changelog.md                  ← Full change history
  ├── MEMORY_AUDIT.md                  ← Post-split audit: dedup, scope, recommendations
  ├── SPLIT_PLAN.md                    ← Migration plan from monolithic CLAUDE.md
  └── _archive/                        ← Pre-split source file snapshots

src/
  ├── config/                          ← (Reserved for future config modules)
  ├── data_processing/
  │   └── MSPBNA_CR_Normalized.py      ← Data fetch, processing, normalization
  ├── local_macro/
  │   ├── CLAUDE.md                    ← Scoped: geography, GDP math, completeness
  │   └── local_macro.py               ← Geography spine, BEA/BLS/Census fetchers
  └── reporting/
      ├── CLAUDE.md                    ← Scoped: chart/report rules, artifact contracts
      └── report_generator.py          ← Charts, scatters, HTML tables

tests/
  ├── CLAUDE.md                        ← Scoped: regression test expectations
  └── test_regression.py               ← Regression tests
```

The root `CLAUDE.md` imports all topic files via `@path` syntax. Editing agents should read the root file first, which will pull in the relevant topic files automatically.
