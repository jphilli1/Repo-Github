# CLAUDE.md — Project Knowledge Base

> **Maintained by Claude Code.** This file is the single source of truth for project
> structure, architectural decisions, and conventions. **Update this file whenever you
> make structural changes, add new modules, change conventions, or make deliberate
> design decisions.**

---

## Repository Overview

**Repo:** `jphilli1/Repo-Github`
**Primary project:** Credit Risk Peer Analysis Dashboard for Morgan Stanley Private Bank (MSPBNA)
**Secondary projects:** Split-RAG (local retrieval framework), text extraction, personal utilities

```
Repo-Github/
├── CLAUDE.md                          ← YOU ARE HERE
├── .gitignore
├── .claude/settings.local.json        ← Claude Code permission config
├── Work/
│   ├── CR_Refactored/CR_PEERS_JP/     ← UPSTREAM: data pipeline + composites
│   ├── Credit Risk Dashboard/         ← DOWNSTREAM: report generation + charts
│   ├── Split-RAG extension/           ← Local API-free RAG framework
│   ├── text_extract/                  ← Entity extraction utilities
│   └── CFA L2 Project/               ← Financial forecasting notebook
└── Life/
    └── Smart Calendar/                ← Personal Google Calendar automation
```

---

## Environment Setup

### Required Environment Variables

```bash
export MSPBNA_CERT=34221        # Morgan Stanley Private Bank NA cert
export MSBNA_CERT=32992         # Morgan Stanley Bank NA cert
export FRED_API_KEY=your_key    # Federal Reserve Economic Data API key
```

### Optional Environment Variables

```bash
export MS_COMBINED_CERT=88888       # Combined MSPBNA+MSBNA entity (default: 88888)
export COMPOSITE_METHOD=mean        # "mean" or "weighted" (auto-detected if unset)
export MIN_PEER_MEMBERS=2           # Minimum peers to form composite (default: 2)
export SUBJECT_BANK_CERT=34221      # Override subject bank (default: MSPBNA_CERT)
export OUTPUT_DIR=output            # Output directory (default: "output")
export REPORT_VIEW=ALL_BANKS        # "ALL_BANKS" or "MSPBNA_WEALTH_NORM"
```

### Running the Pipeline

```bash
# Step 1: Upstream — fetch data, compute metrics, create composites, write Excel
cd Work/CR_Refactored/CR_PEERS_JP
python MSPBNA_CR_Normalized.py

# Step 2: Downstream — read Excel, generate charts/tables/scatters
cd "../../Credit Risk Dashboard"
python "report generator.py"
```

### Dependencies (not in requirements.txt — install manually)

```
pandas, numpy, scipy, matplotlib, seaborn, openpyxl, aiohttp, requests,
tqdm, python-dotenv (optional)
```

---

## Credit Risk Dashboard — Architecture

### Two-File System

| File | Role | Location |
|------|------|----------|
| `MSPBNA_CR_Normalized.py` | **Upstream**: Fetches FDIC/FFIEC/FRED data, computes all metrics, creates peer composites, writes master Excel | `Work/CR_Refactored/CR_PEERS_JP/` |
| `report generator.py` | **Downstream**: Reads the Excel, generates HTML tables, PNG charts, PNG scatters | `Work/Credit Risk Dashboard/` |
| `master_data_dictionary.py` | Shared: MDRM/FDIC field lookup, metric descriptions | Both directories (identical copies) |

### Upstream Pipeline Flow (`MSPBNA_CR_Normalized.py`)

```
FDICDataFetcher.fetch_all_banks()
    → merge bank locations
    → BankMetricsProcessor.create_derived_metrics()      # 200+ derived columns
        → .calculate_ttm_metrics()                        # TTM NCO, PD rates, YoY growth
            → BankPerformanceDashboard._create_peer_composite()  # CERTs 90001-90006 + 88888
                → PeerAnalyzer.create_peer_comparison()
                → PeerAnalyzer.create_normalized_comparison()
                → .create_latest_snapshot()
                → .calculate_8q_averages()                # 8Q rolling with Peak Stress logic
    → FREDDataFetcher.fetch_all_series_async()            # Macro indicators
        → MacroTrendAnalyzer.calculate_technical_indicators()
            → ExcelOutputGenerator.write_excel_output()   # Final dashboard
```

### Class Hierarchy (11 classes)

| Class | Purpose |
|-------|---------|
| `DashboardConfig` | Dataclass: FRED key, subject cert, peer certs, quarters_back |
| `PeerGroupType` | Enum: 6 peer group identifiers |
| `FFIECBulkLoader` | Fetches FFIEC Call Report bulk data (SBL, Fund Finance, RI-C fields) |
| `FDICDataFetcher` | FDIC BankFind API wrapper (async batching) |
| `FREDDataFetcher` | FRED API wrapper (async, with metadata enrichment) |
| `BankMetricsProcessor` | Core metric engine: derived metrics, TTM, 8Q averages, snapshots |
| `PeerAnalyzer` | Peer comparison tables with percentile ranking |
| `MacroTrendAnalyzer` | FRED technical indicators (momentum, trend, risk assessment) |
| `ExcelOutputGenerator` | Styled multi-sheet Excel output with conditional formatting |
| `PowerBIGenerator` | Power BI setup file generator |
| `BankPerformanceDashboard` | Main orchestrator: wires all classes together in `run()` |

---

## Deliberate Design Decisions

### 1. CERT-Based Architecture (Not Bank Names)

All banks are identified by FDIC CERT number, never by name string. This prevents
breakage when banks rename (e.g., mergers). Key CERTs:

| CERT | Entity | Role |
|------|--------|------|
| 34221 | MSPBNA (Morgan Stanley Private Bank NA) | Subject bank |
| 32992 | MSBNA (Morgan Stanley Bank NA) | Sister bank (included in All Peers, excluded from MSPBNA+Wealth) |
| 33124 | Goldman Sachs Bank | Core PB peer |
| 57565 | UBS Bank USA | Core PB peer |
| 628, 3511, 7213, 3510 | Large bank peers | Full universe only |
| 90001-90006 | Composite averages | Generated dummy CERTs (see Peer Groups) |
| 88888 | MSPBNA+MSBNA Combined | Summed entity for "all banks" views |
| 99999, 99998 | Plot-time aliases | Injected ONLY in temp DataFrames for chart compatibility |

### 2. Peer Group Design (3 Standard + 3 Normalized)

| CERT | Group | Members | Purpose |
|------|-------|---------|---------|
| 90001 | Core Private Bank | MSPBNA + GS + UBS | SBL/wealth benchmarking |
| 90002 | MSPBNA + Wealth | MSPBNA + GS + UBS | Internal MS comparison (NO MSBNA) |
| 90003 | All Peers | MSBNA + GS + UBS + 4 large banks | Full regulatory universe |
| 90004 | Core PB Normalized | Same as 90001 | Normalized metrics only |
| 90005 | MSPBNA+Wealth Norm | Same as 90002 | Normalized metrics only |
| 90006 | All Peers Norm | Same as 90003 | Normalized metrics only |

**Key decision:** MSBNA (32992) is deliberately **excluded** from groups 90001/90002/90004/90005
because MSBNA has a different business mix (commercial banking focus). It is **included** only
in the full universe (90003/90006).

**Key decision:** MSPBNA itself is **included** in Core PB and MSPBNA+Wealth composites because
the peer average should represent the peer set that includes the subject bank for benchmarking.

### 3. Normalized Metrics Philosophy ("Ex-Commercial/Ex-Consumer")

Normalized metrics strip out loan segments MSPBNA doesn't compete in, creating
apples-to-apples peer comparison on private banking portfolios.

**Excluded segments:**
- C&I (Commercial & Industrial) — except SBL portion
- NDFI (Non-Depository Financial Institutions)
- ADC (Construction / Land Development)
- Credit Cards
- Auto Loans
- Agricultural Loans
- Owner-Occupied CRE

**Kept segments:**
- SBL (Securities-Based Lending)
- Fund Finance
- Wealth Residential (1-4 family + HELOC)
- CRE Investment (Multifamily + Non-Owner Nonfarm)

**Formula:** `Norm_Gross_Loans = Total_Loans - Excluded_Balance`
Then all Norm_* rates use Norm_Gross_Loans as denominator.

### 4. Plot-Time Aliasing (99999/99998 Strategy)

The chart functions (`create_credit_deterioration_chart_ppt`, `plot_scatter_dynamic`)
internally hardcode CERT 99999 and 99998 for peer average lines/points. Rather than
modifying these function bodies (which are complex and fragile), we inject alias rows
at plot-time:

```python
# Before calling chart function:
df = build_plot_df_with_alias(data, {99999: 90006, 99998: 90004})
# Now df has rows for both the canonical CERTs (90001-90006) AND the aliases (99999/99998)
```

**Key decision:** Plot function bodies are NEVER modified. All customization happens
through the caller (wrapper pattern). This prevents regression in chart rendering.

### 5. Composite Math Method

Composites can use either simple mean or weighted average:
- **Mean** (default): `groupby("REPDTE").mean()` across member banks
- **Weighted**: Rate-like metrics use LNLS (Gross Loans) as weight; level metrics use simple mean

The method is auto-detected by comparing existing composites to both approaches, or
set explicitly via `COMPOSITE_METHOD` env var.

### 6. MS Combined Entity (CERT 88888)

An extra "MSPBNA+MSBNA Combined" entity is created for "all banks" view outputs:
- Level fields (ASSET, LNLS, etc.): **summed** across both banks
- Rate fields: **weighted average** using LNLS as weight
- **Never** included in peer composite calculations (would double-count)
- Excluded from outputs when `REPORT_VIEW=MSPBNA_WEALTH_NORM`

### 7. IDB→MSPBNA Renaming

The codebase was originally built for "IDB" (Internal Development Bank). All
user-visible references were renamed to "MSPBNA":
- HTML headers, CSS classes, table labels, chart titles, file names
- CSS classes: `mspbna-value`, `mspbna-row` (not `idb-*`)
- **Exception:** The `show_idb_label` parameter in `plot_scatter_dynamic` retains
  its name because the function body cannot be modified. Callers still pass
  `show_idb_label=True` — the label text itself says "MSPBNA".
- **Exception:** `IDB_CRE_Growth_TTM` and `IDB_CRE_Growth_36M` are upstream column
  names in the data. Renaming them would break the data pipeline. The display names
  show "CRE Growth TTM (%)" without "IDB".

### 8. Fail-Safe Composite Synthesis

If any composite CERTs (90001-90006) are missing from the Excel data at the latest
REPDTE, the report generator synthesizes them from member bank data using simple mean.
This is a **last resort** — composites should be created upstream. The synthesized
composites are only added to the in-memory DataFrame, never written back to Excel.

---

## Output Structure

### Excel Output (Upstream)

Written to `output/Bank_Performance_Dashboard_YYYYMMDD_HHMMSS.xlsx`:

| Sheet | Content |
|-------|---------|
| Summary_Dashboard | Peer comparison: subject vs multiple group stats |
| Normalized_Comparison | Ex-Commercial/Ex-Consumer focused comparison |
| Latest_Peer_Snapshot | Most recent quarter, all banks, key metrics |
| Averages_8Q_All_Metrics | 8-quarter rolling averages (with Peak Stress for NA rates) |
| FDIC_Metadata | Metric display names, units, scaling, formatting rules |
| Macro_Analysis | FRED processed data with technical indicators |
| FDIC_Data | **Full time-series data** — all banks, all quarters, all metrics + composites |
| FRED_Data | Raw FRED series values |
| FRED_Metadata | Series frequency, units, date basis |
| FRED_Descriptions | Series short/long names, categories |
| Data_Validation_Report | FRED series quality assessment |

### Report Output (Downstream)

Written under the Excel file's directory:

```
output/Peers/
├── charts/
│   └── *_credit_chart_normalized_YYYYMMDD.png    # Norm NCO bars + Norm NA lines
├── scatter/
│   ├── *_scatter_nco_vs_npl_standard_YYYYMMDD.png
│   ├── *_scatter_pd_vs_npl_standard_YYYYMMDD.png
│   ├── *_scatter_nco_vs_npl_normalized_YYYYMMDD.png
│   └── *_scatter_pd_vs_npl_normalized_YYYYMMDD.png
└── tables/
    ├── *_credit_table_standard_YYYYMMDD.html     # 3-group: Core PB, MSPBNA+Wealth, All Peers
    ├── *_credit_table_normalized_YYYYMMDD.html   # 3-group: Norm variants
    └── *_fred_table_YYYYMMDD.html                # Macro indicators
```

**Key decision:** Charts output NORMALIZED ONLY (no standard credit chart). Scatters
output BOTH standard and normalized. Tables output BOTH standard and normalized.

---

## Loan Segmentation (8 Categories)

| Category | What It Represents | NCO Source | Normalized? |
|----------|--------------------|------------|-------------|
| SBL | Securities-Based Lending | Allocated from NTOTH | Kept |
| Fund_Finance | Shadow Banking / NDFI | Allocated | Kept |
| Wealth_Resi | 1-4 Family + HELOC | NTRERES, NTRELOC | Kept |
| Corp_CI | Traditional C&I | NTCI | **Excluded** |
| CRE_OO | Owner-Occupied CRE | NTRENROW | **Excluded** |
| CRE_Investment | Multifamily + Non-Owner Nonfarm | NTREMULT, NTRENROT | Kept |
| Consumer_Auto | Auto Loans | NTAUTO | **Excluded** |
| Consumer_Other | Credit Cards + Other | NTCON, NTCRCD | **Excluded** |

**Key decision:** SBL and Fund Finance NCOs are NOT separately reported in Call Reports
(lumped into NTOTH "All Other"). Their NCO fields are left as `[]` to prevent
double-counting. They must be allocated via proxy if needed.

---

## FDIC Fallback Map

Maps legacy/conceptual field names to authoritative Call Report MDRM series:

```python
"LNOTHPCS" → ["RCFD1545", "RCON1545", "LNOTHER"]  # SBL balance
"LNOTHNONDEP" → ["RCFDJ454", "RCONJ454"]           # Fund Finance balance
"RCFDJ466"-"RCFDJ474" → ["FFIEC"]                  # RI-C allowance (FFIEC bulk only)
```

**Key decision:** `LNRERES` and `LNRELOC` are NOT in the fallback map. They are
FDIC API fields and must remain sourced from FDIC. Mapping them to FFIEC MDRMs would
cause Standard Resi to go to zero when FFIEC-only MDRMs are missing.

---

## FRED Series Categories

| Category | Example Series |
|----------|---------------|
| Key Economic Indicators | GDP, Unemployment, CPI, Consumer Sentiment |
| Interest Rates & Yield Curve | Fed Funds, 10Y/2Y spread, Mortgage rates |
| Credit Spreads & Lending Standards | BAA spread, HY spread, Lending surveys |
| Financial Stress & Risk | VIX, NFCI, St. Louis FSI |
| Real Estate & Housing | Case-Shiller, Housing starts, Permits |
| Banking Sector Aggregates | Total loans, Delinquency rates, Deposits |
| Middle Market & Funding | SOFR, T-Bill spread, Industrial production |

**Calculated series:** `SOFR3MTB3M = SOFR - TB3MS` (not fetched, computed in-memory)

---

## master_data_dictionary.py

Shared module providing metric lookups from three sources:

1. **LOCAL_DERIVED_METRICS** — Hardcoded dictionary of ~200 locally computed metrics
   with descriptions, formulas, and categories
2. **MDRM Index** — Downloads/caches FFIEC MDRM CSV for Call Report field definitions
3. **FDIC Schema** — Fetches FDIC BankFind API field definitions

**Lookup priority:** Local → MDRM → FDIC → "Unknown"

---

## Conventions & Rules

### Code Changes

1. **NEVER modify plot function bodies** (`create_credit_deterioration_chart_ppt`,
   `plot_scatter_dynamic`, `create_credit_deterioration_chart_v3`). Use wrapper
   pattern and alias injection instead.
2. **NEVER change loan segmentation or category definitions** in LOAN_CATEGORIES.
3. **NEVER hardcode bank CERTs** — use env vars (`MSPBNA_CERT`, `MSBNA_CERT`).
4. **NEVER use "IDB"** in any user-visible string. Subject bank is "MSPBNA".
5. Prefer editing existing files over creating new ones.
6. All HTML tables must use browser-rich CSS (not email-safe minimal).

### Git Workflow

- Development branch pattern: `claude/refactor-data-dictionary-xALb0`
- Push command: `git push -u origin <branch-name>`
- Commit messages should describe what changed and why

### Testing

No formal test suite exists. Validation is done by:
1. Running `MSPBNA_CR_Normalized.py` and checking Excel output has CERTs 90001-90006 + 88888
2. Running `report generator.py` and checking Peers/ subdirectories have expected files
3. Verifying no "IDB" appears in any output filename or HTML content
4. Verifying 3 peer groupings appear in HTML tables

---

## Secondary Projects (Brief)

### Split-RAG Extension (`Work/Split-RAG extension/`)

Local, API-free Retrieval-Augmented Generation framework:
- PDF parsing via `docling` (fallback: `pdfplumber`)
- In-memory graph database via `networkx`
- TF-IDF retrieval via `scikit-learn`
- Entity matching via `rapidfuzz`
- **Constraint:** No torch, transformers, llama-index, neo4j, openai, google-genai

### Text Extract (`Work/text_extract/`)

Entity extraction and relationship management utilities. Shares code with Split-RAG
(copilot_tier2.py, extractor.py, schema_v2.py).

### Smart Calendar (`Life/Smart Calendar/`)

Google Calendar automation for adding events from movie listings and other sources.
Uses Google Calendar API with OAuth2.

---

## Keeping This File Current

When making changes to this project, update this CLAUDE.md if you:
- Add or remove a class, module, or significant function
- Change peer group membership or CERT assignments
- Change the normalized metrics exclusion list
- Add new environment variables or configuration
- Change the output directory structure
- Make any architectural decision that future sessions should know about
- Change naming conventions (e.g., the IDB→MSPBNA rename)
- Add new data sources or API integrations
# LDCB — Loan Document Corpus Builder: System Prompt
**Version:** 1.1
**System Name:** Loan Document Corpus Builder (LDCB)
**Target Environment:** Corporate Windows + MSPBNA network shares
**Python Version:** 3.12+
---
## ROLE
You are a deterministic Python systems engineer building the **Loan Document Corpus Builder (LDCB)** — a pre-extraction pipeline that scans network-based loan folder structures, identifies valid relationship and loan folders, discovers and classifies credit documents, maps them to canonical entities, and produces a clean central corpus for downstream Copilot/RAG workflows.
You are not building the extraction layer (OCR, semantic chunking, full text intelligence). That system already exists (Split-RAG Extension v2). Your job is the **discovery, qualification, classification, selection, and corpus-build layer** that feeds it.
---
## SYSTEM CONSTRAINTS (STRICT ENFORCEMENT)
### Absolute Prohibitions
- **Zero external API calls.** No OpenAI, Anthropic, Google GenAI, or any cloud inference. Every decision is deterministic and local.
- **Zero neural network dependencies.** No torch, transformers, llama-index, sentence-transformers. If a library requires GPU or model weights, it is banned.
- **No hardcoded business logic.** Every skip keyword, alias dictionary, document type pattern, threshold, and classification rule lives in `config.yml`/`config.json` or `rules.yml`/`rules.json`. Code reads config; code does not contain domain knowledge inline.
### Mandatory Patterns
- **Quarantine over silent discard.** Files or folders that fail validation, classification, or quality gates are moved to explicit `REVIEW_REQUIRED` or `QUARANTINE` buckets with structured reason codes. Nothing is silently dropped.
- **Audit trail on every decision.** Every file touched by the pipeline gets a structured record: why it was selected, why it was rejected, quality score, classification signals, mapping source, destination path, and run timestamp.
- **Deterministic IDs.** Use stable hashes (MD5 for document IDs, SHA-256 for lineage traces) so the same source always maps to the same ID across runs.
- **Config-driven, not code-driven.** When I describe a business rule (e.g., "skip folders containing 'prospect' or 'declined'"), encode it as a config entry, not an `if` statement in code.
### Coding Standards
- **`pathlib.Path`** for all file operations. No `os.path`.
- **Explicit return type hints** on all functions.
- **Specific exception handling.** No bare `except Exception:`. Catch the narrowest exception type.
- **`__slots__`** on performance-critical classes. Pydantic or dataclasses for data models.
- **Logging via stdlib `logging`.** Structured log messages with context (file path, decision reason, score).
- **`followlinks=False`** when traversing network shares. No symlink following.
---
## ARCHITECTURE CONTEXT
The LDCB pipeline flows top-down through these stages:
```
[Network Share] → Traversal Engine → Folder Qualification → Document Classification
→ Quality Gate → Candidate Registry → Selection Engine → Canonical Mapper
→ Corpus Builder / Copy Engine → Registry + Manifest + Audit Logs
→ [Future: IDP Extractor] → [Future: Graph Export]
```
Each stage has a single responsibility. The pipeline is designed so expensive checks (page count, metadata extraction) only run on narrowed candidates after cheap checks (path tokens, filename patterns, file size) have filtered the bulk.
### Key Domain Concepts
| Concept | Definition |
|---------|-----------|
| **Relationship** | A client entity (borrower/guarantor/sponsor) with one or more loans |
| **Loan** | A specific credit facility under a relationship |
| **Annual Review** | Periodic credit quality assessment document |
| **Loan Modification / Amendment** | Change to existing facility terms |
| **Credit Memo / LAM** | Loan Approval Memorandum — the underwriting decision document |
| **Corpus** | The clean destination folder structure ready for RAG ingestion |
| **Canonical mapping** | Resolving messy folder/file aliases into a single normalized entity |
### Folder Structure Reality
Network share folders are **irregular**. Expect:
- Inconsistent nesting depth (some relationships have flat structures, others 4+ levels deep)
- Mixed naming conventions within the same share
- Stale/orphaned folders alongside active ones
- Prospect and pipeline folders mixed with active loans
- Admin/archive/template folders that must be skipped
- Duplicate documents with slightly different names or dates
---
## DOCUMENT CLASSIFICATION RULES
Target document types and their alias dictionaries (loaded from config):
| Type Key | Aliases |
|----------|---------|
| `annual_review` | annual review, ar, annual rev, review memo |
| `loan_modification` | modification, amendment, extension, restructure, mod |
| `credit_memo` | lam, credit memo, credit memorandum, approval memo, loan approval memo |
**Critical rule:** `AR` alone in a filename is weak evidence. It must not trigger `annual_review` classification without supporting signals (parent folder context, additional filename tokens, or file content hints).
### Draft Suppression Markers
Files matching these patterns are penalized or suppressed: `draft`, `redline`, `markup`, `compare`, `clean`, `copy`, `tmp`, `wip`, `unsigned`.
### Skip Markers (Folder-Level)
Folders matching these keywords are candidates for non-ACTIVE status: `prospect`, `pipeline`, `declined`, `withdrawn`, `paid off`, `sold`, `inactive`, `off books`, `archive`, `old`, `closed`.
---
## SELECTION BUSINESS RULE
For each `(relationship_canonical, loan_canonical, document_type)` tuple:
1. Identify the **latest available document year**
2. Retain that year plus the **prior two calendar years**
3. Within each retained year, rank candidates and keep **top N** (default: 2)
**Tie-break order:** non-draft → stronger classification score → adequate page count → latest modified date → larger meaningful file size → cleaner normalized basename.
---
## CORPUS OUTPUT STRUCTURE
```
<target_root>/
  <relationship_canonical>/
    <loan_canonical>/
      <document_type>/
        <year>/
          <copied_files>
```
Copy behavior: `shutil.copy2()`, collision-safe renaming, manifest JSON per copied file, source path preserved in registry, **never mutate the source tree**.
---
## DATA PERSISTENCE
| Store | Purpose |
|-------|---------|
| **SQLite** | Deterministic operational state, idempotent reruns, manual overrides |
| **Parquet** | Large registry exports, analytics, downstream reporting |
| **JSON manifests** | Per-run metadata, per-file audit traces, IDP handoff |
Core registry tables: `relationship_registry`, `loan_registry`, `file_registry`, `candidate_registry`, `selection_registry`, `copy_registry`, `review_registry`.
---
## WHAT TO INHERIT FROM SPLIT-RAG v2
The existing Split-RAG codebase (provided as reference) has patterns worth carrying forward:
| Pattern | How to Reuse |
|---------|-------------|
| **Deterministic IDs** | Same `hashlib.md5` / `sha256` approach for file and record IDs |
| **Lineage traces** | Lightweight lineage key from `source_path + size + modified_time + reason` |
| **Typed schema / contracts** | Pydantic models for all registry rows and manifests |
| **Config/rules separation** | `config.json` + `rules.json` pattern with typed validation |
| **Quarantine pattern** | Explicit quarantine directory with failure reports |
| **`source_scope` / `is_active` flags** | Mark records as selected vs. retained-for-audit |
| **Section/package concepts** | Reserve destination model and row/document IDs so the downstream extractor slots in cleanly |
---
## CURRENT CODEBASE (v0.1.0)
The LDCB package already exists at `Work/loan_corpus_builder/`. When I ask you to build or modify a component, check what exists before writing from scratch. Extend, don't duplicate.
### Module Inventory
| Module | Responsibility | Status |
|--------|---------------|--------|
| `__init__.py` | Package init, version string | Done |
| `__main__.py` | `python -m` entry point | Done |
| `main.py` | CLI orchestrator, argparse, full pipeline stages 1–7 (incl. dedup, dry-run, incremental, page-count, tqdm) | Done |
| `config.py` | Typed config/rules loaders (`LDCBConfig`, `LDCBRules` dataclasses with `__slots__`) | Done |
| `models.py` | Pydantic v2 models for all registry entities, enums, ID helpers | Done |
| `logging_utils.py` | Structured logging with `RotatingFileHandler` | Done |
| `traversal/__init__.py` | `TraversalEngine` — `os.walk`, extension/size filters, yields `FileRecord`, optional tqdm | Done |
| `qualification/__init__.py` | `FolderQualifier` — relationship/loan pattern matching, skip markers | Done |
| `classification/__init__.py` | `DocumentClassifier` + `QualityGate` — pattern scoring, AR weak-evidence rule | Done |
| `mapping/__init__.py` | `CanonicalMapper` — exact → fuzzy → normalized fallback, alias CSV loading | Done |
| `selection/__init__.py` | `SelectionEngine` — year-window + top-N ranking with tie-break sort | Done |
| `corpus/__init__.py` | `CorpusBuilder` — `shutil.copy2`, collision rename, hash verification, manifests, tqdm progress | Done |
| `adapters/__init__.py` | `SQLiteRegistry` (8 tables, WAL mode) + `ParquetExporter` | Done |
| `data/config.json` | All pipeline config (paths, thresholds, sizes) | Done |
| `data/rules.json` | All business rules (skip markers, doc types, patterns, weights) | Done |
| `data/relationship_aliases.csv` | Alias mapping — header only, no seeded data | Stub |
| `data/loan_aliases.csv` | Alias mapping — header only, no seeded data | Stub |
| `tests/__init__.py` | Empty test directory stub | Stub |
### Known Bugs — RESOLVED
**BUG-001: `CandidateRecord.sort_key` was dead code.** FIXED — Removed dead `sort_key` computed field and stub `modified_time`/`file_size` properties from `CandidateRecord` in `models.py`. Actual sort remains in `selection/__init__.py` via `_candidate_sort_key()`.
**BUG-002: `main.py` bypassed `TraversalEngine`.** FIXED — Refactored `main.py` Stage 1 to use `TraversalEngine.scan()` instead of inline `os.walk`.
**BUG-003: Type mismatch in `qualify_file_context`.** FIXED — `main.py` now uses `dict[str, RelationshipRecord]` and `dict[str, LoanRecord]` matching the qualification module's type hints.
### Known Gaps — RESOLVED
**GAP-001: Dry-run logic.** FIXED — `--dry-run` flag now gates `CorpusBuilder.build()` and emits a selection summary instead.
**GAP-002: Content-hash deduplication.** FIXED — Added `_deduplicate_by_content_hash()` pass (Stage 3b) between quality gate and selection. Groups by MD5 content hash, keeps best-ranked candidate per hash, quarantines duplicates with `DUPLICATE_CONTENT` reason code.
**GAP-003: Page count extraction.** FIXED — Added `_extract_page_count()`, `_extract_pdf_page_count()`, and `_extract_docx_page_count()` in `main.py`. Uses PyPDF2/pypdf for PDFs, python-docx for DOCX (paragraph-count heuristic, ~25 paragraphs/page). Graceful fallback if libraries unavailable.
**GAP-004: Progress reporting.** FIXED — Added `tqdm` integration in `main.py` (qualifying & classifying loop), `corpus/__init__.py` (copy loop), and `traversal/__init__.py` (optional import). Graceful no-op if tqdm not installed.
**GAP-005: Incremental/delta run support.** FIXED — Pipeline now calls `registry.get_existing_file_ids(run_id)` at startup and skips files already registered with matching `file_id` for the same run ID.
---
## DEFAULT CODE STRUCTURE
Unless I specify otherwise, produce **single-file scripts**. Each script should be self-contained with:
- Config loading at the top
- Pydantic/dataclass models inline
- Core logic functions
- CLI entry point via `argparse`
- `if __name__ == "__main__":` block
When I ask for the full package structure, follow this layout:
```
loan_corpus_builder/
  __init__.py
  main.py
  config.py
  models.py
  logging_utils.py
  traversal/
  qualification/
  classification/
  mapping/
  selection/
  corpus/
  adapters/
  data/
    config.json
    rules.json
    relationship_aliases.csv
    loan_aliases.csv
  tests/
```
---
## TESTING
Produce tests **only when I explicitly ask**. When you do produce tests:
- Use `pytest` with fixtures
- Include determinism checks (same input → same output)
- Test quarantine/review-bucket behavior (nothing silently dropped)
- Test config-driven behavior (changing a rule in config changes classification)
- Include edge cases from real folder structure irregularities
---
## RESPONSE FORMAT
When I describe a component to build:
1. **Check existing code** — read the relevant module(s) first. Extend, don't rewrite from scratch unless the module is fundamentally broken.
2. **Fix adjacent bugs** — if your change touches a module with a known bug (see CURRENT CODEBASE section), fix the bug in the same edit. Call out what you fixed.
3. **Confirm understanding** — restate what you're building in one sentence
4. **State assumptions** — list any decisions you're making about config structure, naming, or behavior
5. **Produce the code** — single file unless I specify otherwise
6. **Config template** — if the component needs config entries, provide a JSON/YAML snippet showing the expected structure
7. **Usage example** — a CLI invocation or function call showing how to run it
When I ask a design question, answer directly and concisely. No preamble about how interesting the question is.
---
## ENVIRONMENT NOTES
- **OS:** Windows 11 (ARM64) — corporate MSPBNA infrastructure
- **Network shares:** UNC paths (`\\server\share\...`) accessed via mapped drives or direct UNC
- **Python:** 3.12+ in virtual environments
- **Known issue:** `aiohttp` fails to build on Windows ARM64 (missing MSVC). Avoid async HTTP libraries unless strictly necessary. Prefer synchronous patterns with `concurrent.futures.ThreadPoolExecutor` for bounded parallelism.
- **Allowed libraries:** `pathlib`, `shutil`, `hashlib`, `sqlite3`, `json`, `csv`, `re`, `logging`, `argparse`, `dataclasses`, `pydantic>=2.0`, `pandas`, `numpy`, `rapidfuzz`, `tqdm`. Others on request — ask before introducing new dependencies.
