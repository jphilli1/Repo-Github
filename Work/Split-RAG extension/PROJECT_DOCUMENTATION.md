# Split-RAG Extension — Project Documentation

> **Version:** 2.1.0
> **Last Updated:** 2026-03-08
> **Maintainers:** This document must be updated in tandem with any code changes to prevent drift.

---

## Table of Contents

1. [Scope](#1-scope)
2. [Objective](#2-objective)
3. [Architecture Overview](#3-architecture-overview)
4. [Module Reference](#4-module-reference)
5. [Data Model (Schema v2)](#5-data-model-schema-v2)
6. [Extraction Pipeline (Tier 1)](#6-extraction-pipeline-tier-1)
7. [Knowledge Graph & Retrieval (Tier 2)](#7-knowledge-graph--retrieval-tier-2)
8. [Copilot Integration Tools](#8-copilot-integration-tools)
9. [Rules Engine](#9-rules-engine)
10. [Design Decisions & Rationale](#10-design-decisions--rationale)
11. [CLI Usage](#11-cli-usage)
12. [Testing](#12-testing)
13. [Limitations](#13-limitations)
14. [Potential Add-On Capabilities](#14-potential-add-on-capabilities)
15. [Dependency Policy](#15-dependency-policy)
16. [File Inventory](#16-file-inventory)
17. [Change Log Discipline](#17-change-log-discipline)

---

## 1. Scope

Split-RAG Extension is a **deterministic, API-free document intelligence pipeline** designed for commercial lending and credit analysis workflows. It processes PDF documents (loan approval memoranda, credit memos, term sheets, annual reviews, appraisal summaries, and related banking documents) and produces structured, auditable outputs suitable for consumption by Microsoft Copilot Studio within an 8K token context window.

### In Scope

- PDF text extraction with dual-engine failover (pdfplumber primary, pypdfium2 fallback)
- Deterministic entity extraction via regex patterns and fuzzy matching (RapidFuzz)
- Financial metric normalization (currency, percentages, multiples with scale hints)
- Section hierarchy detection and header classification
- Email block isolation and exclusion
- In-memory Document Knowledge Graph (networkx DiGraph)
- TF-IDF hybrid retrieval routing (scikit-learn, localized per-subgraph)
- Bounding box preservation for every extracted chunk (audit requirement)
- Credit team anonymization (RM_1, CO_1, UW_1, AP_1)
- Copilot Studio ingest dataset generation (CSV + pipe-delimited)
- SharePoint file package generation for per-document retrieval optimization
- Covenant extraction with evidence linkage
- Document type classification (two-stage weighted pattern matching)

### Out of Scope

- LLM-based extraction or summarization
- OCR for scanned documents (structure exists but is disabled)
- Cross-document relationship resolution
- Real-time streaming or API serving
- Any external network calls during processing

---

## 2. Objective

Build a preprocessing pipeline that converts unstructured loan committee packages into structured, token-efficient artifacts that a Copilot Studio agent (constrained to ~8K context tokens, no Power Automate) can consume via SharePoint. The pipeline must be:

1. **Deterministic** — Same input always produces same output (MD5 document IDs, position-based chunk IDs)
2. **Auditable** — Every extracted fact traces back to a specific page, bounding box, and chunk ID via SHA-256 lineage hashes
3. **API-free** — No external LLM, embedding, or cloud API calls; runs entirely on local compute
4. **Token-efficient** — Outputs designed so Copilot loads <=1200 chars initially (topline), escalating to section excerpts only as needed
5. **Privacy-safe** — Credit team names are anonymized before leaving the pipeline

---

## 3. Architecture Overview

```
Raw PDF/DOCX/TXT
       │
       ▼
┌─────────────────────────────────────────────────┐
│  Tier 1: extractor.py ("The Factory")           │
│  - pdfplumber primary extraction                │
│  - pypdfium2 fallback per page                  │
│  - Header hierarchy parser + section state      │
│  - Email block detection                        │
│  - Entity anchoring (regex from rules.json)     │
│  - Financial metric harvesting + normalization  │
│  - Document type classification                 │
│  - Bounding box extraction for every chunk      │
│  Output: ContextGraph JSON (*_v2.json)          │
└─────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  Tier 2: relationship_manager.py                │
│  ("The Gatekeeper")                             │
│  - networkx DiGraph Document Knowledge Graph    │
│  - DOC → PAGE → SECTION → CHUNK topology        │
│  - RapidFuzz entity extraction                  │
│  - TF-IDF vectorization (per-subgraph, local)   │
│  - Hybrid retrieval routing                     │
│  - NEXT_BLOCK reading order edges               │
│  - CONTINUES_SECTION cross-page edges           │
│  - MENTIONED_IN entity linkage                  │
└─────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  Copilot Integration Layer (tools/)             │
│                                                 │
│  build_copilot_ingest.py                        │
│  - Single CSV/TXT with groomed_context column   │
│  - 36-column schema per document                │
│                                                 │
│  build_sharepoint_copilot_package.py            │
│  - Per-document file package (10 files)         │
│  - 19-column global index                       │
│  - SharePoint folder layout optimized for 8K    │
│                                                 │
│  copilot_prompt_stub.txt                        │
│  - Copilot retrieval instructions (<2500 chars) │
└─────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────┐
│  copilot_tier2.py ("The Consumer")              │
│  - Sandbox runtime for Copilot Studio           │
│  - pandas-only dependency constraint            │
│  - Query preprocessing + keyword density        │
│  - Entity resolution from ContextGraph JSON     │
│  - Deterministic context retrieval              │
│  - 30-second execution limit                    │
└─────────────────────────────────────────────────┘
```

### Data Flow

1. **Input:** Raw PDF (or DOCX/TXT) files in a folder
2. **Extraction:** `extractor.py` produces one `*_v2.json` (ContextGraph) per document
3. **Graph Build:** `relationship_manager.py` constructs an in-memory DKG from the ContextGraph
4. **Copilot Ingest:** `build_copilot_ingest.py` reads ContextGraph JSONs and writes a flat ingest dataset
5. **SharePoint Package:** `build_sharepoint_copilot_package.py` writes per-document file packages to a folder structure for SharePoint sync
6. **Runtime Query:** `copilot_tier2.py` loads a ContextGraph JSON and answers queries deterministically (sandbox mode)

---

## 4. Module Reference

### Core Engine Modules

| Module | Role | Key Classes / Functions |
|--------|------|------------------------|
| `schema_v2.py` | Data contract (Pydantic v2) | `ContextGraph`, `ContextNode`, `NodeMetadata`, `ExtractedIntelligence`, `MetricObservation`, `CreditTeamMember`, `ExtractedEntity`, `ExtractionMetrics` |
| `extractor.py` | Tier 1 extraction ("The Factory") | `process_file()`, `run_extraction()`, `extract_entities()`, `_classify_chunk_type()`, `_infer_section_level()`, `_normalize_section_label()`, `_normalize_financial_value()`, `_classify_document_type()`, `_detect_email_blocks()` |
| `relationship_manager.py` | Tier 2 graph + retrieval ("The Gatekeeper") | `DocumentKnowledgeGraph`, `HybridRetrievalRouter`, `load_entity_keywords()`, `prune_empty_nodes()`, `collapse_trivial_sections()` |
| `entity_matcher.py` | Rule-based entity matching | `EntityMatcher`, keyword dictionaries, RapidFuzz/regex fallback |
| `copilot_tier2.py` | Sandbox query engine ("The Consumer") | `preprocess_query()`, `load_context_graph()`, `resolve_entity_query()`, `retrieve_context()`, `get_document_summary()` |
| `main.py` | CLI orchestrator | `SplitRAGPipeline` — wires Tier 1 → Tier 2, supports `--query`, `--interactive`, `--export-graph` |

### Copilot Integration Tools

| Module | Purpose |
|--------|---------|
| `tools/build_copilot_ingest.py` | Flat CSV/TXT ingest dataset (36 columns, single row per doc) |
| `tools/build_sharepoint_copilot_package.py` | Per-document SharePoint file package (10 files per row_id) |
| `tools/copilot_prompt_stub.txt` | Retrieval instruction block for Copilot Studio agent |

### Configuration Files

| File | Purpose |
|------|---------|
| `rules.json` | Entity regex patterns, document type patterns, financial metric ontology, email block markers, entity keywords, stopwords |
| `config.json` | Runtime paths, engine selection, extraction settings, validation thresholds |
| `requirements.txt` | Python dependencies with version constraints |

---

## 5. Data Model (Schema v2)

### ContextGraph (Root)

The top-level container for all extraction output from a single document.

| Field | Type | Description |
|-------|------|-------------|
| `document_id` | `str` (32-char MD5 hex) | Deterministic hash of file bytes |
| `filename` | `str` | Original filename |
| `processed_at` | `str` (ISO 8601) | Processing timestamp |
| `schema_version` | `str` | Currently `"2.1.0"` |
| `borrower_entity` | `str?` | Legacy top-level borrower field |
| `lender_entity` | `str?` | Legacy top-level lender field |
| `guarantor_entity` | `str?` | Legacy top-level guarantor field |
| `intelligence` | `ExtractedIntelligence?` | All structured extraction results |
| `nodes` | `List[ContextNode]` | Ordered chunk list |
| `metrics` | `ExtractionMetrics?` | Run statistics |

### ContextNode

Each extracted chunk (text block, header, table, key-value pair, image caption).

| Field | Type | Description |
|-------|------|-------------|
| `chunk_id` | `str` (32-char MD5) | Deterministic: `MD5(doc_id:page:index:content_sample[:50])` |
| `parent_section_id` | `str?` | Chunk ID of parent section header |
| `content_type` | `Literal["header","text","table","image_caption","kv_pair"]` | Content classification |
| `content` | `str` | Raw extracted text |
| `verified_content` | `bool` | Whether content was cross-verified |
| `metadata` | `NodeMetadata` | Full provenance metadata |
| `lineage_trace` | `str` (64-char SHA-256) | `SHA256(file_hash\|page\|bbox\|extraction_method)` |

### NodeMetadata

| Field | Type | Description |
|-------|------|-------------|
| `page_number` | `int` | 1-based page number |
| `bbox` | `Tuple[float,float,float,float]?` | `(x0, y0, x1, y1)` bounding box coordinates |
| `table_shape` | `List[int]?` | `[rows, cols]` for table content |
| `cell_bboxes` | `List[Tuple[float,float,float,float]]?` | Per-cell bounding boxes for tables |
| `edge_density` | `float` | Heuristic for content complexity |
| `source_scope` | `Literal["primary","corpus"]` | Scope for disambiguation |
| `extraction_method` | `Literal["pdfplumber","pypdfium2"]` | Which engine extracted this chunk |
| `conflict_detected` | `bool` | True if Keep-All Policy triggered |
| `is_active` | `bool` | False for fallback versions retained for audit |
| `section_label` | `str?` | Normalized section header label |
| `section_level` | `int` | Header depth: 0=none, 1=top, 2=sub, 3=sub-sub |
| `is_email_block` | `bool` | True if detected as email content |

### ExtractedIntelligence

| Field | Type | Description |
|-------|------|-------------|
| `document_type` | `str?` | Classified type (e.g., `credit_memo`, `term_sheet`, `annual_review`) |
| `document_type_confidence` | `float` | Confidence score (0.0–1.0) |
| `entities` | `List[ExtractedEntity]` | All extracted entities with provenance |
| `financial_metrics` | `List[MetricObservation]` | Extracted metrics with normalization |
| `credit_team` | `List[CreditTeamMember]` | Identified team members (names present here; anonymized only in Copilot outputs) |
| `covenants` | `List[ExtractedEntity]` | Extracted covenant terms |

### ID Generation (Determinism Guarantees)

| Function | Algorithm | Inputs | Purpose |
|----------|-----------|--------|---------|
| `generate_document_id()` | MD5 | File bytes | Same file → same 32-char hex ID, always |
| `generate_chunk_id()` | MD5 | `doc_id:page:chunk_index:content[:50]` | Position + content → stable chunk reference |
| `generate_lineage_trace()` | SHA-256 | `file_hash\|page\|bbox\|extraction_method` | Audit trail linking chunk → exact source location |

---

## 6. Extraction Pipeline (Tier 1)

### Header Classification (`_classify_chunk_type`)

A heuristic state machine classifies each text chunk. Rules ordered by specificity:

1. ALL CAPS, short text → `header`
2. Roman numeral prefix (`I.`, `II.`, `III.`) → `header`
3. Numbered section (`1.`, `2.`, `1.1`) → `header`
4. Lettered section (`A.`, `B.`) → `header`
5. Short colon-terminated line → `header`
6. Title case (heavily gated to avoid matching names/addresses) → `header`
7. Everything else → `text` (or `table` / `kv_pair` by other signals)

**Design Decision:** Title case detection is intentionally conservative. We observed that aggressively classifying title-case lines as headers caused false positives on borrower names, property addresses, and legal entity names. The gate requires the line be short, not contain common address patterns, and not appear inside a known paragraph block.

### Section Level Inference (`_infer_section_level`)

Section depth is inferred from header formatting:

| Level | Signal |
|-------|--------|
| 1 | ALL CAPS, Roman numeral prefix, or standalone short line |
| 2 | Numbered subsection (e.g., `1.1`, `A.1`), title case with colon |
| 3 | Lettered or deeply nested numbering (e.g., `1.1.1`, `(a)`) |

### Section Label Normalization (`_normalize_section_label`)

Headers are normalized to canonical labels for cross-document consistency:

| Raw Header Examples | Normalized Label |
|---------------------|-----------------|
| `CREDIT QUALITY REVIEW`, `Credit Quality` | `credit_quality_review` |
| `FINANCIAL COVENANTS`, `Covenants` | `financial_covenants` |
| `TRANSACTION OVERVIEW`, `Transaction Summary` | `transaction_overview` |

Normalization uses RapidFuzz fuzzy matching (threshold 85) when available, falling back to exact regex matching.

### Email Block Detection (`_detect_email_blocks`)

Email chains embedded in loan packages are identified via:

- Header patterns: `From:`, `To:`, `Sent:`, `Date:`, `Subject:`, `Cc:`
- Separator patterns: `--- Original Message ---`, `--- Forwarded message ---`, `On ... wrote:`
- Minimum 2 header lines within a 20-line span

**Design Decision:** Email blocks are detected but retained in the ContextGraph (marked `is_email_block=True`). They are excluded from groomed context and top-sections by default. This preserves them for audit while preventing token waste in Copilot consumption.

### Financial Metric Normalization (`_normalize_financial_value`)

| Input | Normalized Output | Unit |
|-------|-------------------|------|
| `$50MM` | `50000000.0` | `currency_usd` |
| `1.25x` | `1.25` | `multiple` |
| `75%` | `75.0` | `percent` |
| `$2.5B` | `2500000000.0` | `currency_usd` |
| `$500K` | `500000.0` | `currency_usd` |

Scale hints (`MM`, `M`, `K`, `B`) are extracted and stored separately for audit. The normalization is purely regex-based — no LLM inference.

### Document Type Classification (`_classify_document_type`)

Two-stage weighted pattern matching from `rules.json`:

1. **Pattern scan:** Each document type definition has a list of regex patterns with integer weights
2. **Score aggregation:** Sum weights of all matching patterns per type
3. **Confidence:** Highest-scoring type's weight sum divided by total matched weight sum
4. **Dominance gate:** `banker_email` requires `requires_dominance: true` — it only wins classification if email patterns dominate the document

Supported document types:

| Type Key | Label |
|----------|-------|
| `credit_memo` | Credit Memo / LAM |
| `annual_review` | Annual Review |
| `loan_modification` | Loan Modification |
| `credit_monitoring_report` | Credit Monitoring Report |
| `term_sheet` | Term Sheet |
| `appraisal_summary` | Appraisal Summary |
| `financial_statement_review` | Financial Statement Review |
| `presentation_deck` | Presentation Deck |
| `banker_email` | Banker Email |

### Dual-Engine Failover

| Priority | Engine | Role |
|----------|--------|------|
| Primary | pdfplumber | `.extract_words()` for exact bbox coordinates |
| Fallback | pypdfium2 | Per-page recovery when pdfplumber fails on corrupted pages |

**Design Decision:** We chose pdfplumber as primary because it provides word-level bounding boxes (`x0, y0, x1, y1`) essential for citation overlay in the frontend. pypdfium2 is used as a page-level fallback — when pdfplumber raises an exception on a specific page, the extractor retries that page with pypdfium2 and merges the result. Both engines mark their chunks with `extraction_method` for audit trail.

### Bounding Box Policy (Non-Negotiable)

Every `ContextNode` **must** carry a bounding box (`bbox` tuple). This is enforced at multiple levels:

- Pydantic validation on the schema
- Test suite assertions (`TestBBoxSanitization`, `TestCellLevelBBox`, `TestTupleBBox`)
- Sanitization: bboxes are clamped to page dimensions, NaN/negative values are rejected
- Tables carry additional `cell_bboxes` for per-cell provenance

**Rationale:** The downstream frontend renders visual citation overlays on the original PDF. Without bboxes, citations cannot be visually verified by credit officers. This was established as an audit requirement early in the project and is non-negotiable.

---

## 7. Knowledge Graph & Retrieval (Tier 2)

### Document Knowledge Graph (DKG)

Built by `relationship_manager.py` using networkx.DiGraph:

```
DOC_<document_id>
  └── PAGE_<document_id>_<page_number>
        └── SEC_<chunk_id>        (one per header)
              └── <chunk_id>      (content chunks)
```

#### Edge Types

| Edge Type | Meaning |
|-----------|---------|
| `HAS_PAGE` | DOC → PAGE |
| `HAS_SECTION` | PAGE → SEC |
| `HAS_CHILD` | SEC → CHUNK (or PAGE → CHUNK for headerless content) |
| `NEXT_BLOCK` | CHUNK → CHUNK (reading order, resets per page) |
| `CONTINUES_SECTION` | PAGE → SEC (when a section spans multiple pages) |
| `CONTAINS_TABLE` | SEC → CHUNK (table-type chunks within a section) |
| `MENTIONED_IN` | ENTITY → CHUNK (entity keyword mentions) |
| `GUARANTEED_BY` | ENTITY → ENTITY (guarantor relationships) |

**Design Decision:** `NEXT_BLOCK` edges reset at page boundaries to prevent cross-page reading-order contamination. Section state, however, persists across pages via `CONTINUES_SECTION` edges. This matches the physical structure of loan documents where sections frequently span multiple pages but reading order within a page is self-contained.

### Hybrid Retrieval Router

`HybridRetrievalRouter` combines:

1. **TF-IDF similarity** (scikit-learn `TfidfVectorizer`) — fitted locally per-subgraph, not globally
2. **Section-scoped routing** — queries are first matched to relevant sections via keyword density
3. **Content type weighting** — headers (3.0x), tables (2.5x), kv_pairs (2.0x), text (1.0x)
4. **Primary scope multiplier** (1.5x) — chunks from `source_scope="primary"` are boosted

**Design Decision:** TF-IDF vectorizers are fitted per-subgraph (not stored globally) to ensure retrieval is localized and memory-bounded. This prevents a single large document from polluting the vector space of other documents in the same session.

### Entity Extraction in DKG

The DKG extracts entities by scanning chunk text against keyword dictionaries from `rules.json`. When RapidFuzz is available, fuzzy matching (threshold 85) catches variations like "Debt Svc Coverage" matching "Debt Service Coverage Ratio". Without RapidFuzz, exact regex matching is used as fallback.

---

## 8. Copilot Integration Tools

### build_copilot_ingest.py (Flat Dataset)

Produces a single-file ingest dataset optimized for Copilot Studio:

- **`copilot_ingest.csv`** — UTF-8 BOM, 36 columns, one row per document
- **`copilot_ingest.txt`** — Pipe-delimited, same schema
- **`copilot_ingest_manifest.json`** — Run metadata, engine paths, warnings

Key column: `groomed_context` — A pre-assembled context string combining:
- **(A) Topline** — <=1200 chars of highest-value facts
- **(B) Section snippets** — First 650 chars per section (up to 20 sections)
- **(C) Top sections** — 5 largest sections by char count (>=2 from early pages)

Total groomed context capped at 6000 chars by default.

### build_sharepoint_copilot_package.py (Per-Document Package)

Produces a per-document folder structure for SharePoint:

```
<output>/
├── rows/
│   ├── 1/
│   │   ├── 1__manifest.json
│   │   ├── 1__topline.txt          (<=1200 chars)
│   │   ├── 1__section_index.csv
│   │   ├── 1__sections_excerpt.txt
│   │   ├── 1__top_sections.txt
│   │   ├── 1__metrics.csv
│   │   ├── 1__entities.csv
│   │   ├── 1__covenants.csv
│   │   ├── 1__credit_team.csv      (anonymized)
│   │   └── 1__chunks_map.csv
│   └── 2/
│       └── ...
├── copilot_ingest_index.csv
├── copilot_ingest_index.txt
└── build_sharepoint_copilot_package.log
```

#### Per-Document Files

| File | Purpose | Token Budget |
|------|---------|-------------|
| `__topline.txt` | Highest-value facts | <=1200 chars (~300 tokens). Copilot always loads this first. |
| `__section_index.csv` | Section routing table | Small; tells Copilot which sections exist and what they contain |
| `__sections_excerpt.txt` | First 650 chars per section | Loaded only if topline insufficient |
| `__top_sections.txt` | Top 5 largest sections (email excluded) | Loaded for narrative answers |
| `__metrics.csv` | Financial metrics with evidence_chunk_id | Loaded for numeric questions |
| `__entities.csv` | Entity extractions with evidence | Loaded for entity lookups |
| `__covenants.csv` | Covenant terms with evidence | Loaded for covenant/compliance questions |
| `__credit_team.csv` | Anonymized roles + evidence | Names replaced: RM_1, CO_1, UW_1, AP_1 |
| `__chunks_map.csv` | Chunk inventory (ID → page, section, email flag, size) | For evidence verification without loading full text |
| `__manifest.json` | File metadata, engine paths, sizes, section stats | Internal; not loaded by Copilot |

#### Retrieval Strategy (Copilot Prompt Stub)

The `copilot_prompt_stub.txt` instructs the Copilot agent to:

1. Always open `topline.txt` first
2. Open `metrics.csv` for numeric questions
3. Open `covenants.csv` for covenant/compliance questions
4. Open `credit_team.csv` for approval chain questions
5. Open `top_sections.txt` only if topline is insufficient
6. Consult `section_index.csv` to route to the right section
7. Open `sections_excerpt.txt` for section detail
8. Open `entities.csv` for entity lookups

This progressive loading strategy keeps the Copilot within its 8K token budget.

#### Index Schema (19 Columns)

| # | Column | Description |
|---|--------|-------------|
| 1 | `row_id` | Sequential integer |
| 2 | `doc_id` | 32-char MD5 document hash |
| 3 | `filename` | Original filename |
| 4 | `relative_row_folder` | Path to row folder (e.g., `rows/1`) |
| 5 | `doc_type` | Classified document type |
| 6 | `doc_type_confidence` | Classification confidence |
| 7 | `borrower` | Extracted borrower name |
| 8 | `relationship_name` | Relationship name |
| 9 | `product_type` | Loan product type |
| 10 | `request_type` | Action requested |
| 11 | `request_amount` | Dollar amount requested |
| 12 | `collateral_summary` | Collateral description |
| 13 | `key_metrics_summary` | Key metrics (LTV, DSCR, etc.) |
| 14 | `decision_summary` | Approval/decline decision |
| 15 | `exceptions_summary` | Policy exceptions |
| 16 | `credit_team_summary` | Anonymized team roles |
| 17 | `recommended_files_to_open` | Comma-separated filenames in priority order |
| 18 | `warnings` | Processing warnings |
| 19 | `created_utc` | ISO 8601 timestamp |

#### Recommended Files Heuristic

The `recommended_files_to_open` column lists files in fixed priority:

1. `topline.txt` — Always first
2. `metrics.csv` — Numeric data
3. `covenants.csv` — Compliance data
4. `credit_team.csv` — Approval chain
5. `top_sections.txt` — Narrative context
6. `section_index.csv` — Section routing
7. `sections_excerpt.txt` — Section detail
8. `entities.csv` — Entity data

### Anonymization

Credit team names are replaced with role-based placeholders before any Copilot-facing output:

| Role | Prefix | Example |
|------|--------|---------|
| `relationship_manager` | `RM` | `RM_1`, `RM_2` |
| `credit_officer` | `CO` | `CO_1` |
| `underwriter` | `UW` | `UW_1` |
| `approver` | `AP` | `AP_1` |

Real names are stored in `ExtractedIntelligence.credit_team` within the ContextGraph JSON but are never emitted into Copilot-facing files.

---

## 9. Rules Engine

`rules.json` is the central configuration for all pattern-based extraction. It contains:

### Entity Patterns

Each entity type has:
- `patterns`: List of regex patterns with named capture groups (`(?P<entity>...)` or `(?P<value>...)`)
- `target_sections`: Optional list of section labels where the entity is most likely found (used for confidence boosting)

Supported entity types: `borrower`, `lender`, `guarantor`, `sponsor`, `parent_company`, `subsidiary`, `co_borrower`, `counterparty`, `relationship_name`, `facility_name`, `property_address`, `property_type`, `collateral_type`, `collateral_description`, `loan_purpose`, `financial_covenant`, `reporting_requirement`, `relationship_manager`, `credit_officer`, `underwriter`, `approver`, `email_sender`, `email_recipient`, `email_subject`, `email_date`, `pledge_type`, `pledged_assets`, `tax_status`, `adjusted_gross_income`

### Financial Metric Ontology

Each metric has:
- `canonical_name`: Normalized key (e.g., `dscr`, `ltv`)
- `aliases`: Alternative names (e.g., `"Debt Service Coverage Ratio"`, `"DS Coverage"`)
- `value_patterns`: Regex patterns for value extraction
- `unit_type`: `"multiple"`, `"percent"`, or `"currency"`

Covered metrics: `dscr`, `ltv`, `noi`, `revenue`, `ebitda`, `net_income`, `total_leverage`, `senior_leverage`, `interest_coverage`, `cap_rate`, `occupancy`, `appraised_value`, `loan_amount`, `cash`, `liquidity`, `debt`

### Document Type Patterns

Each type has weighted regex patterns. Higher weights indicate stronger classification signals. The `banker_email` type has a special `requires_dominance` flag preventing it from winning unless email patterns dominate the document.

### Email Block Markers

Header patterns and separator patterns used to detect embedded email chains.

### Entity Keywords

Domain-specific keyword lists used by the DKG entity extraction and fuzzy matching: `financial_metric`, `contract_term`, `regulatory`, `pricing`, `cre_metric`, `covenant`, `credit_team`, `collateral`, `tax`

---

## 10. Design Decisions & Rationale

### Why No LLM?

The target deployment environment (Copilot Studio) has strict sandbox constraints: no external API calls, no GPU access, ~256MB memory limit, 30-second execution cap. The preprocessing pipeline must produce artifacts that Copilot can consume without any additional inference. Additionally, determinism is a regulatory requirement — the same document must always produce the same extracted facts.

### Why pdfplumber Over PyPDF2/PyMuPDF?

pdfplumber provides word-level bounding boxes via `.extract_words()`, which are essential for visual citation overlay. PyPDF2 provides only text without coordinates. PyMuPDF (fitz) could work but has licensing considerations (AGPL) that were incompatible with the deployment context.

### Why networkx Over Neo4j?

The absolute constraint prohibits Neo4j. networkx provides an in-memory graph that is sufficient for single-document analysis (typical loan packages are 20–100 pages). There is no need for persistence or distributed graph queries.

### Why TF-IDF Over Embeddings?

Embedding models require torch/transformers (prohibited) or external API calls (prohibited). TF-IDF with cosine similarity via scikit-learn provides adequate lexical retrieval for structured financial documents where key terms are well-defined. The per-subgraph fitting approach keeps memory bounded.

### Why Per-Document File Packages?

Copilot Studio's 8K token limit means it cannot load an entire document at once. By splitting each document into 10 purpose-specific files, Copilot can progressively load only what it needs. The topline (<=1200 chars) answers most questions; metrics/covenants CSVs handle numeric queries; top sections handle narrative questions. This avoids wasting tokens on email chains and irrelevant sections.

### Why Exclude Email Blocks?

Loan committee packages frequently contain embedded email chains (approval emails, distribution lists, FYI forwards). These consume significant tokens but rarely contain extractable facts. Email blocks are detected, tagged, and excluded from groomed context by default. They remain in the ContextGraph JSON for audit purposes.

### Why Anonymize Credit Team?

Credit team names are internal PII. The pipeline masks them before outputting to SharePoint/Copilot (which may be accessed by a broader audience than the original document). Role-based placeholders (RM_1, CO_1) preserve the organizational structure without exposing individual names.

### Why Pipe-Delimited Output?

Copilot Studio handles pipe-delimited records more reliably than CSV in prompt-based interactions. The pipe-delimited format avoids quoting/escaping issues with commas in financial values and legal entity names.

### Why Two Ingest Tools?

- **`build_copilot_ingest.py`** (flat): Produces a single CSV/TXT suitable for bulk upload or Power BI analysis. The `groomed_context` column contains a pre-assembled prompt-ready context string.
- **`build_sharepoint_copilot_package.py`** (per-document): Produces a folder structure optimized for Copilot Studio's file-level retrieval. Copilot opens individual files by name, enabling progressive context loading.

Both tools share the same engine modules, guardrails, and extraction logic.

### Why Section-Scoped TF-IDF?

Early experiments with global TF-IDF showed that large sections (e.g., financial spreads) dominated retrieval results regardless of query intent. Section-scoped vectorization ensures that a query about covenants retrieves from the covenants section, not from whichever section has the most text.

### Why the Engine Import Guard?

A stale copy of the engine existed under `Work/text_extract/`. The import guard checks that all engine modules resolve to paths containing `Split-RAG` and rejects any path containing `text_extract`. This prevents accidentally importing outdated code that could produce incompatible outputs.

---

## 11. CLI Usage

### Main Pipeline

```bash
python main.py --file input/document.pdf --query "What is the DSCR?"
python main.py --file input/document.pdf --interactive
python main.py --file input/document.pdf --export-graph output/graph.json
```

### Copilot Ingest (Flat)

```bash
python tools/build_copilot_ingest.py \
    --input ./engine_output/ \
    --output ./copilot_output/ \
    --section_chars 650 \
    --top_section_chars 1200 \
    --max_sections 20 \
    --top_sections 5 \
    --early_pages 3 \
    --max_context_chars 6000 \
    --exclude_email true \
    --extensions "pdf,doc,docx,txt" \
    --fail_fast false
```

### SharePoint Package

```bash
python tools/build_sharepoint_copilot_package.py \
    --input ./engine_output/ \
    --output ./sharepoint_package/ \
    --section_chars 650 \
    --top_section_chars 1200 \
    --max_sections 20 \
    --top_sections 5 \
    --early_pages 3 \
    --exclude_email true \
    --extensions "pdf,doc,docx,txt" \
    --fail_fast false
```

Both tools accept `*_v2.json` ContextGraph files directly. If no JSONs are found, they attempt to run the extraction engine on raw documents in the input folder.

---

## 12. Testing

### Test Suite: `tests/test_refactored_pipeline.py`

**167 tests** across **24 test classes**, covering:

| Test Class | Tests | Coverage Area |
|------------|-------|---------------|
| `TestSchemaV2` | 6 | Schema validation, ID generation, determinism |
| `TestDocumentKnowledgeGraph` | 12 | DKG construction, entity extraction, bbox preservation |
| `TestHybridRetrievalRouter` | 9 | Query routing, result formatting, bbox citations |
| `TestCopilotTier2` | 6 | Document loading, query preprocessing, context retrieval |
| `TestDeterminism` | 4 | Reproducibility across multiple runs |
| `TestNextBlockAndSectionContext` | 8 | Reading order edges, parent section enrichment |
| `TestBBoxSanitization` | 10 | Bbox validation, clamping, coordinate integrity |
| `TestLocalizedSubgraphRetrieval` | 6 | Per-subgraph TF-IDF, no global vectorizer leakage |
| `TestPageStateResetDKG` | complex | Cross-page NEXT_BLOCK isolation, section persistence |
| `TestHeaderHierarchyParser` | 24 | Header classification, section levels, normalization |
| `TestDocumentTypeClassification` | 7 | Two-stage document type detection |
| `TestEmailBlockDetection` | 3 | Email header patterns, separators, isolation |
| `TestFinancialMetricOntology` | 15 | Value normalization ($MM, $B, x, %) |
| `TestSectionScopedExtraction` | 6 | Section-targeted extraction, confidence boosting |
| `TestKVLineHarvester` | 5 | Key-value pattern matching |
| `TestGraphPruning` | 4 | Orphan node removal, trivial section collapse |
| `TestExtractedIntelligence` | 11 | Intelligence schema roundtrip serialization |
| `TestCrossPageSectionContinuity` | 5+ | Multi-page section persistence, CONTINUES_SECTION edges |
| Others | various | Tuple bbox enforcement, cell-level bbox, entity relationship edges |

### Test Architecture

- **Fixture composition:** `sample_context_graph` → `built_dkg` → `router` (each built on the prior)
- **No mocking:** Real graph construction, real TF-IDF fitting
- **Determinism checks:** Same input → same IDs verified across multiple invocations
- **Roundtrip tests:** ContextGraph → JSON → ContextGraph validated
- **Inline function reimplementation:** Extractor functions are reimplemented inline in tests to avoid pdfplumber import failures in test environments

### Running Tests

```bash
cd "Work/Split-RAG extension"
python -m pytest tests/test_refactored_pipeline.py -q
```

---

## 13. Limitations

### Extraction Quality

1. **No OCR:** Scanned documents (image-only PDFs) produce no extractable text. The `enable_ocr` config option exists but is not implemented.
2. **Regex-only extraction:** Entity and metric extraction relies on pattern matching. Novel phrasings not covered by `rules.json` will be missed.
3. **No cross-document resolution:** If the same borrower appears in multiple documents with different naming conventions, there is no entity resolution across documents.
4. **Table extraction quality depends on PDF structure:** Poorly formatted tables (especially those using spaces instead of cell boundaries) may produce garbled content.
5. **Limited header heuristics:** Some documents use non-standard formatting (colored backgrounds, logos as headers) that text-based heuristics cannot detect.

### Copilot Integration

6. **8K token ceiling is rigid:** If a document has extremely dense information, even the topline + metrics may exceed the budget. There is no dynamic compression.
7. **No semantic retrieval:** TF-IDF cannot match semantically similar but lexically different queries (e.g., "loan-to-value" vs. "how much equity does the borrower have").
8. **No multi-document queries:** Copilot can only query one document's package at a time. Cross-document analysis (e.g., "compare these two borrowers") is not supported.
9. **No incremental updates:** If a document is revised, the entire package must be regenerated. There is no diff-based update mechanism.

### Operational

10. **Windows path dependency:** The engine import guard validates paths containing `Split-RAG`. The canonical path uses Windows drive letters (`G:\My Drive\...`). On non-Windows systems, the path structure must still contain `Split-RAG`.
11. **No concurrency:** Documents are processed sequentially. Large batches of 100+ documents will take proportionally longer.
12. **No streaming output:** All outputs are written to disk after complete processing. There is no partial output on failure (though `--fail_fast false` allows continuing past individual document errors).

---

## 14. Potential Add-On Capabilities

### Near-Term (Minimal Code Changes)

1. **Chunk-level text files:** `row_id__chunks_early.txt` (first 2–3 pages) and per-section text files for very large sections. The infrastructure exists but is commented out as optional.
2. **Confidence-gated output:** Only emit entities/metrics above a configurable confidence threshold to reduce noise in Copilot responses.
3. **Custom metric ontology:** Allow users to add custom financial metrics (e.g., institution-specific ratios) by extending `rules.json` without code changes.
4. **Batch parallelization:** Process multiple documents concurrently using `multiprocessing.Pool`. The pipeline is already stateless per-document.
5. **Delta processing:** Track document hashes and skip re-extraction for unchanged files, reducing reprocessing time for incremental SharePoint syncs.

### Medium-Term (Moderate Effort)

6. **OCR integration:** Add Tesseract or EasyOCR for scanned document pages. Would require extending the pypdfium2 fallback path.
7. **Cross-document entity resolution:** Build a relationship graph across documents using fuzzy matching on borrower/guarantor/facility names.
8. **Copilot feedback loop:** Log Copilot's file access patterns (which files it opens, in what order) to refine the recommended files heuristic.
9. **Section importance scoring:** Use TF-IDF or keyword density to rank sections beyond simple char count, improving top-sections selection.
10. **DOCX/DOC native parsing:** Add python-docx or mammoth for native Word document support instead of relying on PDF conversion.

### Long-Term (Significant Effort)

11. **Embedding-based retrieval:** If torch/transformers constraints are relaxed, add sentence-transformers for semantic retrieval alongside TF-IDF.
12. **Graph persistence:** Export the DKG to a portable format (GraphML, JSON-LD) for cross-session reuse.
13. **Active learning:** Use Copilot's query patterns to identify extraction gaps and suggest new `rules.json` patterns.
14. **Multi-language support:** Extend entity patterns for non-English loan documents (e.g., Spanish, French in international banking).
15. **Streaming/webhook integration:** Trigger package rebuild automatically when new documents are uploaded to SharePoint, using Power Automate or Azure Functions.

---

## 15. Dependency Policy

### Absolute Constraint

> **No torch, transformers, llama-index, neo4j, openai, google-genai.**

This is enforced by:
- Comment headers in every source file
- Import guards that exit non-zero on detection
- `requirements.txt` explicitly listing only permitted packages

### Permitted Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `pdfplumber` | >=0.10.0 | Primary PDF extraction engine |
| `pypdfium2` | >=4.0.0 | Fallback PDF extraction engine |
| `networkx` | >=3.0 | In-memory Document Knowledge Graph |
| `scikit-learn` | >=1.3.0 | TF-IDF vectorization + cosine similarity |
| `rapidfuzz` | >=3.0.0 | Fuzzy entity matching (optional; regex fallback) |
| `pydantic` | >=2.0.0 | Schema validation (v2 strictly enforced) |
| `pandas` | >=2.0.0 | Data manipulation (copilot_tier2 sandbox only) |
| `numpy` | >=1.24.0 | Numeric operations |
| `tqdm` | >=4.65.0 | Progress bars |

### Copilot Tier 2 Sandbox

`copilot_tier2.py` is restricted to pandas + standard library only. This matches Copilot Studio's sandbox constraints (no arbitrary pip installs).

### Copilot Integration Tools (tools/)

`build_copilot_ingest.py` and `build_sharepoint_copilot_package.py` use **standard library only** (`csv`, `json`, `pathlib`, `re`, `datetime`, `hashlib`). No pandas dependency. This was a deliberate choice to keep the tools lightweight and portable.

---

## 16. File Inventory

```
Work/Split-RAG extension/
├── __init__.py                          # Package marker
├── schema_v2.py                         # Data contract (Pydantic v2 models)
├── extractor.py                         # Tier 1: PDF extraction engine
├── relationship_manager.py              # Tier 2: DKG builder + hybrid retrieval
├── entity_matcher.py                    # Rule-based entity matching (RapidFuzz/regex)
├── copilot_tier2.py                     # Sandbox query engine for Copilot Studio
├── main.py                              # CLI orchestrator (Tier 1 → Tier 2 pipeline)
├── rules.json                           # Entity patterns, metrics ontology, doc types
├── config.json                          # Runtime configuration
├── requirements.txt                     # Python dependencies
├── PROJECT_DOCUMENTATION.md             # This document
├── tools/
│   ├── build_copilot_ingest.py          # Flat CSV/TXT ingest dataset builder
│   ├── build_sharepoint_copilot_package.py  # Per-document SharePoint package builder
│   └── copilot_prompt_stub.txt          # Copilot retrieval instructions
└── tests/
    ├── __init__.py
    └── test_refactored_pipeline.py      # 167 tests across 24 test classes
```

---

## 17. Change Log Discipline

This document must be updated whenever:

- A new module or tool is added
- The schema (`schema_v2.py`) changes (new fields, removed fields, renamed fields)
- The `rules.json` ontology is extended (new entity types, new metrics, new document types)
- CLI arguments are added, removed, or have their defaults changed
- Output file formats change (new columns, renamed columns, new files in the package)
- Dependency policy changes (new packages added or removed)
- Design decisions are revisited (rationale should be updated, not deleted)
- Test coverage areas change significantly

**Review cadence:** After every PR that modifies files under `Work/Split-RAG extension/`, verify that this document reflects the current state of the codebase. Drift between documentation and code undermines the trust that credit officers place in the system's outputs.
