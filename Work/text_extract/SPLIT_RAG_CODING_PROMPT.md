# SPLIT_RAG_CODING_PROMPT.md
## AI-Native Split-RAG System Implementation Guide v2.1

**Document Version:** 2.1
**Target Audience:** Code-generation AI agents (GitHub Copilot, Claude, ChatGPT)
**Specification Type:** Deterministic implementation contract with phased delivery
**Canonical Reference:** `code_instructions.txt` (v2.1) — binding specification

---

## Mission Briefing

You are the Principal Architect and Lead Developer for the Split-RAG v2.1 document extraction and retrieval system. Your mission is to implement a production-grade, deterministic document processing pipeline that separates heavy extraction (Tier 1) from lightweight retrieval (Tier 2).

### Strategic Context

The Split-RAG architecture addresses three critical enterprise requirements:
1. **Auditability** — Every extracted chunk has a deterministic, reproducible ID
2. **Security** — Tier 2 runs in a sandboxed environment (Copilot Studio) with minimal dependencies
3. **Scalability** — Heavy processing occurs offline; retrieval is sub-second

### Reference Documentation

| Document | Purpose | Binding Status |
|----------|---------|----------------|
| `code_instructions.txt` | Master specification with exact code blocks | **BINDING** |
| `ARCHITECTURE_OVERVIEW.md` | Architectural context and rationale | Non-binding |
| This document | Implementation guide with checkpoints | Procedural guidance |

---

## Implementation Protocol

This implementation follows a **four-phase approach** with mandatory confirmation checkpoints. You must complete each phase and receive explicit user confirmation before proceeding.

### Phase Overview

| Phase | Focus | Key Deliverables | Checkpoint |
|-------|-------|------------------|------------|
| **Phase 1** | Environment Setup | `bootstrap.bat`, `config.json`, `rules.json`, `requirements.txt` | CP-001 |
| **Phase 2** | Schema & Extraction | `schema_v1.py`, `extractor.py` | CP-002 |
| **Phase 3** | Tier 2 Retrieval | `copilot_tier2.py` | CP-003 |
| **Phase 4** | Testing & Validation | Test execution, documentation | CP-004 |

### Delivery Protocol

For each artifact:
1. Output `DELIVERABLE: <filename>` header
2. Provide complete file content in a code block
3. Output `STATUS: READY FOR REVIEW`
4. Output `ACTION: Reply "NEXT" to proceed`
5. **WAIT** for user confirmation before continuing

---

## Architectural Understanding

### Tier 1: The Factory (Local Anaconda Environment)

**Purpose:** Heavy-duty document processing with full library access

```json
{
  "python_version": "3.11.x",
  "platform": "Windows 10/11 x64",
  "shell": "cmd.exe via bootstrap.bat",
  "config_format": "JSON (not YAML)"
}
```

**Approved Imports (Exhaustive List):**
```python
# Extraction
from docling.document_converter import DocumentConverter
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
import pdfplumber

# Validation
from pydantic import BaseModel, Field, field_validator, model_validator

# Data Processing
import pandas as pd
import numpy as np

# Office/XML Parsing
import openpyxl
from lxml import etree
import zipfile

# Imaging
from PIL import Image, ImageFilter

# Standard Library (always permitted)
import argparse, hashlib, json, logging, os, re, sys, io, time
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Tuple, Optional, Union, Any
```

**FORBIDDEN IMPORTS (Will Cause Runtime Failure):**
```python
import fitz           # FORBIDDEN: pymupdf not available
import pymupdf        # FORBIDDEN: alias for fitz
import torch          # FORBIDDEN: heavy dependency
import transformers   # FORBIDDEN: not approved
import openai         # FORBIDDEN: no external API calls
import anthropic      # FORBIDDEN: no external API calls
import langchain      # FORBIDDEN: not approved
import yaml           # FORBIDDEN: use JSON configs (CANON_003)
import PyYAML         # FORBIDDEN: same reason
```

### Tier 2: The Consumer (Copilot Studio Sandbox)

**Purpose:** Lightweight, stateless retrieval with minimal dependencies

```json
{
  "python_version": "3.10.x or 3.11.x",
  "execution_model": "Stateless per-invocation",
  "max_execution_time": "30 seconds",
  "max_memory": "256 MB",
  "external_dependencies": ["pandas"],
  "network_access": "None"
}
```

**Tier 2 Approved Imports:**
```python
import pandas as pd  # ONLY external dependency

# Standard Library (all permitted)
import json, re, sys, io
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict
```

**Tier 2 FORBIDDEN (Will Crash Sandbox):**
```python
from pydantic import *      # FORBIDDEN
import docling              # FORBIDDEN
import pdfplumber           # FORBIDDEN
import openpyxl             # FORBIDDEN
import numpy as np          # FORBIDDEN (not guaranteed)
from PIL import Image       # FORBIDDEN
import yaml                 # FORBIDDEN
```

---

## Governance Protocols

These canonicalization rules are **binding constraints** that resolve ambiguity in implementation decisions.

### CANON_001: StdLib Primacy
The Python standard library is always permitted unless explicitly listed in "Forbidden Imports". Use stdlib solutions before reaching for external dependencies.

### CANON_002: Tier-2 Constraint
In Tier 2 (Copilot Studio sandbox), `pandas` is the **only** permitted external dependency. All other logic must rely on standard modules.

### CANON_003: JSON Standard
All configuration and rule definitions must use **JSON format**. YAML is strictly forbidden. This eliminates the PyYAML dependency.

### CANON_004: Template vs. Exact
- `[EXACT]` blocks must be copied verbatim to ensure compliance
- `[TEMPLATE]` blocks provide structural guidance requiring AI completion

### CANON_005: Assembly Protocol
`extractor.py` is assembled from modular components across multiple specification sections. The AI must merge these components into a cohesive whole.

### CANON_006: Precedence Rule
If two specification sections conflict, the **lower section number** takes precedence (Section 0 > Section 1 > Section 2...).

---

## Verification Checkpoints

After generating each file, perform mental verification against these checkpoints:

### CP-001: Type Safety
**Question:** Does every function have explicit return type hints?
**Failure Action:** Add type hints before delivery

### CP-002: Exception Granularity
**Question:** Does every try block have specific exception types?
**Failure Action:** Replace generic `except Exception:` with specific types (allowed only at library boundaries)

### CP-003: Path Safety
**Question:** Are all file paths handled using `pathlib.Path` or raw strings `r""`?
**Failure Action:** Convert string paths to prevent Windows escape issues

### CP-004: Import Audit
**Question:** Does the code import only from the approved library list?
**Failure Action:** Remove unauthorized imports immediately

---

## Phase 1: Environment Setup

### Objectives
1. Create directory structure (`input/`, `output/`, `logs/`)
2. Configure extraction parameters via JSON
3. Define heuristic rules for content detection
4. Specify dependencies

### Deliverable 1.1: bootstrap.bat

```batch
@echo off
REM Split-RAG Document Extractor - Bootstrap Script v1.0.0

setlocal EnableDelayedExpansion

echo ============================================================================
echo Split-RAG Document Extractor - Environment Setup
echo ============================================================================

set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"

echo Working Directory: %SCRIPT_DIR%

REM Create directory structure
echo [Step 1/4] Creating directory structure...
if not exist "%SCRIPT_DIR%\input" mkdir "%SCRIPT_DIR%\input"
if not exist "%SCRIPT_DIR%\output" mkdir "%SCRIPT_DIR%\output"
if not exist "%SCRIPT_DIR%\logs" mkdir "%SCRIPT_DIR%\logs"

REM Check Python version
echo [Step 2/4] Checking Python environment...
python --version 2>nul || (echo ERROR: Python not found & pause & exit /b 1)

REM Audit library versions
echo [Step 3/4] Auditing library versions...
python -c "import pandas; print(f'pandas: {pandas.__version__}')" 2>nul
python -c "import docling; print(f'docling: {docling.__version__}')" 2>nul
python -c "import pydantic; print(f'pydantic: {pydantic.__version__}')" 2>nul

REM Run extractor
echo [Step 4/4] Launching extractor...
python "%SCRIPT_DIR%\extractor.py" --working_dir "%SCRIPT_DIR%"

echo Extraction complete. Check output\ for results.
pause
```

### Deliverable 1.2: config.json

```json
{
  "_comment": "Split-RAG Document Extractor Configuration v1.0.0",

  "paths": {
    "input_dir": "input",
    "output_dir": "output",
    "log_dir": "logs",
    "manifest_file": "output/manifest.json"
  },

  "chunking": {
    "max_chars_per_chunk": 6000,
    "overlap_chars": 200,
    "table_chunk_rows": 40
  },

  "docling": {
    "tableformer_enabled": true,
    "do_ocr": false,
    "timeout_seconds": 120
  },

  "image_filtering": {
    "edge_density_threshold": 0.10,
    "edge_threshold_value": 40,
    "enabled": true
  },

  "logging": {
    "level": "INFO",
    "format": "%(asctime)s | %(levelname)-8s | %(message)s"
  },

  "processing": {
    "skip_unchanged": true,
    "retry_failed": true
  }
}
```

### Deliverable 1.3: rules.json

```json
{
  "_comment": "Split-RAG Extraction Rules v1.0.0",

  "header_detection": {
    "all_caps_ratio": 0.80,
    "max_len": 100,
    "min_len": 3,
    "numbered_heading_regex": "^\\d+\\.(?:\\d+\\.)*\\s+",
    "additional_patterns": [
      "^Chapter\\s+\\d+",
      "^Section\\s+\\d+",
      "^Appendix\\s+[A-Z]"
    ]
  },

  "content_filtering": {
    "min_text_length": 10,
    "skip_patterns": [
      "^Page\\s+\\d+\\s*(of\\s+\\d+)?$",
      "^\\d+$",
      "^(CONFIDENTIAL|DRAFT|INTERNAL)$"
    ]
  },

  "confidence_adjustments": {
    "base_scores": {
      "docling": 0.95,
      "pdfplumber": 0.85,
      "office_native": 0.90
    },
    "heuristic_header_penalty": 0.10,
    "minimum_confidence": 0.50
  }
}
```

### Deliverable 1.4: requirements.txt

```text
# Split-RAG Dependencies v1.0.0
# Note: PyYAML NOT included (configs are JSON per CANON_003)

docling>=2.0.0
pdfplumber>=0.10.0
pydantic>=2.0.0
pandas>=2.0.0,<3.0.0
numpy>=1.24.0
openpyxl>=3.1.0
lxml>=4.9.0
Pillow>=10.0.0
tqdm>=4.65.0
```

---

### **STOP — PHASE 1 CHECKPOINT**

Before proceeding to Phase 2, confirm the following:

1. Are all four configuration files present and syntactically valid?
2. Is the directory structure created (`input/`, `output/`, `logs/`)?
3. Have forbidden dependencies been excluded from `requirements.txt`?

**Confirmation Question:** "Have you reviewed the Phase 1 deliverables? Reply 'NEXT' to proceed to Phase 2 (Schema & Extraction Engine)."

---

## Phase 2: Schema Definition & Extraction Engine

### Objectives
1. Define Pydantic models for Context Graph schema
2. Implement deterministic ID generation
3. Build extraction pipelines (Docling primary, pdfplumber fallback)
4. Implement Office native extractors (DOCX/XLSX)

### Deliverable 2.1: schema_v1.py

The schema defines the strict data contract for Context Graph JSON output. Key models:

- **ContextGraph** — Root container with document info and nodes
- **ContextNode** — Individual content chunk with metadata
- **DocumentInfo** — Document-level metadata (ID, type, status)
- **NodeMetadata** — Page number, bounding box, table shape

**Critical ID Patterns:**
```python
MD5_PATTERN = re.compile(r"^[a-f0-9]{32}$")
SECTION_ID_PATTERN = re.compile(r"^[a-f0-9]{16}$")
CHUNK_ID_PATTERN = re.compile(r"^[a-f0-9]{8}_[a-z_]+_[a-f0-9]{12}$")
```

**ID Generation Protocol (Deterministic):**
```python
def compute_file_md5(file_path: Path) -> str:
    """MD5 hash of file bytes — basis for document.id"""

def compute_parent_section_id(source_path: str, section_title: str) -> str:
    """MD5(source_path + "|" + normalized_title)[:16]"""

def compute_chunk_id(document_id: str, content_type: str, source_chunk_id: str) -> str:
    """Format: {doc_id[:8]}_{content_type}_{source_chunk_id[:12]}"""
```

### Deliverable 2.2: extractor.py

The extraction engine is **assembled** from specification sections:

| Component | Source Section | Type |
|-----------|----------------|------|
| ID generation | Section 2.3 | [EXACT] |
| File discovery | Section 3.2 | [EXACT] |
| Manifest management | Section 3.3 | [EXACT] |
| Docling extraction | Section 3.4 | [TEMPLATE] |
| pdfplumber fallback | Section 3.5 | [TEMPLATE] |
| Office native | Section 3.6 | [TEMPLATE] |
| Table conversion | Section 3.7 | [EXACT] |
| Text chunking | Section 3.8 | [EXACT] |
| Image filtering | Section 3.9 | [EXACT] |
| Confidence scoring | Section 3.10 | [EXACT] |
| Main skeleton | Section 5.6 | [EXACT] |

**Docling Field Mapping Contract:**
```
ELEMENT TYPE → content_type:
    "title", "section_header", "heading" → "header"
    "paragraph", "text", "list_item"    → "text"
    "table"                              → "table"
    "caption"                            → "image_caption"
    "picture"                            → skip (unless caption exists)
```

**Fallback Logic (Required):**
```python
try:
    elements, page_count, extractor = extract_with_docling(file_path, config)
except ExtractionError:
    logger.warning("Docling failed, switching to pdfplumber")
    elements, page_count, extractor = extract_with_pdfplumber(file_path, config, rules)
```

---

### **STOP — PHASE 2 CHECKPOINT**

Before proceeding to Phase 3, verify:

1. Does `schema_v1.py` validate all required fields with regex patterns?
2. Does `extractor.py` implement deterministic ID generation?
3. Is the Docling → pdfplumber fallback properly wrapped in try/except?
4. Are Office native extractors handling DOCX and XLSX?

**Confirmation Question:** "Have you reviewed the Phase 2 deliverables? Reply 'NEXT' to proceed to Phase 3 (Tier 2 Retrieval Logic)."

---

## Phase 3: Tier 2 Retrieval Logic

### Objectives
1. Implement sandbox-safe retrieval script
2. Create keyword density scoring algorithm
3. Build coherent context grouping
4. Add system prompt enforcement

### Deliverable 3.1: copilot_tier2.py

**Sandbox Constraints Reminder:**
- External dependencies: `pandas` ONLY
- Standard library: all permitted
- No file system writes
- No network access
- Max execution: 30 seconds

**Core Algorithm: Vectorized Keyword Density**

```python
def score_nodes(nodes: List[Dict], keywords: List[str]) -> pd.DataFrame:
    """
    Score = density * (1 + type_boost) * (1 + verified_boost)

    Where:
        density = raw_hits / word_count
        type_boost = 0.25 for tables, 0.10 for headers
        verified_boost = 0.10 if verified_content is True
    """
```

**Context Grouping Strategy:**
1. Find `parent_section_id` with highest aggregate score
2. Return nodes from that section
3. Order: header → text → table → image_caption
4. Truncate at `MAX_OUTPUT_CHARS` (6000)

**System Prompt Enforcement:**
```python
SYSTEM_PREFIX = """SYSTEM: You are a deterministic analysis assistant.
Your response must be based strictly on the provided facts.
Do not be creative. Do not hallucinate. Use temperature 0 behavior."""
```

---

### **STOP — PHASE 3 CHECKPOINT**

Before proceeding to Phase 4, verify:

1. Does `copilot_tier2.py` import ONLY pandas + stdlib?
2. Is the keyword density algorithm correctly implemented?
3. Does the output include the SYSTEM_PREFIX?
4. Is context grouping by parent_section_id working?

**Confirmation Question:** "Have you reviewed the Phase 3 deliverable? Reply 'NEXT' to proceed to Phase 4 (Testing & Validation)."

---

## Phase 4: Testing Suite & Validation

### Objectives
1. Execute determinism tests
2. Validate schema compliance
3. Verify fallback paths
4. Confirm Tier 2 functionality

### Test Matrix

| Test ID | Category | Expected Outcome | Pass Criteria |
|---------|----------|------------------|---------------|
| **T-001** | Determinism | Same MD5 + chunk_ids across runs | SHA-256 match |
| **T-002** | Schema | `validate_context_graph()` returns True | 0 validation errors |
| **T-003** | Docling | Tables as Markdown, headers identified | `extractor_used = "docling"` |
| **T-004** | Fallback | pdfplumber activates on Docling failure | `extractor_used = "pdfplumber"` |
| **T-005** | DOCX | Paragraphs + tables extracted | `extractor_used = "office_native"` |
| **T-006** | XLSX | Sheet-by-sheet table extraction | Table nodes created |
| **T-007** | Images | Data images included, decorative excluded | `is_data_image` flag set |
| **T-008** | Manifest | Unchanged files skipped | Skip logged |
| **T-009** | Tier 2 | Correct query results | Keywords matched |

### Schema Validation CLI Command

```bash
python -c "import json; from pathlib import Path; from schema_v1 import validate_context_graph; \
[print(f.name, 'OK' if validate_context_graph(json.loads(f.read_text())) else 'FAIL') \
for f in Path('output').glob('*.json')]"
```

### Determinism Test Protocol

1. Run `python extractor.py --working_dir .` twice on identical input
2. Compare output JSON files byte-for-byte
3. Verify: `document.id`, `chunk_id`, `parent_section_id` are identical
4. Only `processed_at` timestamp may differ

---

### **STOP — PHASE 4 CHECKPOINT**

Before final delivery, verify:

1. Do all T-001 through T-009 tests pass?
2. Is determinism confirmed across repeated runs?
3. Does at least one PDF exercise the fallback path?
4. Does Tier 2 return correct query results?

**Confirmation Question:** "Have you completed all testing? Reply 'COMPLETE' to finalize the implementation."

---

## Final Deliverables Checklist

### Code Artifacts (7 files)

| # | Filename | Type | Status |
|---|----------|------|--------|
| 1 | `bootstrap.bat` | Environment setup | |
| 2 | `config.json` | Operational config | |
| 3 | `rules.json` | Heuristic rules | |
| 4 | `requirements.txt` | Dependencies | |
| 5 | `schema_v1.py` | Pydantic schema | |
| 6 | `extractor.py` | Extraction engine | |
| 7 | `copilot_tier2.py` | Retrieval script | |

### Directory Structure

```
<WORKDIR>/
  bootstrap.bat
  extractor.py
  schema_v1.py
  copilot_tier2.py
  config.json
  rules.json
  requirements.txt
  input/
    <documents...>
  output/
    <document>.json
    manifest.json
  logs/
    processing.log
```

### Documentation Artifacts

| # | Filename | Purpose |
|---|----------|---------|
| 8 | `README.md` | Project overview |
| 9 | `ARCHITECTURE_OVERVIEW.md` | Non-binding context |
| 10 | `SPLIT_RAG_CODING_PROMPT.md` | This implementation guide |
| 11 | `code_instructions.txt` | Binding specification |

### Validation Artifacts

| # | Artifact | Purpose |
|---|----------|---------|
| 12 | Test PDFs | Docling + fallback coverage |
| 13 | Test DOCX | Office native coverage |
| 14 | Test XLSX | Spreadsheet coverage |
| 15 | `processing.log` | Audit trail |
| 16 | `manifest.json` | Incremental processing state |

---

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | All files processed or skipped successfully |
| 1 | Partial success (one or more files failed) |
| 2 | Total failure (no files processed) |

---

## Go/No-Go Gates

### GO Criteria
- All outputs validate against schema
- Deterministic IDs stable across runs
- Docling path works on at least one PDF
- Fallback path works on at least one PDF
- Office native works on DOCX and XLSX

### NO-GO Criteria
- Any invalid JSON output written
- Any forbidden import present
- Chunk IDs unstable without documented reason
- Tier 2 crashes on sandbox constraints

---

*End of SPLIT_RAG_CODING_PROMPT.md*

**Source of Truth:** `code_instructions.txt` (v2.1)
**This Document:** Procedural implementation guide with confirmation checkpoints
