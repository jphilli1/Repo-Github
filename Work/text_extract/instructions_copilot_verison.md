# instructions_copilot_verison.md
## Implementation Instructions for AI Agents

**Version:** 2.1
**Target:** GitHub Copilot, Claude, ChatGPT, and other code-generation AI models
**Mode:** Deterministic implementation with phased delivery

---

## Quick Start

You are tasked with implementing the Split-RAG v2.1 document extraction and retrieval system. This is a **four-phase implementation** with mandatory confirmation checkpoints.

### Before You Begin

1. **Read** `code_instructions.txt` — this is the binding specification
2. **Reference** `ARCHITECTURE_OVERVIEW.md` — for context (non-binding)
3. **Follow** `SPLIT_RAG_CODING_PROMPT.md` — for phased implementation

### Critical Rules

```
CANON_001: Python stdlib is always permitted
CANON_002: Tier 2 allows ONLY pandas + stdlib
CANON_003: Configuration files use JSON (not YAML)
CANON_004: [EXACT] blocks = copy verbatim; [TEMPLATE] = complete with logic
```

---

## Implementation Phases

### Phase 1: Environment Setup
**Deliverables:** `bootstrap.bat`, `config.json`, `rules.json`, `requirements.txt`

After completing Phase 1, STOP and confirm with user.

### Phase 2: Schema & Extraction
**Deliverables:** `schema_v1.py`, `extractor.py`

The `extractor.py` is assembled from multiple specification sections. Key components:
- ID generation (Section 2.3)
- Docling extraction (Section 3.4)
- pdfplumber fallback (Section 3.5)
- Office native (Section 3.6)

After completing Phase 2, STOP and confirm with user.

### Phase 3: Tier 2 Retrieval
**Deliverables:** `copilot_tier2.py`

Sandbox constraints are strict:
- External deps: `pandas` ONLY
- No pydantic, no numpy, no PIL
- 30 second max execution
- 256 MB max memory

After completing Phase 3, STOP and confirm with user.

### Phase 4: Testing & Validation
**Deliverables:** Test execution, validation results

Run the test matrix (T-001 through T-009) and confirm determinism.

After completing Phase 4, finalize with user.

---

## Delivery Protocol

For each file:

```
DELIVERABLE: <filename>

<code block>

STATUS: READY FOR REVIEW

ACTION: Reply "NEXT" to receive the next deliverable.
```

Wait for user to say "NEXT" before continuing.

---

## Forbidden Imports

### Tier 1 (extractor.py)
```python
# DO NOT USE:
import fitz           # pymupdf not available
import torch          # too heavy
import openai         # no API calls
import langchain      # not approved
import yaml           # use JSON
```

### Tier 2 (copilot_tier2.py)
```python
# DO NOT USE:
from pydantic import *  # not in sandbox
import numpy as np      # not guaranteed
import docling          # not in sandbox
import openpyxl         # not in sandbox
```

---

## Verification Checkpoints

Before each delivery, mentally verify:

| CP | Question | Fix |
|----|----------|-----|
| CP-001 | Type hints on all functions? | Add hints |
| CP-002 | Specific exception types? | Replace generic except |
| CP-003 | Paths use pathlib/raw strings? | Convert |
| CP-004 | Only approved imports? | Remove unauthorized |

---

## Key Algorithms

### Deterministic ID Generation
```python
document_id = MD5(file_bytes)  # 32 hex chars
parent_section_id = MD5(source_path + "|" + title)[:16]
chunk_id = f"{doc_id[:8]}_{content_type}_{source_chunk_id[:12]}"
```

### Keyword Density Scoring (Tier 2)
```python
density = raw_keyword_hits / word_count
type_boost = 0.25 if table else 0.10 if header else 0
verified_boost = 0.10 if verified_content else 0
score = density * (1 + type_boost) * (1 + verified_boost)
```

### Fallback Logic
```python
try:
    elements = extract_with_docling(file)
except ExtractionError:
    elements = extract_with_pdfplumber(file)  # Fallback
```

---

## Final Checklist

- [ ] 7 code artifacts delivered
- [ ] All tests pass (T-001 through T-009)
- [ ] Determinism confirmed
- [ ] No forbidden imports
- [ ] Schema validation passes
- [ ] Tier 2 works in sandbox

---

## Source of Truth

| Document | Purpose |
|----------|---------|
| `code_instructions.txt` | **BINDING** specification |
| `ARCHITECTURE_OVERVIEW.md` | Context (non-binding) |
| `SPLIT_RAG_CODING_PROMPT.md` | Phased implementation guide |
| This file | Quick reference for AI agents |

---

*Version 2.1 — JSON configs, phased delivery, sandbox-safe Tier 2*
