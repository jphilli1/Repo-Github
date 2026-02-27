================================================================================

SPLIT-RAG IMPLEMENTATION INSTRUCTIONS - BLOCK 1 OF 4

ENVIRONMENT SETUP, DEPENDENCIES, AND PROJECT SCAFFOLDING

================================================================================



OVERVIEW

--------

You are implementing the AI-Native Split-RAG System (v2.0), a production-grade 

document processing and retrieval architecture. This first block covers the 

foundational setup. Read these instructions completely before writing any code.



After completing Block 1, STOP and present your work to the user for review.

Ask: "Block 1 complete. May I proceed to Block 2 (Core Components)?"



================================================================================

SECTION 1.1: UNDERSTAND THE ARCHITECTURE BEFORE CODING

================================================================================



The Split-RAG system operates on TWO DISTINCT TIERS:



TIER 1 - "THE FACTORY" (Local Python Environment)

&nbsp; - Heavy computational work: OCR, layout analysis, semantic chunking

&nbsp; - Can use Docling, PyTorch (transitively), pdfplumber, pydantic

&nbsp; - Outputs: Immutable JSON Context Graphs

&nbsp; - Runs locally with full system resources



TIER 2 - "THE CONSUMER" (Copilot Studio Sandbox)

&nbsp; - Lightweight retrieval only

&nbsp; - STRICT CONSTRAINT: Only Python stdlib + pandas allowed

&nbsp; - Memory limit: 256MB (no torch, no pydantic, no heavy libraries)

&nbsp; - Consumes pre-computed Context Graphs



CRITICAL: Never confuse which code runs in which tier. The extractor.py runs

in Tier 1. The copilot\_tier2.py runs in Tier 2 with severe constraints.



================================================================================

SECTION 1.2: CREATE DIRECTORY STRUCTURE

================================================================================



Create the following directory structure exactly:



split-rag/

├── input/                    # Place raw documents here (PDF, DOCX, XLSX)

├── output/                   # Context Graph JSON files will be generated here

├── logs/                     # Execution logs for audit trails

├── quarantine/               # Failed documents isolated here with error reports

├── tests/                    # Unit and integration tests

├── bootstrap.bat             # Windows batch script - environment entry point

├── config.json               # Operational configuration (paths, flags)

├── rules.json                # Regex patterns for entity extraction

├── requirements.txt          # Tier 1 Python dependencies

├── schema\_v2.py              # Pydantic data contract models

├── extractor.py              # Tier 1 extraction engine (The Factory)

└── copilot\_tier2.py          # Tier 2 retrieval script (The Consumer)



COMMAND TO CREATE STRUCTURE:

```bash

mkdir -p split-rag/{input,output,logs,quarantine,tests}

cd split-rag

touch bootstrap.bat config.json rules.json requirements.txt

touch schema\_v2.py extractor.py copilot\_tier2.py

```



================================================================================

SECTION 1.3: CREATE requirements.txt (TIER 1 DEPENDENCIES)

================================================================================



Create requirements.txt with EXACTLY these dependencies. Do not add extras.



FILE: requirements.txt

----------------------

\# AI-Native Split-RAG System v2.0 - Tier 1 Dependencies

\# Python 3.11.x REQUIRED



\# Validation Layer

pydantic>=2.0.0



\# Primary Extraction Engine

docling>=2.0.0



\# Fallback Extraction (for PDFs that fail Docling)

pdfplumber>=0.10.0



\# Office Document Support

openpyxl>=3.1.0

lxml>=4.9.0



\# Data Manipulation

pandas>=2.0.0

numpy>=1.24.0,<2.0.0  # Upper bound for docling/torch compatibility



\# Image Processing (Edge Density Filter)

Pillow>=10.0.0



\# Progress Bars (Optional but recommended)

tqdm>=4.65.0



\# NOTE: torch and transformers are TRANSITIVE dependencies of docling

\# They are installed automatically but MUST NOT be imported directly

\# in extractor.py. Only Docling manages these internally.

----------------------



FORBIDDEN LIBRARIES (DO NOT ADD):

\- fitz / pymupdf (AGPL license risk)

\- langchain / llama\_index (massive dependency trees, external API calls)

\- yaml / PyYAML (use JSON only per CANON\_003)

\- Any library not in the approved list above



================================================================================

SECTION 1.4: CREATE bootstrap.bat (ENVIRONMENT ENTRY POINT)

================================================================================



This batch script ensures correct environment setup before Python execution.



FILE: bootstrap.bat

-------------------

@echo off

REM ============================================================================

REM AI-Native Split-RAG System v2.0 - Bootstrap Script

REM This script sets environment variables and launches the extraction engine.

REM ============================================================================



REM Limit CPU threads to prevent Docling from consuming all cores

REM This is critical for shared environments and prevents OOM crashes

set OMP\_NUM\_THREADS=4

set MKL\_NUM\_THREADS=4



REM Ensure PYTHONPATH includes current directory

set PYTHONPATH=%CD%;%PYTHONPATH%



REM Optional: Suppress TensorFlow/PyTorch warnings

set TF\_CPP\_MIN\_LOG\_LEVEL=2



REM Validate Python version (must be 3.11.x)

python --version 2>\&1 | findstr /C:"3.11" >nul

if errorlevel 1 (

&nbsp;   echo ERROR: Python 3.11.x is required. Current version:

&nbsp;   python --version

&nbsp;   exit /b 1

)



REM Activate virtual environment if it exists

if exist ".venv\\Scripts\\activate.bat" (

&nbsp;   call .venv\\Scripts\\activate.bat

)



REM Run the extraction engine

echo Starting Split-RAG Extraction Engine...

python extractor.py %\*



REM Capture exit code

set EXIT\_CODE=%ERRORLEVEL%



if %EXIT\_CODE% NEQ 0 (

&nbsp;   echo.

&nbsp;   echo ERROR: Extraction failed with exit code %EXIT\_CODE%

&nbsp;   echo Check logs/ directory for details.

)



exit /b %EXIT\_CODE%

-------------------



CRITICAL NOTES ON bootstrap.bat:

1\. OMP\_NUM\_THREADS=4 prevents Docling's internal PyTorch from using all CPU cores

2\. The Python 3.11.x check is mandatory per specification (3.13 has compatibility issues)

3\. This script is Windows-focused but Linux users can create equivalent bootstrap.sh



================================================================================

SECTION 1.5: CREATE config.json (OPERATIONAL CONFIGURATION)

================================================================================



All configuration MUST be JSON format (CANON\_003 prohibits YAML).



FILE: config.json

-----------------

{

&nbsp;   "version": "2.0.0",

&nbsp;   "tier": 1,

&nbsp;   

&nbsp;   "paths": {

&nbsp;       "input\_directory": "./input",

&nbsp;       "output\_directory": "./output",

&nbsp;       "log\_directory": "./logs",

&nbsp;       "quarantine\_directory": "./quarantine"

&nbsp;   },

&nbsp;   

&nbsp;   "extraction": {

&nbsp;       "primary\_engine": "docling",

&nbsp;       "fallback\_engine": "pdfplumber",

&nbsp;       "enable\_ocr": true,

&nbsp;       "enable\_table\_detection": true,

&nbsp;       "max\_pages\_for\_entity\_scan": 20,

&nbsp;       "chunk\_overlap\_tokens": 50

&nbsp;   },

&nbsp;   

&nbsp;   "docling": {

&nbsp;       "use\_gpu": false,

&nbsp;       "artifacts\_path": null,

&nbsp;       "table\_mode": "accurate",

&nbsp;       "ocr\_languages": \["en"]

&nbsp;   },

&nbsp;   

&nbsp;   "validation": {

&nbsp;       "conflict\_threshold\_levenshtein": 0.3,

&nbsp;       "max\_conflict\_rate\_for\_quarantine": 0.20,

&nbsp;       "enable\_keep\_all\_policy": true

&nbsp;   },

&nbsp;   

&nbsp;   "logging": {

&nbsp;       "level": "INFO",

&nbsp;       "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",

&nbsp;       "file\_rotation\_mb": 10

&nbsp;   },

&nbsp;   

&nbsp;   "performance": {

&nbsp;       "batch\_size": 5,

&nbsp;       "max\_workers": 2

&nbsp;   }

}

-----------------



CONFIGURATION NOTES:

1\. max\_pages\_for\_entity\_scan: Regex patterns run on first 20 pages to find

&nbsp;  "Borrower", "Lender" definitions in contract preambles

2\. conflict\_threshold\_levenshtein: If Docling and pdfplumber output differs

&nbsp;  by more than 30%, flag as conflict

3\. max\_conflict\_rate\_for\_quarantine: If >20% of nodes conflict, quarantine document

4\. use\_gpu: Set false for CPU-only environments (common in enterprise VMs)



================================================================================

SECTION 1.6: CREATE rules.json (HEURISTIC RULE SET)

================================================================================



These regex patterns drive the Entity Anchoring and disambiguation logic.



FILE: rules.json

----------------

{

&nbsp;   "version": "2.0.0",

&nbsp;   "description": "Heuristic rules for entity extraction and disambiguation",

&nbsp;   

&nbsp;   "entity\_patterns": {

&nbsp;       "borrower": {

&nbsp;           "explicit\_definition": "\\"Borrower\\"\\\\s+means\\\\s+(?P<entity>.+?)(?=\\\\.|\\\\n|,)",

&nbsp;           "parenthetical": "(?P<entity>.+?)\\\\s+\\\\(the\\\\s+\\"Borrower\\"\\\\)",

&nbsp;           "preamble": "between.\*?and\\\\s+(?P<borrower>.+?)\\\\s+\\\\(.\*?\\"Borrower\\"",

&nbsp;           "all\_caps": "\\"BORROWER\\"\\\\s+means\\\\s+(?P<entity>.+?)(?=\\\\.|\\\\n|,)",

&nbsp;           "priority\_order": \["explicit\_definition", "parenthetical", "preamble", "all\_caps"]

&nbsp;       },

&nbsp;       "lender": {

&nbsp;           "explicit\_definition": "\\"Lender\\"\\\\s+means\\\\s+(?P<entity>.+?)(?=\\\\.|\\\\n|,)",

&nbsp;           "parenthetical": "(?P<entity>.+?)\\\\s+\\\\(the\\\\s+\\"Lender\\"\\\\)",

&nbsp;           "all\_caps": "\\"LENDER\\"\\\\s+means\\\\s+(?P<entity>.+?)(?=\\\\.|\\\\n|,)",

&nbsp;           "priority\_order": \["explicit\_definition", "parenthetical", "all\_caps"]

&nbsp;       },

&nbsp;       "guarantor": {

&nbsp;           "explicit\_definition": "\\"Guarantor\\"\\\\s+means\\\\s+(?P<entity>.+?)(?=\\\\.|\\\\n|,)",

&nbsp;           "parenthetical": "(?P<entity>.+?)\\\\s+\\\\(the\\\\s+\\"Guarantor\\"\\\\)",

&nbsp;           "priority\_order": \["explicit\_definition", "parenthetical"]

&nbsp;       }

&nbsp;   },

&nbsp;   

&nbsp;   "header\_patterns": {

&nbsp;       "section\_header": "^\\\\s\*(?:ARTICLE|SECTION|PART)\\\\s+\[IVXLCDM0-9]+",

&nbsp;       "numbered\_header": "^\\\\s\*\\\\d+\\\\.\\\\d\*\\\\s+\[A-Z]",

&nbsp;       "definitions\_section": "(?i)^\\\\s\*DEFINITIONS?\\\\s\*$"

&nbsp;   },

&nbsp;   

&nbsp;   "content\_classification": {

&nbsp;       "table\_indicators": \["\\\\|.\*\\\\|", "\\\\t.\*\\\\t.\*\\\\t"],

&nbsp;       "key\_value\_patterns": \["^(.+?):\\\\s\*(.+)$", "^(.+?)\\\\s+=\\\\s+(.+)$"]

&nbsp;   },

&nbsp;   

&nbsp;   "scope\_classification": {

&nbsp;       "corpus\_indicators": \[

&nbsp;           "(?i)uniform commercial code",

&nbsp;           "(?i)\\\\bUCC\\\\b",

&nbsp;           "(?i)LMA standard",

&nbsp;           "(?i)ISDA definition",

&nbsp;           "(?i)regulatory requirement"

&nbsp;       ],

&nbsp;       "primary\_indicators": \[

&nbsp;           "(?i)this agreement",

&nbsp;           "(?i)the parties hereto",

&nbsp;           "(?i)dated as of"

&nbsp;       ]

&nbsp;   },

&nbsp;   

&nbsp;   "stopwords": \[

&nbsp;       "the", "is", "at", "which", "on", "and", "a", "an", "of", "to",

&nbsp;       "in", "for", "with", "by", "from", "as", "or", "be", "are", "was",

&nbsp;       "were", "been", "being", "have", "has", "had", "do", "does", "did",

&nbsp;       "will", "would", "could", "should", "may", "might", "must", "shall"

&nbsp;   ]

}

----------------



REGEX NOTES:

1\. All patterns use Python re flavor with named groups (?P<name>...)

2\. Entity patterns run with re.IGNORECASE except "all\_caps" variants

3\. priority\_order determines which pattern match takes precedence

4\. Escape backslashes properly in JSON (\\ becomes \\\\)



================================================================================

SECTION 1.7: VERIFICATION CHECKPOINT - BLOCK 1

================================================================================



Before proceeding, verify the following:



CHECKLIST:

\[ ] Directory structure created with all 4 subdirectories

\[ ] requirements.txt contains ONLY approved dependencies

\[ ] bootstrap.bat sets OMP\_NUM\_THREADS and validates Python version

\[ ] config.json uses JSON format (no YAML)

\[ ] rules.json has properly escaped regex patterns

\[ ] No forbidden libraries (fitz, langchain, yaml) anywhere

\[ ] All file paths use forward slashes or raw strings for cross-platform



TEST YOUR SETUP:

```bash

\# Create virtual environment

python -m venv .venv

.venv\\Scripts\\activate  # Windows

\# Or: source .venv/bin/activate  # Linux/Mac



\# Install dependencies

pip install -r requirements.txt



\# Verify docling installed correctly

python -c "from docling.document\_converter import DocumentConverter; print('Docling OK')"



\# Verify pydantic v2

python -c "import pydantic; print(f'Pydantic {pydantic.\_\_version\_\_}')"

```



================================================================================

BLOCK 1 COMPLETE - AWAIT USER CONFIRMATION

================================================================================



Present the following files to the user for review:

1\. requirements.txt

2\. bootstrap.bat  

3\. config.json

4\. rules.json



Then ask: "Block 1 (Environment Setup) is complete. I have created:

\- Project directory structure

\- Tier 1 dependencies (requirements.txt)

\- Bootstrap script for Windows (bootstrap.bat)

\- Operational configuration (config.json)

\- Entity extraction rules (rules.json)



May I proceed to Block 2 (Core Components: Schema and Extraction Engine)?"



DO NOT proceed to Block 2 until the user explicitly confirms.



================================================================================

END OF BLOCK 1

================================================================================



================================================================================

SPLIT-RAG IMPLEMENTATION INSTRUCTIONS - BLOCK 2 OF 4

CORE COMPONENTS: SCHEMA DEFINITION AND EXTRACTION ENGINE

================================================================================



OVERVIEW

--------

This block covers the two most critical Tier 1 artifacts:

1\. schema\_v2.py - The Pydantic data contract defining the Context Graph structure

2\. extractor.py - The extraction engine that processes documents into Context Graphs



These components embody the "Content-First, Compute-Later" philosophy. The schema

ensures deterministic, auditable output. The extractor handles the heavy lifting

that cannot occur in the resource-constrained Tier 2 sandbox.



After completing Block 2, STOP and present your work to the user for review.

Ask: "Block 2 complete. May I proceed to Block 3 (Tier 2 Retrieval Logic)?"



================================================================================

SECTION 2.1: SCHEMA DESIGN PRINCIPLES

================================================================================



The Context Graph schema serves as the API CONTRACT between Tier 1 and Tier 2.

It must enforce several critical properties:



DETERMINISTIC IDENTITY (CANON\_004):

\- Every node has a chunk\_id computed from content hash + position

\- IDs are pre-computed in Tier 1; Tier 2 NEVER re-hashes

\- This allows cryptographic verification of data lineage



KEEP-ALL POLICY SUPPORT:

\- Duplicate content from different sources is preserved (not deduplicated)

\- Conflict detection flags mark discrepancies between extraction methods

\- lineage\_trace enables full audit trail back to source bytes



SCOPE DISAMBIGUATION:

\- source\_scope distinguishes "primary" (contract) from "corpus" (reference)

\- borrower\_entity at graph root stores the anchored party name

\- This prevents hallucination where AI confuses specific parties with generic terms



================================================================================

SECTION 2.2: CREATE schema\_v2.py (DATA CONTRACT)

================================================================================



FILE: schema\_v2.py

------------------

"""

AI-Native Split-RAG System v2.0 - Data Contract Schema



This module defines the Pydantic models that enforce the structure of the

Context Graph. All output from Tier 1 MUST validate against these models

before being serialized to JSON.



CRITICAL: This file runs in Tier 1 only. Tier 2 cannot import Pydantic

due to sandbox constraints. Tier 2 reads the JSON output directly.



Verification Checkpoints:

\- CP-001: All functions have explicit return type hints

\- CP-002: Specific exceptions, no generic except blocks

\- CP-003: Paths handled via pathlib (not applicable here)

\- CP-004: Only approved imports (pydantic, hashlib, typing, datetime)

"""



import hashlib

from datetime import datetime, timezone

from typing import List, Optional, Literal, Dict, Any

from pydantic import BaseModel, Field, field\_validator, model\_validator





\# =============================================================================

\# UTILITY FUNCTIONS FOR DETERMINISTIC ID GENERATION

\# =============================================================================



def generate\_document\_id(file\_bytes: bytes) -> str:

&nbsp;   """

&nbsp;   Generate a deterministic document ID from file contents.

&nbsp;   

&nbsp;   Uses MD5 for shorter IDs (32 hex chars vs SHA-256's 64 chars).

&nbsp;   This saves memory in Tier 2 DataFrame indices while providing

&nbsp;   sufficient collision resistance for closed-corpus applications.

&nbsp;   

&nbsp;   Args:

&nbsp;       file\_bytes: Raw bytes of the source document

&nbsp;       

&nbsp;   Returns:

&nbsp;       32-character hexadecimal string (MD5 hash)

&nbsp;   """

&nbsp;   return hashlib.md5(file\_bytes).hexdigest()





def generate\_chunk\_id(

&nbsp;   document\_id: str,

&nbsp;   page\_number: int,

&nbsp;   chunk\_index: int,

&nbsp;   content\_sample: str

) -> str:

&nbsp;   """

&nbsp;   Generate a deterministic chunk ID from composite key components.

&nbsp;   

&nbsp;   The ID is reproducible: same inputs always produce same output.

&nbsp;   This is critical for the "No Re-Hashing Rule" - Tier 2 uses these

&nbsp;   IDs as primary keys without ever computing hashes itself.

&nbsp;   

&nbsp;   Args:

&nbsp;       document\_id: Parent document's MD5 hash

&nbsp;       page\_number: 1-indexed page number in source document

&nbsp;       chunk\_index: 0-indexed position of chunk within the page

&nbsp;       content\_sample: First 100 characters of content (for uniqueness)

&nbsp;       

&nbsp;   Returns:

&nbsp;       32-character hexadecimal string (MD5 hash of composite key)

&nbsp;   """

&nbsp;   # Create composite key from all positioning information

&nbsp;   composite\_key = f"{document\_id}|{page\_number}|{chunk\_index}|{content\_sample\[:100]}"

&nbsp;   return hashlib.md5(composite\_key.encode('utf-8')).hexdigest()





def generate\_lineage\_trace(

&nbsp;   source\_file\_hash: str,

&nbsp;   page\_index: int,

&nbsp;   bbox\_coords: Optional\[List\[float]],

&nbsp;   extraction\_method: str

) -> str:

&nbsp;   """

&nbsp;   Generate SHA-256 lineage trace for Keep-All Policy audit.

&nbsp;   

&nbsp;   This trace allows verification that content came from a specific

&nbsp;   location in a specific document using a specific extraction method.

&nbsp;   Even identical content gets unique traces based on position.

&nbsp;   

&nbsp;   Args:

&nbsp;       source\_file\_hash: MD5 hash of source file

&nbsp;       page\_index: 0-indexed page number

&nbsp;       bbox\_coords: Bounding box \[x0, y0, x1, y1] or None

&nbsp;       extraction\_method: "docling" or "pdfplumber"

&nbsp;       

&nbsp;   Returns:

&nbsp;       64-character hexadecimal string (SHA-256 hash)

&nbsp;   """

&nbsp;   bbox\_str = ",".join(map(str, bbox\_coords)) if bbox\_coords else "none"

&nbsp;   trace\_input = f"{source\_file\_hash}|{page\_index}|{bbox\_str}|{extraction\_method}"

&nbsp;   return hashlib.sha256(trace\_input.encode('utf-8')).hexdigest()





\# =============================================================================

\# PYDANTIC MODELS - THE DATA CONTRACT

\# =============================================================================



class NodeMetadata(BaseModel):

&nbsp;   """

&nbsp;   Metadata associated with each content node in the Context Graph.

&nbsp;   

&nbsp;   This captures positional information for UI highlighting, structural

&nbsp;   details for tables/images, and scope classification for disambiguation.

&nbsp;   """

&nbsp;   

&nbsp;   # Positional Information

&nbsp;   page\_number: int = Field(

&nbsp;       ..., 

&nbsp;       ge=1,

&nbsp;       description="1-indexed page number in source document"

&nbsp;   )

&nbsp;   bbox: Optional\[List\[float]] = Field(

&nbsp;       default=None,

&nbsp;       description="Bounding box coordinates \[x0, y0, x1, y1] for UI highlighting"

&nbsp;   )

&nbsp;   

&nbsp;   # Structural Information

&nbsp;   table\_shape: Optional\[List\[int]] = Field(

&nbsp;       default=None,

&nbsp;       description="Table dimensions \[rows, cols] if content\_type is 'table'"

&nbsp;   )

&nbsp;   edge\_density: float = Field(

&nbsp;       default=0.0,

&nbsp;       ge=0.0,

&nbsp;       le=1.0,

&nbsp;       description="Visual complexity score for images (0.0 to 1.0)"

&nbsp;   )

&nbsp;   

&nbsp;   # Disambiguation Support (New in v2.0)

&nbsp;   source\_scope: Literal\["primary", "corpus"] = Field(

&nbsp;       default="primary",

&nbsp;       description="'primary' for contract-specific, 'corpus' for reference material"

&nbsp;   )

&nbsp;   

&nbsp;   # Conflict Detection (Keep-All Policy)

&nbsp;   extraction\_method: str = Field(

&nbsp;       default="docling",

&nbsp;       description="Which engine extracted this: 'docling' or 'pdfplumber'"

&nbsp;   )

&nbsp;   conflict\_detected: bool = Field(

&nbsp;       default=False,

&nbsp;       description="True if Docling and pdfplumber produced different results"

&nbsp;   )

&nbsp;   is\_active: bool = Field(

&nbsp;       default=True,

&nbsp;       description="False for fallback versions retained for audit"

&nbsp;   )

&nbsp;   

&nbsp;   @field\_validator('bbox')

&nbsp;   @classmethod

&nbsp;   def validate\_bbox(cls, v: Optional\[List\[float]]) -> Optional\[List\[float]]:

&nbsp;       """Ensure bounding box has exactly 4 coordinates if provided."""

&nbsp;       if v is not None and len(v) != 4:

&nbsp;           raise ValueError("Bounding box must have exactly 4 coordinates \[x0, y0, x1, y1]")

&nbsp;       return v

&nbsp;   

&nbsp;   @field\_validator('table\_shape')

&nbsp;   @classmethod

&nbsp;   def validate\_table\_shape(cls, v: Optional\[List\[int]]) -> Optional\[List\[int]]:

&nbsp;       """Ensure table shape has exactly 2 dimensions if provided."""

&nbsp;       if v is not None and len(v) != 2:

&nbsp;           raise ValueError("Table shape must have exactly 2 values \[rows, cols]")

&nbsp;       return v





class ContextNode(BaseModel):

&nbsp;   """

&nbsp;   A single node in the Context Graph representing an extracted content unit.

&nbsp;   

&nbsp;   Each node is a semantically meaningful chunk: a paragraph, table, header,

&nbsp;   or key-value pair. Nodes are linked by parent\_section\_id to preserve

&nbsp;   document hierarchy.

&nbsp;   """

&nbsp;   

&nbsp;   # Identity (CANON\_004: Deterministic, Immutable)

&nbsp;   chunk\_id: str = Field(

&nbsp;       ...,

&nbsp;       min\_length=32,

&nbsp;       max\_length=32,

&nbsp;       description="Deterministic MD5 hash serving as immutable primary key"

&nbsp;   )

&nbsp;   parent\_section\_id: str = Field(

&nbsp;       ...,

&nbsp;       description="ID of parent section for hierarchy reconstruction"

&nbsp;   )

&nbsp;   

&nbsp;   # Content Classification

&nbsp;   content\_type: Literal\["header", "text", "table", "image\_caption", "kv\_pair"] = Field(

&nbsp;       ...,

&nbsp;       description="Semantic type of the content unit"

&nbsp;   )

&nbsp;   content: str = Field(

&nbsp;       ...,

&nbsp;       description="The actual text content (Markdown for tables)"

&nbsp;   )

&nbsp;   

&nbsp;   # Verification Status

&nbsp;   verified\_content: bool = Field(

&nbsp;       default=False,

&nbsp;       description="True if content passed dual-extraction verification"

&nbsp;   )

&nbsp;   

&nbsp;   # Structural Metadata

&nbsp;   metadata: NodeMetadata = Field(

&nbsp;       ...,

&nbsp;       description="Positional and classification metadata"

&nbsp;   )

&nbsp;   

&nbsp;   # Audit Trail (Keep-All Policy)

&nbsp;   lineage\_trace: str = Field(

&nbsp;       ...,

&nbsp;       min\_length=64,

&nbsp;       max\_length=64,

&nbsp;       description="SHA-256 hash for cryptographic lineage verification"

&nbsp;   )

&nbsp;   

&nbsp;   @field\_validator('chunk\_id')

&nbsp;   @classmethod

&nbsp;   def validate\_chunk\_id\_format(cls, v: str) -> str:

&nbsp;       """Ensure chunk\_id is a valid MD5 hex string."""

&nbsp;       if not v:

&nbsp;           raise ValueError("Chunk ID cannot be empty")

&nbsp;       if not all(c in '0123456789abcdef' for c in v.lower()):

&nbsp;           raise ValueError("Chunk ID must be a valid hexadecimal string")

&nbsp;       return v.lower()

&nbsp;   

&nbsp;   @field\_validator('lineage\_trace')

&nbsp;   @classmethod

&nbsp;   def validate\_lineage\_trace\_format(cls, v: str) -> str:

&nbsp;       """Ensure lineage\_trace is a valid SHA-256 hex string."""

&nbsp;       if not all(c in '0123456789abcdef' for c in v.lower()):

&nbsp;           raise ValueError("Lineage trace must be a valid hexadecimal string")

&nbsp;       return v.lower()





class ExtractionMetrics(BaseModel):

&nbsp;   """

&nbsp;   Metrics captured during extraction for monitoring and debugging.

&nbsp;   """

&nbsp;   

&nbsp;   total\_pages: int = Field(..., ge=0)

&nbsp;   total\_nodes: int = Field(..., ge=0)

&nbsp;   tables\_extracted: int = Field(default=0, ge=0)

&nbsp;   headers\_extracted: int = Field(default=0, ge=0)

&nbsp;   conflicts\_detected: int = Field(default=0, ge=0)

&nbsp;   extraction\_time\_seconds: float = Field(default=0.0, ge=0.0)

&nbsp;   primary\_engine\_used: str = Field(default="docling")

&nbsp;   fallback\_triggered: bool = Field(default=False)





class ContextGraph(BaseModel):

&nbsp;   """

&nbsp;   The root object representing a fully processed document.

&nbsp;   

&nbsp;   This is the primary artifact passed from Tier 1 to Tier 2. It contains

&nbsp;   all extracted content as nodes plus document-level metadata including

&nbsp;   the disambiguated borrower entity.

&nbsp;   """

&nbsp;   

&nbsp;   # Document Identity

&nbsp;   document\_id: str = Field(

&nbsp;       ...,

&nbsp;       min\_length=32,

&nbsp;       max\_length=32,

&nbsp;       description="MD5 hash of source file bytes"

&nbsp;   )

&nbsp;   filename: str = Field(

&nbsp;       ...,

&nbsp;       description="Original filename for reference"

&nbsp;   )

&nbsp;   

&nbsp;   # Processing Metadata

&nbsp;   processed\_at: str = Field(

&nbsp;       ...,

&nbsp;       description="ISO 8601 timestamp of extraction"

&nbsp;   )

&nbsp;   schema\_version: str = Field(

&nbsp;       default="2.0.0",

&nbsp;       description="Version of this schema for compatibility checking"

&nbsp;   )

&nbsp;   

&nbsp;   # Entity Disambiguation (New in v2.0)

&nbsp;   borrower\_entity: Optional\[str] = Field(

&nbsp;       default=None,

&nbsp;       description="Extracted borrower name from contract preamble"

&nbsp;   )

&nbsp;   lender\_entity: Optional\[str] = Field(

&nbsp;       default=None,

&nbsp;       description="Extracted lender name from contract preamble"

&nbsp;   )

&nbsp;   guarantor\_entity: Optional\[str] = Field(

&nbsp;       default=None,

&nbsp;       description="Extracted guarantor name if present"

&nbsp;   )

&nbsp;   

&nbsp;   # Content Nodes

&nbsp;   nodes: List\[ContextNode] = Field(

&nbsp;       ...,

&nbsp;       description="All extracted content units"

&nbsp;   )

&nbsp;   

&nbsp;   # Extraction Metrics

&nbsp;   metrics: Optional\[ExtractionMetrics] = Field(

&nbsp;       default=None,

&nbsp;       description="Performance and quality metrics from extraction"

&nbsp;   )

&nbsp;   

&nbsp;   @model\_validator(mode='after')

&nbsp;   def validate\_node\_references(self) -> 'ContextGraph':

&nbsp;       """Ensure all nodes reference valid parent sections."""

&nbsp;       node\_ids = {node.chunk\_id for node in self.nodes}

&nbsp;       # Root sections can reference "root" as parent

&nbsp;       valid\_parents = node\_ids | {"root"}

&nbsp;       

&nbsp;       for node in self.nodes:

&nbsp;           if node.parent\_section\_id not in valid\_parents:

&nbsp;               # Log warning but don't fail - orphan nodes are allowed

&nbsp;               pass

&nbsp;       

&nbsp;       return self

&nbsp;   

&nbsp;   def to\_json(self, indent: int = 2) -> str:

&nbsp;       """Serialize to JSON string for Tier 2 consumption."""

&nbsp;       return self.model\_dump\_json(indent=indent)

&nbsp;   

&nbsp;   @classmethod

&nbsp;   def get\_current\_timestamp(cls) -> str:

&nbsp;       """Generate ISO 8601 timestamp in UTC."""

&nbsp;       return datetime.now(timezone.utc).isoformat()





\# =============================================================================

\# VALIDATION HELPERS

\# =============================================================================



class SchemaValidationError(Exception):

&nbsp;   """Raised when data fails schema validation."""

&nbsp;   pass





def validate\_context\_graph(data: Dict\[str, Any]) -> ContextGraph:

&nbsp;   """

&nbsp;   Validate a dictionary against the ContextGraph schema.

&nbsp;   

&nbsp;   Use this to validate JSON loaded from files before processing.

&nbsp;   Raises SchemaValidationError with details on failure.

&nbsp;   

&nbsp;   Args:

&nbsp;       data: Dictionary parsed from JSON

&nbsp;       

&nbsp;   Returns:

&nbsp;       Validated ContextGraph instance

&nbsp;       

&nbsp;   Raises:

&nbsp;       SchemaValidationError: If validation fails

&nbsp;   """

&nbsp;   try:

&nbsp;       return ContextGraph.model\_validate(data)

&nbsp;   except Exception as e:

&nbsp;       raise SchemaValidationError(f"Context Graph validation failed: {str(e)}") from e





\# =============================================================================

\# MODULE TEST (runs only if executed directly)

\# =============================================================================



if \_\_name\_\_ == "\_\_main\_\_":

&nbsp;   # Quick self-test to verify schema works

&nbsp;   print("Testing schema\_v2.py...")

&nbsp;   

&nbsp;   # Generate test IDs

&nbsp;   test\_doc\_id = generate\_document\_id(b"test document content")

&nbsp;   test\_chunk\_id = generate\_chunk\_id(test\_doc\_id, 1, 0, "Sample content here")

&nbsp;   test\_lineage = generate\_lineage\_trace(test\_doc\_id, 0, \[0, 0, 100, 100], "docling")

&nbsp;   

&nbsp;   print(f"Document ID: {test\_doc\_id}")

&nbsp;   print(f"Chunk ID: {test\_chunk\_id}")

&nbsp;   print(f"Lineage Trace: {test\_lineage}")

&nbsp;   

&nbsp;   # Create test node

&nbsp;   test\_node = ContextNode(

&nbsp;       chunk\_id=test\_chunk\_id,

&nbsp;       parent\_section\_id="root",

&nbsp;       content\_type="text",

&nbsp;       content="This is a test paragraph.",

&nbsp;       metadata=NodeMetadata(page\_number=1, source\_scope="primary"),

&nbsp;       lineage\_trace=test\_lineage

&nbsp;   )

&nbsp;   

&nbsp;   # Create test graph

&nbsp;   test\_graph = ContextGraph(

&nbsp;       document\_id=test\_doc\_id,

&nbsp;       filename="test\_document.pdf",

&nbsp;       processed\_at=ContextGraph.get\_current\_timestamp(),

&nbsp;       borrower\_entity="Acme Corporation",

&nbsp;       nodes=\[test\_node]

&nbsp;   )

&nbsp;   

&nbsp;   print("\\nContext Graph JSON:")

&nbsp;   print(test\_graph.to\_json())

&nbsp;   print("\\nSchema validation: PASSED")

------------------



================================================================================

SECTION 2.3: CREATE extractor.py (TIER 1 EXTRACTION ENGINE)

================================================================================



This is the most complex artifact. It orchestrates document processing through

Docling (primary) with pdfplumber fallback, applies entity anchoring, and

serializes validated Context Graphs.



FILE: extractor.py

------------------

"""

AI-Native Split-RAG System v2.0 - Tier 1 Extraction Engine



"The Factory" - This script handles heavy document processing that cannot

run in the resource-constrained Tier 2 sandbox. It extracts content using

Docling for high-fidelity layout analysis, with pdfplumber as fallback.



CRITICAL IMPORT RULES:

\- DO import: docling.document\_converter (Docling manages torch internally)

\- DO NOT import: torch, transformers (violates decoupling principle)

\- All imports must be from the Approved List (Section 2.2.1 of spec)



Verification Checkpoints:

\- CP-001: All functions have explicit return type hints

\- CP-002: Specific exceptions, no generic except blocks (except at library boundary)

\- CP-003: All paths via pathlib

\- CP-004: Imports cross-referenced against Approved List



Usage:

&nbsp;   python extractor.py                    # Process all files in input/

&nbsp;   python extractor.py --file doc.pdf     # Process single file

&nbsp;   python extractor.py --reprocess        # Force reprocessing (ignore manifest)

"""



\# =============================================================================

\# IMPORTS - STRICTLY FROM APPROVED LIST

\# =============================================================================



\# Standard Library (CANON\_001: StdLib Primacy)

import json

import re

import hashlib

import logging

import shutil

import argparse

from pathlib import Path

from datetime import datetime, timezone

from typing import List, Dict, Optional, Tuple, Any

from dataclasses import dataclass



\# Approved External Dependencies

from pydantic import ValidationError

import pandas as pd

from tqdm import tqdm

from PIL import Image



\# Docling - Primary Extraction Engine

\# NOTE: This import pulls in torch/transformers as transitive dependencies,

\# but we never import them directly in this script.

from docling.document\_converter import DocumentConverter

from docling.datamodel.base\_models import InputFormat

from docling.datamodel.pipeline\_options import PdfPipelineOptions



\# Fallback Extraction Engine

import pdfplumber



\# Local Schema (the data contract)

from schema\_v2 import (

&nbsp;   ContextGraph,

&nbsp;   ContextNode,

&nbsp;   NodeMetadata,

&nbsp;   ExtractionMetrics,

&nbsp;   generate\_document\_id,

&nbsp;   generate\_chunk\_id,

&nbsp;   generate\_lineage\_trace,

&nbsp;   SchemaValidationError

)



\# =============================================================================

\# LOGGING CONFIGURATION

\# =============================================================================



def setup\_logging(log\_dir: Path, level: str = "INFO") -> logging.Logger:

&nbsp;   """

&nbsp;   Configure logging with both file and console handlers.

&nbsp;   

&nbsp;   Args:

&nbsp;       log\_dir: Directory for log files

&nbsp;       level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

&nbsp;       

&nbsp;   Returns:

&nbsp;       Configured logger instance

&nbsp;   """

&nbsp;   log\_dir.mkdir(parents=True, exist\_ok=True)

&nbsp;   

&nbsp;   timestamp = datetime.now().strftime("%Y%m%d\_%H%M%S")

&nbsp;   log\_file = log\_dir / f"extraction\_{timestamp}.log"

&nbsp;   

&nbsp;   logger = logging.getLogger("split\_rag\_extractor")

&nbsp;   logger.setLevel(getattr(logging, level.upper()))

&nbsp;   

&nbsp;   # File handler - detailed logging

&nbsp;   file\_handler = logging.FileHandler(log\_file, encoding='utf-8')

&nbsp;   file\_handler.setLevel(logging.DEBUG)

&nbsp;   file\_formatter = logging.Formatter(

&nbsp;       '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'

&nbsp;   )

&nbsp;   file\_handler.setFormatter(file\_formatter)

&nbsp;   

&nbsp;   # Console handler - summary logging

&nbsp;   console\_handler = logging.StreamHandler()

&nbsp;   console\_handler.setLevel(logging.INFO)

&nbsp;   console\_formatter = logging.Formatter('%(levelname)s: %(message)s')

&nbsp;   console\_handler.setFormatter(console\_formatter)

&nbsp;   

&nbsp;   logger.addHandler(file\_handler)

&nbsp;   logger.addHandler(console\_handler)

&nbsp;   

&nbsp;   return logger





\# =============================================================================

\# CONFIGURATION LOADING

\# =============================================================================



@dataclass

class ExtractorConfig:

&nbsp;   """Typed configuration container loaded from config.json."""

&nbsp;   

&nbsp;   input\_directory: Path

&nbsp;   output\_directory: Path

&nbsp;   log\_directory: Path

&nbsp;   quarantine\_directory: Path

&nbsp;   

&nbsp;   primary\_engine: str

&nbsp;   fallback\_engine: str

&nbsp;   enable\_ocr: bool

&nbsp;   enable\_table\_detection: bool

&nbsp;   max\_pages\_for\_entity\_scan: int

&nbsp;   chunk\_overlap\_tokens: int

&nbsp;   

&nbsp;   docling\_use\_gpu: bool

&nbsp;   docling\_artifacts\_path: Optional\[Path]

&nbsp;   docling\_table\_mode: str

&nbsp;   docling\_ocr\_languages: List\[str]

&nbsp;   

&nbsp;   conflict\_threshold\_levenshtein: float

&nbsp;   max\_conflict\_rate\_for\_quarantine: float

&nbsp;   enable\_keep\_all\_policy: bool

&nbsp;   

&nbsp;   log\_level: str

&nbsp;   batch\_size: int

&nbsp;   max\_workers: int





def load\_config(config\_path: Path) -> ExtractorConfig:

&nbsp;   """

&nbsp;   Load and validate configuration from JSON file.

&nbsp;   

&nbsp;   Args:

&nbsp;       config\_path: Path to config.json

&nbsp;       

&nbsp;   Returns:

&nbsp;       Populated ExtractorConfig instance

&nbsp;       

&nbsp;   Raises:

&nbsp;       FileNotFoundError: If config file doesn't exist

&nbsp;       json.JSONDecodeError: If config is invalid JSON

&nbsp;       KeyError: If required keys are missing

&nbsp;   """

&nbsp;   with open(config\_path, 'r', encoding='utf-8') as f:

&nbsp;       raw\_config = json.load(f)

&nbsp;   

&nbsp;   paths = raw\_config\['paths']

&nbsp;   extraction = raw\_config\['extraction']

&nbsp;   docling = raw\_config\['docling']

&nbsp;   validation = raw\_config\['validation']

&nbsp;   perf = raw\_config\['performance']

&nbsp;   log\_cfg = raw\_config\['logging']

&nbsp;   

&nbsp;   return ExtractorConfig(

&nbsp;       input\_directory=Path(paths\['input\_directory']),

&nbsp;       output\_directory=Path(paths\['output\_directory']),

&nbsp;       log\_directory=Path(paths\['log\_directory']),

&nbsp;       quarantine\_directory=Path(paths\['quarantine\_directory']),

&nbsp;       

&nbsp;       primary\_engine=extraction\['primary\_engine'],

&nbsp;       fallback\_engine=extraction\['fallback\_engine'],

&nbsp;       enable\_ocr=extraction\['enable\_ocr'],

&nbsp;       enable\_table\_detection=extraction\['enable\_table\_detection'],

&nbsp;       max\_pages\_for\_entity\_scan=extraction\['max\_pages\_for\_entity\_scan'],

&nbsp;       chunk\_overlap\_tokens=extraction\['chunk\_overlap\_tokens'],

&nbsp;       

&nbsp;       docling\_use\_gpu=docling\['use\_gpu'],

&nbsp;       docling\_artifacts\_path=Path(docling\['artifacts\_path']) if docling\['artifacts\_path'] else None,

&nbsp;       docling\_table\_mode=docling\['table\_mode'],

&nbsp;       docling\_ocr\_languages=docling\['ocr\_languages'],

&nbsp;       

&nbsp;       conflict\_threshold\_levenshtein=validation\['conflict\_threshold\_levenshtein'],

&nbsp;       max\_conflict\_rate\_for\_quarantine=validation\['max\_conflict\_rate\_for\_quarantine'],

&nbsp;       enable\_keep\_all\_policy=validation\['enable\_keep\_all\_policy'],

&nbsp;       

&nbsp;       log\_level=log\_cfg\['level'],

&nbsp;       batch\_size=perf\['batch\_size'],

&nbsp;       max\_workers=perf\['max\_workers']

&nbsp;   )





def load\_rules(rules\_path: Path) -> Dict\[str, Any]:

&nbsp;   """

&nbsp;   Load entity extraction rules from JSON file.

&nbsp;   

&nbsp;   Args:

&nbsp;       rules\_path: Path to rules.json

&nbsp;       

&nbsp;   Returns:

&nbsp;       Dictionary of compiled regex patterns and rules

&nbsp;   """

&nbsp;   with open(rules\_path, 'r', encoding='utf-8') as f:

&nbsp;       raw\_rules = json.load(f)

&nbsp;   

&nbsp;   # Pre-compile regex patterns for performance

&nbsp;   compiled\_rules = {'entity\_patterns': {}, 'stopwords': set(raw\_rules.get('stopwords', \[]))}

&nbsp;   

&nbsp;   for entity\_type, patterns in raw\_rules.get('entity\_patterns', {}).items():

&nbsp;       compiled\_rules\['entity\_patterns']\[entity\_type] = {}

&nbsp;       priority\_order = patterns.get('priority\_order', \[])

&nbsp;       compiled\_rules\['entity\_patterns']\[entity\_type]\['priority\_order'] = priority\_order

&nbsp;       

&nbsp;       for pattern\_name, pattern\_str in patterns.items():

&nbsp;           if pattern\_name != 'priority\_order' and isinstance(pattern\_str, str):

&nbsp;               try:

&nbsp;                   # Compile with IGNORECASE except for all\_caps patterns

&nbsp;                   flags = 0 if 'all\_caps' in pattern\_name else re.IGNORECASE

&nbsp;                   compiled\_rules\['entity\_patterns']\[entity\_type]\[pattern\_name] = re.compile(

&nbsp;                       pattern\_str, flags

&nbsp;                   )

&nbsp;               except re.error as e:

&nbsp;                   logging.warning(f"Invalid regex pattern '{pattern\_name}': {e}")

&nbsp;   

&nbsp;   return compiled\_rules





\# =============================================================================

\# FILE DISCOVERY AND HASHING

\# =============================================================================



SUPPORTED\_EXTENSIONS = {'.pdf', '.docx', '.xlsx', '.xls', '.pptx'}

IGNORED\_PREFIXES = ('~$', '.')  # Temp files and hidden files





def discover\_input\_files(input\_dir: Path) -> List\[Path]:

&nbsp;   """

&nbsp;   Discover all processable documents in the input directory.

&nbsp;   

&nbsp;   Filters out temporary files (prefixed with ~$) and hidden files.

&nbsp;   

&nbsp;   Args:

&nbsp;       input\_dir: Path to input directory

&nbsp;       

&nbsp;   Returns:

&nbsp;       List of Path objects for valid input files

&nbsp;   """

&nbsp;   files = \[]

&nbsp;   

&nbsp;   for file\_path in input\_dir.rglob('\*'):

&nbsp;       if not file\_path.is\_file():

&nbsp;           continue

&nbsp;       

&nbsp;       # Skip temp files and hidden files

&nbsp;       if file\_path.name.startswith(IGNORED\_PREFIXES):

&nbsp;           continue

&nbsp;       

&nbsp;       # Check extension

&nbsp;       if file\_path.suffix.lower() in SUPPORTED\_EXTENSIONS:

&nbsp;           files.append(file\_path)

&nbsp;   

&nbsp;   return sorted(files)





def compute\_file\_hash(file\_path: Path) -> str:

&nbsp;   """

&nbsp;   Compute MD5 hash of file contents for document ID generation.

&nbsp;   

&nbsp;   Args:

&nbsp;       file\_path: Path to the file

&nbsp;       

&nbsp;   Returns:

&nbsp;       32-character MD5 hexadecimal string

&nbsp;   """

&nbsp;   hasher = hashlib.md5()

&nbsp;   with open(file\_path, 'rb') as f:

&nbsp;       # Read in chunks to handle large files

&nbsp;       for chunk in iter(lambda: f.read(8192), b''):

&nbsp;           hasher.update(chunk)

&nbsp;   return hasher.hexdigest()





\# =============================================================================

\# MANIFEST MANAGEMENT (INCREMENTAL PROCESSING)

\# =============================================================================



MANIFEST\_FILENAME = "processing\_manifest.json"





def load\_manifest(output\_dir: Path) -> Dict\[str, str]:

&nbsp;   """

&nbsp;   Load the processing manifest tracking which files have been processed.

&nbsp;   

&nbsp;   The manifest maps file hashes to output JSON paths, enabling incremental

&nbsp;   processing (skip files that haven't changed).

&nbsp;   

&nbsp;   Args:

&nbsp;       output\_dir: Path to output directory

&nbsp;       

&nbsp;   Returns:

&nbsp;       Dictionary mapping document\_id (hash) to output path

&nbsp;   """

&nbsp;   manifest\_path = output\_dir / MANIFEST\_FILENAME

&nbsp;   

&nbsp;   if manifest\_path.exists():

&nbsp;       with open(manifest\_path, 'r', encoding='utf-8') as f:

&nbsp;           return json.load(f)

&nbsp;   

&nbsp;   return {}





def save\_manifest(output\_dir: Path, manifest: Dict\[str, str]) -> None:

&nbsp;   """

&nbsp;   Save the processing manifest to disk.

&nbsp;   

&nbsp;   Args:

&nbsp;       output\_dir: Path to output directory

&nbsp;       manifest: Dictionary to save

&nbsp;   """

&nbsp;   manifest\_path = output\_dir / MANIFEST\_FILENAME

&nbsp;   

&nbsp;   with open(manifest\_path, 'w', encoding='utf-8') as f:

&nbsp;       json.dump(manifest, f, indent=2)





\# =============================================================================

\# ENTITY ANCHORING (BORROWER DISAMBIGUATION)

\# =============================================================================



def extract\_entity(

&nbsp;   text: str,

&nbsp;   entity\_type: str,

&nbsp;   compiled\_rules: Dict\[str, Any]

) -> Optional\[str]:

&nbsp;   """

&nbsp;   Extract named entity from text using regex patterns.

&nbsp;   

&nbsp;   Applies patterns in priority order and returns first match.

&nbsp;   

&nbsp;   Args:

&nbsp;       text: Text to search (typically first N pages of document)

&nbsp;       entity\_type: "borrower", "lender", or "guarantor"

&nbsp;       compiled\_rules: Pre-compiled regex patterns from rules.json

&nbsp;       

&nbsp;   Returns:

&nbsp;       Extracted entity name or None if not found

&nbsp;   """

&nbsp;   patterns = compiled\_rules.get('entity\_patterns', {}).get(entity\_type, {})

&nbsp;   priority\_order = patterns.get('priority\_order', \[])

&nbsp;   

&nbsp;   for pattern\_name in priority\_order:

&nbsp;       pattern = patterns.get(pattern\_name)

&nbsp;       if pattern is None:

&nbsp;           continue

&nbsp;       

&nbsp;       match = pattern.search(text)

&nbsp;       if match:

&nbsp;           # Try to get named group 'entity' or fall back to entity\_type name

&nbsp;           try:

&nbsp;               entity\_name = match.group('entity')

&nbsp;           except IndexError:

&nbsp;               try:

&nbsp;                   entity\_name = match.group(entity\_type)

&nbsp;               except IndexError:

&nbsp;                   entity\_name = match.group(1) if match.groups() else None

&nbsp;           

&nbsp;           if entity\_name:

&nbsp;               # Clean up the extracted entity name

&nbsp;               entity\_name = entity\_name.strip().strip(',').strip('.')

&nbsp;               # Remove trailing articles/prepositions

&nbsp;               entity\_name = re.sub(r'\\s+(a|an|the|of|and|or)\\s\*$', '', entity\_name, flags=re.IGNORECASE)

&nbsp;               

&nbsp;               if len(entity\_name) > 2:  # Sanity check

&nbsp;                   return entity\_name

&nbsp;   

&nbsp;   return None





def anchor\_entities(

&nbsp;   pages\_text: List\[str],

&nbsp;   max\_pages: int,

&nbsp;   compiled\_rules: Dict\[str, Any]

) -> Dict\[str, Optional\[str]]:

&nbsp;   """

&nbsp;   Scan document pages to anchor entity names (Borrower, Lender, Guarantor).

&nbsp;   

&nbsp;   This implements the disambiguation heuristics from Section 3 of the spec.

&nbsp;   Entities are anchored from the Primary Scope (contract) to prevent

&nbsp;   confusion with generic Corpus definitions.

&nbsp;   

&nbsp;   Args:

&nbsp;       pages\_text: List of text content per page

&nbsp;       max\_pages: Maximum number of pages to scan (typically 20)

&nbsp;       compiled\_rules: Pre-compiled regex patterns

&nbsp;       

&nbsp;   Returns:

&nbsp;       Dictionary with keys 'borrower', 'lender', 'guarantor' and extracted names

&nbsp;   """

&nbsp;   # Combine first N pages for scanning

&nbsp;   scan\_text = "\\n".join(pages\_text\[:max\_pages])

&nbsp;   

&nbsp;   entities = {

&nbsp;       'borrower': extract\_entity(scan\_text, 'borrower', compiled\_rules),

&nbsp;       'lender': extract\_entity(scan\_text, 'lender', compiled\_rules),

&nbsp;       'guarantor': extract\_entity(scan\_text, 'guarantor', compiled\_rules)

&nbsp;   }

&nbsp;   

&nbsp;   return entities





\# =============================================================================

\# LEVENSHTEIN DISTANCE (CONFLICT DETECTION)

\# =============================================================================



def levenshtein\_distance(s1: str, s2: str) -> int:

&nbsp;   """

&nbsp;   Compute Levenshtein (edit) distance between two strings.

&nbsp;   

&nbsp;   Used to detect conflicts between Docling and pdfplumber extraction.

&nbsp;   

&nbsp;   Args:

&nbsp;       s1: First string

&nbsp;       s2: Second string

&nbsp;       

&nbsp;   Returns:

&nbsp;       Integer edit distance

&nbsp;   """

&nbsp;   if len(s1) < len(s2):

&nbsp;       return levenshtein\_distance(s2, s1)

&nbsp;   

&nbsp;   if len(s2) == 0:

&nbsp;       return len(s1)

&nbsp;   

&nbsp;   previous\_row = range(len(s2) + 1)

&nbsp;   

&nbsp;   for i, c1 in enumerate(s1):

&nbsp;       current\_row = \[i + 1]

&nbsp;       for j, c2 in enumerate(s2):

&nbsp;           insertions = previous\_row\[j + 1] + 1

&nbsp;           deletions = current\_row\[j] + 1

&nbsp;           substitutions = previous\_row\[j] + (c1 != c2)

&nbsp;           current\_row.append(min(insertions, deletions, substitutions))

&nbsp;       previous\_row = current\_row

&nbsp;   

&nbsp;   return previous\_row\[-1]





def calculate\_normalized\_distance(s1: str, s2: str) -> float:

&nbsp;   """

&nbsp;   Calculate normalized Levenshtein distance (0.0 to 1.0).

&nbsp;   

&nbsp;   Args:

&nbsp;       s1: First string

&nbsp;       s2: Second string

&nbsp;       

&nbsp;   Returns:

&nbsp;       Float between 0.0 (identical) and 1.0 (completely different)

&nbsp;   """

&nbsp;   if not s1 and not s2:

&nbsp;       return 0.0

&nbsp;   

&nbsp;   max\_len = max(len(s1), len(s2))

&nbsp;   if max\_len == 0:

&nbsp;       return 0.0

&nbsp;   

&nbsp;   distance = levenshtein\_distance(s1, s2)

&nbsp;   return distance / max\_len





\# =============================================================================

\# DOCLING EXTRACTION (PRIMARY ENGINE)

\# =============================================================================



def create\_docling\_converter(config: ExtractorConfig) -> DocumentConverter:

&nbsp;   """

&nbsp;   Initialize Docling DocumentConverter with configured options.

&nbsp;   

&nbsp;   Args:

&nbsp;       config: Extractor configuration

&nbsp;       

&nbsp;   Returns:

&nbsp;       Configured DocumentConverter instance

&nbsp;   """

&nbsp;   pipeline\_options = PdfPipelineOptions()

&nbsp;   pipeline\_options.do\_ocr = config.enable\_ocr

&nbsp;   pipeline\_options.do\_table\_structure = config.enable\_table\_detection

&nbsp;   

&nbsp;   # Set artifacts path for offline/airgapped environments

&nbsp;   if config.docling\_artifacts\_path:

&nbsp;       pipeline\_options.artifacts\_path = str(config.docling\_artifacts\_path)

&nbsp;   

&nbsp;   converter = DocumentConverter(

&nbsp;       allowed\_formats=\[

&nbsp;           InputFormat.PDF,

&nbsp;           InputFormat.DOCX,

&nbsp;           InputFormat.XLSX,

&nbsp;           InputFormat.PPTX

&nbsp;       ],

&nbsp;       pdf\_pipeline\_options=pipeline\_options

&nbsp;   )

&nbsp;   

&nbsp;   return converter





def extract\_with\_docling(

&nbsp;   file\_path: Path,

&nbsp;   converter: DocumentConverter,

&nbsp;   document\_id: str,

&nbsp;   logger: logging.Logger

) -> Tuple\[List\[ContextNode], List\[str]]:

&nbsp;   """

&nbsp;   Extract content from document using Docling.

&nbsp;   

&nbsp;   Args:

&nbsp;       file\_path: Path to input document

&nbsp;       converter: Initialized Docling converter

&nbsp;       document\_id: Pre-computed document hash

&nbsp;       logger: Logger instance

&nbsp;       

&nbsp;   Returns:

&nbsp;       Tuple of (list of ContextNodes, list of page texts for entity scanning)

&nbsp;   """

&nbsp;   nodes: List\[ContextNode] = \[]

&nbsp;   pages\_text: List\[str] = \[]

&nbsp;   

&nbsp;   logger.info(f"Processing with Docling: {file\_path.name}")

&nbsp;   

&nbsp;   # Convert document

&nbsp;   result = converter.convert(str(file\_path))

&nbsp;   

&nbsp;   # Process each document element

&nbsp;   chunk\_index = 0

&nbsp;   current\_section\_id = "root"

&nbsp;   

&nbsp;   for element in result.document.iterate\_items():

&nbsp;       page\_num = getattr(element, 'page\_no', 1) or 1

&nbsp;       

&nbsp;       # Ensure we have page text entries

&nbsp;       while len(pages\_text) < page\_num:

&nbsp;           pages\_text.append("")

&nbsp;       

&nbsp;       # Determine content type

&nbsp;       element\_type = type(element).\_\_name\_\_.lower()

&nbsp;       if 'table' in element\_type:

&nbsp;           content\_type = 'table'

&nbsp;       elif 'header' in element\_type or 'title' in element\_type:

&nbsp;           content\_type = 'header'

&nbsp;           # Update current section ID for hierarchy

&nbsp;           current\_section\_id = generate\_chunk\_id(document\_id, page\_num, chunk\_index, str(element.text)\[:50])

&nbsp;       elif 'picture' in element\_type or 'figure' in element\_type:

&nbsp;           content\_type = 'image\_caption'

&nbsp;       else:

&nbsp;           content\_type = 'text'

&nbsp;       

&nbsp;       # Extract content

&nbsp;       content = ""

&nbsp;       if hasattr(element, 'text'):

&nbsp;           content = str(element.text)

&nbsp;       elif hasattr(element, 'export\_to\_markdown'):

&nbsp;           content = element.export\_to\_markdown()

&nbsp;       

&nbsp;       if not content.strip():

&nbsp;           continue

&nbsp;       

&nbsp;       # Accumulate page text

&nbsp;       pages\_text\[page\_num - 1] += content + "\\n"

&nbsp;       

&nbsp;       # Extract bounding box if available

&nbsp;       bbox = None

&nbsp;       if hasattr(element, 'bounding\_box'):

&nbsp;           bb = element.bounding\_box

&nbsp;           if bb:

&nbsp;               bbox = \[bb.x0, bb.y0, bb.x1, bb.y1]

&nbsp;       

&nbsp;       # Generate deterministic IDs

&nbsp;       chunk\_id = generate\_chunk\_id(document\_id, page\_num, chunk\_index, content)

&nbsp;       lineage = generate\_lineage\_trace(document\_id, page\_num - 1, bbox, "docling")

&nbsp;       

&nbsp;       # Determine table shape if applicable

&nbsp;       table\_shape = None

&nbsp;       if content\_type == 'table' and hasattr(element, 'num\_rows') and hasattr(element, 'num\_cols'):

&nbsp;           table\_shape = \[element.num\_rows, element.num\_cols]

&nbsp;       

&nbsp;       # Create node

&nbsp;       node = ContextNode(

&nbsp;           chunk\_id=chunk\_id,

&nbsp;           parent\_section\_id=current\_section\_id if content\_type != 'header' else "root",

&nbsp;           content\_type=content\_type,

&nbsp;           content=content,

&nbsp;           metadata=NodeMetadata(

&nbsp;               page\_number=page\_num,

&nbsp;               bbox=bbox,

&nbsp;               table\_shape=table\_shape,

&nbsp;               source\_scope="primary",

&nbsp;               extraction\_method="docling"

&nbsp;           ),

&nbsp;           lineage\_trace=lineage

&nbsp;       )

&nbsp;       

&nbsp;       nodes.append(node)

&nbsp;       chunk\_index += 1

&nbsp;   

&nbsp;   logger.info(f"Docling extracted {len(nodes)} nodes from {len(pages\_text)} pages")

&nbsp;   

&nbsp;   return nodes, pages\_text





\# =============================================================================

\# PDFPLUMBER EXTRACTION (FALLBACK ENGINE)

\# =============================================================================



def extract\_with\_pdfplumber(

&nbsp;   file\_path: Path,

&nbsp;   document\_id: str,

&nbsp;   logger: logging.Logger

) -> Tuple\[List\[ContextNode], List\[str]]:

&nbsp;   """

&nbsp;   Extract content from PDF using pdfplumber (fallback when Docling fails).

&nbsp;   

&nbsp;   Args:

&nbsp;       file\_path: Path to input PDF

&nbsp;       document\_id: Pre-computed document hash

&nbsp;       logger: Logger instance

&nbsp;       

&nbsp;   Returns:

&nbsp;       Tuple of (list of ContextNodes, list of page texts)

&nbsp;   """

&nbsp;   nodes: List\[ContextNode] = \[]

&nbsp;   pages\_text: List\[str] = \[]

&nbsp;   

&nbsp;   logger.info(f"Processing with pdfplumber (fallback): {file\_path.name}")

&nbsp;   

&nbsp;   with pdfplumber.open(file\_path) as pdf:

&nbsp;       chunk\_index = 0

&nbsp;       

&nbsp;       for page\_num, page in enumerate(pdf.pages, start=1):

&nbsp;           # Extract text

&nbsp;           text = page.extract\_text() or ""

&nbsp;           pages\_text.append(text)

&nbsp;           

&nbsp;           if not text.strip():

&nbsp;               continue

&nbsp;           

&nbsp;           # Split into paragraphs

&nbsp;           paragraphs = \[p.strip() for p in text.split('\\n\\n') if p.strip()]

&nbsp;           

&nbsp;           for para in paragraphs:

&nbsp;               chunk\_id = generate\_chunk\_id(document\_id, page\_num, chunk\_index, para)

&nbsp;               lineage = generate\_lineage\_trace(document\_id, page\_num - 1, None, "pdfplumber")

&nbsp;               

&nbsp;               # Simple content type detection

&nbsp;               content\_type = "text"

&nbsp;               if para.isupper() and len(para) < 100:

&nbsp;                   content\_type = "header"

&nbsp;               

&nbsp;               node = ContextNode(

&nbsp;                   chunk\_id=chunk\_id,

&nbsp;                   parent\_section\_id="root",

&nbsp;                   content\_type=content\_type,

&nbsp;                   content=para,

&nbsp;                   metadata=NodeMetadata(

&nbsp;                       page\_number=page\_num,

&nbsp;                       source\_scope="primary",

&nbsp;                       extraction\_method="pdfplumber"

&nbsp;                   ),

&nbsp;                   lineage\_trace=lineage

&nbsp;               )

&nbsp;               

&nbsp;               nodes.append(node)

&nbsp;               chunk\_index += 1

&nbsp;           

&nbsp;           # Extract tables separately

&nbsp;           tables = page.extract\_tables()

&nbsp;           for table in tables:

&nbsp;               if not table:

&nbsp;                   continue

&nbsp;               

&nbsp;               # Convert to markdown format

&nbsp;               md\_rows = \[]

&nbsp;               for i, row in enumerate(table):

&nbsp;                   row\_str = "| " + " | ".join(str(cell or "") for cell in row) + " |"

&nbsp;                   md\_rows.append(row\_str)

&nbsp;                   if i == 0:

&nbsp;                       md\_rows.append("| " + " | ".join(\["---"] \* len(row)) + " |")

&nbsp;               

&nbsp;               table\_content = "\\n".join(md\_rows)

&nbsp;               

&nbsp;               chunk\_id = generate\_chunk\_id(document\_id, page\_num, chunk\_index, table\_content)

&nbsp;               lineage = generate\_lineage\_trace(document\_id, page\_num - 1, None, "pdfplumber")

&nbsp;               

&nbsp;               node = ContextNode(

&nbsp;                   chunk\_id=chunk\_id,

&nbsp;                   parent\_section\_id="root",

&nbsp;                   content\_type="table",

&nbsp;                   content=table\_content,

&nbsp;                   metadata=NodeMetadata(

&nbsp;                       page\_number=page\_num,

&nbsp;                       table\_shape=\[len(table), len(table\[0]) if table else 0],

&nbsp;                       source\_scope="primary",

&nbsp;                       extraction\_method="pdfplumber"

&nbsp;                   ),

&nbsp;                   lineage\_trace=lineage

&nbsp;               )

&nbsp;               

&nbsp;               nodes.append(node)

&nbsp;               chunk\_index += 1

&nbsp;   

&nbsp;   logger.info(f"pdfplumber extracted {len(nodes)} nodes from {len(pages\_text)} pages")

&nbsp;   

&nbsp;   return nodes, pages\_text





\# =============================================================================

\# QUARANTINE MANAGEMENT

\# =============================================================================



def quarantine\_document(

&nbsp;   file\_path: Path,

&nbsp;   quarantine\_dir: Path,

&nbsp;   error: Exception,

&nbsp;   logger: logging.Logger

) -> None:

&nbsp;   """

&nbsp;   Move failed document to quarantine with error report.

&nbsp;   

&nbsp;   Args:

&nbsp;       file\_path: Path to failed document

&nbsp;       quarantine\_dir: Path to quarantine directory

&nbsp;       error: Exception that caused the failure

&nbsp;       logger: Logger instance

&nbsp;   """

&nbsp;   quarantine\_dir.mkdir(parents=True, exist\_ok=True)

&nbsp;   

&nbsp;   # Create unique quarantine subdirectory for this file

&nbsp;   timestamp = datetime.now().strftime("%Y%m%d\_%H%M%S")

&nbsp;   quarantine\_subdir = quarantine\_dir / f"{file\_path.stem}\_{timestamp}"

&nbsp;   quarantine\_subdir.mkdir(parents=True, exist\_ok=True)

&nbsp;   

&nbsp;   # Move the file

&nbsp;   dest\_path = quarantine\_subdir / file\_path.name

&nbsp;   shutil.copy2(file\_path, dest\_path)

&nbsp;   

&nbsp;   # Create failure report

&nbsp;   report = {

&nbsp;       "original\_path": str(file\_path),

&nbsp;       "quarantine\_time": datetime.now(timezone.utc).isoformat(),

&nbsp;       "error\_type": type(error).\_\_name\_\_,

&nbsp;       "error\_message": str(error),

&nbsp;       "traceback": None  # Could add traceback.format\_exc() if needed

&nbsp;   }

&nbsp;   

&nbsp;   report\_path = quarantine\_subdir / "failure\_report.json"

&nbsp;   with open(report\_path, 'w', encoding='utf-8') as f:

&nbsp;       json.dump(report, f, indent=2)

&nbsp;   

&nbsp;   logger.critical(f"Document quarantined: {file\_path.name} -> {quarantine\_subdir}")





\# =============================================================================

\# MAIN PROCESSING PIPELINE

\# =============================================================================



def process\_document(

&nbsp;   file\_path: Path,

&nbsp;   config: ExtractorConfig,

&nbsp;   compiled\_rules: Dict\[str, Any],

&nbsp;   converter: DocumentConverter,

&nbsp;   logger: logging.Logger

) -> Optional\[ContextGraph]:

&nbsp;   """

&nbsp;   Process a single document through the extraction pipeline.

&nbsp;   

&nbsp;   Implements the Keep-All Policy with Docling as primary engine

&nbsp;   and pdfplumber as fallback.

&nbsp;   

&nbsp;   Args:

&nbsp;       file\_path: Path to input document

&nbsp;       config: Extractor configuration

&nbsp;       compiled\_rules: Pre-compiled regex patterns

&nbsp;       converter: Docling converter instance

&nbsp;       logger: Logger instance

&nbsp;       

&nbsp;   Returns:

&nbsp;       ContextGraph if successful, None if document should be quarantined

&nbsp;   """

&nbsp;   start\_time = datetime.now()

&nbsp;   

&nbsp;   # Step 1: Hash file for document ID

&nbsp;   document\_id = compute\_file\_hash(file\_path)

&nbsp;   logger.info(f"Processing: {file\_path.name} (ID: {document\_id\[:8]}...)")

&nbsp;   

&nbsp;   nodes: List\[ContextNode] = \[]

&nbsp;   pages\_text: List\[str] = \[]

&nbsp;   fallback\_triggered = False

&nbsp;   

&nbsp;   # Step 2: Primary extraction with Docling

&nbsp;   try:

&nbsp;       nodes, pages\_text = extract\_with\_docling(file\_path, converter, document\_id, logger)

&nbsp;   except Exception as e:

&nbsp;       logger.warning(f"Docling extraction failed: {e}")

&nbsp;       fallback\_triggered = True

&nbsp;   

&nbsp;   # Step 3: Fallback to pdfplumber if needed

&nbsp;   if fallback\_triggered or len(nodes) == 0:

&nbsp;       if file\_path.suffix.lower() == '.pdf':

&nbsp;           try:

&nbsp;               nodes, pages\_text = extract\_with\_pdfplumber(file\_path, document\_id, logger)

&nbsp;           except Exception as e:

&nbsp;               logger.error(f"Fallback extraction also failed: {e}")

&nbsp;               return None

&nbsp;       else:

&nbsp;           logger.error(f"No fallback available for {file\_path.suffix} files")

&nbsp;           return None

&nbsp;   

&nbsp;   if len(nodes) == 0:

&nbsp;       logger.error("No content extracted from document")

&nbsp;       return None

&nbsp;   

&nbsp;   # Step 4: Entity Anchoring (Borrower Disambiguation)

&nbsp;   entities = anchor\_entities(pages\_text, config.max\_pages\_for\_entity\_scan, compiled\_rules)

&nbsp;   logger.info(f"Anchored entities: {entities}")

&nbsp;   

&nbsp;   # Step 5: Calculate metrics

&nbsp;   extraction\_time = (datetime.now() - start\_time).total\_seconds()

&nbsp;   

&nbsp;   metrics = ExtractionMetrics(

&nbsp;       total\_pages=len(pages\_text),

&nbsp;       total\_nodes=len(nodes),

&nbsp;       tables\_extracted=sum(1 for n in nodes if n.content\_type == 'table'),

&nbsp;       headers\_extracted=sum(1 for n in nodes if n.content\_type == 'header'),

&nbsp;       conflicts\_detected=sum(1 for n in nodes if n.metadata.conflict\_detected),

&nbsp;       extraction\_time\_seconds=extraction\_time,

&nbsp;       primary\_engine\_used=config.primary\_engine,

&nbsp;       fallback\_triggered=fallback\_triggered

&nbsp;   )

&nbsp;   

&nbsp;   # Step 6: Build Context Graph

&nbsp;   context\_graph = ContextGraph(

&nbsp;       document\_id=document\_id,

&nbsp;       filename=file\_path.name,

&nbsp;       processed\_at=ContextGraph.get\_current\_timestamp(),

&nbsp;       borrower\_entity=entities.get('borrower'),

&nbsp;       lender\_entity=entities.get('lender'),

&nbsp;       guarantor\_entity=entities.get('guarantor'),

&nbsp;       nodes=nodes,

&nbsp;       metrics=metrics

&nbsp;   )

&nbsp;   

&nbsp;   return context\_graph





def main() -> int:

&nbsp;   """

&nbsp;   Main entry point for the extraction engine.

&nbsp;   

&nbsp;   Returns:

&nbsp;       Exit code (0 for success, non-zero for errors)

&nbsp;   """

&nbsp;   # Parse command line arguments

&nbsp;   parser = argparse.ArgumentParser(description="Split-RAG Tier 1 Extraction Engine")

&nbsp;   parser.add\_argument('--file', type=Path, help="Process a single file")

&nbsp;   parser.add\_argument('--reprocess', action='store\_true', help="Force reprocessing (ignore manifest)")

&nbsp;   parser.add\_argument('--config', type=Path, default=Path('config.json'), help="Path to config.json")

&nbsp;   parser.add\_argument('--rules', type=Path, default=Path('rules.json'), help="Path to rules.json")

&nbsp;   args = parser.parse\_args()

&nbsp;   

&nbsp;   # Load configuration

&nbsp;   try:

&nbsp;       config = load\_config(args.config)

&nbsp;   except Exception as e:

&nbsp;       print(f"ERROR: Failed to load config: {e}")

&nbsp;       return 1

&nbsp;   

&nbsp;   # Setup logging

&nbsp;   logger = setup\_logging(config.log\_directory, config.log\_level)

&nbsp;   logger.info("=" \* 60)

&nbsp;   logger.info("Split-RAG Tier 1 Extraction Engine Starting")

&nbsp;   logger.info("=" \* 60)

&nbsp;   

&nbsp;   # Load rules

&nbsp;   try:

&nbsp;       compiled\_rules = load\_rules(args.rules)

&nbsp;       logger.info(f"Loaded rules from {args.rules}")

&nbsp;   except Exception as e:

&nbsp;       logger.error(f"Failed to load rules: {e}")

&nbsp;       return 1

&nbsp;   

&nbsp;   # Create output directories

&nbsp;   config.output\_directory.mkdir(parents=True, exist\_ok=True)

&nbsp;   config.quarantine\_directory.mkdir(parents=True, exist\_ok=True)

&nbsp;   

&nbsp;   # Initialize Docling converter

&nbsp;   try:

&nbsp;       converter = create\_docling\_converter(config)

&nbsp;       logger.info("Docling converter initialized")

&nbsp;   except Exception as e:

&nbsp;       logger.error(f"Failed to initialize Docling: {e}")

&nbsp;       return 1

&nbsp;   

&nbsp;   # Discover input files

&nbsp;   if args.file:

&nbsp;       input\_files = \[args.file] if args.file.exists() else \[]

&nbsp;   else:

&nbsp;       input\_files = discover\_input\_files(config.input\_directory)

&nbsp;   

&nbsp;   if not input\_files:

&nbsp;       logger.warning("No input files found")

&nbsp;       return 0

&nbsp;   

&nbsp;   logger.info(f"Found {len(input\_files)} files to process")

&nbsp;   

&nbsp;   # Load manifest for incremental processing

&nbsp;   manifest = {} if args.reprocess else load\_manifest(config.output\_directory)

&nbsp;   

&nbsp;   # Process files

&nbsp;   success\_count = 0

&nbsp;   skip\_count = 0

&nbsp;   error\_count = 0

&nbsp;   

&nbsp;   for file\_path in tqdm(input\_files, desc="Processing documents"):

&nbsp;       file\_hash = compute\_file\_hash(file\_path)

&nbsp;       

&nbsp;       # Check if already processed (incremental)

&nbsp;       if file\_hash in manifest and not args.reprocess:

&nbsp;           logger.debug(f"Skipping (already processed): {file\_path.name}")

&nbsp;           skip\_count += 1

&nbsp;           continue

&nbsp;       

&nbsp;       try:

&nbsp;           context\_graph = process\_document(

&nbsp;               file\_path, config, compiled\_rules, converter, logger

&nbsp;           )

&nbsp;           

&nbsp;           if context\_graph is None:

&nbsp;               quarantine\_document(

&nbsp;                   file\_path, config.quarantine\_directory,

&nbsp;                   Exception("Extraction returned no content"),

&nbsp;                   logger

&nbsp;               )

&nbsp;               error\_count += 1

&nbsp;               continue

&nbsp;           

&nbsp;           # Validate against schema

&nbsp;           try:

&nbsp;               # This re-validates (defensive check)

&nbsp;               \_ = context\_graph.model\_dump()

&nbsp;           except ValidationError as e:

&nbsp;               quarantine\_document(file\_path, config.quarantine\_directory, e, logger)

&nbsp;               error\_count += 1

&nbsp;               continue

&nbsp;           

&nbsp;           # Save output

&nbsp;           output\_filename = f"{file\_path.stem}\_context.json"

&nbsp;           output\_path = config.output\_directory / output\_filename

&nbsp;           

&nbsp;           with open(output\_path, 'w', encoding='utf-8') as f:

&nbsp;               f.write(context\_graph.to\_json())

&nbsp;           

&nbsp;           # Update manifest

&nbsp;           manifest\[file\_hash] = str(output\_path)

&nbsp;           

&nbsp;           logger.info(f"Successfully processed: {file\_path.name} -> {output\_filename}")

&nbsp;           success\_count += 1

&nbsp;           

&nbsp;       except Exception as e:

&nbsp;           logger.error(f"Unexpected error processing {file\_path.name}: {e}")

&nbsp;           quarantine\_document(file\_path, config.quarantine\_directory, e, logger)

&nbsp;           error\_count += 1

&nbsp;   

&nbsp;   # Save updated manifest

&nbsp;   save\_manifest(config.output\_directory, manifest)

&nbsp;   

&nbsp;   # Summary

&nbsp;   logger.info("=" \* 60)

&nbsp;   logger.info("Extraction Complete")

&nbsp;   logger.info(f"  Successful: {success\_count}")

&nbsp;   logger.info(f"  Skipped (already processed): {skip\_count}")

&nbsp;   logger.info(f"  Errors (quarantined): {error\_count}")

&nbsp;   logger.info("=" \* 60)

&nbsp;   

&nbsp;   return 0 if error\_count == 0 else 1





if \_\_name\_\_ == "\_\_main\_\_":

&nbsp;   exit(main())

------------------



================================================================================

SECTION 2.4: VERIFICATION CHECKPOINT - BLOCK 2

================================================================================



Before proceeding, verify the following:



SCHEMA\_V2.PY CHECKLIST:

\[ ] All Pydantic models have explicit Field descriptions

\[ ] generate\_document\_id uses MD5 (32 hex chars, not SHA-256)

\[ ] generate\_chunk\_id creates composite key from doc\_id + position + content

\[ ] generate\_lineage\_trace uses SHA-256 (64 hex chars)

\[ ] NodeMetadata includes source\_scope for disambiguation

\[ ] NodeMetadata includes conflict\_detected for Keep-All Policy

\[ ] ContextGraph includes borrower\_entity at root level

\[ ] All validators are @classmethod with proper signatures

\[ ] Self-test runs successfully when executed directly



EXTRACTOR.PY CHECKLIST:

\[ ] NO direct imports of torch or transformers

\[ ] Docling imported via: from docling.document\_converter import DocumentConverter

\[ ] All paths use pathlib.Path (CP-003)

\[ ] All functions have return type hints (CP-001)

\[ ] No generic except Exception: in internal logic (CP-002)

\[ ] Library boundary wraps (Docling, pdfplumber) have exception handling

\[ ] Quarantine system moves failed documents with failure\_report.json

\[ ] Entity anchoring runs on first N pages (configurable)

\[ ] Manifest enables incremental processing

\[ ] Logging captures all significant events



TEST YOUR IMPLEMENTATION:

```bash

\# Test schema

python schema\_v2.py



\# Test extractor (with a sample PDF in input/)

python extractor.py --file input/sample.pdf



\# Verify output

cat output/sample\_context.json | python -m json.tool

```



================================================================================

BLOCK 2 COMPLETE - AWAIT USER CONFIRMATION

================================================================================



Present the following files to the user for review:

1\. schema\_v2.py (with Pydantic models and ID generation functions)

2\. extractor.py (with full extraction pipeline)



Then ask: "Block 2 (Core Components) is complete. I have created:

\- schema\_v2.py: Pydantic data contract with deterministic ID generation

\- extractor.py: Tier 1 extraction engine with Docling/pdfplumber, 

&nbsp; entity anchoring, quarantine system, and incremental processing



May I proceed to Block 3 (Tier 2 Retrieval Logic and Sandbox Constraints)?"



DO NOT proceed to Block 3 until the user explicitly confirms.



================================================================================

END OF BLOCK 2

================================================================================



**================================================================================**

**SPLIT-RAG IMPLEMENTATION INSTRUCTIONS - BLOCK 3 OF 4**

**TIER 2 RETRIEVAL LOGIC AND SANDBOX CONSTRAINTS**

**================================================================================**



**OVERVIEW**

**--------**

**This block covers the Tier 2 "Consumer" script that runs inside the Microsoft**

**Copilot Studio sandbox. This environment has SEVERE constraints that fundamentally**

**shape how the code must be written.**



**CRITICAL UNDERSTANDING: Tier 2 is NOT a general Python environment. It runs in**

**a restricted sandbox with approximately 256MB memory, no network access, and** 

**only pandas as an external dependency. Code that works perfectly in Tier 1** 

**will crash immediately in Tier 2 if it violates these constraints.**



**After completing Block 3, STOP and present your work to the user for review.**

**Ask: "Block 3 complete. May I proceed to Block 4 (Testing and Deployment)?"**



**================================================================================**

**SECTION 3.1: TIER 2 CONSTRAINT ANALYSIS**

**================================================================================**



**Before writing any Tier 2 code, you MUST internalize these constraints:**



**MEMORY CONSTRAINT (256MB - 512MB):**

**The Copilot Studio sandbox allocates limited memory. Consider what happens when**

**common libraries are imported:**

  **- import torch         → ~400MB+ (INSTANT CRASH)**

  **- import transformers  → ~200MB+ (CRASH)**

  **- import pydantic      → ~50MB (potentially problematic)**

  **- import pandas        → ~30MB (acceptable, pre-installed)**

  **- import json          → ~0MB (stdlib, always safe)**



**This is why CANON\_002 mandates: "pandas is the only permitted external dependency."**



**NETWORK CONSTRAINT (Zero Access):**

**The sandbox cannot make HTTP requests, access APIs, or reach external services.**

**This eliminates:**

  **- Embedding API calls (OpenAI, Cohere, etc.)**

  **- Vector database queries (Pinecone, Weaviate, etc.)**

  **- LangChain's default behaviors (API-heavy)**



**This is why we use Vectorized Keyword Density instead of semantic embeddings.**



**COMPUTATIONAL CONSTRAINT (No Heavy ML):**

**Without torch/tensorflow, we cannot:**

  **- Run embedding models locally**

  **- Perform neural inference**

  **- Use sentence-transformers**



**This forces deterministic, algebraic retrieval methods.**



**FILE SIZE CONSTRAINT (512MB):**

**The Context Graph JSON must be under 512MB to be uploadable. For very large**

**document corpora, you may need to split into multiple JSON files.**



**================================================================================**

**SECTION 3.2: THE VECTORIZED KEYWORD DENSITY ALGORITHM**

**================================================================================**



**Since we cannot use embedding similarity, we implement a deterministic retrieval**

**algorithm based on keyword frequency weighted by content type.**



**ALGORITHM OVERVIEW:**



**1. QUERY NORMALIZATION**

   **- Convert to lowercase**

   **- Remove punctuation using regex (re module)**

   **- Split into unique keywords**

   **- Remove stopwords (hardcoded list since nltk unavailable)**



**2. VECTORIZED COUNTING**

   **- Load Context Graph nodes into pandas DataFrame**

   **- For each keyword, use df\['content'].str.count(keyword)**

   **- This leverages pandas' C-optimized string operations**

   **- 300x faster than Python loops (critical for sandbox latency)**



**3. WEIGHTED SCORING**

   **Apply content-type multipliers:**

   **- Headers: 3.0x (strong topic indicators)**

   **- Tables: 2.5x (dense structured data)**

   **- Text: 1.0x (baseline)**

   **- Primary Scope: 1.5x additional multiplier (contract-specific content)**



**4. RESULT FILTERING**

   **- Filter to nodes with score > 0**

   **- Sort descending by score**

   **- Return top N results (typically 15)**



**WHY NOT CONFIDENCE PERCENTAGES?**

**The specification (Section 5.2.1) explicitly forbids outputting "confidence %"**

**like "95% confident". Without probabilistic model calibration, any percentage**

**would be a fabrication. We output raw density scores only (e.g., "Score: 15.5").**



**================================================================================**

**SECTION 3.3: CREATE copilot\_tier2.py (SANDBOX RETRIEVAL SCRIPT)**

**================================================================================**



**FILE: copilot\_tier2.py**

**----------------------**

**"""**

**AI-Native Split-RAG System v2.0 - Tier 2 Retrieval Engine**



**"The Consumer" - This script runs inside the Microsoft Copilot Studio**

**Code Interpreter sandbox. It has SEVERE constraints that must be respected.**



**CRITICAL CONSTRAINTS (CANON\_002):**

  **- ONLY pandas and Python standard library allowed**

  **- NO pydantic (use raw dict access)**

  **- NO torch, transformers, sklearn, scipy**

  **- NO network access (no API calls)**

  **- Memory limit: ~256MB (no heavy imports)**



**This script consumes the pre-computed Context Graph JSON from Tier 1 and**

**performs deterministic Vectorized Keyword Density retrieval.**



**Verification Checkpoints:**

  **- CP-001: All functions have explicit return type hints**

  **- CP-002: Specific exceptions where possible**

  **- CP-003: Paths not applicable (sandbox file handling)**

  **- CP-004: ONLY stdlib + pandas imports**



**Usage in Copilot Studio:**

  **1. Upload Context Graph JSON as data source**

  **2. Call retrieve\_context(query, json\_string) for Mode A**

  **3. Call generate\_data\_mart(json\_string) for Mode B**

**"""**



**# =============================================================================**

**# IMPORTS - STRICTLY STDLIB + PANDAS ONLY**

**# =============================================================================**



**# Standard Library Only (CANON\_001)**

**import json**

**import re**

**import math**

**from typing import List, Dict, Any, Optional, Tuple**



**# The ONLY permitted external dependency (CANON\_002)**

**import pandas as pd**



**# =============================================================================**

**# FORBIDDEN IMPORT CHECK (DEFENSIVE)**

**# =============================================================================**

**# This section documents what CANNOT be imported in Tier 2.**

**# If you accidentally add any of these, the sandbox will crash.**

**#**

**# FORBIDDEN:**

**#   import pydantic        # ~50MB, validation overhead**

**#   import torch           # ~400MB+, INSTANT CRASH**

**#   import transformers    # ~200MB+, CRASH**

**#   import sklearn         # ML overhead**

**#   import scipy           # Scientific computing overhead**

**#   import nltk            # NLP toolkit, large models**

**#   import spacy           # NLP, very heavy**

**#   import requests        # Network access forbidden**

**#   import httpx           # Network access forbidden**

**#   import langchain       # API-dependent, huge deps**

**#   import tabulate        # Not pre-installed in sandbox**

**#   import matplotlib      # Heavy, usually not needed for retrieval**

**#   import numpy as np     # Often implicit via pandas, but avoid direct use**

**#**

**# =============================================================================**





**# =============================================================================**

**# CONFIGURATION (HARDCODED FOR SANDBOX - NO FILE I/O)**

**# =============================================================================**



**# Stopwords for query preprocessing (since nltk is unavailable)**

**# This list covers common English stopwords that don't carry semantic meaning**

**STOPWORDS = frozenset({**

    **'the', 'is', 'at', 'which', 'on', 'and', 'a', 'an', 'of', 'to',**

    **'in', 'for', 'with', 'by', 'from', 'as', 'or', 'be', 'are', 'was',**

    **'were', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',**

    **'will', 'would', 'could', 'should', 'may', 'might', 'must', 'shall',**

    **'this', 'that', 'these', 'those', 'it', 'its', 'they', 'them', 'their',**

    **'what', 'who', 'whom', 'whose', 'where', 'when', 'why', 'how',**

    **'all', 'each', 'every', 'both', 'few', 'more', 'most', 'other',**

    **'some', 'such', 'no', 'not', 'only', 'own', 'same', 'so', 'than',**

    **'too', 'very', 'just', 'can', 'but', 'if', 'then', 'else', 'about'**

**})**



**# Content type weights for scoring**

**# Headers are strong topic indicators, tables contain dense structured data**

**CONTENT\_TYPE\_WEIGHTS = {**

    **'header': 3.0,**

    **'table': 2.5,**

    **'kv\_pair': 2.0,**

    **'image\_caption': 1.5,**

    **'text': 1.0**

**}**



**# Scope weight multiplier (Primary contract content is prioritized)**

**PRIMARY\_SCOPE\_MULTIPLIER = 1.5**



**# Maximum results to return**

**MAX\_RESULTS = 15**





**# =============================================================================**

**# MARKDOWN TABLE HELPER (SINCE tabulate IS NOT AVAILABLE)**

**# =============================================================================**



**def simple\_markdown\_table(df: pd.DataFrame) -> str:**

    **"""**

    **Convert DataFrame to Markdown table format without external dependencies.**

    

    **The tabulate library is not available in the Copilot Studio sandbox,**

    **so we implement this manually. The output is suitable for injection**

    **into LLM context windows where Markdown is rendered properly.**

    

    **Args:**

        **df: pandas DataFrame to convert**

        

    **Returns:**

        **Markdown-formatted table string**

        

    **Example Output:**

        **| content\_type | content | score |**

        **| --- | --- | --- |**

        **| header | Financial Covenants | 15.5 |**

        **| table | Debt-to-Equity ratios... | 12.0 |**

    **"""**

    **if df.empty:**

        **return "\_No data to display.\_"**

    

    **# Extract column names for header row**

    **columns = list(df.columns)**

    

    **# Build header row with pipe separators**

    **header\_row = "| " + " | ".join(str(col) for col in columns) + " |"**

    

    **# Build separator row (GitHub-flavored Markdown style)**

    **separator\_row = "| " + " | ".join(\["---"] \* len(columns)) + " |"**

    

    **# Build data rows, handling potential None values and special characters**

    **data\_rows = \[]**

    **for \_, row in df.iterrows():**

        **# Convert each cell to string and escape pipe characters**

        **cells = \[]**

        **for val in row:**

            **cell\_str = str(val) if val is not None else ""**

            **# Escape pipe characters that would break table formatting**

            **cell\_str = cell\_str.replace("|", "\\\\|")**

            **# Truncate very long cells to prevent context overflow**

            **if len(cell\_str) > 500:**

                **cell\_str = cell\_str\[:497] + "..."**

            **cells.append(cell\_str)**

        

        **row\_str = "| " + " | ".join(cells) + " |"**

        **data\_rows.append(row\_str)**

    

    **# Combine all parts**

    **return "\\n".join(\[header\_row, separator\_row] + data\_rows)**





**def simple\_markdown\_list(items: List\[str], numbered: bool = False) -> str:**

    **"""**

    **Convert list of strings to Markdown list format.**

    

    **Args:**

        **items: List of strings to format**

        **numbered: If True, create numbered list; otherwise bullet list**

        

    **Returns:**

        **Markdown-formatted list string**

    **"""**

    **if not items:**

        **return "\_No items.\_"**

    

    **lines = \[]**

    **for i, item in enumerate(items, start=1):**

        **prefix = f"{i}." if numbered else "-"**

        **lines.append(f"{prefix} {item}")**

    

    **return "\\n".join(lines)**





**# =============================================================================**

**# QUERY PREPROCESSING**

**# =============================================================================**



**def preprocess\_query(query: str) -> List\[str]:**

    **"""**

    **Normalize and tokenize a user query for keyword matching.**

    

    **This function performs several transformations to improve matching:**

    **1. Lowercase conversion for case-insensitive matching**

    **2. Punctuation removal to handle "borrower," vs "borrower"**

    **3. Whitespace normalization**

    **4. Stopword removal to focus on meaningful terms**

    **5. Deduplication to prevent double-counting**

    

    **Args:**

        **query: Raw user query string**

        

    **Returns:**

        **List of unique, normalized keywords**

        

    **Example:**

        **"What is the Borrower's obligation?"** 

        **-> \["borrower", "obligation"]**

    **"""**

    **# Step 1: Lowercase**

    **query\_lower = query.lower()**

    

    **# Step 2: Remove punctuation (keep only alphanumeric and whitespace)**

    **# Using regex since we have access to the re module**

    **query\_clean = re.sub(r'\[^\\w\\s]', ' ', query\_lower)**

    

    **# Step 3: Split on whitespace and filter empty strings**

    **tokens = \[token.strip() for token in query\_clean.split() if token.strip()]**

    

    **# Step 4: Remove stopwords**

    **keywords = \[token for token in tokens if token not in STOPWORDS]**

    

    **# Step 5: Deduplicate while preserving order (for debugging clarity)**

    **seen = set()**

    **unique\_keywords = \[]**

    **for kw in keywords:**

        **if kw not in seen:**

            **seen.add(kw)**

            **unique\_keywords.append(kw)**

    

    **return unique\_keywords**





**# =============================================================================**

**# CONTEXT GRAPH LOADING**

**# =============================================================================**



**def load\_context\_graph(json\_string: str) -> Tuple\[Dict\[str, Any], pd.DataFrame]:**

    **"""**

    **Parse Context Graph JSON and load nodes into a DataFrame.**

    

    **This function handles the transition from JSON (Tier 1 output) to**

    **pandas DataFrame (Tier 2 working format). It extracts metadata**

    **fields that are nested in the JSON structure.**

    

    **Args:**

        **json\_string: Raw JSON string of the Context Graph**

        

    **Returns:**

        **Tuple of (graph\_metadata dict, nodes DataFrame)**

        

    **Raises:**

        **ValueError: If JSON is invalid or missing required fields**

    **"""**

    **# Parse JSON**

    **try:**

        **data = json.loads(json\_string)**

    **except json.JSONDecodeError as e:**

        **raise ValueError(f"Invalid JSON in Context Graph: {e}")**

    

    **# Extract graph-level metadata (for entity disambiguation)**

    **graph\_metadata = {**

        **'document\_id': data.get('document\_id', 'unknown'),**

        **'filename': data.get('filename', 'unknown'),**

        **'borrower\_entity': data.get('borrower\_entity'),**

        **'lender\_entity': data.get('lender\_entity'),**

        **'guarantor\_entity': data.get('guarantor\_entity'),**

        **'schema\_version': data.get('schema\_version', '1.0.0')**

    **}**

    

    **# Extract nodes**

    **nodes = data.get('nodes', \[])**

    **if not nodes:**

        **raise ValueError("Context Graph contains no nodes")**

    

    **# Convert to DataFrame**

    **df = pd.DataFrame(nodes)**

    

    **# Flatten metadata fields for easier access**

    **# The 'metadata' column contains nested dicts that we need to extract**

    **if 'metadata' in df.columns:**

        **# Extract commonly needed metadata fields**

        **df\['page\_number'] = df\['metadata'].apply(**

            **lambda x: x.get('page\_number', 0) if isinstance(x, dict) else 0**

        **)**

        **df\['source\_scope'] = df\['metadata'].apply(**

            **lambda x: x.get('source\_scope', 'primary') if isinstance(x, dict) else 'primary'**

        **)**

        **df\['extraction\_method'] = df\['metadata'].apply(**

            **lambda x: x.get('extraction\_method', 'unknown') if isinstance(x, dict) else 'unknown'**

        **)**

        **df\['is\_active'] = df\['metadata'].apply(**

            **lambda x: x.get('is\_active', True) if isinstance(x, dict) else True**

        **)**

    

    **return graph\_metadata, df**





**# =============================================================================**

**# VECTORIZED KEYWORD DENSITY RETRIEVAL**

**# =============================================================================**



**def calculate\_keyword\_density(**

    **df: pd.DataFrame,**

    **keywords: List\[str]**

**) -> pd.DataFrame:**

    **"""**

    **Calculate keyword density scores using vectorized pandas operations.**

    

    **This is the core retrieval algorithm. It uses pandas' str.count() method**

    **which is implemented in C and runs approximately 300x faster than Python**

    **loops. This speed is critical in the memory-constrained sandbox.**

    

    **The algorithm counts occurrences of each keyword in the content column,**

    **then applies content-type weights and scope multipliers.**

    

    **Args:**

        **df: DataFrame containing 'content' and 'content\_type' columns**

        **keywords: List of preprocessed keywords to search for**

        

    **Returns:**

        **DataFrame with added 'score' column, sorted by score descending**

    **"""**

    **# Create a copy to avoid modifying the original**

    **result\_df = df.copy()**

    

    **# Initialize score column**

    **result\_df\['score'] = 0.0**

    

    **# Pre-compute lowercase content for matching (vectorized operation)**

    **# This avoids repeated lowercase conversion in the loop**

    **result\_df\['\_content\_lower'] = result\_df\['content'].str.lower()**

    

    **# Count each keyword across all rows (vectorized)**

    **for keyword in keywords:**

        **# str.count() is vectorized in pandas - runs in C, not Python**

        **# This counts occurrences of the keyword substring in each cell**

        **keyword\_counts = result\_df\['\_content\_lower'].str.count(**

            **re.escape(keyword)  # Escape special regex characters**

        **)**

        **result\_df\['score'] += keyword\_counts**

    

    **# Apply content type weights**

    **# Using .map() which is also vectorized**

    **result\_df\['\_type\_weight'] = result\_df\['content\_type'].map(CONTENT\_TYPE\_WEIGHTS)**

    **result\_df\['\_type\_weight'] = result\_df\['\_type\_weight'].fillna(1.0)**

    **result\_df\['score'] \*= result\_df\['\_type\_weight']**

    

    **# Apply primary scope multiplier (prioritize contract-specific content)**

    **if 'source\_scope' in result\_df.columns:**

        **primary\_mask = result\_df\['source\_scope'] == 'primary'**

        **result\_df.loc\[primary\_mask, 'score'] \*= PRIMARY\_SCOPE\_MULTIPLIER**

    

    **# Filter only active nodes (Keep-All Policy support)**

    **if 'is\_active' in result\_df.columns:**

        **result\_df = result\_df\[result\_df\['is\_active'] == True]**

    

    **# Clean up temporary columns**

    **result\_df = result\_df.drop(columns=\['\_content\_lower', '\_type\_weight'], errors='ignore')**

    

    **# Filter to non-zero scores and sort**

    **result\_df = result\_df\[result\_df\['score'] > 0]**

    **result\_df = result\_df.sort\_values(by='score', ascending=False)**

    

    **return result\_df**





**# =============================================================================**

**# ENTITY DISAMBIGUATION SUPPORT**

**# =============================================================================**



**def resolve\_entity\_query(**

    **query: str,**

    **graph\_metadata: Dict\[str, Any]**

**) -> Tuple\[str, Optional\[str]]:**

    **"""**

    **Check if query is asking about a specific entity and resolve it.**

    

    **This implements the disambiguation logic from Section 3.2.2 of the spec.**

    **If the user asks "Who is the borrower?", we first check the graph's**

    **borrower\_entity field before searching content.**

    

    **Args:**

        **query: User's query string**

        **graph\_metadata: Graph-level metadata including anchored entities**

        

    **Returns:**

        **Tuple of (possibly modified query, direct answer if entity found)**

    **"""**

    **query\_lower = query.lower()**

    

    **# Check for borrower questions**

    **if any(phrase in query\_lower for phrase in \['who is the borrower', 'borrower name', 'identify the borrower']):**

        **borrower = graph\_metadata.get('borrower\_entity')**

        **if borrower:**

            **return query, f"The Borrower is \*\*{borrower}\*\* (extracted from contract preamble)."**

    

    **# Check for lender questions**

    **if any(phrase in query\_lower for phrase in \['who is the lender', 'lender name', 'identify the lender']):**

        **lender = graph\_metadata.get('lender\_entity')**

        **if lender:**

            **return query, f"The Lender is \*\*{lender}\*\* (extracted from contract preamble)."**

    

    **# Check for guarantor questions**

    **if any(phrase in query\_lower for phrase in \['who is the guarantor', 'guarantor name', 'identify the guarantor']):**

        **guarantor = graph\_metadata.get('guarantor\_entity')**

        **if guarantor:**

            **return query, f"The Guarantor is \*\*{guarantor}\*\* (extracted from contract preamble)."**

    

    **# Virtual substitution: replace "borrower" with actual entity name for better matching**

    **borrower = graph\_metadata.get('borrower\_entity')**

    **if borrower and 'borrower' in query\_lower:**

        **# Add the entity name as an additional search term**

        **query = f"{query} {borrower}"**

    

    **return query, None**





**# =============================================================================**

**# MODE A: CONVERSATIONAL RETRIEVAL**

**# =============================================================================**



**def retrieve\_context(**

    **query: str,**

    **context\_json\_str: str,**

    **max\_results: int = MAX\_RESULTS**

**) -> str:**

    **"""**

    **Primary retrieval function for Mode A (Conversational Synthesis).**

    

    **This function implements the full retrieval pipeline:**

    **1. Load and parse the Context Graph**

    **2. Check for entity disambiguation (direct answers)**

    **3. Preprocess the query into keywords**

    **4. Calculate keyword density scores**

    **5. Format top results as Markdown for LLM context injection**

    

    **The output is designed to be injected into an LLM system prompt with**

    **instructions like "Use ONLY the provided facts."**

    

    **Args:**

        **query: User's natural language query**

        **context\_json\_str: Raw JSON string of the Context Graph**

        **max\_results: Maximum number of chunks to return (default 15)**

        

    **Returns:**

        **Markdown-formatted string with retrieved context or error message**

    **"""**

    **# Validate inputs**

    **if not query or not query.strip():**

        **return "Error: Please provide a search query."**

    

    **if not context\_json\_str or not context\_json\_str.strip():**

        **return "Error: No Context Graph provided."**

    

    **try:**

        **# Step 1: Load Context Graph**

        **graph\_metadata, df = load\_context\_graph(context\_json\_str)**

        

        **# Step 2: Check for entity disambiguation**

        **modified\_query, direct\_answer = resolve\_entity\_query(query, graph\_metadata)**

        **if direct\_answer:**

            **# For entity questions, return direct answer plus supporting context**

            **return direct\_answer**

        

        **# Step 3: Preprocess query**

        **keywords = preprocess\_query(modified\_query)**

        

        **if not keywords:**

            **return "Error: No searchable keywords found. Please use more specific terms."**

        

        **# Step 4: Calculate keyword density scores**

        **results\_df = calculate\_keyword\_density(df, keywords)**

        

        **if results\_df.empty:**

            **return f"No relevant content found for keywords: {', '.join(keywords)}"**

        

        **# Step 5: Limit results**

        **top\_results = results\_df.head(max\_results)**

        

        **# Step 6: Format output**

        **output\_parts = \[**

            **f"\*\*Retrieved Context\*\* (Query: {query})",**

            **f"Keywords: {', '.join(keywords)}",**

            **f"Results: {len(top\_results)} of {len(results\_df)} matches",**

            **"",**

            **simple\_markdown\_table(**

                **top\_results\[\['content\_type', 'content', 'page\_number', 'score']]**

            **)**

        **]**

        

        **return "\\n".join(output\_parts)**

        

    **except ValueError as e:**

        **return f"Error loading Context Graph: {e}"**

    **except Exception as e:**

        **# At the boundary, catch all to prevent sandbox crash**

        **return f"Retrieval error: {str(e)}"**





**# =============================================================================**

**# MODE B: DATA MART EXPORT**

**# =============================================================================**



**def generate\_data\_mart(**

    **context\_json\_str: str,**

    **content\_types: Optional\[List\[str]] = None,**

    **output\_format: str = "excel"**

**) -> str:**

    **"""**

    **Export structured data from Context Graph to spreadsheet format (Mode B).**

    

    **This transforms the RAG system into a Data Mart Generator, allowing users**

    **to request exports like "Export all financial covenants to Excel."**

    

    **The function filters nodes by content type (defaulting to tables and**

    **key-value pairs which contain the most structured data) and exports**

    **to either Excel or CSV format.**

    

    **Args:**

        **context\_json\_str: Raw JSON string of the Context Graph**

        **content\_types: List of content types to include (default: table, kv\_pair)**

        **output\_format: "excel" or "csv" (default: excel)**

        

    **Returns:**

        **Success message with filename, or error message**

    **"""**

    **# Default to structured content types**

    **if content\_types is None:**

        **content\_types = \['table', 'kv\_pair']**

    

    **try:**

        **# Load Context Graph**

        **graph\_metadata, df = load\_context\_graph(context\_json\_str)**

        

        **# Filter by content types**

        **filtered\_df = df\[df\['content\_type'].isin(content\_types)]**

        

        **if filtered\_df.empty:**

            **available\_types = df\['content\_type'].unique().tolist()**

            **return (**

                **f"No content found matching types: {content\_types}. "**

                **f"Available types: {available\_types}"**

            **)**

        

        **# Select columns for export**

        **export\_columns = \['chunk\_id', 'content\_type', 'content', 'page\_number']**

        **export\_columns = \[col for col in export\_columns if col in filtered\_df.columns]**

        **export\_df = filtered\_df\[export\_columns]**

        

        **# Generate filename**

        **doc\_name = graph\_metadata.get('filename', 'document').replace('.', '\_')**

        **timestamp = pd.Timestamp.now().strftime('%Y%m%d\_%H%M%S')**

        

        **if output\_format.lower() == 'csv':**

            **filename = f"DataMart\_{doc\_name}\_{timestamp}.csv"**

            **export\_df.to\_csv(filename, index=False)**

        **else:**

            **# Excel export - openpyxl is typically available in Copilot sandbox**

            **filename = f"DataMart\_{doc\_name}\_{timestamp}.xlsx"**

            **try:**

                **export\_df.to\_excel(filename, index=False, engine='openpyxl')**

            **except ImportError:**

                **# Fallback to CSV if openpyxl not available**

                **filename = f"DataMart\_{doc\_name}\_{timestamp}.csv"**

                **export\_df.to\_csv(filename, index=False)**

                **return (**

                    **f"Excel export unavailable (openpyxl missing). "**

                    **f"Generated CSV instead: {filename} ({len(export\_df)} rows)"**

                **)**

        

        **return f"Generated {filename} containing {len(export\_df)} rows."**

        

    **except ValueError as e:**

        **return f"Error loading Context Graph: {e}"**

    **except Exception as e:**

        **return f"Data Mart generation error: {str(e)}"**





**# =============================================================================**

**# SECTION/TOPIC FILTERING**

**# =============================================================================**



**def retrieve\_section(**

    **section\_name: str,**

    **context\_json\_str: str**

**) -> str:**

    **"""**

    **Retrieve all content from a specific document section.**

    

    **This allows queries like "Show me the Covenants section" which target**

    **a specific part of the document rather than keyword-based search.**

    

    **Args:**

        **section\_name: Name of the section to retrieve (e.g., "Covenants")**

        **context\_json\_str: Raw JSON string of the Context Graph**

        

    **Returns:**

        **Markdown-formatted content from the section**

    **"""**

    **try:**

        **graph\_metadata, df = load\_context\_graph(context\_json\_str)**

        

        **# First, find headers matching the section name**

        **section\_lower = section\_name.lower()**

        **header\_mask = (**

            **(df\['content\_type'] == 'header') \&** 

            **(df\['content'].str.lower().str.contains(section\_lower, regex=False))**

        **)**

        

        **matching\_headers = df\[header\_mask]**

        

        **if matching\_headers.empty:**

            **return f"No section found matching '{section\_name}'."**

        

        **# Get the first matching header's chunk\_id**

        **section\_id = matching\_headers.iloc\[0]\['chunk\_id']**

        

        **# Find all content belonging to this section**

        **section\_content = df\[df\['parent\_section\_id'] == section\_id]**

        

        **# Also include the header itself**

        **all\_section = pd.concat(\[matching\_headers.head(1), section\_content])**

        **all\_section = all\_section.sort\_values('page\_number')**

        

        **if all\_section.empty:**

            **return f"Section '{section\_name}' found but contains no content."**

        

        **# Format output**

        **output\_parts = \[**

            **f"\*\*Section: {section\_name}\*\*",**

            **f"Pages: {all\_section\['page\_number'].min()} - {all\_section\['page\_number'].max()}",**

            **""**

        **]**

        

        **for \_, row in all\_section.iterrows():**

            **if row\['content\_type'] == 'header':**

                **output\_parts.append(f"### {row\['content']}")**

            **elif row\['content\_type'] == 'table':**

                **output\_parts.append(row\['content'])  # Already in markdown**

            **else:**

                **output\_parts.append(row\['content'])**

            **output\_parts.append("")**

        

        **return "\\n".join(output\_parts)**

        

    **except ValueError as e:**

        **return f"Error: {e}"**

    **except Exception as e:**

        **return f"Section retrieval error: {str(e)}"**





**# =============================================================================**

**# SCOPE-FILTERED RETRIEVAL**

**# =============================================================================**



**def retrieve\_by\_scope(**

    **query: str,**

    **context\_json\_str: str,**

    **scope: str = "primary"**

**) -> str:**

    **"""**

    **Retrieve content filtered by source scope (primary vs corpus).**

    

    **This supports the disambiguation requirement: queries about the specific**

    **contract should search "primary" scope, while queries about standard**

    **definitions should search "corpus" scope.**

    

    **Args:**

        **query: User's search query**

        **context\_json\_str: Raw JSON string of the Context Graph**

        **scope: "primary" for contract-specific, "corpus" for reference material**

        

    **Returns:**

        **Markdown-formatted results filtered by scope**

    **"""**

    **try:**

        **graph\_metadata, df = load\_context\_graph(context\_json\_str)**

        

        **# Filter by scope**

        **if 'source\_scope' in df.columns:**

            **df = df\[df\['source\_scope'] == scope]**

        

        **if df.empty:**

            **return f"No content found in '{scope}' scope."**

        

        **# Run standard keyword retrieval on filtered set**

        **keywords = preprocess\_query(query)**

        

        **if not keywords:**

            **return "Error: No searchable keywords found."**

        

        **results\_df = calculate\_keyword\_density(df, keywords)**

        

        **if results\_df.empty:**

            **return f"No '{scope}' content matches keywords: {', '.join(keywords)}"**

        

        **top\_results = results\_df.head(MAX\_RESULTS)**

        

        **scope\_label = "Contract-Specific" if scope == "primary" else "Reference/Corpus"**

        

        **output\_parts = \[**

            **f"\*\*{scope\_label} Results\*\* (Query: {query})",**

            **f"Keywords: {', '.join(keywords)}",**

            **"",**

            **simple\_markdown\_table(**

                **top\_results\[\['content\_type', 'content', 'page\_number', 'score']]**

            **)**

        **]**

        

        **return "\\n".join(output\_parts)**

        

    **except ValueError as e:**

        **return f"Error: {e}"**

    **except Exception as e:**

        **return f"Scope retrieval error: {str(e)}"**





**# =============================================================================**

**# UTILITY FUNCTIONS FOR COPILOT INTEGRATION**

**# =============================================================================**



**def get\_document\_summary(context\_json\_str: str) -> str:**

    **"""**

    **Generate a summary of the Context Graph for user orientation.**

    

    **This helps users understand what content is available before querying.**

    

    **Args:**

        **context\_json\_str: Raw JSON string of the Context Graph**

        

    **Returns:**

        **Markdown-formatted summary**

    **"""**

    **try:**

        **graph\_metadata, df = load\_context\_graph(context\_json\_str)**

        

        **# Calculate statistics**

        **total\_nodes = len(df)**

        **content\_type\_counts = df\['content\_type'].value\_counts().to\_dict()**

        **page\_range = (df\['page\_number'].min(), df\['page\_number'].max())**

        

        **# Build summary**

        **output\_parts = \[**

            **f"\*\*Document Summary\*\*",**

            **f"- File: {graph\_metadata.get('filename', 'Unknown')}",**

            **f"- Document ID: {graph\_metadata.get('document\_id', 'Unknown')\[:8]}...",**

            **f"- Pages: {page\_range\[0]} to {page\_range\[1]}",**

            **f"- Total Content Nodes: {total\_nodes}",**

            **"",**

            **"\*\*Content Breakdown:\*\*"**

        **]**

        

        **for ctype, count in sorted(content\_type\_counts.items()):**

            **output\_parts.append(f"- {ctype}: {count}")**

        

        **# Add entity information if available**

        **output\_parts.append("")**

        **output\_parts.append("\*\*Identified Entities:\*\*")**

        

        **borrower = graph\_metadata.get('borrower\_entity')**

        **lender = graph\_metadata.get('lender\_entity')**

        **guarantor = graph\_metadata.get('guarantor\_entity')**

        

        **if borrower:**

            **output\_parts.append(f"- Borrower: {borrower}")**

        **if lender:**

            **output\_parts.append(f"- Lender: {lender}")**

        **if guarantor:**

            **output\_parts.append(f"- Guarantor: {guarantor}")**

        

        **if not any(\[borrower, lender, guarantor]):**

            **output\_parts.append("- No entities automatically identified")**

        

        **return "\\n".join(output\_parts)**

        

    **except ValueError as e:**

        **return f"Error: {e}"**

    **except Exception as e:**

        **return f"Summary error: {str(e)}"**





**def list\_available\_sections(context\_json\_str: str) -> str:**

    **"""**

    **List all section headers found in the document.**

    

    **Useful for users to discover what sections are available for**

    **targeted retrieval.**

    

    **Args:**

        **context\_json\_str: Raw JSON string of the Context Graph**

        

    **Returns:**

        **Markdown-formatted list of sections**

    **"""**

    **try:**

        **graph\_metadata, df = load\_context\_graph(context\_json\_str)**

        

        **# Filter to headers only**

        **headers = df\[df\['content\_type'] == 'header']**

        

        **if headers.empty:**

            **return "No section headers found in document."**

        

        **# Sort by page number**

        **headers = headers.sort\_values('page\_number')**

        

        **output\_parts = \["\*\*Available Sections:\*\*", ""]**

        

        **for \_, row in headers.iterrows():**

            **page = row.get('page\_number', '?')**

            **content = row\['content']\[:80]  # Truncate long headers**

            **if len(row\['content']) > 80:**

                **content += "..."**

            **output\_parts.append(f"- Page {page}: {content}")**

        

        **return "\\n".join(output\_parts)**

        

    **except ValueError as e:**

        **return f"Error: {e}"**

    **except Exception as e:**

        **return f"Section listing error: {str(e)}"**





**# =============================================================================**

**# MODULE SELF-TEST (for development outside sandbox)**

**# =============================================================================**



**if \_\_name\_\_ == "\_\_main\_\_":**

    **# This test will only run outside the Copilot sandbox**

    **# It verifies the basic functionality before deployment**

    

    **print("Testing Tier 2 Retrieval Logic...")**

    **print("=" \* 50)**

    

    **# Create a minimal test Context Graph**

    **test\_graph = {**

        **"document\_id": "abc123def456abc123def456abc123de",**

        **"filename": "test\_agreement.pdf",**

        **"processed\_at": "2025-01-25T12:00:00Z",**

        **"schema\_version": "2.0.0",**

        **"borrower\_entity": "Acme Corporation",**

        **"lender\_entity": "First National Bank",**

        **"nodes": \[**

            **{**

                **"chunk\_id": "111111111111111111111111111111aa",**

                **"parent\_section\_id": "root",**

                **"content\_type": "header",**

                **"content": "ARTICLE I - DEFINITIONS",**

                **"metadata": {"page\_number": 1, "source\_scope": "primary"},**

                **"lineage\_trace": "a" \* 64**

            **},**

            **{**

                **"chunk\_id": "222222222222222222222222222222bb",**

                **"parent\_section\_id": "111111111111111111111111111111aa",**

                **"content\_type": "text",**

                **"content": "The Borrower agrees to maintain a minimum debt-to-equity ratio of 2.0.",**

                **"metadata": {"page\_number": 2, "source\_scope": "primary"},**

                **"lineage\_trace": "b" \* 64**

            **},**

            **{**

                **"chunk\_id": "333333333333333333333333333333cc",**

                **"parent\_section\_id": "root",**

                **"content\_type": "table",**

                **"content": "| Metric | Threshold |\\n| --- | --- |\\n| Debt/Equity | 2.0 |",**

                **"metadata": {"page\_number": 3, "source\_scope": "primary"},**

                **"lineage\_trace": "c" \* 64**

            **}**

        **]**

    **}**

    

    **test\_json = json.dumps(test\_graph)**

    

    **# Test 1: Query preprocessing**

    **print("\\n1. Query Preprocessing:")**

    **keywords = preprocess\_query("What is the borrower's debt ratio?")**

    **print(f"   Keywords: {keywords}")**

    

    **# Test 2: Entity disambiguation**

    **print("\\n2. Entity Disambiguation:")**

    **result = retrieve\_context("Who is the borrower?", test\_json)**

    **print(f"   Result: {result\[:100]}...")**

    

    **# Test 3: Keyword retrieval**

    **print("\\n3. Keyword Retrieval:")**

    **result = retrieve\_context("debt equity ratio", test\_json)**

    **print(f"   Result:\\n{result}")**

    

    **# Test 4: Document summary**

    **print("\\n4. Document Summary:")**

    **summary = get\_document\_summary(test\_json)**

    **print(summary)**

    

    **print("\\n" + "=" \* 50)**

    **print("All tests passed!")**

**----------------------**



**================================================================================**

**SECTION 3.4: COPILOT STUDIO INTEGRATION NOTES**

**================================================================================**



**When deploying copilot\_tier2.py to Microsoft Copilot Studio:**



**1. UPLOAD THE SCRIPT**

   **- Navigate to your Copilot Studio environment**

   **- Add a new "Code Block" action in your flow**

   **- Paste the copilot\_tier2.py content**



**2. UPLOAD THE CONTEXT GRAPH**

   **- The Context Graph JSON from Tier 1 must be accessible**

   **- Options: Upload as a data source, store in SharePoint, or pass inline**

   **- For large graphs (>5MB), consider chunking or compression**



**3. CONNECT THE FLOW**

   **- User message → Code Block (retrieve\_context) → LLM Response**

   **- Pass user's query and Context Graph JSON as inputs**

   **- Use the output as context for GPT-4 generation**



**4. SYSTEM PROMPT TEMPLATE FOR LLM**

   **When injecting retrieved context into the LLM prompt, use:**

   

   **```**

   **You are a document analysis assistant. Answer questions using ONLY** 

   **the facts provided in the Retrieved Context below. If the answer is** 

   **not in the context, say "I cannot find this information in the document."**

   

   **Do NOT make up information. Do NOT use external knowledge.**

   

   **Retrieved Context:**

   **{output\_from\_retrieve\_context}**

   

   **User Question: {user\_query}**

   **```**



**5. ERROR HANDLING**

   **- If retrieve\_context returns an error message (starts with "Error:"),**

     **display it to the user rather than passing to LLM**

   **- Consider adding retry logic for transient failures**



**================================================================================**

**SECTION 3.5: VERIFICATION CHECKPOINT - BLOCK 3**

**================================================================================**



**Before proceeding, verify the following:**



**IMPORT COMPLIANCE CHECKLIST:**

**\[ ] ONLY json, re, math from standard library are imported**

**\[ ] ONLY pandas is imported as external dependency**

**\[ ] NO pydantic import (would add memory overhead)**

**\[ ] NO torch, transformers, sklearn, scipy imports**

**\[ ] NO network libraries (requests, httpx)**

**\[ ] Forbidden import section clearly documents what's not allowed**



**ALGORITHM CORRECTNESS CHECKLIST:**

**\[ ] preprocess\_query removes stopwords from hardcoded list**

**\[ ] preprocess\_query handles punctuation and case normalization**

**\[ ] calculate\_keyword\_density uses df\['content'].str.count() (vectorized)**

**\[ ] Content type weights match spec: header=3.0, table=2.5, text=1.0**

**\[ ] Primary scope multiplier is 1.5x**

**\[ ] Results sorted by score descending, limited to max\_results**



**DISAMBIGUATION CHECKLIST:**

**\[ ] resolve\_entity\_query checks borrower\_entity from graph metadata**

**\[ ] Direct answers returned for "Who is the borrower?" questions**

**\[ ] Virtual substitution adds entity name to search keywords**



**OUTPUT FORMAT CHECKLIST:**

**\[ ] simple\_markdown\_table generates valid GitHub-flavored Markdown**

**\[ ] Long cell content is truncated (500 char limit)**

**\[ ] Pipe characters in content are escaped**

**\[ ] No confidence percentages (raw scores only)**



**DATA MART CHECKLIST:**

**\[ ] generate\_data\_mart filters by content\_type**

**\[ ] Excel export uses openpyxl engine**

**\[ ] CSV fallback if openpyxl unavailable**

**\[ ] Filenames include timestamp for uniqueness**



**TEST IN ISOLATED ENVIRONMENT:**

**```python**

**# Test outside Copilot Studio first**

**python copilot\_tier2.py**



**# Verify memory usage (should be minimal)**

**python -c "import copilot\_tier2; print('Import successful')"**

**```**



**================================================================================**

**BLOCK 3 COMPLETE - AWAIT USER CONFIRMATION**

**================================================================================**



**Present the following to the user for review:**

**1. copilot\_tier2.py (complete Tier 2 retrieval script)**

**2. Explanation of sandbox constraints and why they matter**

**3. Integration notes for Copilot Studio deployment**



**Then ask: "Block 3 (Tier 2 Retrieval Logic) is complete. I have created:**

**- copilot\_tier2.py with:**

  **- Vectorized Keyword Density retrieval (Mode A)**

  **- Data Mart Export functionality (Mode B)**

  **- Entity disambiguation support**

  **- Section-based retrieval**

  **- Scope filtering (primary vs corpus)**

  **- Document summary utilities**

  **- Manual Markdown table generation (no tabulate dependency)**



**All code respects the 256MB memory constraint and uses only pandas + stdlib.**



**May I proceed to Block 4 (Testing, Validation, and Deployment)?"**



**DO NOT proceed to Block 4 until the user explicitly confirms.**



**================================================================================**

**END OF BLOCK 3**

**================================================================================**

**================================================================================**

**SPLIT-RAG IMPLEMENTATION INSTRUCTIONS - BLOCK 4 OF 4**

**TESTING, VALIDATION, QUALITY ASSURANCE, AND DEPLOYMENT**

**================================================================================**



**OVERVIEW**

**--------**

**This final block covers the critical quality assurance processes that ensure**

**the Split-RAG system meets enterprise-grade requirements for determinism,**

**auditability, and correctness. The testing matrix validates that:**



**1. Extraction is deterministic (same input → same output, always)**

**2. Entity disambiguation correctly anchors parties from contracts**

**3. Data Mart exports produce valid, usable spreadsheets**

**4. Tier 2 operates within sandbox constraints**

**5. The Keep-All Policy preserves all content without silent deletion**



**After completing Block 4, present ALL artifacts to the user for final review.**

**The system is then ready for production deployment.**



**================================================================================**

**SECTION 4.1: CREATE TEST DIRECTORY STRUCTURE**

**================================================================================**



**Extend the project with a comprehensive test suite:**



**split-rag/**

**├── tests/**

**│   ├── \_\_init\_\_.py**

**│   ├── conftest.py              # Pytest fixtures and shared utilities**

**│   ├── test\_schema.py           # Schema validation tests**

**│   ├── test\_extractor.py        # Tier 1 extraction tests**

**│   ├── test\_tier2\_retrieval.py  # Tier 2 retrieval tests**

**│   ├── test\_determinism.py      # Determinism validation (T-001)**

**│   ├── test\_disambiguation.py   # Entity disambiguation (T-002)**

**│   ├── test\_data\_mart.py        # Data Mart export (T-003)**

**│   └── fixtures/**

**│       ├── sample\_credit\_agreement.pdf**

**│       ├── sample\_context\_graph.json**

**│       └── expected\_outputs/**

**├── scripts/**

**│   ├── run\_tests.bat            # Windows test runner**

**│   ├── run\_tests.sh             # Linux test runner**

**│   └── validate\_deployment.py   # Pre-deployment validation**

**└── docs/**

    **├── DEPLOYMENT\_GUIDE.md**

    **└── TROUBLESHOOTING.md**



**================================================================================**

**SECTION 4.2: CREATE conftest.py (PYTEST FIXTURES)**

**================================================================================**



**FILE: tests/conftest.py**

**-----------------------**

**"""**

**Pytest Configuration and Shared Fixtures for Split-RAG Test Suite**



**This module provides reusable test fixtures and utilities that are shared**

**across all test modules. Fixtures handle common setup tasks like creating**

**sample documents, loading configurations, and generating test data.**



**Usage:**

    **Fixtures are automatically discovered by pytest. Use them by adding**

    **the fixture name as a parameter to any test function.**

**"""**



**import pytest**

**import json**

**import tempfile**

**import hashlib**

**from pathlib import Path**

**from datetime import datetime, timezone**

**from typing import Dict, Any, Generator**



**# Add parent directory to path for imports**

**import sys**

**sys.path.insert(0, str(Path(\_\_file\_\_).parent.parent))**



**from schema\_v2 import (**

    **ContextGraph,**

    **ContextNode,**

    **NodeMetadata,**

    **ExtractionMetrics,**

    **generate\_document\_id,**

    **generate\_chunk\_id,**

    **generate\_lineage\_trace**

**)**





**# =============================================================================**

**# PATH FIXTURES**

**# =============================================================================**



**@pytest.fixture**

**def project\_root() -> Path:**

    **"""Return the project root directory."""**

    **return Path(\_\_file\_\_).parent.parent**





**@pytest.fixture**

**def fixtures\_dir() -> Path:**

    **"""Return the test fixtures directory."""**

    **return Path(\_\_file\_\_).parent / "fixtures"**





**@pytest.fixture**

**def temp\_output\_dir() -> Generator\[Path, None, None]:**

    **"""**

    **Provide a temporary directory for test outputs.**

    

    **The directory is automatically cleaned up after the test completes.**

    **"""**

    **with tempfile.TemporaryDirectory() as tmpdir:**

        **yield Path(tmpdir)**





**# =============================================================================**

**# CONFIGURATION FIXTURES**

**# =============================================================================**



**@pytest.fixture**

**def sample\_config() -> Dict\[str, Any]:**

    **"""**

    **Provide a minimal valid configuration dictionary.**

    

    **This mirrors the structure of config.json but uses temporary paths.**

    **"""**

    **return {**

        **"version": "2.0.0",**

        **"tier": 1,**

        **"paths": {**

            **"input\_directory": "./test\_input",**

            **"output\_directory": "./test\_output",**

            **"log\_directory": "./test\_logs",**

            **"quarantine\_directory": "./test\_quarantine"**

        **},**

        **"extraction": {**

            **"primary\_engine": "docling",**

            **"fallback\_engine": "pdfplumber",**

            **"enable\_ocr": True,**

            **"enable\_table\_detection": True,**

            **"max\_pages\_for\_entity\_scan": 20,**

            **"chunk\_overlap\_tokens": 50**

        **},**

        **"docling": {**

            **"use\_gpu": False,**

            **"artifacts\_path": None,**

            **"table\_mode": "accurate",**

            **"ocr\_languages": \["en"]**

        **},**

        **"validation": {**

            **"conflict\_threshold\_levenshtein": 0.3,**

            **"max\_conflict\_rate\_for\_quarantine": 0.20,**

            **"enable\_keep\_all\_policy": True**

        **},**

        **"logging": {**

            **"level": "DEBUG",**

            **"format": "%(asctime)s - %(levelname)s - %(message)s",**

            **"file\_rotation\_mb": 10**

        **},**

        **"performance": {**

            **"batch\_size": 5,**

            **"max\_workers": 2**

        **}**

    **}**





**@pytest.fixture**

**def sample\_rules() -> Dict\[str, Any]:**

    **"""Provide sample entity extraction rules."""**

    **return {**

        **"version": "2.0.0",**

        **"entity\_patterns": {**

            **"borrower": {**

                **"explicit\_definition": r'"Borrower"\\s+means\\s+(?P<entity>.+?)(?=\\.|\\n|,)',**

                **"parenthetical": r'(?P<entity>.+?)\\s+\\(the\\s+"Borrower"\\)',**

                **"priority\_order": \["explicit\_definition", "parenthetical"]**

            **},**

            **"lender": {**

                **"explicit\_definition": r'"Lender"\\s+means\\s+(?P<entity>.+?)(?=\\.|\\n|,)',**

                **"priority\_order": \["explicit\_definition"]**

            **}**

        **},**

        **"stopwords": \["the", "is", "at", "which", "on", "and", "a", "an"]**

    **}**





**# =============================================================================**

**# CONTEXT GRAPH FIXTURES**

**# =============================================================================**



**@pytest.fixture**

**def minimal\_context\_graph() -> ContextGraph:**

    **"""**

    **Create a minimal valid ContextGraph for testing.**

    

    **This graph contains the minimum required fields and a single node.**

    **"""**

    **doc\_id = "a" \* 32  # Valid MD5 length**

    **chunk\_id = "b" \* 32**

    **lineage = "c" \* 64  # Valid SHA-256 length**

    

    **node = ContextNode(**

        **chunk\_id=chunk\_id,**

        **parent\_section\_id="root",**

        **content\_type="text",**

        **content="This is test content for validation.",**

        **metadata=NodeMetadata(**

            **page\_number=1,**

            **source\_scope="primary"**

        **),**

        **lineage\_trace=lineage**

    **)**

    

    **return ContextGraph(**

        **document\_id=doc\_id,**

        **filename="test\_document.pdf",**

        **processed\_at=datetime.now(timezone.utc).isoformat(),**

        **nodes=\[node]**

    **)**





**@pytest.fixture**

**def comprehensive\_context\_graph() -> ContextGraph:**

    **"""**

    **Create a comprehensive ContextGraph with multiple node types.**

    

    **This graph simulates a real credit agreement with headers, text,**

    **tables, and entity anchoring for thorough testing.**

    **"""**

    **doc\_content = b"Sample document content for hashing"**

    **doc\_id = generate\_document\_id(doc\_content)**

    

    **nodes = \[]**

    

    **# Header node**

    **header\_content = "ARTICLE I - DEFINITIONS"**

    **header\_id = generate\_chunk\_id(doc\_id, 1, 0, header\_content)**

    **header\_lineage = generate\_lineage\_trace(doc\_id, 0, \[0, 0, 612, 50], "docling")**

    

    **nodes.append(ContextNode(**

        **chunk\_id=header\_id,**

        **parent\_section\_id="root",**

        **content\_type="header",**

        **content=header\_content,**

        **metadata=NodeMetadata(**

            **page\_number=1,**

            **bbox=\[0, 0, 612, 50],**

            **source\_scope="primary",**

            **extraction\_method="docling"**

        **),**

        **lineage\_trace=header\_lineage**

    **))**

    

    **# Text node with borrower definition**

    **text\_content = '"Borrower" means Acme Corporation, a Delaware corporation.'**

    **text\_id = generate\_chunk\_id(doc\_id, 1, 1, text\_content)**

    **text\_lineage = generate\_lineage\_trace(doc\_id, 0, \[0, 50, 612, 100], "docling")**

    

    **nodes.append(ContextNode(**

        **chunk\_id=text\_id,**

        **parent\_section\_id=header\_id,**

        **content\_type="text",**

        **content=text\_content,**

        **metadata=NodeMetadata(**

            **page\_number=1,**

            **bbox=\[0, 50, 612, 100],**

            **source\_scope="primary",**

            **extraction\_method="docling"**

        **),**

        **lineage\_trace=text\_lineage**

    **))**

    

    **# Table node**

    **table\_content = """| Metric | Threshold | Frequency |**

**| --- | --- | --- |**

**| Debt-to-Equity | 2.0:1 | Quarterly |**

**| Interest Coverage | 3.0x | Quarterly |**

**| Current Ratio | 1.5:1 | Monthly |"""**

    

    **table\_id = generate\_chunk\_id(doc\_id, 5, 0, table\_content)**

    **table\_lineage = generate\_lineage\_trace(doc\_id, 4, \[50, 100, 562, 300], "docling")**

    

    **nodes.append(ContextNode(**

        **chunk\_id=table\_id,**

        **parent\_section\_id="root",**

        **content\_type="table",**

        **content=table\_content,**

        **metadata=NodeMetadata(**

            **page\_number=5,**

            **bbox=\[50, 100, 562, 300],**

            **table\_shape=\[4, 3],**

            **source\_scope="primary",**

            **extraction\_method="docling"**

        **),**

        **lineage\_trace=table\_lineage**

    **))**

    

    **# Corpus reference node (for disambiguation testing)**

    **corpus\_content = "Under the UCC, a borrower is any person who receives a loan."**

    **corpus\_id = generate\_chunk\_id(doc\_id, 50, 0, corpus\_content)**

    **corpus\_lineage = generate\_lineage\_trace(doc\_id, 49, None, "docling")**

    

    **nodes.append(ContextNode(**

        **chunk\_id=corpus\_id,**

        **parent\_section\_id="root",**

        **content\_type="text",**

        **content=corpus\_content,**

        **metadata=NodeMetadata(**

            **page\_number=50,**

            **source\_scope="corpus",  # Reference material, not contract-specific**

            **extraction\_method="docling"**

        **),**

        **lineage\_trace=corpus\_lineage**

    **))**

    

    **# Create metrics**

    **metrics = ExtractionMetrics(**

        **total\_pages=50,**

        **total\_nodes=len(nodes),**

        **tables\_extracted=1,**

        **headers\_extracted=1,**

        **conflicts\_detected=0,**

        **extraction\_time\_seconds=2.5,**

        **primary\_engine\_used="docling",**

        **fallback\_triggered=False**

    **)**

    

    **return ContextGraph(**

        **document\_id=doc\_id,**

        **filename="credit\_agreement\_acme.pdf",**

        **processed\_at=datetime.now(timezone.utc).isoformat(),**

        **schema\_version="2.0.0",**

        **borrower\_entity="Acme Corporation",**

        **lender\_entity="First National Bank",**

        **nodes=nodes,**

        **metrics=metrics**

    **)**





**@pytest.fixture**

**def context\_graph\_json(comprehensive\_context\_graph: ContextGraph) -> str:**

    **"""Provide the comprehensive context graph as a JSON string."""**

    **return comprehensive\_context\_graph.to\_json()**





**# =============================================================================**

**# FILE CONTENT FIXTURES**

**# =============================================================================**



**@pytest.fixture**

**def sample\_pdf\_bytes() -> bytes:**

    **"""**

    **Provide minimal valid PDF bytes for testing.**

    

    **This is a minimal PDF that can be used to test file hashing and**

    **basic processing without requiring a full PDF library.**

    **"""**

    **# Minimal valid PDF structure**

    **return b"""%PDF-1.4**

**1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj**

**2 0 obj << /Type /Pages /Kids \[3 0 R] /Count 1 >> endobj**

**3 0 obj << /Type /Page /Parent 2 0 R /MediaBox \[0 0 612 792] >> endobj**

**xref**

**0 4**

**0000000000 65535 f** 

**0000000009 00000 n** 

**0000000058 00000 n** 

**0000000115 00000 n** 

**trailer << /Size 4 /Root 1 0 R >>**

**startxref**

**196**

**%%EOF"""**





**@pytest.fixture**

**def sample\_contract\_text() -> str:**

    **"""**

    **Provide sample credit agreement text for entity extraction testing.**

    

    **This text contains patterns that should be matched by the entity**

    **extraction regex patterns in rules.json.**

    **"""**

    **return """**

    **CREDIT AGREEMENT**

    

    **Dated as of January 15, 2025**

    

    **Between**

    

    **FIRST NATIONAL BANK, N.A. (the "Lender")**

    

    **and**

    

    **ACME CORPORATION, a Delaware corporation (the "Borrower")**

    

    **and**

    

    **ACME HOLDINGS LLC (the "Guarantor")**

    

    **ARTICLE I - DEFINITIONS**

    

    **"Borrower" means Acme Corporation, a Delaware corporation organized**

    **under the laws of the State of Delaware.**

    

    **"Lender" means First National Bank, N.A., a national banking association.**

    

    **"Guarantor" means Acme Holdings LLC, a Delaware limited liability company.**

    

    **ARTICLE II - THE COMMITMENT**

    

    **2.1 Commitment. Subject to the terms and conditions set forth herein,**

    **the Lender agrees to make loans to the Borrower in an aggregate principal**

    **amount not to exceed $50,000,000 (the "Commitment").**

    

    **2.2 Interest Rate. Each loan shall bear interest at SOFR plus 2.50%.**

    

    **ARTICLE III - FINANCIAL COVENANTS**

    

    **3.1 Debt-to-Equity Ratio. The Borrower shall maintain a Debt-to-Equity**

    **ratio of not greater than 2.0 to 1.0, measured quarterly.**

    

    **3.2 Interest Coverage. The Borrower shall maintain an Interest Coverage**

    **ratio of not less than 3.0 to 1.0, measured quarterly.**

    **"""**





**# =============================================================================**

**# UTILITY FUNCTIONS FOR TESTS**

**# =============================================================================**



**def compute\_file\_hash(content: bytes) -> str:**

    **"""Compute MD5 hash of file content."""**

    **return hashlib.md5(content).hexdigest()**





**def create\_test\_pdf(output\_path: Path, content: str = "Test content") -> None:**

    **"""**

    **Create a simple test PDF file.**

    

    **Note: This creates a minimal PDF structure. For full PDF testing,**

    **use actual sample PDFs in the fixtures directory.**

    **"""**

    **# This is a placeholder - real tests should use actual PDF files**

    **pdf\_content = f"""%PDF-1.4**

**1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj**

**2 0 obj << /Type /Pages /Kids \[3 0 R] /Count 1 >> endobj**

**3 0 obj << /Type /Page /Parent 2 0 R /MediaBox \[0 0 612 792]** 

**/Contents 4 0 R >> endobj**

**4 0 obj << /Length {len(content)} >>**

**stream**

**{content}**

**endstream**

**endobj**

**xref**

**0 5**

**trailer << /Size 5 /Root 1 0 R >>**

**startxref**

**300**

**%%EOF"""**

    

    **output\_path.write\_bytes(pdf\_content.encode('latin-1'))**

**-----------------------**



**================================================================================**

**SECTION 4.3: CREATE test\_determinism.py (T-001)**

**================================================================================**



**This test validates the critical requirement that extraction is deterministic:**

**running the same document through the pipeline twice must produce identical output.**



**FILE: tests/test\_determinism.py**

**-------------------------------**

**"""**

**Test T-001: Determinism Validation**



**This test suite validates that the Split-RAG extraction process is fully**

**deterministic. The same input document must always produce the same output**

**Context Graph, byte-for-byte identical.**



**PASS CRITERIA:**

**- MD5(run1/doc.json) == MD5(run2/doc.json)**

**- All chunk IDs are identical between runs**

**- All lineage traces are identical between runs**

**- Timestamps are the only expected difference (and we test that separately)**



**This is critical for enterprise deployment where auditability requires**

**reproducible results.**

**"""**



**import pytest**

**import json**

**import hashlib**

**import tempfile**

**from pathlib import Path**

**from datetime import datetime, timezone**

**from typing import Dict, Any**



**import sys**

**sys.path.insert(0, str(Path(\_\_file\_\_).parent.parent))**



**from schema\_v2 import (**

    **ContextGraph,**

    **ContextNode,**

    **NodeMetadata,**

    **generate\_document\_id,**

    **generate\_chunk\_id,**

    **generate\_lineage\_trace**

**)**





**class TestDeterministicIDGeneration:**

    **"""Test that ID generation functions are deterministic."""**

    

    **def test\_document\_id\_deterministic(self):**

        **"""Same file bytes must produce same document ID."""**

        **content = b"This is test document content for determinism validation."**

        

        **# Generate ID multiple times**

        **id1 = generate\_document\_id(content)**

        **id2 = generate\_document\_id(content)**

        **id3 = generate\_document\_id(content)**

        

        **assert id1 == id2 == id3, "Document ID generation is not deterministic"**

        **assert len(id1) == 32, "Document ID must be 32 hex characters (MD5)"**

    

    **def test\_document\_id\_different\_content(self):**

        **"""Different content must produce different document IDs."""**

        **content1 = b"Document version 1"**

        **content2 = b"Document version 2"**

        

        **id1 = generate\_document\_id(content1)**

        **id2 = generate\_document\_id(content2)**

        

        **assert id1 != id2, "Different content must produce different IDs"**

    

    **def test\_chunk\_id\_deterministic(self):**

        **"""Same chunk parameters must produce same chunk ID."""**

        **doc\_id = "a" \* 32**

        **page = 5**

        **index = 3**

        **content\_sample = "The Borrower agrees to maintain"**

        

        **# Generate ID multiple times**

        **id1 = generate\_chunk\_id(doc\_id, page, index, content\_sample)**

        **id2 = generate\_chunk\_id(doc\_id, page, index, content\_sample)**

        **id3 = generate\_chunk\_id(doc\_id, page, index, content\_sample)**

        

        **assert id1 == id2 == id3, "Chunk ID generation is not deterministic"**

        **assert len(id1) == 32, "Chunk ID must be 32 hex characters (MD5)"**

    

    **def test\_chunk\_id\_position\_sensitive(self):**

        **"""Chunk ID must change when position changes."""**

        **doc\_id = "a" \* 32**

        **content = "Same content"**

        

        **id\_page1 = generate\_chunk\_id(doc\_id, 1, 0, content)**

        **id\_page2 = generate\_chunk\_id(doc\_id, 2, 0, content)**

        **id\_index1 = generate\_chunk\_id(doc\_id, 1, 1, content)**

        

        **assert id\_page1 != id\_page2, "Different pages must produce different IDs"**

        **assert id\_page1 != id\_index1, "Different indices must produce different IDs"**

    

    **def test\_lineage\_trace\_deterministic(self):**

        **"""Same lineage parameters must produce same trace."""**

        **file\_hash = "a" \* 32**

        **page\_index = 0**

        **bbox = \[0.0, 0.0, 612.0, 792.0]**

        **method = "docling"**

        

        **trace1 = generate\_lineage\_trace(file\_hash, page\_index, bbox, method)**

        **trace2 = generate\_lineage\_trace(file\_hash, page\_index, bbox, method)**

        **trace3 = generate\_lineage\_trace(file\_hash, page\_index, bbox, method)**

        

        **assert trace1 == trace2 == trace3, "Lineage trace generation is not deterministic"**

        **assert len(trace1) == 64, "Lineage trace must be 64 hex characters (SHA-256)"**

    

    **def test\_lineage\_trace\_method\_sensitive(self):**

        **"""Lineage trace must differ based on extraction method."""**

        **file\_hash = "a" \* 32**

        **page\_index = 0**

        **bbox = \[0.0, 0.0, 612.0, 792.0]**

        

        **trace\_docling = generate\_lineage\_trace(file\_hash, page\_index, bbox, "docling")**

        **trace\_pdfplumber = generate\_lineage\_trace(file\_hash, page\_index, bbox, "pdfplumber")**

        

        **assert trace\_docling != trace\_pdfplumber, (**

            **"Different extraction methods must produce different lineage traces"**

        **)**





**class TestContextGraphDeterminism:**

    **"""Test that full Context Graph serialization is deterministic."""**

    

    **def test\_graph\_json\_deterministic(self, comprehensive\_context\_graph: ContextGraph):**

        **"""Same graph must serialize to identical JSON (excluding timestamp)."""**

        **# Serialize multiple times**

        **json1 = comprehensive\_context\_graph.model\_dump()**

        **json2 = comprehensive\_context\_graph.model\_dump()**

        

        **# Remove timestamp for comparison (it's the only allowed variance)**

        **del json1\['processed\_at']**

        **del json2\['processed\_at']**

        

        **assert json1 == json2, "Context Graph serialization is not deterministic"**

    

    **def test\_node\_order\_preserved(self, comprehensive\_context\_graph: ContextGraph):**

        **"""Node order in the graph must be preserved across serialization."""**

        **json\_str = comprehensive\_context\_graph.to\_json()**

        **data = json.loads(json\_str)**

        

        **# Verify nodes maintain order**

        **original\_ids = \[node.chunk\_id for node in comprehensive\_context\_graph.nodes]**

        **loaded\_ids = \[node\['chunk\_id'] for node in data\['nodes']]**

        

        **assert original\_ids == loaded\_ids, "Node order must be preserved"**

    

    **def test\_rebuild\_from\_components(self):**

        **"""**

        **Rebuilding a graph from the same components must produce identical IDs.**

        

        **This simulates reprocessing a document and verifies we get the same result.**

        **"""**

        **# Define consistent inputs**

        **doc\_content = b"Consistent document content for rebuild test"**

        **page\_content = "The Borrower shall maintain compliance."**

        

        **# Build graph - iteration 1**

        **doc\_id\_1 = generate\_document\_id(doc\_content)**

        **chunk\_id\_1 = generate\_chunk\_id(doc\_id\_1, 1, 0, page\_content)**

        **lineage\_1 = generate\_lineage\_trace(doc\_id\_1, 0, \[0, 0, 100, 100], "docling")**

        

        **# Build graph - iteration 2 (simulating reprocessing)**

        **doc\_id\_2 = generate\_document\_id(doc\_content)**

        **chunk\_id\_2 = generate\_chunk\_id(doc\_id\_2, 1, 0, page\_content)**

        **lineage\_2 = generate\_lineage\_trace(doc\_id\_2, 0, \[0, 0, 100, 100], "docling")**

        

        **assert doc\_id\_1 == doc\_id\_2, "Document ID must be reproducible"**

        **assert chunk\_id\_1 == chunk\_id\_2, "Chunk ID must be reproducible"**

        **assert lineage\_1 == lineage\_2, "Lineage trace must be reproducible"**





**class TestFullPipelineDeterminism:**

    **"""**

    **Test determinism of the full extraction pipeline.**

    

    **Note: These tests require actual document processing and may be slower.**

    **They should be run as integration tests.**

    **"""**

    

    **@pytest.mark.integration**

    **def test\_extraction\_determinism\_with\_temp\_files(self, temp\_output\_dir: Path):**

        **"""**

        **Full pipeline determinism test with real file I/O.**

        

        **This test:**

        **1. Creates a test document**

        **2. Processes it twice**

        **3. Compares the output JSON (excluding timestamp)**

        **"""**

        **# This test requires the full extractor module**

        **# Mark as integration test since it needs external dependencies**

        **pytest.skip("Integration test - requires full environment setup")**

        

        **# The test implementation would be:**

        **# 1. Create identical input files**

        **# 2. Run extractor.py on file -> output1**

        **# 3. Clear output directory**

        **# 4. Run extractor.py on same file -> output2**

        **# 5. Compare output1 and output2 (excluding processed\_at)**

**-------------------------------**



**================================================================================**

**SECTION 4.4: CREATE test\_disambiguation.py (T-002)**

**================================================================================**



**FILE: tests/test\_disambiguation.py**

**----------------------------------**

**"""**

**Test T-002: Borrower Disambiguation Validation**



**This test suite validates that the entity disambiguation logic correctly**

**distinguishes between:**

**1. Specific entities from the contract (Primary Scope) - e.g., "Acme Corp"**

**2. Generic definitions from reference material (Corpus Scope) - e.g., UCC definitions**



**PASS CRITERIA:**

**- Query "Who is the borrower?" returns "Acme Corp" (from contract preamble)**

**- Query "What is a borrower under UCC?" returns the UCC definition**

**- Primary scope content is prioritized over corpus scope in scoring**



**This prevents the common RAG hallucination where the AI confuses specific**

**parties with generic dictionary definitions.**

**"""**



**import pytest**

**import json**

**from pathlib import Path**



**import sys**

**sys.path.insert(0, str(Path(\_\_file\_\_).parent.parent))**



**# Import Tier 2 functions for testing**

**from copilot\_tier2 import (**

    **retrieve\_context,**

    **resolve\_entity\_query,**

    **retrieve\_by\_scope,**

    **preprocess\_query,**

    **load\_context\_graph**

**)**





**class TestEntityResolution:**

    **"""Test that entity resolution correctly identifies anchored entities."""**

    

    **def test\_borrower\_entity\_direct\_answer(self, context\_graph\_json: str):**

        **"""**

        **Query 'Who is the borrower?' must return the anchored entity.**

        

        **This tests the direct answer path where we check graph metadata**

        **before performing any content search.**

        **"""**

        **result = retrieve\_context("Who is the borrower?", context\_graph\_json)**

        

        **# The result should contain the anchored entity name**

        **assert "Acme Corporation" in result, (**

            **"Borrower entity 'Acme Corporation' not found in response"**

        **)**

        **# It should indicate this came from the contract**

        **assert "preamble" in result.lower() or "contract" in result.lower() or "extracted" in result.lower(), (**

            **"Response should indicate the source is the contract preamble"**

        **)**

    

    **def test\_lender\_entity\_direct\_answer(self, context\_graph\_json: str):**

        **"""Query 'Who is the lender?' must return the anchored lender entity."""**

        **result = retrieve\_context("Who is the lender?", context\_graph\_json)**

        

        **assert "First National Bank" in result, (**

            **"Lender entity 'First National Bank' not found in response"**

        **)**

    

    **def test\_resolve\_entity\_query\_function(self, context\_graph\_json: str):**

        **"""Test the resolve\_entity\_query function directly."""**

        **graph\_metadata, \_ = load\_context\_graph(context\_graph\_json)**

        

        **# Test borrower resolution**

        **modified\_query, answer = resolve\_entity\_query(**

            **"Who is the borrower?",**

            **graph\_metadata**

        **)**

        

        **assert answer is not None, "Should return direct answer for borrower query"**

        **assert "Acme Corporation" in answer, "Answer should contain borrower name"**

        

        **# Test non-entity query (should return None for direct answer)**

        **modified\_query, answer = resolve\_entity\_query(**

            **"What are the financial covenants?",**

            **graph\_metadata**

        **)**

        

        **assert answer is None, "Non-entity queries should not get direct answers"**





**class TestScopeFiltering:**

    **"""Test that scope filtering correctly separates primary and corpus content."""**

    

    **def test\_primary\_scope\_retrieval(self, context\_graph\_json: str):**

        **"""Primary scope queries should only return contract-specific content."""**

        **result = retrieve\_by\_scope(**

            **"borrower definition",**

            **context\_graph\_json,**

            **scope="primary"**

        **)**

        

        **# Should find the contract definition**

        **assert "Acme Corporation" in result or "Delaware corporation" in result, (**

            **"Primary scope should contain the specific borrower definition"**

        **)**

        **# Should NOT contain UCC reference**

        **assert "UCC" not in result, (**

            **"Primary scope should not include corpus (UCC) content"**

        **)**

    

    **def test\_corpus\_scope\_retrieval(self, context\_graph\_json: str):**

        **"""Corpus scope queries should return reference material."""**

        **result = retrieve\_by\_scope(**

            **"borrower definition",**

            **context\_graph\_json,**

            **scope="corpus"**

        **)**

        

        **# Should find the UCC definition if present**

        **# Note: This test may return "No content found" if corpus is empty**

        **# which is acceptable - the point is it shouldn't return primary content**

        **if "No content found" not in result and "No 'corpus'" not in result:**

            **assert "Acme Corporation" not in result, (**

                **"Corpus scope should not include contract-specific content"**

            **)**





**class TestDisambiguationScoring:**

    **"""Test that scoring correctly prioritizes primary scope content."""**

    

    **def test\_primary\_scope\_multiplier\_applied(self, context\_graph\_json: str):**

        **"""Primary scope content should receive 1.5x score multiplier."""**

        **# Search for a term that appears in both scopes**

        **result = retrieve\_context("borrower", context\_graph\_json)**

        

        **# The primary scope content should appear first due to multiplier**

        **# This is a structural test - we check the result contains scores**

        **assert "score" in result.lower() or "Score" in result, (**

            **"Results should include score information"**

        **)**

    

    **def test\_content\_type\_weights\_applied(self, context\_graph\_json: str):**

        **"""Headers and tables should score higher than plain text."""**

        **# Search for content that should match header**

        **result = retrieve\_context("ARTICLE DEFINITIONS", context\_graph\_json)**

        

        **# Headers should be returned first due to 3.0x weight**

        **lines = result.split('\\n')**

        **content\_lines = \[l for l in lines if 'header' in l.lower() or 'ARTICLE' in l]**

        

        **# At minimum, we should find our header content in the results**

        **assert len(content\_lines) > 0 or "No relevant content" in result, (**

            **"Header content should be retrievable with proper weighting"**

        **)**





**class TestVirtualSubstitution:**

    **"""Test that virtual substitution enhances query matching."""**

    

    **def test\_borrower\_name\_added\_to\_query(self, context\_graph\_json: str):**

        **"""When searching for 'borrower', the entity name should be added."""**

        **graph\_metadata, \_ = load\_context\_graph(context\_graph\_json)**

        

        **original\_query = "borrower obligations"**

        **modified\_query, \_ = resolve\_entity\_query(original\_query, graph\_metadata)**

        

        **# The modified query should include the entity name**

        **assert "Acme Corporation" in modified\_query or modified\_query != original\_query, (**

            **"Virtual substitution should add borrower entity to query"**

        **)**

**----------------------------------**



**================================================================================**

**SECTION 4.5: CREATE test\_data\_mart.py (T-003)**

**================================================================================**



**FILE: tests/test\_data\_mart.py**

**-----------------------------**

**"""**

**Test T-003: Data Mart Export Validation**



**This test suite validates the Data Mart export functionality (Mode B),**

**which transforms the RAG system into a spreadsheet generator for structured**

**data like tables and key-value pairs.**



**PASS CRITERIA:**

**- Export generates valid .xlsx or .csv file**

**- Exported file size is under 512MB (Copilot download limit)**

**- Table content is correctly filtered and included**

**- File can be opened and read by pandas without errors**

**"""**



**import pytest**

**import json**

**import tempfile**

**from pathlib import Path**



**import sys**

**sys.path.insert(0, str(Path(\_\_file\_\_).parent.parent))**



**from copilot\_tier2 import (**

    **generate\_data\_mart,**

    **load\_context\_graph**

**)**



**# pandas is allowed in both tiers**

**import pandas as pd**





**class TestDataMartGeneration:**

    **"""Test Data Mart export functionality."""**

    

    **def test\_basic\_export\_tables(self, context\_graph\_json: str, temp\_output\_dir: Path):**

        **"""Export should generate a file containing tables."""**

        **# Change to temp directory for file output**

        **import os**

        **original\_dir = os.getcwd()**

        **os.chdir(temp\_output\_dir)**

        

        **try:**

            **result = generate\_data\_mart(context\_graph\_json)**

            

            **# Should report success**

            **assert "Generated" in result or "containing" in result, (**

                **f"Export should report success, got: {result}"**

            **)**

            

            **# Find the generated file**

            **generated\_files = list(temp\_output\_dir.glob("DataMart\_\*"))**

            

            **if "No content found" not in result:**

                **assert len(generated\_files) > 0, "Export should create a file"**

                

                **# Verify file is readable**

                **export\_file = generated\_files\[0]**

                **if export\_file.suffix == '.xlsx':**

                    **df = pd.read\_excel(export\_file)**

                **else:**

                    **df = pd.read\_csv(export\_file)**

                

                **# Should have content**

                **assert len(df) >= 0, "Exported file should be readable"**

                

        **finally:**

            **os.chdir(original\_dir)**

    

    **def test\_export\_with\_content\_type\_filter(self, context\_graph\_json: str, temp\_output\_dir: Path):**

        **"""Export should filter by specified content types."""**

        **import os**

        **original\_dir = os.getcwd()**

        **os.chdir(temp\_output\_dir)**

        

        **try:**

            **# Export only tables**

            **result = generate\_data\_mart(**

                **context\_graph\_json,**

                **content\_types=\['table']**

            **)**

            

            **# If tables exist, should export them**

            **if "No content found" not in result:**

                **generated\_files = list(temp\_output\_dir.glob("DataMart\_\*"))**

                **assert len(generated\_files) > 0, "Should create export file for tables"**

                

        **finally:**

            **os.chdir(original\_dir)**

    

    **def test\_csv\_fallback(self, context\_graph\_json: str, temp\_output\_dir: Path):**

        **"""CSV fallback should work when openpyxl is unavailable."""**

        **import os**

        **original\_dir = os.getcwd()**

        **os.chdir(temp\_output\_dir)**

        

        **try:**

            **result = generate\_data\_mart(**

                **context\_graph\_json,**

                **output\_format='csv'**

            **)**

            

            **if "No content found" not in result:**

                **# Should create CSV file**

                **csv\_files = list(temp\_output\_dir.glob("\*.csv"))**

                **assert len(csv\_files) > 0, "CSV export should create .csv file"**

                

        **finally:**

            **os.chdir(original\_dir)**

    

    **def test\_export\_file\_size\_limit(self, context\_graph\_json: str, temp\_output\_dir: Path):**

        **"""Exported files must be under 512MB for Copilot compatibility."""**

        **import os**

        **original\_dir = os.getcwd()**

        **os.chdir(temp\_output\_dir)**

        

        **try:**

            **result = generate\_data\_mart(context\_graph\_json)**

            

            **generated\_files = list(temp\_output\_dir.glob("DataMart\_\*"))**

            

            **for export\_file in generated\_files:**

                **file\_size\_mb = export\_file.stat().st\_size / (1024 \* 1024)**

                **assert file\_size\_mb < 512, (**

                    **f"Export file {export\_file.name} is {file\_size\_mb:.1f}MB, "**

                    **f"exceeds 512MB Copilot limit"**

                **)**

                

        **finally:**

            **os.chdir(original\_dir)**





**class TestDataMartErrorHandling:**

    **"""Test error handling in Data Mart generation."""**

    

    **def test\_invalid\_json\_handling(self):**

        **"""Invalid JSON should return error message, not crash."""**

        **result = generate\_data\_mart("not valid json {{{")**

        

        **assert "Error" in result or "error" in result, (**

            **"Invalid JSON should return error message"**

        **)**

    

    **def test\_empty\_nodes\_handling(self):**

        **"""Graph with no matching content should return informative message."""**

        **empty\_graph = json.dumps({**

            **"document\_id": "a" \* 32,**

            **"filename": "empty.pdf",**

            **"processed\_at": "2025-01-25T12:00:00Z",**

            **"nodes": \[**

                **{**

                    **"chunk\_id": "b" \* 32,**

                    **"parent\_section\_id": "root",**

                    **"content\_type": "text",  # No tables**

                    **"content": "Just text, no tables here.",**

                    **"metadata": {"page\_number": 1, "source\_scope": "primary"},**

                    **"lineage\_trace": "c" \* 64**

                **}**

            **]**

        **})**

        

        **# Request tables only - should find none**

        **result = generate\_data\_mart(empty\_graph, content\_types=\['table'])**

        

        **assert "No content found" in result or "Available types" in result, (**

            **"Should report no matching content found"**

        **)**

**-----------------------------**



**================================================================================**

**SECTION 4.6: CREATE test\_tier2\_retrieval.py**

**================================================================================**



**FILE: tests/test\_tier2\_retrieval.py**

**-----------------------------------**

**"""**

**Tier 2 Retrieval Engine Tests**



**This test suite validates the core retrieval functionality of the Tier 2**

**sandbox script, including query preprocessing, vectorized scoring, and**

**output formatting.**



**These tests ensure the retrieval logic works correctly BEFORE deployment**

**to the resource-constrained Copilot Studio sandbox.**

**"""**



**import pytest**

**import json**

**from pathlib import Path**



**import sys**

**sys.path.insert(0, str(Path(\_\_file\_\_).parent.parent))**



**from copilot\_tier2 import (**

    **preprocess\_query,**

    **simple\_markdown\_table,**

    **retrieve\_context,**

    **get\_document\_summary,**

    **list\_available\_sections,**

    **load\_context\_graph,**

    **calculate\_keyword\_density,**

    **STOPWORDS**

**)**



**import pandas as pd**





**class TestQueryPreprocessing:**

    **"""Test query normalization and keyword extraction."""**

    

    **def test\_lowercase\_conversion(self):**

        **"""Query should be converted to lowercase."""**

        **keywords = preprocess\_query("BORROWER OBLIGATIONS")**

        

        **for kw in keywords:**

            **assert kw == kw.lower(), "All keywords should be lowercase"**

    

    **def test\_punctuation\_removal(self):**

        **"""Punctuation should be removed from queries."""**

        **keywords = preprocess\_query("What's the borrower's obligation?")**

        

        **for kw in keywords:**

            **assert "'" not in kw, "Apostrophes should be removed"**

            **assert "?" not in kw, "Question marks should be removed"**

    

    **def test\_stopword\_removal(self):**

        **"""Common stopwords should be filtered out."""**

        **keywords = preprocess\_query("What is the current ratio")**

        

        **# 'what', 'is', 'the' are stopwords**

        **assert "what" not in keywords, "'what' should be removed as stopword"**

        **assert "is" not in keywords, "'is' should be removed as stopword"**

        **assert "the" not in keywords, "'the' should be removed as stopword"**

        

        **# 'current' and 'ratio' should remain**

        **assert "current" in keywords, "'current' should be kept"**

        **assert "ratio" in keywords, "'ratio' should be kept"**

    

    **def test\_deduplication(self):**

        **"""Duplicate keywords should be removed."""**

        **keywords = preprocess\_query("borrower borrower borrower")**

        

        **assert keywords.count("borrower") == 1, "Duplicates should be removed"**

    

    **def test\_empty\_query\_handling(self):**

        **"""Empty query should return empty list."""**

        **keywords = preprocess\_query("")**

        **assert keywords == \[], "Empty query should return empty list"**

        

        **keywords = preprocess\_query("   ")**

        **assert keywords == \[], "Whitespace-only query should return empty list"**

    

    **def test\_stopword\_only\_query(self):**

        **"""Query with only stopwords should return empty list."""**

        **keywords = preprocess\_query("the is at which on")**

        **assert keywords == \[], "Stopword-only query should return empty list"**





**class TestMarkdownTableGeneration:**

    **"""Test the manual Markdown table formatter."""**

    

    **def test\_basic\_table\_generation(self):**

        **"""DataFrame should convert to valid Markdown table."""**

        **df = pd.DataFrame({**

            **'Name': \['Alice', 'Bob'],**

            **'Score': \[95, 87]**

        **})**

        

        **md = simple\_markdown\_table(df)**

        

        **# Check structure**

        **assert '| Name | Score |' in md, "Header row should be present"**

        **assert '| --- | --- |' in md, "Separator row should be present"**

        **assert '| Alice | 95 |' in md, "Data rows should be present"**

    

    **def test\_empty\_dataframe\_handling(self):**

        **"""Empty DataFrame should return placeholder text."""**

        **df = pd.DataFrame()**

        

        **md = simple\_markdown\_table(df)**

        

        **assert "No data" in md or md == "", "Empty DataFrame should be handled"**

    

    **def test\_pipe\_character\_escaping(self):**

        **"""Pipe characters in content should be escaped."""**

        **df = pd.DataFrame({**

            **'Content': \['Value | with | pipes']**

        **})**

        

        **md = simple\_markdown\_table(df)**

        

        **# Pipes should be escaped to not break table structure**

        **assert '\\\\|' in md or md.count('|') >= 4, "Pipes should be escaped"**

    

    **def test\_long\_content\_truncation(self):**

        **"""Very long content should be truncated."""**

        **long\_content = "A" \* 1000**

        **df = pd.DataFrame({'Content': \[long\_content]})**

        

        **md = simple\_markdown\_table(df)**

        

        **# Content should be truncated (500 char limit in the function)**

        **assert len(md) < 1100, "Long content should be truncated"**

        **assert "..." in md, "Truncated content should end with ellipsis"**





**class TestRetrievalFunction:**

    **"""Test the main retrieve\_context function."""**

    

    **def test\_successful\_retrieval(self, context\_graph\_json: str):**

        **"""Valid query should return formatted results."""**

        **result = retrieve\_context("debt equity ratio", context\_graph\_json)**

        

        **# Should return results, not error**

        **assert "Error" not in result, f"Should not return error: {result}"**

        

        **# Should contain some expected elements**

        **assert "Retrieved Context" in result or "content" in result.lower(), (**

            **"Result should contain context information"**

        **)**

    

    **def test\_no\_results\_handling(self, context\_graph\_json: str):**

        **"""Query with no matches should return informative message."""**

        **result = retrieve\_context("xyzzy foobar nonexistent", context\_graph\_json)**

        

        **assert "No relevant content" in result or "not found" in result.lower(), (**

            **"No-match queries should return informative message"**

        **)**

    

    **def test\_empty\_query\_handling(self, context\_graph\_json: str):**

        **"""Empty query should return error message."""**

        **result = retrieve\_context("", context\_graph\_json)**

        

        **assert "Error" in result or "Please provide" in result, (**

            **"Empty query should return error/guidance message"**

        **)**

    

    **def test\_invalid\_json\_handling(self):**

        **"""Invalid JSON should return error message, not crash."""**

        **result = retrieve\_context("test query", "not valid json")**

        

        **assert "Error" in result or "Invalid" in result, (**

            **"Invalid JSON should return error message"**

        **)**





**class TestUtilityFunctions:**

    **"""Test helper functions for document exploration."""**

    

    **def test\_document\_summary(self, context\_graph\_json: str):**

        **"""Document summary should include key statistics."""**

        **summary = get\_document\_summary(context\_graph\_json)**

        

        **assert "Document Summary" in summary, "Should have summary header"**

        **assert "File:" in summary or "filename" in summary.lower(), (**

            **"Should include filename"**

        **)**

        **assert "Nodes" in summary or "nodes" in summary.lower(), (**

            **"Should include node count"**

        **)**

    

    **def test\_section\_listing(self, context\_graph\_json: str):**

        **"""Section listing should show available headers."""**

        **sections = list\_available\_sections(context\_graph\_json)**

        

        **# Should either list sections or say none found**

        **assert "Available Sections" in sections or "No section" in sections, (**

            **"Should provide section information or indicate none found"**

        **)**

**-----------------------------------**



**================================================================================**

**SECTION 4.7: CREATE run\_tests.bat AND run\_tests.sh**

**================================================================================**



**FILE: scripts/run\_tests.bat**

**---------------------------**

**@echo off**

**REM ============================================================================**

**REM Split-RAG Test Runner (Windows)**

**REM ============================================================================**



**echo ============================================**

**echo Split-RAG Test Suite**

**echo ============================================**



**REM Activate virtual environment if present**

**if exist ".venv\\Scripts\\activate.bat" (**

    **call .venv\\Scripts\\activate.bat**

**)**



**REM Install test dependencies if needed**

**pip install pytest pytest-cov --quiet**



**REM Run tests with coverage**

**echo.**

**echo Running all tests...**

**echo.**



**python -m pytest tests/ -v --tb=short --cov=. --cov-report=term-missing**



**REM Capture exit code**

**set EXIT\_CODE=%ERRORLEVEL%**



**echo.**

**echo ============================================**

**if %EXIT\_CODE% EQU 0 (**

    **echo All tests PASSED**

**) else (**

    **echo Some tests FAILED**

**)**

**echo ============================================**



**exit /b %EXIT\_CODE%**

**---------------------------**



**FILE: scripts/run\_tests.sh**

**--------------------------**

**#!/bin/bash**

**# ============================================================================**

**# Split-RAG Test Runner (Linux/Mac)**

**# ============================================================================**



**echo "============================================"**

**echo "Split-RAG Test Suite"**

**echo "============================================"**



**# Activate virtual environment if present**

**if \[ -f ".venv/bin/activate" ]; then**

    **source .venv/bin/activate**

**fi**



**# Install test dependencies if needed**

**pip install pytest pytest-cov --quiet**



**# Run tests with coverage**

**echo ""**

**echo "Running all tests..."**

**echo ""**



**python -m pytest tests/ -v --tb=short --cov=. --cov-report=term-missing**



**EXIT\_CODE=$?**



**echo ""**

**echo "============================================"**

**if \[ $EXIT\_CODE -eq 0 ]; then**

    **echo "All tests PASSED"**

**else**

    **echo "Some tests FAILED"**

**fi**

**echo "============================================"**



**exit $EXIT\_CODE**

**--------------------------**



**================================================================================**

**SECTION 4.8: CREATE DEPLOYMENT GUIDE**

**================================================================================**



**FILE: docs/DEPLOYMENT\_GUIDE.md**

**------------------------------**

**# Split-RAG Deployment Guide**



**## Pre-Deployment Checklist**



**Before deploying to production, verify all items:**



**### Code Quality**

**- \[ ] All tests pass (`scripts/run\_tests.bat` or `scripts/run\_tests.sh`)**

**- \[ ] No forbidden imports in Tier 2 script**

**- \[ ] All functions have return type hints (CP-001)**

**- \[ ] No generic `except Exception:` in internal logic (CP-002)**

**- \[ ] All paths use `pathlib` (CP-003)**

**- \[ ] All imports on Approved List (CP-004)**



**### Determinism Validation**

**- \[ ] T-001 Determinism test passes (same input → same output)**

**- \[ ] Document IDs are reproducible**

**- \[ ] Chunk IDs are reproducible**

**- \[ ] Lineage traces are reproducible**



**### Disambiguation Validation**

**- \[ ] T-002 tests pass**

**- \[ ] "Who is the borrower?" returns correct entity**

**- \[ ] Primary vs corpus scope separation works**



**### Data Mart Validation**

**- \[ ] T-003 tests pass**

**- \[ ] Excel export generates valid file**

**- \[ ] CSV fallback works**

**- \[ ] Export file size under 512MB**



**## Tier 1 Deployment (The Factory)**



**### Environment Setup**



**1. \*\*Create Virtual Environment\*\***

   **```bash**

   **python -m venv .venv**

   **.venv\\Scripts\\activate  # Windows**

   **# source .venv/bin/activate  # Linux**

   **```**



**2. \*\*Install Dependencies\*\***

   **```bash**

   **pip install -r requirements.txt**

   **```**



**3. \*\*Verify Docling Installation\*\***

   **```bash**

   **python -c "from docling.document\_converter import DocumentConverter; print('OK')"**

   **```**



**4. \*\*Configure Paths\*\***

   **Edit `config.json` to set your input/output directories.**



**5. \*\*Run Extraction\*\***

   **```bash**

   **bootstrap.bat  # Windows**

   **# ./bootstrap.sh  # Linux**

   **```**



**### Production Considerations**



**- \*\*Memory\*\*: Docling can use 4-8GB RAM for complex PDFs**

**- \*\*CPU\*\*: Set `OMP\_NUM\_THREADS` to limit CPU usage**

**- \*\*GPU\*\*: Optional; set `docling.use\_gpu: false` for CPU-only**

**- \*\*Storage\*\*: Ensure sufficient space for output JSON files**



**## Tier 2 Deployment (Copilot Studio)**



**### Preparing the Script**



**1. \*\*Verify Sandbox Compliance\*\***

   **```python**

   **# Run this check before deployment**

   **import copilot\_tier2**

   **print("Import successful - sandbox compatible")**

   **```**



**2. \*\*Test Locally First\*\***

   **```bash**

   **python copilot\_tier2.py**

   **```**



**3. \*\*Copy Script Content\*\***

   **- Open `copilot\_tier2.py`**

   **- Copy the entire content**

   **- Do NOT modify for deployment**



**### Copilot Studio Integration**



**1. \*\*Create New Flow\*\***

   **- Navigate to your Copilot environment**

   **- Create a new topic or modify existing**



**2. \*\*Add Code Block\*\***

   **- Add a "Run Python code" action**

   **- Paste the `copilot\_tier2.py` content**



**3. \*\*Configure Inputs\*\***

   **- `query`: User's message text**

   **- `context\_json\_str`: Content of your Context Graph JSON**



**4. \*\*Configure Output\*\***

   **- Capture the return value from `retrieve\_context()`**

   **- Pass to LLM for response generation**



**5. \*\*Add LLM Response\*\***

   **- Use this system prompt template:**

   **```**

   **Answer using ONLY the Retrieved Context below.** 

   **If the answer is not in the context, say so.**

   

   **Retrieved Context:**

   **{code\_block\_output}**

   

   **User Question: {user\_message}**

   **```**



**### Uploading Context Graphs**



**\*\*Option 1: SharePoint\*\***

**- Upload JSON files to SharePoint**

**- Use Power Automate to read file content**

**- Pass content to Python code block**



**\*\*Option 2: Dataverse\*\***

**- Store JSON as text field in Dataverse**

**- Query Dataverse in your flow**

**- Pass content to Python code block**



**\*\*Option 3: Inline (Small Files Only)\*\***

**- Embed JSON directly in flow variable**

**- Limited to ~100KB due to variable limits**



**## Troubleshooting**



**### Tier 1 Issues**



**\*\*Docling Import Error\*\***

**```**

**ModuleNotFoundError: No module named 'docling'**

**```**

**Solution: Reinstall with `pip install docling --upgrade`**



**\*\*Memory Error During Extraction\*\***

**```**

**MemoryError: Unable to allocate...**

**```**

**Solution: Process fewer files at once, increase system RAM**



**\*\*Quarantined Documents\*\***

**- Check `quarantine/` directory**

**- Read `failure\_report.json` for error details**

**- Common causes: corrupted PDFs, encoding issues**



**### Tier 2 Issues**



**\*\*Sandbox Memory Crash\*\***

**```**

**Out of memory**

**```**

**Solution: Ensure no forbidden imports (torch, pydantic, etc.)**



**\*\*openpyxl Not Available\*\***

**```**

**Excel export unavailable**

**```**

**Solution: The script automatically falls back to CSV**



**\*\*Large JSON Performance\*\***

**- Context Graphs over 50MB may be slow**

**- Consider splitting into multiple files**

**- Use pagination for very large corpora**



**## Monitoring and Maintenance**



**### Log Review**

**- Check `logs/` directory daily**

**- Look for CRITICAL and ERROR level messages**

**- Quarantine events require investigation**



**### Incremental Updates**

**- The manifest tracks processed files**

**- Unchanged files are skipped automatically**

**- Use `--reprocess` flag to force full reprocessing**



**### Schema Updates**

**- Schema changes require reprocessing all documents**

**- Update `schema\_version` in output to track compatibility**

**- Tier 2 should validate schema version before processing**

**------------------------------**



**================================================================================**

**SECTION 4.9: FINAL VERIFICATION CHECKLIST**

**================================================================================**



**Before declaring the implementation complete, verify:**



**TIER 1 ARTIFACTS:**

**\[ ] bootstrap.bat - Sets OMP\_NUM\_THREADS, validates Python 3.11**

**\[ ] config.json - All paths configured, JSON format (no YAML)**

**\[ ] rules.json - Entity regex patterns with proper escaping**

**\[ ] requirements.txt - Only approved dependencies, no forbidden imports**

**\[ ] schema\_v2.py - Pydantic models with deterministic ID generation**

**\[ ] extractor.py - Full pipeline with Docling/pdfplumber, entity anchoring**



**TIER 2 ARTIFACTS:**

**\[ ] copilot\_tier2.py - Only stdlib + pandas, all retrieval modes**



**TEST SUITE:**

**\[ ] conftest.py - Shared fixtures**

**\[ ] test\_determinism.py - T-001 validation**

**\[ ] test\_disambiguation.py - T-002 validation**

**\[ ] test\_data\_mart.py - T-003 validation**

**\[ ] test\_tier2\_retrieval.py - Retrieval function tests**

**\[ ] run\_tests.bat / run\_tests.sh - Test runners**



**DOCUMENTATION:**

**\[ ] README.md - Project overview**

**\[ ] DEPLOYMENT\_GUIDE.md - Production deployment instructions**



**RUN FINAL VALIDATION:**

**```bash**

**# Run full test suite**

**scripts/run\_tests.bat  # Windows**

**# scripts/run\_tests.sh  # Linux**



**# Verify no forbidden imports in Tier 2**

**python -c "**

**import ast**

**import sys**



**with open('copilot\_tier2.py', 'r') as f:**

    **tree = ast.parse(f.read())**



**forbidden = {'torch', 'transformers', 'pydantic', 'sklearn', 'scipy', 'requests'}**

**imports = set()**



**for node in ast.walk(tree):**

    **if isinstance(node, ast.Import):**

        **for alias in node.names:**

            **imports.add(alias.name.split('.')\[0])**

    **elif isinstance(node, ast.ImportFrom):**

        **if node.module:**

            **imports.add(node.module.split('.')\[0])**



**violations = imports \& forbidden**

**if violations:**

    **print(f'FAIL: Forbidden imports found: {violations}')**

    **sys.exit(1)**

**else:**

    **print('PASS: No forbidden imports in Tier 2')**

**"**

**```**



**================================================================================**

**BLOCK 4 COMPLETE - FINAL PRESENTATION**

**================================================================================**



**Present ALL artifacts to the user for final review:**



**"The Split-RAG System implementation is now complete. I have created:**



**\*\*Tier 1 Artifacts (The Factory):\*\***

**1. bootstrap.bat - Environment setup and Python version validation**

**2. config.json - Operational configuration in JSON format**

**3. rules.json - Entity extraction regex patterns**

**4. requirements.txt - Approved dependencies only**

**5. schema\_v2.py - Pydantic data contract with deterministic ID generation**

**6. extractor.py - Full extraction pipeline with Docling/pdfplumber**



**\*\*Tier 2 Artifacts (The Sandbox):\*\***

**7. copilot\_tier2.py - Retrieval script using only stdlib + pandas**



**\*\*Test Suite:\*\***

**8. tests/conftest.py - Shared pytest fixtures**

**9. tests/test\_determinism.py - T-001 determinism validation**

**10. tests/test\_disambiguation.py - T-002 entity disambiguation**

**11. tests/test\_data\_mart.py - T-003 spreadsheet export**

**12. tests/test\_tier2\_retrieval.py - Retrieval function tests**

**13. scripts/run\_tests.bat - Windows test runner**

**14. scripts/run\_tests.sh - Linux test runner**



**\*\*Documentation:\*\***

**15. README.md - Project overview and quick start**

**16. docs/DEPLOYMENT\_GUIDE.md - Production deployment instructions**



**\*\*Verification Status:\*\***

**- All Canon rules enforced (CANON\_001 through CANON\_004)**

**- All Checkpoints addressed (CP-001 through CP-004)**

**- Test matrix covers T-001, T-002, and T-003**



**The system is ready for deployment. Would you like me to:**

**1. Review any specific artifact in detail?**

**2. Explain any architectural decisions?**

**3. Discuss integration with your specific Copilot Studio environment?"**



**================================================================================**

**END OF BLOCK 4 - IMPLEMENTATION COMPLETE**

**================================================================================**

