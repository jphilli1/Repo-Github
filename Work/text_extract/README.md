AI-Native Split-RAG System (v2.0)
Executive Summary
The AI-Native Split-RAG System is a production-grade Retrieval-Augmented Generation architecture designed for enterprise environments in highly regulated sectors such as financial services, legal audit, and defense. Unlike monolithic RAG implementations, this system enforces strict decoupling between heavy-duty document processing (Tier 1) and lightweight, secure retrieval (Tier 2).
The "Split" Philosophy
Traditional RAG systems suffer from "Semantic Drift" where the same document yields different answers depending on embedding model variations, chunking strategies, or silent cloud provider updates. Split-RAG eliminates this drift by treating extracted knowledge as a static, versioned artifact called the Context Graph.
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPLIT-RAG ARCHITECTURE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   TIER 1: THE FACTORY                    TIER 2: THE CONSUMER               │
│   (Local Python Environment)             (Copilot Studio Sandbox)           │
│                                                                             │
│   ┌─────────────────────────┐           ┌─────────────────────────┐        │
│   │  • High-Fidelity OCR    │           │  • StdLib + pandas ONLY │        │
│   │  • Layout Analysis      │    JSON   │  • Keyword Density      │        │
│   │  • Semantic Chunking    │  ───────► │  • Sub-second Latency   │        │
│   │  • Entity Anchoring     │  Context  │  • 256MB Memory Limit   │        │
│   │  • Docling + pdfplumber │   Graph   │  • Zero Network Access  │        │
│   └─────────────────────────┘           └─────────────────────────┘        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
Key Features
Deterministic Processing

Immutable IDs: SHA-256/MD5-based lineage tracking ensures every chunk is cryptographically traceable to its source byte-range
Version Control: Context Graphs are static artifacts that can be versioned and audited
No Re-Hashing: Tier 2 never computes hashes, preserving CPU cycles in the sandbox

Keep-All Policy

Zero Silent Deletion: Duplicate content from different locations is preserved with linked lineage IDs
Conflict Detection: Dual extraction paths (Docling + pdfplumber) with Levenshtein distance comparison
Quarantine System: Failed documents are isolated with detailed failure reports for human review

Borrower-Specific Disambiguation

Entity Anchoring: Regex heuristics extract specific party names from contract preambles
Scope Separation: Primary (contract-specific) vs. Corpus (reference/UCC) scope tagging
Virtual Substitution: Query enhancement using anchored entity metadata

Sandbox-Safe Retrieval

Vectorized Keyword Density: Deterministic alternative to embedding similarity using pandas string operations
Content-Type Weighting: Headers (3.0x), Tables (2.5x), Primary Scope (1.5x multiplier)
Data Mart Export: Transform RAG results into downloadable Excel/CSV artifacts

System Requirements
Tier 1 (The Factory)
ComponentSpecificationOSWindows (Primary), Linux (Compatible)Python3.11.x (Mandatory)RAM8GB+ recommendedDependenciesdocling, pydantic>=2.0, pandas, pdfplumber, openpyxl
Tier 2 (The Sandbox)
ComponentSpecificationRuntimeMicrosoft Copilot Studio Code InterpreterMemory Limit256MB - 512MBFile Size Limit512MBDependenciesPython Standard Library + pandas ONLY
Directory Structure
split-rag/
├── input/                    # Raw source documents (PDF, DOCX, XLSX)
├── output/                   # Generated Context Graph JSON files
├── logs/                     # Execution logs for audit trails
├── quarantine/               # Documents failing validation
├── bootstrap.bat             # Environment entry point
├── config.json               # Operational configuration
├── rules.json                # Regex heuristics for entity extraction
├── requirements.txt          # Tier 1 dependency specification
├── schema_v2.py              # Pydantic data contract
├── extractor.py              # Tier 1 extraction engine
└── copilot_tier2.py          # Tier 2 sandbox retrieval script
Governance Protocols (Canon Rules)
RuleDescriptionCANON_001Standard Library Primacy - use stdlib unless impossibleCANON_002Tier-2 Constraint - only pandas allowed as external dependencyCANON_003JSON Standard - no YAML, all config in JSON formatCANON_004Deterministic Lineage - hash-based IDs, no re-hashing in Tier 2
Verification Checkpoints
CheckpointRequirementCP-001Explicit return type hints on all functionsCP-002No generic except Exception: blocks (except at library boundaries)CP-003All paths via pathlib or raw strings for Windows compatibilityCP-004All imports cross-referenced against Approved Import List
Quick Start

Clone Repository

bash   git clone https://github.com/your-org/split-rag.git
   cd split-rag

Setup Environment

bash   python -m venv .venv
   .venv\Scripts\activate  # Windows
   pip install -r requirements.txt

Configure

Edit config.json with your paths and settings
Customize rules.json for your document types


Run Extraction

bash   bootstrap.bat
   # Or directly: python extractor.py

Deploy Tier 2

Upload copilot_tier2.py to Copilot Studio
Load Context Graph JSON as data source



Testing Matrix
Test IDDescriptionPass CriteriaT-001Determinism ValidationMD5(run1) == MD5(run2) for same inputT-002Borrower DisambiguationCorrect entity extraction vs. corpus definitionsT-003Data Mart ExportValid .xlsx under 512MB generated
Implementation Instructions in "instructions_copilot_verison.md"
This repository includes four instruction blocks for AI-assisted implementation:

INSTRUCTIONS_BLOCK_1.txt - Environment setup, dependencies, project scaffolding
INSTRUCTIONS_BLOCK_2.txt - Core components: schema, configuration, extraction engine
INSTRUCTIONS_BLOCK_3.txt - Tier 2 retrieval logic, sandbox constraints, output modes
INSTRUCTIONS_BLOCK_4.txt - Testing, validation, deployment, and quality assurance

References

Docling Documentation
Microsoft Copilot Studio Limits
Pydantic v2 Documentation