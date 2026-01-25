# Split-RAG Document Extraction System v2.1

AI-Native document extraction and retrieval system with deterministic processing.

## Quick Start

1. Read `code_instructions.txt` first (binding specification)
2. Follow `SPLIT_RAG_CODING_PROMPT.md` for phased implementation
3. Deliver exactly 7 artifacts, one per message

## Documentation Files

| File | Purpose | Status |
|------|---------|--------|
| `code_instructions.txt` | Master specification | **BINDING** |
| `SPLIT_RAG_CODING_PROMPT.md` | Phased implementation guide | Procedural |
| `instructions_copilot_verison.md` | AI agent quick reference | Procedural |
| `Split-RAG_Architecture_Update_Specification.txt` | Design document | Authoritative |
| `ARCHITECTURE_OVERVIEW.md` | Detailed context | Non-binding |

## Key Rules

- **JSON not YAML** - All configs use JSON format
- **Ignore v2.0 docs** - Follow v2.1 specification
- **Exact filenames** - `config.json`, `rules.json` (not YAML)
- **Canonical directory** - `Work/text_extract/`

## Artifacts to Deliver

1. `bootstrap.bat` - Environment setup
2. `config.json` - Operational configuration
3. `rules.json` - Heuristic rules
4. `requirements.txt` - Dependencies
5. `schema_v1.py` - Pydantic schema
6. `extractor.py` - Extraction engine
7. `copilot_tier2.py` - Tier 2 retrieval

## Source of Truth

`code_instructions.txt` is the binding specification. All other documents provide context or procedural guidance.
