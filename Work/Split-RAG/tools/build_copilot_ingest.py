#!/usr/bin/env python3
"""
build_copilot_ingest.py — Copilot Studio ingest dataset builder.

Consumes the deterministic Split-RAG extraction engine outputs
(ContextGraph JSON) and produces:
    1. copilot_ingest.csv      (UTF-8 BOM, one row per document)
    2. copilot_ingest.txt      (pipe-delimited, same schema)
    3. copilot_ingest_manifest.json  (run metadata + warnings)

No LLM. No torch. Deterministic only.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Engine import guard — resolve the ONLY valid engine location
# ---------------------------------------------------------------------------

_THIS_DIR = Path(__file__).resolve().parent          # .../tools/
_ENGINE_DIR = _THIS_DIR.parent                        # .../Split-RAG extension/ (or Split-RAG\)

# Platform-agnostic check: the engine dir must be under Work/Split-RAG
# (handles "Split-RAG extension", "Split-RAG" on Windows, etc.)
_engine_dir_str = str(_ENGINE_DIR)
_VALID_MARKERS = ("Split-RAG",)
_BLOCKED_MARKERS = ("text_extract",)


def _validate_engine_dir(engine_dir: Path) -> None:
    """Abort if we are importing from the wrong directory tree."""
    d = str(engine_dir)
    if any(b in d for b in _BLOCKED_MARKERS):
        sys.exit(
            f"FATAL: Engine directory resolves to a blocked path: {d}\n"
            f"Only Work/Split-RAG* is permitted."
        )
    if not any(m in d for m in _VALID_MARKERS):
        sys.exit(
            f"FATAL: Engine directory {d} does not contain any of {_VALID_MARKERS}.\n"
            f"Aborting to prevent stale-code usage."
        )


_validate_engine_dir(_ENGINE_DIR)

# Add engine dir to sys.path *before* any engine imports
if str(_ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(_ENGINE_DIR))

_RULES_PATH = _ENGINE_DIR / "rules.json"
_RM_PATH = _ENGINE_DIR / "relationship_manager.py"

# Lazy engine imports — deferred so pure helpers remain testable even when
# pdfplumber / cryptography deps are broken (common in sandboxed envs).
split_extractor = None  # type: Any
schema = None           # type: Any


def _ensure_engine_imports() -> None:
    """Import and validate engine modules on first use."""
    global split_extractor, schema
    if split_extractor is not None:
        return

    import importlib
    split_extractor = importlib.import_module("extractor")
    schema = importlib.import_module("schema_v2")

    # Post-import validation
    for _mod, _label in [(split_extractor, "extractor"), (schema, "schema_v2")]:
        _mod_path = str(Path(_mod.__file__).resolve())
        if any(b in _mod_path for b in _BLOCKED_MARKERS):
            sys.exit(f"FATAL: {_label} resolved to blocked path: {_mod_path}")
        if not any(m in _mod_path for m in _VALID_MARKERS):
            sys.exit(f"FATAL: {_label} resolved outside Split-RAG tree: {_mod_path}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COLUMNS = [
    "row_id", "doc_id", "filename", "relative_path", "file_ext",
    "doc_type", "doc_type_confidence",
    "borrower", "sponsor", "parent_company", "counterparty",
    "relationship_name", "facility_name",
    "product_type", "request_type", "request_amount",
    "collateral_summary", "property_type", "property_address",
    "ltv", "dscr", "noi", "occupancy", "cap_rate", "appraised_value",
    "covenants_summary", "exceptions_summary",
    "decision_summary", "comments_summary",
    "credit_team_summary", "topline_summary",
    "groomed_context", "warnings", "engine_version", "created_utc",
]

_ROLE_PREFIX_MAP = {
    "relationship_manager": "RM",
    "credit_officer": "CO",
    "underwriter": "UW",
    "approver": "AP",
}

# Metrics we want to extract by column name → ontology key
_METRIC_COL_MAP = {
    "ltv": "ltv",
    "dscr": "dscr",
    "noi": "noi",
    "occupancy": "occupancy",
    "cap_rate": "cap_rate",
    "appraised_value": "appraised_value",
}

# Entity types → column name
_ENTITY_COL_MAP = {
    "borrower": "borrower",
    "sponsor": "sponsor",
    "parent_company": "parent_company",
    "counterparty": "counterparty",
    "relationship_name": "relationship_name",
    "facility_name": "facility_name",
    "property_type": "property_type",
    "property_address": "property_address",
    "collateral_type": "collateral_summary",
    "collateral_description": "collateral_summary",
    "loan_purpose": "product_type",
}

# ---------------------------------------------------------------------------
# Pure helper functions (unit-testable)
# ---------------------------------------------------------------------------


def anonymize_team(
    members: List[Dict[str, str]], role_prefix_map: Optional[Dict[str, str]] = None
) -> str:
    """Replace real names with role-based placeholders: RM_1, CO_1, etc."""
    if role_prefix_map is None:
        role_prefix_map = _ROLE_PREFIX_MAP
    counters: Dict[str, int] = {}
    parts: List[str] = []
    for m in members:
        role = m.get("role", "UNKNOWN")
        prefix = role_prefix_map.get(role, role.upper()[:4])
        counters[prefix] = counters.get(prefix, 0) + 1
        parts.append(f"{prefix}_{counters[prefix]}")
    return "; ".join(parts)


def first_entity_value(
    entities: List[Dict[str, Any]], entity_type: str
) -> str:
    """Return the raw_value of the first matching entity, or empty string."""
    for e in entities:
        if e.get("entity_type") == entity_type:
            return str(e.get("raw_value", "")).strip()
    return ""


def best_metric_value(
    metrics: List[Dict[str, Any]], metric_name: str
) -> str:
    """Return the best (highest-confidence) metric raw_value, or empty string."""
    candidates = [
        m for m in metrics if m.get("metric_name") == metric_name
    ]
    if not candidates:
        return ""
    best = max(candidates, key=lambda m: m.get("confidence_score", 0.0))
    raw = best.get("raw_value", "")
    return str(raw).strip() if raw else ""


def truncate_to_limit(text: str, limit: int) -> Tuple[str, bool]:
    """Truncate text to limit characters. Returns (text, was_truncated)."""
    if len(text) <= limit:
        return text, False
    return text[:limit], True


def sanitize_pipe(text: str) -> str:
    """Escape pipe characters for pipe-delimited output."""
    return str(text).replace("|", "\\|").replace("\n", " ").replace("\r", "")


def sanitize_csv(text: str) -> str:
    """Normalize newlines for CSV cells."""
    return str(text).replace("\r\n", " ").replace("\r", " ").replace("\n", " ")


def collect_sections_from_nodes(
    nodes: List[Dict[str, Any]],
    exclude_email: bool = True,
) -> Dict[str, Dict[str, Any]]:
    """
    Group node content by section_label. Returns:
    {section_label: {level: int, pages: set, chars: int, chunks: [str, ...], is_email: bool}}
    """
    sections: Dict[str, Dict[str, Any]] = {}
    current_section = "__no_section__"
    current_level = 0

    for n in nodes:
        meta = n.get("metadata", {})
        ct = n.get("content_type", "text")
        content = n.get("content", "")
        is_email = meta.get("is_email_block", False)
        page = meta.get("page_number", 0)

        if ct == "header":
            label = meta.get("section_label") or content.strip()[:80]
            current_section = label
            current_level = meta.get("section_level", 1)

        sec_label = meta.get("section_label") or current_section

        if sec_label not in sections:
            sections[sec_label] = {
                "level": current_level,
                "pages": set(),
                "chars": 0,
                "chunks": [],
                "is_email": False,
            }

        sec = sections[sec_label]
        sec["pages"].add(page)
        sec["is_email"] = sec["is_email"] or is_email

        if exclude_email and is_email:
            continue

        sec["chars"] += len(content)
        sec["chunks"].append(content)

    return sections


def build_section_snippets(
    sections: Dict[str, Dict[str, Any]],
    max_sections: int,
    section_chars: int,
    exclude_email: bool,
) -> List[str]:
    """Build per-section snippet lines for groomed context part B."""
    lines: List[str] = []
    count = 0
    for label, sec in sections.items():
        if count >= max_sections:
            break
        if label == "__no_section__":
            continue
        if exclude_email and sec.get("is_email", False):
            continue
        combined = " ".join(sec["chunks"])[:section_chars]
        level = sec.get("level", 0)
        lines.append(f"SECTION={label}|LEVEL={level}|SNIPPET={combined}")
        count += 1
    return lines


def build_top_sections(
    sections: Dict[str, Dict[str, Any]],
    top_n: int,
    top_section_chars: int,
    early_pages: int,
) -> List[str]:
    """Build top-N largest sections (by char count, excluding email) for part C."""
    eligible = [
        (label, sec)
        for label, sec in sections.items()
        if label != "__no_section__" and not sec.get("is_email", False) and sec["chars"] > 0
    ]
    if not eligible:
        return []

    # Separate early-page vs. later-page sections
    early = [
        (l, s) for l, s in eligible if min(s["pages"], default=999) <= early_pages
    ]
    later = [
        (l, s) for l, s in eligible if min(s["pages"], default=999) > early_pages
    ]

    early_sorted = sorted(early, key=lambda x: x[1]["chars"], reverse=True)
    later_sorted = sorted(later, key=lambda x: x[1]["chars"], reverse=True)

    # Guarantee at least 2 from early pages if possible
    min_early = min(2, len(early_sorted))
    selected: List[Tuple[str, Dict[str, Any]]] = early_sorted[:min_early]
    remaining_slots = top_n - len(selected)

    # Fill remaining from combined pool (sorted by size, dedup)
    selected_labels = {s[0] for s in selected}
    combined = sorted(
        [(l, s) for l, s in eligible if l not in selected_labels],
        key=lambda x: x[1]["chars"],
        reverse=True,
    )
    selected.extend(combined[:remaining_slots])

    lines: List[str] = []
    for label, sec in selected:
        pages = sorted(sec["pages"])
        page_range = f"{pages[0]}-{pages[-1]}" if pages else "?"
        snippet = " ".join(sec["chunks"])[:top_section_chars]
        lines.append(
            f"TOP_SECTION={label}|PAGES={page_range}|CHARS={sec['chars']}|SNIPPET={snippet}"
        )
    return lines


def build_topline(row: Dict[str, str], max_chars: int = 1200) -> str:
    """Assemble the topline summary (700–1200 chars) from row fields."""
    parts: List[str] = []

    def _add(label: str, key: str) -> None:
        val = row.get(key, "").strip()
        if val:
            parts.append(f"{label}: {val}")

    _add("Borrower", "borrower")
    _add("Sponsor", "sponsor")
    _add("Parent", "parent_company")
    _add("Counterparty", "counterparty")
    _add("Relationship", "relationship_name")
    _add("Facility", "facility_name")
    _add("Product", "product_type")
    _add("Request Type", "request_type")
    _add("Request Amount", "request_amount")
    _add("Collateral", "collateral_summary")
    _add("Property Type", "property_type")
    _add("Property Address", "property_address")
    _add("LTV", "ltv")
    _add("DSCR", "dscr")
    _add("NOI", "noi")
    _add("Occupancy", "occupancy")
    _add("Cap Rate", "cap_rate")
    _add("Appraised Value", "appraised_value")
    _add("Exceptions", "exceptions_summary")
    _add("Decision", "decision_summary")
    _add("Conditions", "comments_summary")
    _add("Credit Team", "credit_team_summary")
    _add("Doc Type", "doc_type")
    _add("Confidence", "doc_type_confidence")

    topline = " | ".join(parts)
    return topline[:max_chars]


def build_groomed_context(
    row: Dict[str, str],
    sections: Dict[str, Dict[str, Any]],
    *,
    section_chars: int = 650,
    max_sections: int = 20,
    top_sections: int = 5,
    top_section_chars: int = 1200,
    early_pages: int = 3,
    max_context_chars: int = 6000,
    exclude_email: bool = True,
) -> Tuple[str, List[str]]:
    """
    Assemble groomed_context in deterministic order:
        A) TOPLINE
        B) Per-section snippets
        C) Top-N largest sections
    Returns (groomed_context, warnings).
    """
    warnings: List[str] = []

    # A) Topline
    topline = row.get("topline_summary", "")
    parts = [f"[TOPLINE] {topline}"]

    # B) Section snippets
    snippets = build_section_snippets(sections, max_sections, section_chars, exclude_email)
    parts.extend(snippets)

    # C) Top sections
    top_secs = build_top_sections(sections, top_sections, top_section_chars, early_pages)
    parts.extend(top_secs)

    combined = "\n".join(parts)
    if len(combined) > max_context_chars:
        warnings.append(
            f"groomed_context truncated from {len(combined)} to {max_context_chars} chars"
        )
        # Keep topline + early content first
        combined = combined[:max_context_chars]

    return combined, warnings


# ---------------------------------------------------------------------------
# Fallback regex extractors (when intelligence is absent)
# ---------------------------------------------------------------------------

_FALLBACK_PATTERNS: Dict[str, re.Pattern] = {
    "borrower": re.compile(
        r"(?:borrower|obligor)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
    "sponsor": re.compile(
        r"(?:sponsor|principal)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
    "facility_name": re.compile(
        r"(?:facility|credit facility)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
    "product_type": re.compile(
        r"(?:product\s*type|loan\s*type)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
    "request_type": re.compile(
        r"(?:request\s*type|action\s*requested)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
    "request_amount": re.compile(
        r"(?:request(?:ed)?\s*amount|loan\s*amount)\s*[:=\-]\s*(\$[\d,.\s]*\w*)",
        re.IGNORECASE,
    ),
    "property_type": re.compile(
        r"(?:property\s*type)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
    "property_address": re.compile(
        r"(?:property\s*address|location)\s*[:=\-]\s*(.+?)(?:\n|$)", re.IGNORECASE
    ),
}


def fallback_extract(early_text: str, field: str) -> str:
    """Regex fallback for a single field from raw early-page text."""
    pat = _FALLBACK_PATTERNS.get(field)
    if not pat:
        return ""
    m = pat.search(early_text)
    return m.group(1).strip() if m else ""


# ---------------------------------------------------------------------------
# Core pipeline: ContextGraph JSON → row dict
# ---------------------------------------------------------------------------


def load_rules() -> Dict[str, Any]:
    """Load rules.json from engine directory."""
    with open(_RULES_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _context_graph_from_json(json_path: Path):
    """Deserialize a ContextGraph JSON file."""
    _ensure_engine_imports()
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return schema.ContextGraph(**data)


def _nodes_to_dicts(nodes: List) -> List[Dict[str, Any]]:
    """Convert Pydantic ContextNode list to plain dicts for section analysis."""
    result = []
    for n in nodes:
        d = {
            "content_type": n.content_type,
            "content": n.content,
            "metadata": {
                "page_number": n.metadata.page_number,
                "section_label": n.metadata.section_label,
                "section_level": n.metadata.section_level,
                "is_email_block": n.metadata.is_email_block,
                "is_active": n.metadata.is_active,
            },
        }
        result.append(d)
    return result


def _extract_row_from_graph(
    graph,
    json_path: Path,
    input_folder: Path,
    row_id: int,
    early_pages: int,
) -> Dict[str, str]:
    """Extract a single row dict from a ContextGraph."""
    intel = graph.intelligence
    row: Dict[str, str] = {col: "" for col in COLUMNS}

    row["row_id"] = str(row_id)
    row["doc_id"] = graph.document_id
    row["filename"] = graph.filename

    try:
        row["relative_path"] = str(json_path.relative_to(input_folder))
    except ValueError:
        row["relative_path"] = json_path.name

    row["file_ext"] = Path(graph.filename).suffix.lower()
    row["engine_version"] = graph.schema_version
    row["created_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Early-page text buffer for fallback extraction
    early_nodes = [n for n in graph.nodes if n.metadata.page_number <= early_pages]
    early_text = "\n".join(n.content for n in early_nodes)

    # --- Populate from intelligence (preferred) or fallback ---
    entities_dicts: List[Dict[str, Any]] = []
    metrics_dicts: List[Dict[str, Any]] = []
    team_dicts: List[Dict[str, str]] = []
    covenants_dicts: List[Dict[str, Any]] = []

    if intel:
        row["doc_type"] = intel.document_type or ""
        row["doc_type_confidence"] = f"{intel.document_type_confidence:.2f}"

        entities_dicts = [e.model_dump() for e in intel.entities]
        metrics_dicts = [m.model_dump() for m in intel.financial_metrics]
        team_dicts = [{"role": t.role, "name": t.name} for t in intel.credit_team]
        covenants_dicts = [c.model_dump() for c in intel.covenants]

    # Also pull legacy top-level entity fields
    if graph.borrower_entity and not first_entity_value(entities_dicts, "borrower"):
        entities_dicts.append(
            {"entity_type": "borrower", "raw_value": graph.borrower_entity}
        )

    # Map entities to columns
    for entity_type, col_name in _ENTITY_COL_MAP.items():
        val = first_entity_value(entities_dicts, entity_type)
        if val and not row[col_name]:
            row[col_name] = val

    # Fallback regex for missing fields
    fallback_fields = [
        "borrower", "sponsor", "facility_name", "product_type",
        "request_type", "request_amount", "property_type", "property_address",
    ]
    for field in fallback_fields:
        if not row.get(field):
            row[field] = fallback_extract(early_text, field)

    # Map metrics to columns
    for col_name, metric_key in _METRIC_COL_MAP.items():
        val = best_metric_value(metrics_dicts, metric_key)
        if val:
            row[col_name] = val

    # Covenants summary
    if covenants_dicts:
        cov_texts = [c.get("raw_value", "") for c in covenants_dicts[:5]]
        row["covenants_summary"] = "; ".join(t for t in cov_texts if t)

    # Credit team (anonymized)
    if team_dicts:
        row["credit_team_summary"] = anonymize_team(team_dicts)

    return row


def process_single_json(
    json_path: Path,
    input_folder: Path,
    row_id: int,
    args: argparse.Namespace,
    logger: logging.Logger,
) -> Optional[Dict[str, str]]:
    """Process one ContextGraph JSON into a row dict. Returns None on failure."""
    try:
        graph = _context_graph_from_json(json_path)
    except Exception as exc:
        logger.error("Failed to load %s: %s", json_path.name, exc)
        return None

    row = _extract_row_from_graph(graph, json_path, input_folder, row_id, args.early_pages)

    # Build sections for groomed context
    node_dicts = _nodes_to_dicts(graph.nodes)
    sections = collect_sections_from_nodes(node_dicts, exclude_email=args.exclude_email)

    # Build topline (before groomed context, since groomed context uses it)
    row["topline_summary"] = build_topline(row)

    # Groomed context
    groomed, ctx_warnings = build_groomed_context(
        row,
        sections,
        section_chars=args.section_chars,
        max_sections=args.max_sections,
        top_sections=args.top_sections,
        top_section_chars=args.top_section_chars,
        early_pages=args.early_pages,
        max_context_chars=args.max_context_chars,
        exclude_email=args.exclude_email,
    )
    row["groomed_context"] = groomed

    # Collect warnings
    all_warnings = list(ctx_warnings)
    if not row["borrower"]:
        all_warnings.append("borrower not extracted")
    if not row["doc_type"]:
        all_warnings.append("doc_type not classified")
    row["warnings"] = "; ".join(all_warnings)

    return row


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


def write_csv(rows: List[Dict[str, str]], output_path: Path) -> None:
    """Write rows to CSV with UTF-8 BOM."""
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            sanitized = {k: sanitize_csv(str(v)) for k, v in row.items()}
            writer.writerow(sanitized)


def write_pipe_delimited(rows: List[Dict[str, str]], output_path: Path) -> None:
    """Write rows to pipe-delimited TXT."""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("|".join(COLUMNS) + "\n")
        for row in rows:
            values = [sanitize_pipe(str(row.get(c, ""))) for c in COLUMNS]
            f.write("|".join(values) + "\n")


def write_manifest(
    rows: List[Dict[str, str]],
    output_dir: Path,
    run_time: float,
    all_warnings: List[str],
    args: argparse.Namespace,
) -> None:
    """Write the ingest manifest JSON."""
    _ensure_engine_imports()
    manifest = {
        "run_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "run_duration_seconds": round(run_time, 2),
        "total_documents": len(rows),
        "documents_with_warnings": sum(1 for r in rows if r.get("warnings")),
        "parameters": {
            "input": str(args.input),
            "output": str(args.output),
            "section_chars": args.section_chars,
            "top_section_chars": args.top_section_chars,
            "max_sections": args.max_sections,
            "early_pages": args.early_pages,
            "top_sections": args.top_sections,
            "max_context_chars": args.max_context_chars,
            "exclude_email": args.exclude_email,
            "extensions": args.extensions,
            "fail_fast": args.fail_fast,
        },
        "engine": {
            "extractor_path": str(Path(split_extractor.__file__).resolve()),
            "schema_path": str(Path(schema.__file__).resolve()),
            "rules_path": str(_RULES_PATH.resolve()),
            "relationship_manager_path": str(_RM_PATH.resolve()),
            "schema_version": getattr(schema.ContextGraph, "model_fields", {})
            .get("schema_version", {})
            .default
            if hasattr(schema.ContextGraph, "model_fields")
            else "unknown",
        },
        "warnings": all_warnings,
        "columns": COLUMNS,
    }
    out = output_dir / "copilot_ingest_manifest.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def discover_json_outputs(input_dir: Path, extensions: str) -> List[Path]:
    """
    Find ContextGraph JSON files (output of the extraction engine).
    These are *_v2.json files in the input directory.
    """
    jsons = sorted(input_dir.rglob("*_v2.json"))
    return jsons


def discover_raw_documents(input_dir: Path, extensions: str) -> List[Path]:
    """Find raw document files to process through the engine first."""
    exts = {f".{e.strip().lstrip('.')}" for e in extensions.split(",")}
    files: List[Path] = []
    for f in sorted(input_dir.rglob("*")):
        if f.suffix.lower() in exts and not f.name.startswith(("~", ".")):
            files.append(f)
    return files


# ---------------------------------------------------------------------------
# Self-check
# ---------------------------------------------------------------------------


def run_self_check(logger: logging.Logger) -> bool:
    """Quick validation that the engine paths are correct."""
    _ensure_engine_imports()
    ok = True
    for label, path_str in [
        ("extractor", str(Path(split_extractor.__file__).resolve())),
        ("schema_v2", str(Path(schema.__file__).resolve())),
        ("rules.json", str(_RULES_PATH.resolve())),
        ("relationship_manager", str(_RM_PATH.resolve())),
    ]:
        has_marker = any(m in path_str for m in _VALID_MARKERS)
        has_blocked = any(b in path_str for b in _BLOCKED_MARKERS)
        status = "OK" if (has_marker and not has_blocked) else "FAIL"
        logger.info("Engine check: %s = %s (%s)", label, path_str, status)
        if status == "FAIL":
            ok = False
    return ok


# ---------------------------------------------------------------------------
# CLI + main
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Copilot Studio ingest dataset from Split-RAG engine outputs."
    )
    parser.add_argument(
        "--input", type=Path, required=True,
        help="Folder containing *_v2.json engine outputs (or raw documents to process).",
    )
    parser.add_argument(
        "--output", type=Path, required=True,
        help="Output folder for CSV, TXT, and manifest.",
    )
    parser.add_argument("--section_chars", type=int, default=650)
    parser.add_argument("--top_section_chars", type=int, default=1200)
    parser.add_argument("--max_sections", type=int, default=20)
    parser.add_argument("--early_pages", type=int, default=3)
    parser.add_argument("--top_sections", type=int, default=5)
    parser.add_argument("--max_context_chars", type=int, default=6000)
    parser.add_argument(
        "--exclude_email",
        type=lambda v: v.lower() in ("true", "1", "yes"),
        default=True,
    )
    parser.add_argument("--extensions", type=str, default="pdf,doc,docx,txt")
    parser.add_argument(
        "--fail_fast",
        type=lambda v: v.lower() in ("true", "1", "yes"),
        default=False,
    )
    return parser.parse_args(argv)


def setup_logging(output_dir: Path) -> logging.Logger:
    """Configure logging to console + file."""
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("copilot_ingest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(output_dir / "build_copilot_ingest.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logger = setup_logging(args.output)

    logger.info("=== Copilot Ingest Build Start ===")
    logger.info("Input:  %s", args.input)
    logger.info("Output: %s", args.output)

    # Engine self-check
    if not run_self_check(logger):
        logger.error("Engine path validation FAILED. Aborting.")
        return 1

    start = time.time()
    args.output.mkdir(parents=True, exist_ok=True)
    all_warnings: List[str] = []

    # Discover pre-computed ContextGraph JSONs
    json_files = discover_json_outputs(args.input, args.extensions)

    if not json_files:
        logger.warning("No *_v2.json files found in %s.", args.input)
        # Try processing raw documents through the engine
        raw_docs = discover_raw_documents(args.input, args.extensions)
        if raw_docs:
            logger.info("Found %d raw documents. Running extraction engine...", len(raw_docs))
            try:
                config = split_extractor.load_config(_ENGINE_DIR / "config.json")
                # Override output to a temp location within output dir
                engine_output = args.output / "_engine_output"
                engine_output.mkdir(parents=True, exist_ok=True)
                config.output_dir = engine_output
                config.quarantine_dir = args.output / "_quarantine"
                config.quarantine_dir.mkdir(parents=True, exist_ok=True)

                rules = load_rules()
                ext_logger = split_extractor.setup_logging(args.output / "_engine_logs")

                for doc_path in raw_docs:
                    try:
                        success = split_extractor.process_file(
                            doc_path, config, rules, ext_logger
                        )
                        if not success:
                            all_warnings.append(f"Engine failed: {doc_path.name}")
                    except Exception as exc:
                        msg = f"Engine error on {doc_path.name}: {exc}"
                        logger.error(msg)
                        all_warnings.append(msg)
                        if args.fail_fast:
                            return 1

                json_files = discover_json_outputs(engine_output, args.extensions)
            except Exception as exc:
                logger.error("Could not run extraction engine: %s", exc)
                all_warnings.append(f"Engine init failed: {exc}")
        else:
            logger.warning("No raw documents found either.")

    if not json_files:
        logger.info("No documents to process. Writing empty schema files.")
        write_csv([], args.output / "copilot_ingest.csv")
        write_pipe_delimited([], args.output / "copilot_ingest.txt")
        write_manifest([], args.output, time.time() - start, all_warnings, args)
        logger.info("Empty-run output written.")
        return 0

    logger.info("Processing %d ContextGraph JSON files...", len(json_files))

    rows: List[Dict[str, str]] = []
    for i, jf in enumerate(json_files, start=1):
        logger.info("[%d/%d] %s", i, len(json_files), jf.name)
        try:
            row = process_single_json(jf, args.input, i, args, logger)
            if row:
                rows.append(row)
            else:
                all_warnings.append(f"Skipped (parse error): {jf.name}")
        except Exception as exc:
            msg = f"Error processing {jf.name}: {exc}"
            logger.error(msg)
            all_warnings.append(msg)
            if args.fail_fast:
                return 1

    # Write outputs
    csv_path = args.output / "copilot_ingest.csv"
    txt_path = args.output / "copilot_ingest.txt"

    write_csv(rows, csv_path)
    logger.info("Wrote %s (%d rows)", csv_path, len(rows))

    write_pipe_delimited(rows, txt_path)
    logger.info("Wrote %s (%d rows)", txt_path, len(rows))

    write_manifest(rows, args.output, time.time() - start, all_warnings, args)
    logger.info("Wrote manifest to %s", args.output / "copilot_ingest_manifest.json")

    logger.info("=== Copilot Ingest Build Complete: %d documents ===", len(rows))
    return 0


if __name__ == "__main__":
    sys.exit(main())
