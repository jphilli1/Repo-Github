#!/usr/bin/env python3
"""
build_sharepoint_copilot_package.py — SharePoint package builder for Copilot Studio.

Reads deterministic Split-RAG engine outputs (ContextGraph JSON) and produces
a per-document file package optimized for Copilot Studio's 8K token window:

Per row_id:
    <row_id>__manifest.json        Document metadata + file pointers
    <row_id>__topline.txt          <=1200 chars of highest-value facts
    <row_id>__section_index.csv    Section inventory for routing
    <row_id>__sections_excerpt.txt First N chars per section
    <row_id>__top_sections.txt     Top 5 largest sections by char count
    <row_id>__metrics.csv          Financial metrics with evidence
    <row_id>__entities.csv         Entity extractions with evidence
    <row_id>__covenants.csv        Covenant extractions with evidence
    <row_id>__credit_team.csv      Anonymized roles with evidence
    <row_id>__chunks_map.csv       Chunk-level index for evidence lookup

Global:
    copilot_ingest_index.csv       One row per doc, UTF-8 BOM
    copilot_ingest_index.txt       Pipe-delimited, same schema

No LLM. No torch. Deterministic only.
"""

from __future__ import annotations

import argparse
import csv
import datetime
import hashlib
import io
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Engine import guard
# ---------------------------------------------------------------------------

_THIS_DIR = Path(__file__).resolve().parent
_ENGINE_DIR = _THIS_DIR.parent

_VALID_MARKERS = ("Split-RAG",)
_BLOCKED_MARKERS = ("text_extract",)


def _validate_engine_dir(engine_dir: Path) -> None:
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

if str(_ENGINE_DIR) not in sys.path:
    sys.path.insert(0, str(_ENGINE_DIR))

_RULES_PATH = _ENGINE_DIR / "rules.json"
_RM_PATH = _ENGINE_DIR / "relationship_manager.py"

# Lazy engine imports
split_extractor = None  # type: Any
schema = None           # type: Any


def _ensure_engine_imports() -> None:
    global split_extractor, schema
    if split_extractor is not None:
        return
    import importlib
    split_extractor = importlib.import_module("extractor")
    schema = importlib.import_module("schema_v2")
    for _mod, _label in [(split_extractor, "extractor"), (schema, "schema_v2")]:
        _mod_path = str(Path(_mod.__file__).resolve())
        if any(b in _mod_path for b in _BLOCKED_MARKERS):
            sys.exit(f"FATAL: {_label} resolved to blocked path: {_mod_path}")
        if not any(m in _mod_path for m in _VALID_MARKERS):
            sys.exit(f"FATAL: {_label} resolved outside Split-RAG tree: {_mod_path}")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INDEX_COLUMNS = [
    "row_id", "doc_id", "filename", "relative_row_folder",
    "doc_type", "doc_type_confidence",
    "borrower", "relationship_name", "product_type",
    "request_type", "request_amount", "collateral_summary",
    "key_metrics_summary", "decision_summary", "exceptions_summary",
    "credit_team_summary", "recommended_files_to_open",
    "warnings", "created_utc",
]

_ROLE_PREFIX_MAP = {
    "relationship_manager": "RM",
    "credit_officer": "CO",
    "underwriter": "UW",
    "approver": "AP",
}

_METRIC_KEYS = [
    "ltv", "dscr", "noi", "occupancy", "cap_rate", "appraised_value",
    "revenue", "ebitda", "net_income", "total_leverage", "senior_leverage",
    "interest_coverage", "loan_amount", "cash", "liquidity", "debt",
]

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

# Recommended files in priority order
_RECOMMENDED_FILES = [
    "topline.txt",
    "metrics.csv",
    "covenants.csv",
    "credit_team.csv",
    "top_sections.txt",
    "section_index.csv",
    "sections_excerpt.txt",
    "entities.csv",
]

# ---------------------------------------------------------------------------
# Pure helper functions (unit-testable)
# ---------------------------------------------------------------------------


def anonymize_team(
    members: List[Dict[str, str]],
    role_prefix_map: Optional[Dict[str, str]] = None,
) -> str:
    """Replace real names with role placeholders: RM_1, CO_1, etc."""
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


def anonymize_team_rows(
    members: List[Dict[str, Any]],
    role_prefix_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    """Return list of dicts with anonymized names + evidence fields."""
    if role_prefix_map is None:
        role_prefix_map = _ROLE_PREFIX_MAP
    counters: Dict[str, int] = {}
    rows: List[Dict[str, str]] = []
    for m in members:
        role = m.get("role", "UNKNOWN")
        prefix = role_prefix_map.get(role, role.upper()[:4])
        counters[prefix] = counters.get(prefix, 0) + 1
        rows.append({
            "role": role,
            "anonymized_name": f"{prefix}_{counters[prefix]}",
            "confidence_score": str(m.get("confidence_score", "")),
            "evidence_chunk_id": str(m.get("evidence_chunk_id", "")),
        })
    return rows


def first_entity_value(
    entities: List[Dict[str, Any]], entity_type: str,
) -> str:
    for e in entities:
        if e.get("entity_type") == entity_type:
            return str(e.get("raw_value", "")).strip()
    return ""


def best_metric_value(
    metrics: List[Dict[str, Any]], metric_name: str,
) -> str:
    candidates = [m for m in metrics if m.get("metric_name") == metric_name]
    if not candidates:
        return ""
    best = max(candidates, key=lambda m: m.get("confidence_score", 0.0))
    raw = best.get("raw_value", "")
    return str(raw).strip() if raw else ""


def fallback_extract(early_text: str, field: str) -> str:
    pat = _FALLBACK_PATTERNS.get(field)
    if not pat:
        return ""
    m = pat.search(early_text)
    return m.group(1).strip() if m else ""


def sanitize_pipe(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ").replace("\r", "")


def sanitize_csv_cell(text: str) -> str:
    return str(text).replace("\r\n", " ").replace("\r", " ").replace("\n", " ")


def collect_sections_from_nodes(
    nodes: List[Dict[str, Any]],
    exclude_email: bool = True,
) -> Dict[str, Dict[str, Any]]:
    """
    Group node content by section_label.
    Returns {label: {level, pages: set, chars, chunks: [str], is_email,
                     has_metrics, has_covenants, has_decision, chunk_ids: [str]}}
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
        chunk_id = n.get("chunk_id", "")

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
                "chunk_ids": [],
                "is_email": False,
                "has_metrics": False,
                "has_covenants": False,
                "has_decision": False,
            }

        sec = sections[sec_label]
        sec["pages"].add(page)
        sec["chunk_ids"].append(chunk_id)
        sec["is_email"] = sec["is_email"] or is_email

        # Detect content signals
        content_lower = content.lower()
        if any(kw in content_lower for kw in ("dscr", "ltv", "noi", "ebitda", "leverage")):
            sec["has_metrics"] = True
        if any(kw in content_lower for kw in ("covenant", "compliance", "reporting requirement")):
            sec["has_covenants"] = True
        if any(kw in content_lower for kw in ("approved", "declined", "decision", "recommendation")):
            sec["has_decision"] = True

        if exclude_email and is_email:
            continue

        sec["chars"] += len(content)
        sec["chunks"].append(content)

    return sections


def build_topline(fields: Dict[str, str], max_chars: int = 1200) -> str:
    """Assemble topline summary (<=1200 chars) from extracted fields."""
    parts: List[str] = []

    def _add(label: str, key: str) -> None:
        val = fields.get(key, "").strip()
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


def build_section_excerpts(
    sections: Dict[str, Dict[str, Any]],
    max_sections: int,
    section_chars: int,
    exclude_email: bool,
) -> str:
    """Build sections_excerpt.txt content."""
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
    return "\n".join(lines)


def select_top_sections(
    sections: Dict[str, Dict[str, Any]],
    top_n: int,
    early_pages: int,
) -> List[Tuple[str, Dict[str, Any]]]:
    """Select top-N sections by char count (email excluded), with early-page bias."""
    eligible = [
        (label, sec)
        for label, sec in sections.items()
        if label != "__no_section__" and not sec.get("is_email", False) and sec["chars"] > 0
    ]
    if not eligible:
        return []

    early = [
        (l, s) for l, s in eligible if min(s["pages"], default=999) <= early_pages
    ]
    early_sorted = sorted(early, key=lambda x: x[1]["chars"], reverse=True)

    min_early = min(2, len(early_sorted))
    selected: List[Tuple[str, Dict[str, Any]]] = early_sorted[:min_early]
    remaining_slots = top_n - len(selected)

    selected_labels = {s[0] for s in selected}
    combined = sorted(
        [(l, s) for l, s in eligible if l not in selected_labels],
        key=lambda x: x[1]["chars"],
        reverse=True,
    )
    selected.extend(combined[:remaining_slots])
    return selected


def build_top_sections_text(
    sections: Dict[str, Dict[str, Any]],
    top_n: int,
    top_section_chars: int,
    early_pages: int,
) -> str:
    """Build top_sections.txt content."""
    selected = select_top_sections(sections, top_n, early_pages)
    lines: List[str] = []
    for label, sec in selected:
        pages = sorted(sec["pages"])
        page_range = f"{pages[0]}-{pages[-1]}" if pages else "?"
        snippet = " ".join(sec["chunks"])[:top_section_chars]
        lines.append(
            f"TOP_SECTION={label}|PAGES={page_range}|CHARS={sec['chars']}|SNIPPET={snippet}"
        )
    return "\n".join(lines)


def key_metrics_summary(metrics_dicts: List[Dict[str, Any]]) -> str:
    """Build a concise key metrics summary string."""
    parts: List[str] = []
    seen: Set[str] = set()
    for m in metrics_dicts:
        name = m.get("metric_name", "")
        raw = m.get("raw_value", "")
        if name and raw and name not in seen:
            parts.append(f"{name.upper()}={raw}")
            seen.add(name)
    return "; ".join(parts[:8])


# ---------------------------------------------------------------------------
# CSV writers (pure, no engine imports)
# ---------------------------------------------------------------------------


def _write_csv_rows(
    path: Path,
    fieldnames: List[str],
    rows: List[Dict[str, str]],
    bom: bool = False,
) -> int:
    """Write a CSV file. Returns byte size."""
    encoding = "utf-8-sig" if bom else "utf-8"
    with open(path, "w", newline="", encoding=encoding) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: sanitize_csv_cell(str(v)) for k, v in row.items()})
    return path.stat().st_size


def _write_text(path: Path, content: str) -> int:
    """Write a text file. Returns byte size."""
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path.stat().st_size


# ---------------------------------------------------------------------------
# Core: extract structured data from a ContextGraph
# ---------------------------------------------------------------------------


def _nodes_to_dicts(nodes: List) -> List[Dict[str, Any]]:
    """Convert Pydantic ContextNode list to plain dicts."""
    result = []
    for n in nodes:
        result.append({
            "chunk_id": n.chunk_id,
            "content_type": n.content_type,
            "content": n.content,
            "metadata": {
                "page_number": n.metadata.page_number,
                "section_label": n.metadata.section_label,
                "section_level": n.metadata.section_level,
                "is_email_block": n.metadata.is_email_block,
                "is_active": n.metadata.is_active,
            },
        })
    return result


def extract_document_data(graph, early_pages: int) -> Dict[str, Any]:
    """
    Extract all structured data from a ContextGraph into a plain dict.
    Returns a dict with keys: fields, entities_dicts, metrics_dicts,
    team_dicts, covenants_dicts, node_dicts.
    """
    intel = graph.intelligence
    fields: Dict[str, str] = {}

    fields["doc_id"] = graph.document_id
    fields["filename"] = graph.filename
    fields["file_ext"] = Path(graph.filename).suffix.lower()
    fields["engine_version"] = graph.schema_version

    # Early-page text for fallback
    early_nodes = [n for n in graph.nodes if n.metadata.page_number <= early_pages]
    early_text = "\n".join(n.content for n in early_nodes)

    entities_dicts: List[Dict[str, Any]] = []
    metrics_dicts: List[Dict[str, Any]] = []
    team_dicts: List[Dict[str, Any]] = []
    covenants_dicts: List[Dict[str, Any]] = []

    if intel:
        fields["doc_type"] = intel.document_type or ""
        fields["doc_type_confidence"] = f"{intel.document_type_confidence:.2f}"
        entities_dicts = [e.model_dump() for e in intel.entities]
        metrics_dicts = [m.model_dump() for m in intel.financial_metrics]
        team_dicts = [
            {
                "role": t.role,
                "name": t.name,
                "confidence_score": t.confidence_score,
                "evidence_chunk_id": t.evidence_chunk_id or "",
            }
            for t in intel.credit_team
        ]
        covenants_dicts = [c.model_dump() for c in intel.covenants]
    else:
        fields["doc_type"] = ""
        fields["doc_type_confidence"] = ""

    # Legacy top-level entity
    if graph.borrower_entity and not first_entity_value(entities_dicts, "borrower"):
        entities_dicts.append(
            {"entity_type": "borrower", "raw_value": graph.borrower_entity}
        )

    # Map entities to field names
    for entity_type, col_name in _ENTITY_COL_MAP.items():
        val = first_entity_value(entities_dicts, entity_type)
        if val and not fields.get(col_name):
            fields[col_name] = val

    # Fallback regex
    for field in [
        "borrower", "sponsor", "facility_name", "product_type",
        "request_type", "request_amount", "property_type", "property_address",
    ]:
        if not fields.get(field):
            fields[field] = fallback_extract(early_text, field)

    # Metrics to fields
    for mk in _METRIC_KEYS:
        val = best_metric_value(metrics_dicts, mk)
        if val:
            fields[mk] = val

    # Covenants summary
    if covenants_dicts:
        cov_texts = [c.get("raw_value", "") for c in covenants_dicts[:5]]
        fields["covenants_summary"] = "; ".join(t for t in cov_texts if t)

    # Credit team (anonymized)
    if team_dicts:
        fields["credit_team_summary"] = anonymize_team(team_dicts)

    # Node dicts
    node_dicts = _nodes_to_dicts(graph.nodes)

    return {
        "fields": fields,
        "entities_dicts": entities_dicts,
        "metrics_dicts": metrics_dicts,
        "team_dicts": team_dicts,
        "covenants_dicts": covenants_dicts,
        "node_dicts": node_dicts,
    }


# ---------------------------------------------------------------------------
# Per-row_id package writer
# ---------------------------------------------------------------------------


def write_row_package(
    row_id: int,
    data: Dict[str, Any],
    output_dir: Path,
    args: argparse.Namespace,
) -> Tuple[Dict[str, int], List[str]]:
    """
    Write the full per-row_id file package.
    Returns (file_sizes: {filename: bytes}, warnings: [str]).
    """
    row_id_str = str(row_id)
    row_dir = output_dir / "rows" / row_id_str
    row_dir.mkdir(parents=True, exist_ok=True)

    fields = data["fields"]
    entities_dicts = data["entities_dicts"]
    metrics_dicts = data["metrics_dicts"]
    team_dicts = data["team_dicts"]
    covenants_dicts = data["covenants_dicts"]
    node_dicts = data["node_dicts"]

    file_sizes: Dict[str, int] = {}
    warnings: List[str] = []

    prefix = f"{row_id_str}__"

    # --- 1. topline.txt ---
    topline = build_topline(fields, max_chars=1200)
    if len(topline) > 1200:
        warnings.append("topline truncated to 1200 chars")
        topline = topline[:1200]
    fname = f"{prefix}topline.txt"
    file_sizes[fname] = _write_text(row_dir / fname, topline)

    # --- 2. section_index.csv ---
    sections = collect_sections_from_nodes(node_dicts, exclude_email=args.exclude_email)
    sec_index_rows: List[Dict[str, str]] = []
    for label, sec in sections.items():
        if label == "__no_section__":
            continue
        pages = sorted(sec["pages"])
        page_span = f"{pages[0]}-{pages[-1]}" if pages else ""
        sec_index_rows.append({
            "section_label": label,
            "level": str(sec["level"]),
            "char_count": str(sec["chars"]),
            "page_span": page_span,
            "is_email": str(sec["is_email"]),
            "has_metrics": str(sec["has_metrics"]),
            "has_covenants": str(sec["has_covenants"]),
            "has_decision": str(sec["has_decision"]),
        })
    sec_index_fields = [
        "section_label", "level", "char_count", "page_span",
        "is_email", "has_metrics", "has_covenants", "has_decision",
    ]
    fname = f"{prefix}section_index.csv"
    file_sizes[fname] = _write_csv_rows(row_dir / fname, sec_index_fields, sec_index_rows)

    # --- 3. sections_excerpt.txt ---
    fname = f"{prefix}sections_excerpt.txt"
    excerpt_text = build_section_excerpts(
        sections, args.max_sections, args.section_chars, args.exclude_email,
    )
    file_sizes[fname] = _write_text(row_dir / fname, excerpt_text)

    # --- 4. top_sections.txt ---
    fname = f"{prefix}top_sections.txt"
    top_text = build_top_sections_text(
        sections, args.top_sections, args.top_section_chars, args.early_pages,
    )
    file_sizes[fname] = _write_text(row_dir / fname, top_text)

    # --- 5. metrics.csv ---
    metrics_rows: List[Dict[str, str]] = []
    for m in metrics_dicts:
        metrics_rows.append({
            "metric_name": str(m.get("metric_name", "")),
            "raw_value": str(m.get("raw_value", "")),
            "normalized_value": str(m.get("normalized_value", "")),
            "unit": str(m.get("unit", "")),
            "normalized_unit": str(m.get("normalized_unit", "")),
            "source_section": str(m.get("source_section", "")),
            "page_number": str(m.get("page_number", "")),
            "confidence_score": str(m.get("confidence_score", "")),
            "evidence_chunk_id": str(m.get("evidence_chunk_id", "")),
        })
    metrics_fields = [
        "metric_name", "raw_value", "normalized_value", "unit", "normalized_unit",
        "source_section", "page_number", "confidence_score", "evidence_chunk_id",
    ]
    fname = f"{prefix}metrics.csv"
    file_sizes[fname] = _write_csv_rows(row_dir / fname, metrics_fields, metrics_rows)

    # --- 6. entities.csv ---
    entities_rows: List[Dict[str, str]] = []
    for e in entities_dicts:
        entities_rows.append({
            "entity_type": str(e.get("entity_type", "")),
            "raw_value": str(e.get("raw_value", "")),
            "normalized_value": str(e.get("normalized_value", "")),
            "source_section": str(e.get("source_section", "")),
            "page_number": str(e.get("page_number", "")),
            "confidence_score": str(e.get("confidence_score", "")),
            "evidence_chunk_id": str(e.get("evidence_chunk_id", "")),
        })
    entities_fields = [
        "entity_type", "raw_value", "normalized_value",
        "source_section", "page_number", "confidence_score", "evidence_chunk_id",
    ]
    fname = f"{prefix}entities.csv"
    file_sizes[fname] = _write_csv_rows(row_dir / fname, entities_fields, entities_rows)

    # --- 7. covenants.csv ---
    covenants_rows: List[Dict[str, str]] = []
    for c in covenants_dicts:
        covenants_rows.append({
            "entity_type": str(c.get("entity_type", "")),
            "raw_value": str(c.get("raw_value", "")),
            "normalized_value": str(c.get("normalized_value", "")),
            "source_section": str(c.get("source_section", "")),
            "page_number": str(c.get("page_number", "")),
            "confidence_score": str(c.get("confidence_score", "")),
            "evidence_chunk_id": str(c.get("evidence_chunk_id", "")),
        })
    covenants_fields = [
        "entity_type", "raw_value", "normalized_value",
        "source_section", "page_number", "confidence_score", "evidence_chunk_id",
    ]
    fname = f"{prefix}covenants.csv"
    file_sizes[fname] = _write_csv_rows(row_dir / fname, covenants_fields, covenants_rows)

    # --- 8. credit_team.csv (anonymized) ---
    anon_rows = anonymize_team_rows(team_dicts)
    team_fields = ["role", "anonymized_name", "confidence_score", "evidence_chunk_id"]
    fname = f"{prefix}credit_team.csv"
    file_sizes[fname] = _write_csv_rows(row_dir / fname, team_fields, anon_rows)

    # --- 9. chunks_map.csv ---
    chunks_rows: List[Dict[str, str]] = []
    for nd in node_dicts:
        meta = nd.get("metadata", {})
        chunks_rows.append({
            "chunk_id": str(nd.get("chunk_id", "")),
            "page_number": str(meta.get("page_number", "")),
            "section_label": str(meta.get("section_label", "")),
            "content_type": str(nd.get("content_type", "")),
            "is_email": str(meta.get("is_email_block", False)),
            "char_count": str(len(nd.get("content", ""))),
        })
    chunks_fields = [
        "chunk_id", "page_number", "section_label",
        "content_type", "is_email", "char_count",
    ]
    fname = f"{prefix}chunks_map.csv"
    file_sizes[fname] = _write_csv_rows(row_dir / fname, chunks_fields, chunks_rows)

    # --- 10. manifest.json ---
    _ensure_engine_imports()
    section_count = len([s for s in sections if s != "__no_section__"])
    email_chunks = sum(
        1 for nd in node_dicts if nd.get("metadata", {}).get("is_email_block", False)
    )
    total_chunks = len(node_dicts)
    email_ratio = email_chunks / total_chunks if total_chunks else 0.0

    manifest = {
        "row_id": row_id,
        "doc_id": fields.get("doc_id", ""),
        "filename": fields.get("filename", ""),
        "doc_type": fields.get("doc_type", ""),
        "engine": {
            "extractor_path": str(Path(split_extractor.__file__).resolve()),
            "schema_path": str(Path(schema.__file__).resolve()),
            "rules_path": str(_RULES_PATH.resolve()),
            "relationship_manager_path": str(_RM_PATH.resolve()),
            "engine_version": fields.get("engine_version", ""),
        },
        "section_count": section_count,
        "total_chunks": total_chunks,
        "email_chunks": email_chunks,
        "email_ratio": round(email_ratio, 3),
        "entity_count": len(entities_dicts),
        "metric_count": len(metrics_dicts),
        "covenant_count": len(covenants_dicts),
        "credit_team_count": len(team_dicts),
        "files": {k: {"size_bytes": v} for k, v in file_sizes.items()},
        "warnings": warnings,
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    fname = f"{prefix}manifest.json"
    manifest_path = row_dir / fname
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)
    file_sizes[fname] = manifest_path.stat().st_size

    return file_sizes, warnings


# ---------------------------------------------------------------------------
# Index writers
# ---------------------------------------------------------------------------


def build_index_row(
    row_id: int,
    data: Dict[str, Any],
    row_folder_rel: str,
) -> Dict[str, str]:
    """Build a single index row from extracted document data."""
    fields = data["fields"]
    metrics_dicts = data["metrics_dicts"]

    row_id_str = str(row_id)
    rec_files = [f"{row_id_str}__{f}" for f in _RECOMMENDED_FILES]

    index_row: Dict[str, str] = {col: "" for col in INDEX_COLUMNS}
    index_row["row_id"] = row_id_str
    index_row["doc_id"] = fields.get("doc_id", "")
    index_row["filename"] = fields.get("filename", "")
    index_row["relative_row_folder"] = row_folder_rel
    index_row["doc_type"] = fields.get("doc_type", "")
    index_row["doc_type_confidence"] = fields.get("doc_type_confidence", "")
    index_row["borrower"] = fields.get("borrower", "")
    index_row["relationship_name"] = fields.get("relationship_name", "")
    index_row["product_type"] = fields.get("product_type", "")
    index_row["request_type"] = fields.get("request_type", "")
    index_row["request_amount"] = fields.get("request_amount", "")
    index_row["collateral_summary"] = fields.get("collateral_summary", "")
    index_row["key_metrics_summary"] = key_metrics_summary(metrics_dicts)
    index_row["decision_summary"] = fields.get("decision_summary", "")
    index_row["exceptions_summary"] = fields.get("exceptions_summary", "")
    index_row["credit_team_summary"] = fields.get("credit_team_summary", "")
    index_row["recommended_files_to_open"] = ",".join(rec_files)
    index_row["created_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    return index_row


def write_index_csv(
    rows: List[Dict[str, str]], output_path: Path,
) -> None:
    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=INDEX_COLUMNS, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: sanitize_csv_cell(str(v)) for k, v in row.items()})


def write_index_pipe(
    rows: List[Dict[str, str]], output_path: Path,
) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("|".join(INDEX_COLUMNS) + "\n")
        for row in rows:
            values = [sanitize_pipe(str(row.get(c, ""))) for c in INDEX_COLUMNS]
            f.write("|".join(values) + "\n")


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------


def discover_json_outputs(input_dir: Path) -> List[Path]:
    return sorted(input_dir.rglob("*_v2.json"))


def discover_raw_documents(input_dir: Path, extensions: str) -> List[Path]:
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
        description="Build SharePoint Copilot package from Split-RAG engine outputs."
    )
    parser.add_argument(
        "--input", type=Path, required=True,
        help="Folder containing *_v2.json engine outputs (or raw documents).",
    )
    parser.add_argument(
        "--output", type=Path, required=True,
        help="Output root folder (synced to SharePoint).",
    )
    parser.add_argument(
        "--rules", type=Path, default=None,
        help="Path to rules.json (default: engine dir rules.json).",
    )
    parser.add_argument("--section_chars", type=int, default=650)
    parser.add_argument("--top_section_chars", type=int, default=1200)
    parser.add_argument("--max_sections", type=int, default=20)
    parser.add_argument("--early_pages", type=int, default=3)
    parser.add_argument("--top_sections", type=int, default=5)
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
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("sharepoint_copilot_package")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(
        output_dir / "build_sharepoint_copilot_package.log", encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    logger = setup_logging(args.output)

    logger.info("=== SharePoint Copilot Package Build Start ===")
    logger.info("Input:  %s", args.input)
    logger.info("Output: %s", args.output)

    if not run_self_check(logger):
        logger.error("Engine path validation FAILED. Aborting.")
        return 1

    start = time.time()
    args.output.mkdir(parents=True, exist_ok=True)
    all_warnings: List[str] = []

    # Discover ContextGraph JSONs
    json_files = discover_json_outputs(args.input)

    if not json_files:
        logger.warning("No *_v2.json files found in %s.", args.input)
        raw_docs = discover_raw_documents(args.input, args.extensions)
        if raw_docs:
            logger.info(
                "Found %d raw documents. Running extraction engine...", len(raw_docs),
            )
            try:
                config = split_extractor.load_config(_ENGINE_DIR / "config.json")
                engine_output = args.output / "_engine_output"
                engine_output.mkdir(parents=True, exist_ok=True)
                config.output_dir = engine_output
                config.quarantine_dir = args.output / "_quarantine"
                config.quarantine_dir.mkdir(parents=True, exist_ok=True)

                rules_path = args.rules or _RULES_PATH
                rules = json.loads(rules_path.read_text(encoding="utf-8"))
                ext_logger = split_extractor.setup_logging(args.output / "_engine_logs")

                for doc_path in raw_docs:
                    try:
                        success = split_extractor.process_file(
                            doc_path, config, rules, ext_logger,
                        )
                        if not success:
                            all_warnings.append(f"Engine failed: {doc_path.name}")
                    except Exception as exc:
                        msg = f"Engine error on {doc_path.name}: {exc}"
                        logger.error(msg)
                        all_warnings.append(msg)
                        if args.fail_fast:
                            return 1

                json_files = discover_json_outputs(engine_output)
            except Exception as exc:
                logger.error("Could not run extraction engine: %s", exc)
                all_warnings.append(f"Engine init failed: {exc}")
        else:
            logger.warning("No raw documents found either.")

    if not json_files:
        logger.info("No documents to process. Writing empty index files.")
        write_index_csv([], args.output / "copilot_ingest_index.csv")
        write_index_pipe([], args.output / "copilot_ingest_index.txt")
        logger.info("Empty-run output written.")
        return 0

    logger.info("Processing %d ContextGraph JSON files...", len(json_files))

    index_rows: List[Dict[str, str]] = []

    for i, jf in enumerate(json_files, start=1):
        row_id = i
        logger.info("[%d/%d] %s", i, len(json_files), jf.name)

        try:
            _ensure_engine_imports()
            with open(jf, "r", encoding="utf-8") as f:
                raw = json.load(f)
            graph = schema.ContextGraph(**raw)
        except Exception as exc:
            msg = f"Failed to load {jf.name}: {exc}"
            logger.error(msg)
            all_warnings.append(msg)
            if args.fail_fast:
                return 1
            continue

        try:
            data = extract_document_data(graph, args.early_pages)
            file_sizes, row_warnings = write_row_package(
                row_id, data, args.output, args,
            )
            all_warnings.extend(row_warnings)

            row_folder_rel = f"rows/{row_id}"
            index_row = build_index_row(row_id, data, row_folder_rel)
            index_row["warnings"] = "; ".join(row_warnings)
            index_rows.append(index_row)

            logger.info(
                "  -> %d files, %d bytes total",
                len(file_sizes),
                sum(file_sizes.values()),
            )
        except Exception as exc:
            msg = f"Error building package for {jf.name}: {exc}"
            logger.error(msg)
            all_warnings.append(msg)
            if args.fail_fast:
                return 1

    # Write global index files
    csv_path = args.output / "copilot_ingest_index.csv"
    txt_path = args.output / "copilot_ingest_index.txt"

    write_index_csv(index_rows, csv_path)
    logger.info("Wrote %s (%d rows)", csv_path, len(index_rows))

    write_index_pipe(index_rows, txt_path)
    logger.info("Wrote %s (%d rows)", txt_path, len(index_rows))

    elapsed = time.time() - start
    logger.info(
        "=== SharePoint Copilot Package Build Complete: %d documents in %.1fs ===",
        len(index_rows),
        elapsed,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
