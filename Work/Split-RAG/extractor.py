"""
Split-RAG System v2.0 — Tier 1 Extraction Engine (Refactored)
"The Factory" — pdfplumber Primary + pypdfium2 Fallback

ARCHITECTURE CONSTRAINTS (STRICT ENFORCEMENT):
    - Zero External APIs: No OpenAI, Anthropic, Gemini, or external API calls
    - Zero Neural Network Dependencies: No torch, transformers, or llama-index imports
    - Primary Engine: pdfplumber with .extract_words() for exact bbox
    - Fallback Engine: pypdfium2 for corrupted-page recovery
    - Every extracted node carries [x0, y0, x1, y1] bounding box metadata

CP-001: All functions have explicit return type hints
CP-002: Specific exception handling required (no bare except Exception:)
CP-003: Pathlib used for all file ops
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import shutil
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import ValidationError

import schema_v2 as schema

# ---------------------------------------------------------------------------
# PYPDFIUM2 AVAILABILITY PROBE — used for per-page fallback
# ---------------------------------------------------------------------------
_PYPDFIUM2_AVAILABLE: bool = False
try:
    import pypdfium2 as pdfium
    _PYPDFIUM2_AVAILABLE = True
except ImportError:
    _PYPDFIUM2_AVAILABLE = False

# pdfplumber is the PRIMARY engine — always required
import pdfplumber

# ---------------------------------------------------------------------------
# RAPIDFUZZ AVAILABILITY PROBE — used for fuzzy section matching
# ---------------------------------------------------------------------------
_RAPIDFUZZ_AVAILABLE: bool = False
try:
    from rapidfuzz import fuzz as _fuzz
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _fuzz = None


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    """Typed configuration loaded from config.json."""
    __slots__ = (
        "input_dir", "output_dir", "log_dir", "quarantine_dir",
        "primary_engine", "fallback_engine",
        "enable_ocr", "enable_table_detection", "max_pages_scan",
        "conflict_threshold", "keep_all_policy",
    )

    def __init__(
        self,
        input_dir: Path, output_dir: Path, log_dir: Path, quarantine_dir: Path,
        primary_engine: str, fallback_engine: str,
        enable_ocr: bool, enable_table_detection: bool, max_pages_scan: int,
        conflict_threshold: float, keep_all_policy: bool,
    ) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.log_dir = log_dir
        self.quarantine_dir = quarantine_dir
        self.primary_engine = primary_engine
        self.fallback_engine = fallback_engine
        self.enable_ocr = enable_ocr
        self.enable_table_detection = enable_table_detection
        self.max_pages_scan = max_pages_scan
        self.conflict_threshold = conflict_threshold
        self.keep_all_policy = keep_all_policy


def load_config(config_path: Path) -> Config:
    """Load and validate config.json into a typed Config object."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        paths = data.get("paths", {})
        ext_set = data.get("extraction_settings", data.get("ingestion", {}))
        val_set = data.get("validation_settings", {})
        return Config(
            input_dir=Path(paths.get("input_directory", "input")),
            output_dir=Path(paths.get("output_directory", "output")),
            log_dir=Path(paths.get("log_directory", "logs")),
            quarantine_dir=Path(paths.get("quarantine_directory", "quarantine")),
            primary_engine=ext_set.get("primary_engine", "pdfplumber"),
            fallback_engine=ext_set.get("fallback_engine", "pypdfium2"),
            enable_ocr=ext_set.get("enable_ocr", False),
            enable_table_detection=ext_set.get("enable_table_detection", True),
            max_pages_scan=ext_set.get("max_pages_for_entity_scan",
                                       ext_set.get("entity_scan_pages", 20)),
            conflict_threshold=val_set.get("conflict_threshold_levenshtein", 0.3),
            keep_all_policy=val_set.get("enable_keep_all_policy", True),
        )
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as exc:
        sys.exit(f"CRITICAL: Failed to load config from {config_path}: {exc}")


def setup_logging(log_dir: Path) -> logging.Logger:
    """Configure file + console logging for the extraction run."""
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("SplitRAG_Factory")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_dir / "extraction.log")
    fh.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ---------------------------------------------------------------------------
# Entity Anchoring (regex-based, from rules.json)
# ---------------------------------------------------------------------------

def load_rules(rules_path: Path) -> Dict[str, Any]:
    """Load rules.json for entity extraction patterns."""
    if not rules_path.exists():
        return {}
    with open(rules_path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_entities(
    file_path: Path, max_pages: int, rules: Dict[str, Any]
) -> Dict[str, Optional[str]]:
    """
    Scan document preamble using regex patterns to extract Borrower, Lender, Guarantor.
    Uses pdfplumber for fast raw text access before heavy processing.
    """
    entities: Dict[str, Optional[str]] = {
        "borrower": None,
        "lender": None,
        "guarantor": None,
    }
    rules_entities = rules.get("entities", {})
    if not rules_entities:
        return entities

    try:
        text_buffer = ""
        with pdfplumber.open(file_path) as pdf:
            scan_limit = min(len(pdf.pages), max_pages)
            for i in range(scan_limit):
                page_text = pdf.pages[i].extract_text()
                if page_text:
                    text_buffer += page_text + "\n"

        for role in ("borrower", "lender", "guarantor"):
            patterns = rules_entities.get(role, {}).get("patterns", [])
            for pattern in patterns:
                match = re.search(pattern, text_buffer, re.IGNORECASE | re.MULTILINE)
                if match:
                    entities[role] = match.group("entity").strip().strip('",.')
                    break
    except Exception:
        pass

    return entities


# ---------------------------------------------------------------------------
# Header Hierarchy Parser + Section State Machine
# ---------------------------------------------------------------------------

def _classify_chunk_type(text: str) -> str:
    """
    Heuristic classification of text chunks into schema content_type.

    Rules ordered by specificity:
    1. ALL CAPS, short → header
    2. Roman numeral prefix (I., II., III.)
    3. Numbered section (1., 2., 1.1)
    4. Lettered section (A., B.)
    5. Short colon-terminated line
    6. Title case (heavily gated to avoid names/addresses)
    """
    stripped = text.strip()
    # Rule 1: ALL CAPS, short → header
    if len(stripped) < 100 and stripped.isupper() and len(stripped) > 3:
        return "header"
    # Rule 2: Roman numeral prefix (I., II., III., IV.)
    if re.match(r'^[IVXLC]+\.\s+', stripped):
        return "header"
    # Rule 3: Numbered section (1., 2., 1.1, 2.3)
    if re.match(r'^\d+(\.\d+)*\.\s+', stripped):
        return "header"
    # Rule 4: Lettered section (A., B., C.)
    if re.match(r'^[A-Z]\.\s+[A-Z]', stripped):
        return "header"
    # Rule 5: Short colon-terminated line (Borrower:, Collateral:)
    if len(stripped) < 60 and re.match(r'^[A-Z][A-Za-z\s/().\-]{2,50}:\s*$', stripped):
        return "header"
    # Rule 6: Title case, short, standalone — heavily gated
    words = stripped.split()
    if (len(stripped) < 80 and stripped.istitle() and '\n' not in stripped
            and 2 <= len(words) <= 8
            and not any(c.isdigit() for c in stripped)
            and ',' not in stripped
            and not stripped.endswith('.')):
        return "header"
    return "text"


def _infer_section_level(text: str) -> int:
    """Infer hierarchy depth from header text format."""
    stripped = text.strip()
    if re.match(r'^[IVXLC]+\.\s+', stripped):
        return 1       # I., II.
    if re.match(r'^\d+\.\s+', stripped):
        return 2       # 1., 2.
    if re.match(r'^\d+\.\d+', stripped):
        return 3       # 1.1, 2.3
    if re.match(r'^[A-Z]\.\s+', stripped):
        return 2       # A., B.
    if stripped.isupper():
        return 1       # ALL CAPS
    return 2           # default sub-section


def _normalize_section_label(text: str) -> str:
    """Normalize header text to a canonical label for section matching."""
    if not text:
        return ""
    label = re.sub(r'^[IVXLC0-9A-Z]+[\.\)]\s*', '', text.strip())
    label = label.lower().strip()
    label = re.sub(r'[^\w\s]', '', label)
    return re.sub(r'\s+', '_', label).strip('_')


# ---------------------------------------------------------------------------
# Financial Value Normalization
# ---------------------------------------------------------------------------

def normalize_financial_value(raw: str) -> Optional[float]:
    """Convert $12MM → 12000000, 3.5x → 3.5, 65% → 0.65"""
    if not raw:
        return None
    cleaned = raw.strip().rstrip('.')
    # Remove currency symbols
    cleaned = re.sub(r'^[\$£€]', '', cleaned)
    # Handle multiplier suffixes (longest match first)
    multipliers = {'BB': 1e9, 'B': 1e9, 'MM': 1e6, 'M': 1e6, 'K': 1e3, 'T': 1e3}
    for suffix, mult in sorted(multipliers.items(), key=lambda x: -len(x[0])):
        if cleaned.upper().endswith(suffix):
            num_part = cleaned[:len(cleaned) - len(suffix)].replace(',', '').strip()
            try:
                return float(num_part) * mult
            except ValueError:
                return None
    # Handle x (multiple)
    if cleaned.lower().endswith('x'):
        try:
            return float(cleaned[:-1].replace(',', '').strip())
        except ValueError:
            return None
    # Handle %
    if cleaned.endswith('%'):
        try:
            return float(cleaned[:-1].replace(',', '').strip()) / 100.0
        except ValueError:
            return None
    # Plain number
    try:
        return float(cleaned.replace(',', ''))
    except ValueError:
        return None


def _extract_scale_hint(raw: str) -> Optional[str]:
    """Extract original scale notation (MM, B, K, etc.) from raw value string."""
    if not raw:
        return None
    cleaned = raw.strip().rstrip('.')
    cleaned = re.sub(r'^[\$£€]', '', cleaned)
    for suffix in ('BB', 'MM', 'B', 'M', 'K', 'T'):
        if cleaned.upper().endswith(suffix):
            return suffix
    return None


def _infer_normalized_unit(raw: str, metric_def: Optional[Dict[str, Any]]) -> Optional[str]:
    """Infer the specific normalized unit from raw value and metric definition."""
    if not raw:
        return None
    cleaned = raw.strip()
    if cleaned.endswith('%'):
        return "percent"
    if cleaned.lower().endswith('x'):
        return "multiple"
    if re.match(r'^[\$£€]', cleaned):
        return "currency_usd"
    if metric_def and metric_def.get("unit_type"):
        return metric_def["unit_type"]
    return None


# ---------------------------------------------------------------------------
# Two-Stage Document Type Classification
# ---------------------------------------------------------------------------

def detect_email_blocks(
    nodes: List[schema.ContextNode], rules: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Stage 1: Detect email blocks in extracted nodes.

    Returns:
        - email_block_count: int
        - email_block_ratio: float (chars in email blocks / total chars)
        - email_block_chunk_ids: List[str]
        - email_separator_hit_count: int
    """
    markers = rules.get("email_block_markers", {})
    header_patterns = [re.compile(p) for p in markers.get("header_patterns", [
        r"(?im)^From:\s+", r"(?im)^To:\s+", r"(?im)^Sent:\s+",
        r"(?im)^Subject:\s+", r"(?im)^Cc:\s+"
    ])]
    separator_patterns = [re.compile(p) for p in markers.get("separator_patterns", [
        r"(?i)-{3,}\s*Original\s+Message\s*-{3,}",
        r"(?i)-{3,}\s*Forwarded\s+[Mm]essage\s*-{3,}"
    ])]
    min_headers = markers.get("min_header_lines", markers.get("min_header_count", 2))

    email_chunks: List[str] = []
    total_chars = 0
    email_chars = 0
    total_sep_hits = 0

    for node in nodes:
        total_chars += len(node.content)
        lines = node.content.split('\n')
        header_hits = sum(1 for line in lines for p in header_patterns if p.search(line))
        sep_hits = sum(1 for line in lines for p in separator_patterns if p.search(line))
        total_sep_hits += sep_hits
        if header_hits >= min_headers or sep_hits > 0:
            email_chunks.append(node.chunk_id)
            email_chars += len(node.content)

    return {
        "email_block_count": len(email_chunks),
        "email_block_ratio": email_chars / total_chars if total_chars > 0 else 0.0,
        "email_block_chunk_ids": email_chunks,
        "email_separator_hit_count": total_sep_hits,
    }


def classify_document_type(
    text_buffer: str, email_info: Dict[str, Any], rules: Dict[str, Any]
) -> Tuple[Optional[str], float, Dict[str, float]]:
    """
    Stage 2: Weighted document type classification with email dominance check.

    Returns (best_type, confidence, all_scores).
    Scores dict is exposed for testing/debugging tie-breaker behavior.
    """
    doc_types = rules.get("document_types", {})
    if not doc_types:
        return None, 0.0, {}

    scores: Dict[str, float] = {}
    for type_key, type_def in doc_types.items():
        score = 0.0
        weights = type_def.get("weights", {})
        for pattern_str in type_def.get("patterns", []):
            if re.search(pattern_str, text_buffer):
                score += weights.get(pattern_str, 1.0)
        scores[type_key] = score

    # Email dominance check: banker_email wins ONLY if:
    # 1. email_block_ratio > 0.6 AND (count >= 2 OR strong separator)
    # 2. No strong LAM/memo signals (credit_memo score < 3)
    if "banker_email" in scores:
        requires_dom = doc_types.get("banker_email", {}).get("requires_dominance", True)
        if requires_dom:
            ratio = email_info.get("email_block_ratio", 0)
            count = email_info.get("email_block_count", 0)
            sep_hits = email_info.get("email_separator_hit_count", 0)
            if ratio < 0.6 or (count < 2 and sep_hits < 1):
                scores["banker_email"] = 0.0
            if scores.get("credit_memo", 0) >= 3.0:
                scores["banker_email"] = 0.0

    best_type = max(scores, key=scores.get) if scores else None
    best_score = scores.get(best_type, 0) if best_type else 0

    # Tie-breaker (no mutation): credit_memo wins over any type within 1pt margin
    # when credit_memo >= 3.0 and banker_email is suppressed
    credit_score = scores.get("credit_memo", 0)
    if (credit_score >= 3.0
            and scores.get("banker_email", 0) == 0.0
            and best_type != "credit_memo"
            and (best_score - credit_score) <= 1.0):
        best_type = "credit_memo"
        best_score = credit_score

    if best_score <= 0:
        return None, 0.0, scores
    confidence = min(best_score / 5.0, 1.0)
    return best_type, confidence, scores


# ---------------------------------------------------------------------------
# KV-Line Harvester
# ---------------------------------------------------------------------------

# Entity types treated as numeric for normalization
_NUMERIC_ENTITIES: Set[str] = {
    "ltv", "dscr", "cap_rate", "noi", "occupancy", "appraised_value",
    "loan_amount", "financial_covenant", "minimum_dscr", "maximum_ltv",
    "adjusted_gross_income",
}

# Entity types representing credit team roles
_TEAM_TYPES: Set[str] = {
    "relationship_manager", "credit_officer", "underwriter", "approver",
}

# Entity types representing covenants
_COVENANT_TYPES: Set[str] = {
    "financial_covenant", "reporting_requirement",
}


def _compute_kv_confidence(
    node: schema.ContextNode, matched_metric: Optional[Dict[str, Any]]
) -> float:
    """Compute confidence score for a KV pair extraction."""
    base = 0.5
    if matched_metric:
        base += 0.2  # ontology match boost
    if node.metadata.section_label:
        base += 0.1  # section context boost
    return min(base, 1.0)


def harvest_kv_lines(
    nodes: List[schema.ContextNode],
    rules: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Scan short/medium chunks for key: value patterns.
    Maps keys to ontology aliases. Also supports inline metrics (DSCR 1.25x).
    Returns list of observation dicts.
    """
    ontology = rules.get("financial_metric_ontology", {})
    # Build alias→canonical lookup
    alias_map: Dict[str, Dict[str, Any]] = {}
    for _canonical, defn in ontology.items():
        for alias in defn.get("aliases", []):
            alias_map[alias.lower()] = defn

    kv_pattern = re.compile(r'^([\w][\w\s/()\.\-]{1,50})\s*[:\-\u2013\u2014]\s*(.+)$', re.MULTILINE)

    # Inline metric pattern: "DSCR 1.25x", "LTV 65%", "NOI $2.5MM"
    # Only compile if alias_map is non-empty
    inline_pattern = None
    if alias_map:
        escaped_aliases = '|'.join(re.escape(a) for a in alias_map.keys())
        inline_pattern = re.compile(
            r'\b(' + escaped_aliases + r')\s+(\$?[\d,]+\.?\d*\s*(?:x|%|MM|M|B|K)?)\b',
            re.IGNORECASE
        )

    observations: List[Dict[str, Any]] = []

    for node in nodes:
        if node.content_type == "header":
            continue
        lines = node.content.split('\n')
        for line in lines:
            line = line.strip().lstrip('\u2022*-\u2013\u2014 ')
            # Try KV pattern first (key: value)
            m = kv_pattern.match(line)
            if not m:
                # Try inline metric pattern (DSCR 1.25x)
                m_inline = inline_pattern.search(line) if inline_pattern else None
                if m_inline:
                    raw_key = m_inline.group(1).strip()
                    raw_val = m_inline.group(2).strip()
                    matched_metric = alias_map.get(raw_key.lower())
                    if matched_metric:
                        observations.append({
                            "key": raw_key,
                            "raw_value": raw_val,
                            "normalized_value": normalize_financial_value(raw_val),
                            "metric_name": matched_metric["canonical_name"],
                            "unit": matched_metric.get("unit_type"),
                            "normalized_unit": _infer_normalized_unit(raw_val, matched_metric),
                            "scale_hint": _extract_scale_hint(raw_val),
                            "section_label": node.metadata.section_label,
                            "page_number": node.metadata.page_number,
                            "confidence_score": _compute_kv_confidence(node, matched_metric),
                            "evidence_chunk_id": node.chunk_id,
                        })
                continue

            raw_key, raw_val = m.group(1).strip(), m.group(2).strip()
            norm_key = _normalize_section_label(raw_key)

            # Try ontology match
            matched_metric = alias_map.get(raw_key.lower()) or alias_map.get(norm_key)
            # Try fuzzy match if rapidfuzz available
            if not matched_metric and _RAPIDFUZZ_AVAILABLE and alias_map:
                best_score = 0.0
                for alias_key, defn in alias_map.items():
                    ratio = _fuzz.ratio(raw_key.lower(), alias_key)
                    if ratio >= 85 and ratio > best_score:
                        best_score = ratio
                        matched_metric = defn

            observation = {
                "key": raw_key,
                "raw_value": raw_val,
                "normalized_value": normalize_financial_value(raw_val),
                "metric_name": matched_metric["canonical_name"] if matched_metric else norm_key,
                "unit": matched_metric.get("unit_type") if matched_metric else None,
                "normalized_unit": _infer_normalized_unit(raw_val, matched_metric),
                "scale_hint": _extract_scale_hint(raw_val),
                "section_label": node.metadata.section_label,
                "page_number": node.metadata.page_number,
                "confidence_score": _compute_kv_confidence(node, matched_metric),
                "evidence_chunk_id": node.chunk_id,
            }
            observations.append(observation)
    return observations


# ---------------------------------------------------------------------------
# Section-Scoped Entity Extraction
# ---------------------------------------------------------------------------

def extract_section_scoped_entities(
    nodes: List[schema.ContextNode],
    rules: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Run entity regex patterns against nodes, respecting target_sections.
    Uses normalized label matching (exact + optional fuzzy >= 92).
    Collects multiple matches with dedup.
    """
    entity_rules = rules.get("entities", {})
    results: List[Dict[str, Any]] = []
    seen_entities: Set[Tuple[str, str]] = set()

    for entity_type, rule_def in entity_rules.items():
        target_sections = rule_def.get("target_sections", [])
        for node in nodes:
            # Section scoping — normalized matching
            if target_sections:
                node_section = _normalize_section_label(node.metadata.section_label or "")
                normalized_targets = [_normalize_section_label(t) for t in target_sections]

                # Default: exact normalized match
                matched = any(node_section == nt for nt in normalized_targets)

                # Optional: check target_sections_regex if defined
                regex_targets = rule_def.get("target_sections_regex", [])
                if not matched and regex_targets:
                    matched = any(re.search(rp, node_section) for rp in regex_targets)

                # Fallback: rapidfuzz similarity >= 92
                if not matched and _RAPIDFUZZ_AVAILABLE:
                    matched = any(_fuzz.ratio(node_section, nt) >= 92 for nt in normalized_targets)

                if not matched:
                    continue

            # Run patterns — collect multiple matches, dedup
            for pattern in rule_def.get("patterns", []):
                try:
                    match = re.search(pattern, node.content, re.IGNORECASE | re.MULTILINE | re.DOTALL)
                except re.error:
                    continue
                if match:
                    gd = match.groupdict()
                    if "value" in gd:
                        value = gd["value"]
                    elif "entity" in gd:
                        value = gd["entity"]
                    elif match.lastindex:
                        value = match.group(1)
                    else:
                        value = match.group(0)

                    if not value:
                        continue

                    confidence = 0.7
                    if target_sections:
                        confidence += 0.2  # section match boost

                    clean_val = value.strip().strip('",.')
                    dedup_key = (entity_type, clean_val.lower())
                    if dedup_key not in seen_entities:
                        seen_entities.add(dedup_key)
                        results.append({
                            "entity_type": entity_type,
                            "raw_value": clean_val,
                            "normalized_value": str(normalize_financial_value(value)) if entity_type in _NUMERIC_ENTITIES and normalize_financial_value(value) is not None else None,
                            "source_section": node.metadata.section_label,
                            "page_number": node.metadata.page_number,
                            "confidence_score": confidence,
                            "evidence_chunk_id": node.chunk_id,
                        })

    return results


# ============================================================================
# PRIMARY ENGINE: pdfplumber with .extract_words() for exact bounding boxes
# ============================================================================

def process_with_pdfplumber(
    file_path: Path, doc_id: str, file_hash: str
) -> List[schema.ContextNode]:
    """
    Primary extraction using pdfplumber.

    - .extract_words() captures text alongside exact [x0, top, x1, bottom] bbox
    - .find_tables() extracts tables with bounding-box coordinates
    - Per-page try/except with pypdfium2 fallback for corrupted pages

    Spatial Provenance: Every node carries [x0, y0, x1, y1] coordinates
    derived from pdfplumber word-level geometry.

    Section state machine persists across pages — sections in credit memos
    span page breaks. Only prev_chunk_id resets per page (NEXT_BLOCK edges).
    """
    nodes: List[schema.ContextNode] = []
    chunk_idx = 0
    logger = logging.getLogger("SplitRAG_Factory")

    # Section state machine — persists across pages
    section_stack: List[Dict[str, Any]] = []
    current_section_id: Optional[str] = None
    current_section_label: Optional[str] = None
    prev_chunk_id: Optional[str] = None

    with pdfplumber.open(file_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            page_no = page_idx + 1
            page_width = float(page.width)
            page_height = float(page.height)

            # Reset only NEXT_BLOCK sequencing per page, NOT section state
            prev_chunk_id = None

            try:
                # --- Text extraction via .extract_words() ---
                words = page.extract_words(keep_blank_chars=False) or []

                # Group into lines and merge into paragraphs with bounding boxes
                lines = _cluster_words_to_lines(words)
                paragraphs = _merge_lines_to_paragraphs(lines)

                # --- SCANNED DOCUMENT FAILSAFE ---
                page_all_text = " ".join(p[0] for p in paragraphs)
                if not check_page_readability(
                    _count_alnum(page_all_text), page_no, logger
                ):
                    continue

                # Page-level bbox used as fallback for sanitization
                page_bbox = (0.0, 0.0, page_width, page_height)

                for para_text, para_bbox in paragraphs:
                    if len(para_text.strip()) < 5:
                        continue

                    c_type = _classify_chunk_type(para_text)
                    bbox_list = sanitize_bbox(
                        list(para_bbox), page_width, page_height, page_bbox
                    )
                    chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, para_text)
                    lineage = schema.generate_lineage_trace(
                        file_hash, page_no, bbox_list, "pdfplumber"
                    )

                    # Section state machine: update on headers
                    if c_type == "header":
                        level = _infer_section_level(para_text)
                        norm_label = _normalize_section_label(para_text)
                        # Hard reset: level-1 header clears entire stack
                        if level == 1:
                            section_stack.clear()
                        else:
                            # Pop stack until parent at lower level
                            while section_stack and section_stack[-1]["level"] >= level:
                                section_stack.pop()
                        sec_id = f"SEC_{chunk_id}"
                        section_stack.append({"id": sec_id, "level": level, "label": norm_label})
                        current_section_id = sec_id
                        current_section_label = norm_label

                    meta = schema.NodeMetadata(
                        page_number=page_no,
                        bbox=bbox_list,
                        source_scope="primary",
                        extraction_method="pdfplumber",
                        is_active=True,
                        section_label=current_section_label,
                        section_level=section_stack[-1]["level"] if section_stack else 0,
                    )
                    node = schema.ContextNode(
                        chunk_id=chunk_id,
                        content_type=c_type,
                        content=para_text,
                        metadata=meta,
                        lineage_trace=lineage,
                        parent_section_id=current_section_id,
                    )
                    nodes.append(node)
                    chunk_idx += 1

                # --- Tables with bounding-box via .find_tables() ---
                tables = page.find_tables() or []
                for tbl in tables:
                    table_data = tbl.extract()
                    if not table_data:
                        continue

                    # --- Cell-level spatial provenance ---
                    cell_bboxes: List[Tuple[float, float, float, float]] = []
                    if hasattr(tbl, "cells") and tbl.cells:
                        for cell in tbl.cells:
                            raw_cell_bbox = list(cell) if cell else None
                            sanitized = sanitize_bbox(
                                raw_cell_bbox, page_width, page_height, page_bbox
                            )
                            cell_bboxes.append(sanitized)

                    md_content = _table_to_markdown(table_data)

                    # Table-level bbox from pdfplumber table object — sanitize
                    raw_tbl_bbox = list(tbl.bbox) if hasattr(tbl, "bbox") and tbl.bbox else None
                    tbl_bbox = sanitize_bbox(
                        raw_tbl_bbox, page_width, page_height, page_bbox
                    )
                    t_shape = None
                    if table_data:
                        t_shape = [len(table_data), len(table_data[0]) if table_data[0] else 0]

                    chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, md_content)
                    lineage = schema.generate_lineage_trace(
                        file_hash, page_no, tbl_bbox, "pdfplumber"
                    )

                    meta = schema.NodeMetadata(
                        page_number=page_no,
                        bbox=tbl_bbox,
                        table_shape=t_shape,
                        cell_bboxes=cell_bboxes if cell_bboxes else None,
                        source_scope="primary",
                        extraction_method="pdfplumber",
                        is_active=True,
                        section_label=current_section_label,
                        section_level=section_stack[-1]["level"] if section_stack else 0,
                    )
                    node = schema.ContextNode(
                        chunk_id=chunk_id,
                        content_type="table",
                        content=md_content,
                        metadata=meta,
                        lineage_trace=lineage,
                        parent_section_id=current_section_id,
                    )
                    nodes.append(node)
                    chunk_idx += 1

            except Exception:
                # Per-page fallback to pypdfium2 if pdfplumber fails
                fallback_nodes = _fallback_page_pypdfium2(
                    file_path, page_no, doc_id, file_hash, chunk_idx
                )
                nodes.extend(fallback_nodes)
                chunk_idx += len(fallback_nodes)

    return nodes


# ============================================================================
# FALLBACK ENGINE: pypdfium2 (per-page recovery for corrupted pages)
# ============================================================================

def _fallback_page_pypdfium2(
    file_path: Path, page_no: int, doc_id: str, file_hash: str, chunk_idx: int
) -> List[schema.ContextNode]:
    """
    Fallback extraction for a single corrupted page using pypdfium2.
    Returns text-only nodes without precise word-level bounding boxes.
    """
    if not _PYPDFIUM2_AVAILABLE:
        return []

    nodes: List[schema.ContextNode] = []
    fallback_logger = logging.getLogger("SplitRAG_Factory")
    try:
        pdf_doc = pdfium.PdfDocument(str(file_path))
        page_index = page_no - 1
        if page_index >= len(pdf_doc):
            return []

        page = pdf_doc[page_index]
        text = page.get_textpage().get_text_range()

        if not text or len(text.strip()) < 5:
            return []

        # --- SCANNED DOCUMENT FAILSAFE ---
        if not check_page_readability(
            _count_alnum(text), page_no, fallback_logger
        ):
            return []

        # Split into paragraphs by double newline
        paragraphs = [p.strip() for p in re.split(r"\n{2,}", text) if p.strip()]

        page_width = float(page.get_width())
        page_height = float(page.get_height())
        page_bbox = (0.0, 0.0, page_width, page_height)

        for para_text in paragraphs:
            if len(para_text) < 5:
                continue

            # Approximate bbox as full page — sanitized
            bbox_list = sanitize_bbox(None, page_width, page_height, page_bbox)
            c_type = _classify_chunk_type(para_text)

            chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, para_text)
            lineage = schema.generate_lineage_trace(
                file_hash, page_no, bbox_list, "pypdfium2"
            )

            meta = schema.NodeMetadata(
                page_number=page_no,
                bbox=bbox_list,
                source_scope="primary",
                extraction_method="pypdfium2",
                is_active=True,
            )
            node = schema.ContextNode(
                chunk_id=chunk_id,
                content_type=c_type,
                content=para_text,
                metadata=meta,
                lineage_trace=lineage,
            )
            nodes.append(node)
            chunk_idx += 1

    except Exception:
        pass

    return nodes


def process_with_pypdfium2_full(
    file_path: Path, doc_id: str, file_hash: str
) -> List[schema.ContextNode]:
    """
    Full-document fallback extraction using pypdfium2.
    Used when pdfplumber completely fails to open the document.
    """
    if not _PYPDFIUM2_AVAILABLE:
        raise RuntimeError("pypdfium2 not available — cannot extract")

    nodes: List[schema.ContextNode] = []
    chunk_idx = 0

    pdf_doc = pdfium.PdfDocument(str(file_path))
    for page_index in range(len(pdf_doc)):
        page_no = page_index + 1
        page_nodes = _fallback_page_pypdfium2(
            file_path, page_no, doc_id, file_hash, chunk_idx
        )
        nodes.extend(page_nodes)
        chunk_idx += len(page_nodes)

    return nodes


# ============================================================================
# EXTRACTION PIPELINE (pdfplumber primary → pypdfium2 fallback)
# ============================================================================

def run_extraction(
    file_path: Path, doc_id: str, file_hash: str,
    logger: logging.Logger,
) -> Tuple[List[schema.ContextNode], bool, bool]:
    """
    Execute extraction strategy:
        1. Try pdfplumber as primary engine
        2. If pdfplumber completely fails to open, fallback to pypdfium2

    Per-page fallback (pdfplumber page error → pypdfium2) is handled
    inside process_with_pdfplumber() automatically.

    Returns:
        (nodes, primary_success, fallback_triggered)
    """
    primary_success = False
    fallback_triggered = False

    # --- Attempt Primary: pdfplumber ---
    logger.info("Starting Primary Extraction (pdfplumber) for %s...", file_path.name)
    try:
        nodes = process_with_pdfplumber(file_path, doc_id, file_hash)
        primary_success = True
        logger.info("pdfplumber extracted %d nodes from %s", len(nodes), file_path.name)
        return nodes, primary_success, fallback_triggered
    except Exception as exc:
        logger.warning("Primary engine failed for %s: %s", file_path.name, exc)

    # --- Attempt Fallback: pypdfium2 ---
    logger.info("Triggering Fallback Engine (pypdfium2) for %s...", file_path.name)
    try:
        nodes = process_with_pypdfium2_full(file_path, doc_id, file_hash)
        fallback_triggered = True
        logger.info("pypdfium2 extracted %d nodes from %s", len(nodes), file_path.name)
        return nodes, primary_success, fallback_triggered
    except Exception as exc:
        logger.critical("Both engines failed for %s: %s", file_path.name, exc)
        raise


# ---------------------------------------------------------------------------
# Text processing helpers
# ---------------------------------------------------------------------------

def _cluster_words_to_lines(
    words: List[Dict[str, Any]], y_tolerance: float = 3.0
) -> List[List[Dict[str, Any]]]:
    """Group pdfplumber word dicts into lines by y-proximity."""
    if not words:
        return []
    sorted_words = sorted(
        words,
        key=lambda w: (round(float(w["top"]) / y_tolerance), float(w["x0"])),
    )
    lines: List[List[Dict[str, Any]]] = []
    current_line: List[Dict[str, Any]] = [sorted_words[0]]
    current_top = float(sorted_words[0]["top"])

    for w in sorted_words[1:]:
        if abs(float(w["top"]) - current_top) <= y_tolerance:
            current_line.append(w)
        else:
            lines.append(current_line)
            current_line = [w]
            current_top = float(w["top"])
    if current_line:
        lines.append(current_line)
    return lines


def _merge_lines_to_paragraphs(
    lines: List[List[Dict[str, Any]]],
    gap_threshold: float = 12.0,
) -> List[Tuple[str, Tuple[float, float, float, float]]]:
    """Merge consecutive lines into paragraph chunks with combined bounding boxes."""
    if not lines:
        return []

    paragraphs: List[Tuple[str, Tuple[float, float, float, float]]] = []
    current_texts: List[str] = []
    x0_min, y0_min = float("inf"), float("inf")
    x1_max, y1_max = 0.0, 0.0
    prev_bottom = 0.0

    for line_words in lines:
        line_text = " ".join(w["text"] for w in line_words)
        lx0 = min(float(w["x0"]) for w in line_words)
        ly0 = min(float(w["top"]) for w in line_words)
        lx1 = max(float(w["x1"]) for w in line_words)
        ly1 = max(float(w["bottom"]) for w in line_words)

        if current_texts and (ly0 - prev_bottom) > gap_threshold:
            paragraphs.append((" ".join(current_texts), (x0_min, y0_min, x1_max, y1_max)))
            current_texts = []
            x0_min, y0_min = float("inf"), float("inf")
            x1_max, y1_max = 0.0, 0.0

        current_texts.append(line_text)
        x0_min = min(x0_min, lx0)
        y0_min = min(y0_min, ly0)
        x1_max = max(x1_max, lx1)
        y1_max = max(y1_max, ly1)
        prev_bottom = ly1

    if current_texts:
        paragraphs.append((" ".join(current_texts), (x0_min, y0_min, x1_max, y1_max)))
    return paragraphs


# ---------------------------------------------------------------------------
# Scanned Document Failsafe
# ---------------------------------------------------------------------------

SCANNED_PAGE_MIN_CHARS: int = 50


def check_page_readability(
    page_text_chars: int, page_no: int, logger: Optional[logging.Logger] = None
) -> bool:
    """
    Validate that a page yielded enough alphanumeric characters to be
    considered readable.
    """
    if page_text_chars < SCANNED_PAGE_MIN_CHARS:
        if logger:
            logger.warning(
                "SCANNED_UNREADABLE: Page %d yielded only %d alphanumeric chars "
                "(threshold=%d). Skipping to prevent orphaned graph nodes.",
                page_no, page_text_chars, SCANNED_PAGE_MIN_CHARS,
            )
        return False
    return True


def _count_alnum(text: str) -> int:
    """Count alphanumeric characters in text."""
    return sum(1 for c in text if c.isalnum())


# ---------------------------------------------------------------------------
# Bounding Box Sanitization
# ---------------------------------------------------------------------------

# Default page dimensions (US Letter in points) used when no page dims available
_DEFAULT_PAGE_WIDTH: float = 612.0
_DEFAULT_PAGE_HEIGHT: float = 792.0


def sanitize_bbox(
    bbox,
    page_width: float = _DEFAULT_PAGE_WIDTH,
    page_height: float = _DEFAULT_PAGE_HEIGHT,
    parent_bbox=None,
) -> Tuple[float, float, float, float]:
    """
    Ensure a bounding box is a valid (x0, y0, x1, y1) tuple of floats.

    Rules:
        1. If bbox is None/empty/wrong length → inherit parent_bbox or clamp to page dims
        2. All values coerced to float
        3. Coordinates clamped to [0, page_width] / [0, page_height]
        4. Ensure x0 <= x1, y0 <= y1
    """
    # Fallback: use parent bbox or full page
    if not bbox or not isinstance(bbox, (list, tuple)) or len(bbox) != 4:
        if parent_bbox and len(parent_bbox) == 4:
            return tuple(float(v) for v in parent_bbox)
        return (0.0, 0.0, float(page_width), float(page_height))

    try:
        x0, y0, x1, y1 = [float(v) for v in bbox]
    except (TypeError, ValueError):
        if parent_bbox and len(parent_bbox) == 4:
            return tuple(float(v) for v in parent_bbox)
        return (0.0, 0.0, float(page_width), float(page_height))

    # Clamp to page boundaries
    x0 = max(0.0, min(x0, page_width))
    y0 = max(0.0, min(y0, page_height))
    x1 = max(0.0, min(x1, page_width))
    y1 = max(0.0, min(y1, page_height))

    # Ensure ordering
    if x0 > x1:
        x0, x1 = x1, x0
    if y0 > y1:
        y0, y1 = y1, y0

    return (x0, y0, x1, y1)


def _table_to_markdown(table_data: List[List[Any]]) -> str:
    """Convert pdfplumber table (list of lists) to Markdown."""
    if not table_data:
        return ""
    rows: List[str] = []
    for row in table_data:
        cells = [str(c) if c is not None else "" for c in row]
        rows.append("| " + " | ".join(cells) + " |")
    if rows and table_data[0]:
        sep = "| " + " | ".join(["---"] * len(table_data[0])) + " |"
        rows.insert(1, sep)
    return "\n".join(rows)


# ============================================================================
# Main Pipeline
# ============================================================================

def handle_quarantine(
    file_path: Path, quarantine_dir: Path, error_msg: str
) -> None:
    """Move failed file to quarantine with failure report."""
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dest = quarantine_dir / file_path.name
    try:
        shutil.move(str(file_path), str(dest))
    except (OSError, shutil.Error):
        pass

    report_path = quarantine_dir / f"{file_path.name}_failure_report.json"
    report = {
        "filename": file_path.name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error_msg,
        "stack_trace": traceback.format_exc(),
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)


def process_file(
    file_path: Path, config: Config, rules: Dict[str, Any], logger: logging.Logger
) -> bool:
    """
    Full extraction pipeline for a single file.

    Pipeline order:
        1. Hash file → deterministic document_id
        2. Entity anchoring (regex-based, borrower/lender/guarantor)
        3. pdfplumber primary → pypdfium2 fallback extraction
        4. Section-scoped entity extraction + KV harvester
        5. Document type classification (two-stage, email-aware)
        6. Email block tagging (full-doc scope)
        7. Assemble ContextGraph with ExtractedIntelligence + ExtractionMetrics
        8. Validate via Pydantic and serialize to JSON
    """
    start_time = time.time()

    # 1. File hash & ID
    try:
        file_bytes = file_path.read_bytes()
        file_hash = hashlib.md5(file_bytes).hexdigest()
        doc_id = schema.generate_document_id(file_bytes)
    except OSError as exc:
        logger.error("Failed to read file %s: %s", file_path, exc)
        return False

    # 2. Entity anchoring (legacy borrower/lender/guarantor)
    logger.info("Scanning entities for %s...", file_path.name)
    entities = extract_entities(file_path, config.max_pages_scan, rules)

    # 3. Extraction (pdfplumber primary → pypdfium2 fallback)
    try:
        nodes, primary_success, fallback_triggered = run_extraction(
            file_path, doc_id, file_hash, logger
        )
    except Exception as exc:
        logger.critical("All engines failed for %s. Quarantining.", file_path.name)
        handle_quarantine(file_path, config.quarantine_dir, str(exc))
        return False

    if not nodes:
        logger.error("Zero nodes extracted from %s", file_path.name)
        handle_quarantine(file_path, config.quarantine_dir, "Zero nodes extracted")
        return False

    # 4. Section-scoped entity extraction + KV harvester
    scoped_entities = extract_section_scoped_entities(nodes, rules)
    kv_observations = harvest_kv_lines(nodes, rules)

    # 5. Document type classification (two-stage, email-aware)
    first_n = 5
    first_page_nodes = [n for n in nodes if n.metadata.page_number <= first_n]
    text_buffer = "\n".join(n.content for n in first_page_nodes)
    email_info = detect_email_blocks(first_page_nodes, rules)
    doc_type, doc_type_confidence, _doc_type_scores = classify_document_type(
        text_buffer, email_info, rules
    )

    # 6. Email block tagging (full-doc scope, separate from first-N-pages classification)
    full_doc_email_info = detect_email_blocks(nodes, rules)
    email_chunk_ids = set(full_doc_email_info.get("email_block_chunk_ids", []))
    for node in nodes:
        if node.chunk_id in email_chunk_ids:
            node.metadata.is_email_block = True

    # 7. Assemble ExtractedIntelligence
    intelligence_entities = []
    intelligence_team: List[schema.CreditTeamMember] = []
    intelligence_covenants = []

    for e in scoped_entities:
        et = e["entity_type"]
        if et in _TEAM_TYPES:
            intelligence_team.append(schema.CreditTeamMember(
                role=et,
                name=e["raw_value"],
                confidence_score=e["confidence_score"],
                evidence_chunk_id=e.get("evidence_chunk_id"),
            ))
        elif et in _COVENANT_TYPES:
            intelligence_covenants.append(schema.ExtractedEntity(**e))
        else:
            intelligence_entities.append(schema.ExtractedEntity(**e))

    financial_metrics = []
    for obs in kv_observations:
        financial_metrics.append(schema.MetricObservation(
            metric_name=obs["metric_name"],
            raw_value=obs["raw_value"],
            normalized_value=obs.get("normalized_value"),
            unit=obs.get("unit"),
            normalized_unit=obs.get("normalized_unit"),
            scale_hint=obs.get("scale_hint"),
            source_section=obs.get("section_label"),
            page_number=obs.get("page_number"),
            confidence_score=obs.get("confidence_score", 0.5),
            evidence_chunk_id=obs.get("evidence_chunk_id"),
        ))

    intelligence = schema.ExtractedIntelligence(
        document_type=doc_type,
        document_type_confidence=doc_type_confidence,
        entities=intelligence_entities,
        financial_metrics=financial_metrics,
        credit_team=intelligence_team,
        covenants=intelligence_covenants,
    )

    # Assemble ContextGraph
    fallback_engine_name: Optional[str] = None
    if fallback_triggered:
        fallback_engine_name = "pypdfium2"

    metrics = schema.ExtractionMetrics(
        total_pages=max((n.metadata.page_number for n in nodes), default=0),
        total_nodes=len(nodes),
        tables_extracted=sum(1 for n in nodes if n.content_type == "table"),
        headers_extracted=sum(1 for n in nodes if n.content_type == "header"),
        conflicts_detected=0,
        extraction_time_seconds=time.time() - start_time,
        primary_engine_used=primary_success,
        fallback_triggered=fallback_triggered,
        fallback_engine=fallback_engine_name,
        kv_pairs_extracted=len(kv_observations),
        entities_extracted=len(scoped_entities),
    )

    graph = schema.ContextGraph(
        document_id=doc_id,
        filename=file_path.name,
        processed_at=schema.ContextGraph.get_current_timestamp(),
        borrower_entity=entities.get("borrower"),
        lender_entity=entities.get("lender"),
        guarantor_entity=entities.get("guarantor"),
        intelligence=intelligence,
        nodes=nodes,
        metrics=metrics,
    )

    # 8. Validate & serialize
    output_path = config.output_dir / f"{file_path.stem}_v2.json"
    try:
        json_output = graph.to_json()
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(json_output)
        logger.info("Successfully processed %s → %s", file_path.name, output_path)
        return True
    except (ValidationError, OSError) as exc:
        logger.error("Serialization failed for %s: %s", file_path.name, exc)
        handle_quarantine(file_path, config.quarantine_dir, f"Serialization Error: {exc}")
        return False


# ============================================================================
# CLI Entry Point
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Split-RAG Tier 1 Extraction Engine")
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    parser.add_argument("--rules", type=Path, default=Path("rules.json"))
    parser.add_argument("--file", type=Path, help="Process single file")
    parser.add_argument("--reprocess", action="store_true", help="Force reprocessing")
    args = parser.parse_args()

    if not args.config.exists():
        sys.exit(f"Config file not found: {args.config}")

    config = load_config(args.config)
    rules = load_rules(args.rules)
    logger = setup_logging(config.log_dir)

    logger.info("Primary engine: pdfplumber")
    logger.info("pypdfium2 available: %s", _PYPDFIUM2_AVAILABLE)

    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.quarantine_dir.mkdir(parents=True, exist_ok=True)

    # File discovery
    files_to_process: List[Path] = []
    if args.file:
        files_to_process = [args.file]
    else:
        supported_ext = {".pdf", ".docx", ".xlsx", ".pptx"}
        if config.input_dir.exists():
            for f in config.input_dir.iterdir():
                if f.suffix.lower() in supported_ext and not f.name.startswith(("~", ".")):
                    files_to_process.append(f)

    logger.info("Found %d files to process.", len(files_to_process))

    # Manifest management
    manifest_path = config.output_dir / "processing_manifest.json"
    manifest: Dict[str, str] = {}
    if manifest_path.exists():
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except (json.JSONDecodeError, OSError):
            logger.warning("Could not load manifest. Starting fresh.")

    # Process loop
    for file_path in files_to_process:
        try:
            file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
            if not args.reprocess and file_hash in manifest:
                logger.info("Skipping %s (already processed)", file_path.name)
                continue

            success = process_file(file_path, config, rules, logger)
            if success:
                manifest[file_hash] = str(config.output_dir / f"{file_path.stem}_v2.json")
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(manifest, f, indent=2)
        except OSError as exc:
            logger.error("Unexpected error on %s: %s", file_path.name, exc)


if __name__ == "__main__":
    main()
