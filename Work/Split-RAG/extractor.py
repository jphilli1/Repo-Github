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
from typing import Any, Dict, List, Optional, Tuple

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
    except (OSError, pdfplumber.exceptions.PSException):
        pass

    return entities


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
    """
    nodes: List[schema.ContextNode] = []
    chunk_idx = 0
    logger = logging.getLogger("SplitRAG_Factory")

    with pdfplumber.open(file_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            page_no = page_idx + 1
            page_width = float(page.width)
            page_height = float(page.height)

            try:
                # --- Text extraction via .extract_words() ---
                words = page.extract_words(keep_blank_chars=False) or []

                # Group into lines and merge into paragraphs with bounding boxes
                lines = _cluster_words_to_lines(words)
                paragraphs = _merge_lines_to_paragraphs(lines)

                # --- SCANNED DOCUMENT FAILSAFE ---
                # Count total alnum chars on page; skip if below threshold
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

                    meta = schema.NodeMetadata(
                        page_number=page_no,
                        bbox=bbox_list,
                        source_scope="primary",
                        extraction_method="pdfplumber",
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

                # --- Tables with bounding-box via .find_tables() ---
                tables = page.find_tables() or []
                for tbl in tables:
                    table_data = tbl.extract()
                    if not table_data:
                        continue

                    # --- Cell-level spatial provenance ---
                    # Iterate individual cells, sanitize each bbox before
                    # assembling the table markdown. cell_bboxes stores
                    # per-cell [x0, y0, x1, y1] for downstream audit.
                    cell_bboxes: List[Tuple[float, float, float, float]] = []
                    if hasattr(tbl, "cells") and tbl.cells:
                        for cell in tbl.cells:
                            # pdfplumber cells are (x0, top, x1, bottom)
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
                        source_scope="primary",
                        extraction_method="pdfplumber",
                        is_active=True,
                    )
                    node = schema.ContextNode(
                        chunk_id=chunk_id,
                        content_type="table",
                        content=md_content,
                        metadata=meta,
                        lineage_trace=lineage,
                    )
                    # Attach cell-level bbox provenance for audit
                    node._cell_bboxes = cell_bboxes
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
    except (OSError, pdfplumber.exceptions.PSException) as exc:
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
    considered readable. Scanned/image-only pages produce near-zero text
    from geometry-based extraction (pdfplumber / pypdfium2).

    Returns True if readable, False if SCANNED_UNREADABLE.
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


def _classify_chunk_type(text: str) -> str:
    """Heuristic classification of text chunks into schema content_type."""
    stripped = text.strip()
    if len(stripped) < 80 and stripped.isupper():
        return "header"
    if re.match(r"^\d+\.\s", stripped):
        return "header"
    return "text"


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

    Steps:
        1. Hash file → deterministic document_id
        2. Entity anchoring (regex-based)
        3. pdfplumber primary → pypdfium2 fallback extraction
        4. Assemble ContextGraph with ExtractionMetrics
        5. Validate via Pydantic and serialize to JSON
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

    # 2. Entity anchoring
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

    # 4. Assemble ContextGraph
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
    )

    graph = schema.ContextGraph(
        document_id=doc_id,
        filename=file_path.name,
        processed_at=schema.ContextGraph.get_current_timestamp(),
        borrower_entity=entities.get("borrower"),
        lender_entity=entities.get("lender"),
        guarantor_entity=entities.get("guarantor"),
        nodes=nodes,
        metrics=metrics,
    )

    # 5. Validate & serialize
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
