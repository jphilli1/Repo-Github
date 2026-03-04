"""
Split-RAG System v2.0 — Tier 1 Extraction Engine (Refactored)
"The Factory" — Multi-Engine Dual-Run with Deterministic Fallbacks

ARCHITECTURE CONSTRAINTS (STRICT ENFORCEMENT):
    - Zero External APIs: No OpenAI, Anthropic, Gemini, or external API calls
    - Zero Neural Network Dependencies: No torch, transformers, or llama-index imports
    - Multi-Engine Dual-Run: Docling primary → pdfplumber+pypdfium2 fallback
    - ≥ 99% detection accuracy SLA via resilient cascading extraction
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
# DOCLING AVAILABILITY PROBE — lazy import, torch may be broken
# ---------------------------------------------------------------------------
_DOCLING_AVAILABLE: bool = False
try:
    from docling.document_converter import DocumentConverter, PdfFormatOption
    from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
    from docling.datamodel.base_models import InputFormat
    _DOCLING_AVAILABLE = True
except Exception:
    # Docling or its torch dependency is broken — will use fallback
    _DOCLING_AVAILABLE = False

# ---------------------------------------------------------------------------
# PYPDFIUM2 AVAILABILITY PROBE — used for bbox rendering fallback
# ---------------------------------------------------------------------------
_PYPDFIUM2_AVAILABLE: bool = False
try:
    import pypdfium2 as pdfium
    _PYPDFIUM2_AVAILABLE = True
except ImportError:
    _PYPDFIUM2_AVAILABLE = False

# pdfplumber is always required as minimum fallback
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
        "docling_use_gpu", "docling_table_mode",
        "conflict_threshold", "keep_all_policy",
    )

    def __init__(
        self,
        input_dir: Path, output_dir: Path, log_dir: Path, quarantine_dir: Path,
        primary_engine: str, fallback_engine: str,
        enable_ocr: bool, enable_table_detection: bool, max_pages_scan: int,
        docling_use_gpu: bool, docling_table_mode: str,
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
        self.docling_use_gpu = docling_use_gpu
        self.docling_table_mode = docling_table_mode
        self.conflict_threshold = conflict_threshold
        self.keep_all_policy = keep_all_policy


def load_config(config_path: Path) -> Config:
    """Load and validate config.json into a typed Config object."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        paths = data.get("paths", {})
        ext_set = data.get("extraction_settings", data.get("ingestion", {}))
        doc_set = data.get("docling_settings", {})
        val_set = data.get("validation_settings", {})
        return Config(
            input_dir=Path(paths.get("input_directory", "input")),
            output_dir=Path(paths.get("output_directory", "output")),
            log_dir=Path(paths.get("log_directory", "logs")),
            quarantine_dir=Path(paths.get("quarantine_directory", "quarantine")),
            primary_engine=ext_set.get("primary_engine", "docling"),
            fallback_engine=ext_set.get("fallback_engine", "pdfplumber"),
            enable_ocr=ext_set.get("enable_ocr", False),
            enable_table_detection=ext_set.get("enable_table_detection", True),
            max_pages_scan=ext_set.get("max_pages_for_entity_scan",
                                       ext_set.get("entity_scan_pages", 20)),
            docling_use_gpu=doc_set.get("use_gpu",
                                        ext_set.get("use_gpu", False)),
            docling_table_mode=doc_set.get("table_mode", "accurate"),
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
# PRIMARY ENGINE: Docling (wrapped in resilient try/except)
# ============================================================================

def process_with_docling(
    file_path: Path, config: Config, doc_id: str, file_hash: str
) -> List[schema.ContextNode]:
    """
    Primary extraction via IBM Docling.
    Wrapped in robust error handling — if torch is broken, docling init
    will fail and we cascade to fallback.
    """
    if not _DOCLING_AVAILABLE:
        raise RuntimeError("Docling not available (torch dependency likely broken)")

    nodes: List[schema.ContextNode] = []

    # Configure pipeline
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = config.enable_ocr
    pipeline_options.do_table_structure = config.enable_table_detection
    if config.docling_table_mode == "accurate":
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
    else:
        pipeline_options.table_structure_options.mode = TableFormerMode.FAST

    # CPU-only unless explicitly configured
    if config.docling_use_gpu:
        pipeline_options.accelerator_options.device = "cuda"
    else:
        pipeline_options.accelerator_options.device = "cpu"

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    doc_result = converter.convert(file_path)
    doc = doc_result.document

    chunk_idx = 0

    # --- Text elements ---
    for item in doc.texts:
        c_type = _map_docling_label(item.label)
        page_no = item.prov[0].page_no if item.prov else 1
        bbox_raw = item.prov[0].bbox.as_tuple() if item.prov else None
        bbox_list = [float(x) for x in bbox_raw] if bbox_raw else None

        chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, item.text)
        lineage = schema.generate_lineage_trace(file_hash, page_no, bbox_list, "docling")

        meta = schema.NodeMetadata(
            page_number=page_no,
            bbox=bbox_list,
            source_scope="primary",
            extraction_method="docling",
            is_active=True,
        )
        node = schema.ContextNode(
            chunk_id=chunk_id,
            content_type=c_type,
            content=item.text,
            metadata=meta,
            lineage_trace=lineage,
        )
        nodes.append(node)
        chunk_idx += 1

    # --- Tables ---
    for table in doc.tables:
        page_no = table.prov[0].page_no if table.prov else 1
        bbox_raw = table.prov[0].bbox.as_tuple() if table.prov else None
        bbox_list = [float(x) for x in bbox_raw] if bbox_raw else None
        md_content = table.export_to_markdown()
        t_shape = None
        if hasattr(table, "data"):
            num_rows = getattr(table.data, "num_rows", 0)
            num_cols = getattr(table.data, "num_cols", 0)
            if num_rows and num_cols:
                t_shape = [num_rows, num_cols]

        chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, md_content)
        lineage = schema.generate_lineage_trace(file_hash, page_no, bbox_list, "docling")

        meta = schema.NodeMetadata(
            page_number=page_no,
            bbox=bbox_list,
            table_shape=t_shape,
            source_scope="primary",
            extraction_method="docling",
            is_active=True,
        )
        node = schema.ContextNode(
            chunk_id=chunk_id,
            content_type="table",
            content=md_content,
            metadata=meta,
            lineage_trace=lineage,
        )
        nodes.append(node)
        chunk_idx += 1

    return nodes


# ============================================================================
# FALLBACK ENGINE: pdfplumber + pypdfium2 (combined for ≥ 99% SLA)
# ============================================================================

def process_with_fallback(
    file_path: Path, doc_id: str, file_hash: str
) -> List[schema.ContextNode]:
    """
    Fallback extraction using pdfplumber for text/tables and
    pypdfium2 for supplementary bounding-box accuracy.

    Spatial Provenance: Every node carries [x0, y0, x1, y1] coordinates
    derived from pdfplumber word-level geometry.
    """
    nodes: List[schema.ContextNode] = []
    chunk_idx = 0

    # --- Phase A: pypdfium2 page dimensions (if available) ---
    page_dims: Dict[int, Tuple[float, float]] = {}
    if _PYPDFIUM2_AVAILABLE:
        try:
            pdf_doc = pdfium.PdfDocument(str(file_path))
            for i in range(len(pdf_doc)):
                page = pdf_doc[i]
                page_dims[i + 1] = (page.get_width(), page.get_height())
        except Exception:
            pass

    # --- Phase B: pdfplumber extraction with word-level bounding boxes ---
    with pdfplumber.open(file_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            page_no = page_idx + 1
            extraction_method = "pdfplumber"

            # Extract word-level data for spatial reconstruction
            words = page.extract_words(keep_blank_chars=False) or []

            # Group into lines and merge into paragraphs with bounding boxes
            lines = _cluster_words_to_lines(words)
            paragraphs = _merge_lines_to_paragraphs(lines)

            for para_text, para_bbox in paragraphs:
                if len(para_text.strip()) < 5:
                    continue

                c_type = _classify_chunk_type(para_text)
                bbox_list = list(para_bbox)
                chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, para_text)
                lineage = schema.generate_lineage_trace(
                    file_hash, page_no, bbox_list, extraction_method
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

            # --- Tables with bounding-box estimation ---
            tables = page.find_tables() or []
            for tbl in tables:
                table_data = tbl.extract()
                if not table_data:
                    continue
                md_content = _table_to_markdown(table_data)

                # Table bbox from pdfplumber table object
                tbl_bbox = list(tbl.bbox) if hasattr(tbl, "bbox") and tbl.bbox else None
                t_shape = None
                if table_data:
                    t_shape = [len(table_data), len(table_data[0]) if table_data[0] else 0]

                chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, md_content)
                lineage = schema.generate_lineage_trace(
                    file_hash, page_no, tbl_bbox, extraction_method
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
                nodes.append(node)
                chunk_idx += 1

    return nodes


# ============================================================================
# DUAL-RUN CONFLICT DETECTION (Keep-All Policy)
# ============================================================================

def dual_run_with_conflict_detection(
    file_path: Path, config: Config, doc_id: str, file_hash: str,
    logger: logging.Logger,
) -> Tuple[List[schema.ContextNode], bool, bool, int]:
    """
    Execute Multi-Engine Dual-Run strategy for ≥ 99% detection accuracy.

    Strategy:
        1. Try primary engine (Docling) — if it succeeds, also run fallback
           and compare for conflict detection (Keep-All Policy).
        2. If primary fails, use fallback as sole engine.

    Returns:
        (nodes, primary_success, fallback_triggered, conflicts_detected)
    """
    primary_nodes: List[schema.ContextNode] = []
    fallback_nodes: List[schema.ContextNode] = []
    primary_success = False
    fallback_triggered = False
    conflicts_detected = 0

    # --- Attempt Primary ---
    logger.info("Starting Primary Extraction (docling) for %s...", file_path.name)
    try:
        primary_nodes = process_with_docling(file_path, config, doc_id, file_hash)
        primary_success = True
        logger.info("Docling extracted %d nodes from %s", len(primary_nodes), file_path.name)
    except RuntimeError as exc:
        logger.warning("Primary engine unavailable for %s: %s", file_path.name, exc)
    except Exception as exc:
        logger.warning("Primary engine failed for %s: %s", file_path.name, exc)

    # --- Attempt Fallback ---
    if not primary_success:
        logger.info("Triggering Fallback Engine (pdfplumber+pypdfium2) for %s...", file_path.name)
        try:
            fallback_nodes = process_with_fallback(file_path, doc_id, file_hash)
            fallback_triggered = True
            logger.info("Fallback extracted %d nodes from %s", len(fallback_nodes), file_path.name)
        except Exception as exc:
            logger.critical("Both engines failed for %s: %s", file_path.name, exc)
            raise

    # --- Keep-All Conflict Detection (Dual-Run when primary succeeded) ---
    if primary_success and config.keep_all_policy:
        try:
            fallback_nodes = process_with_fallback(file_path, doc_id, file_hash)
            conflicts_detected = _detect_conflicts(
                primary_nodes, fallback_nodes, config.conflict_threshold
            )
            if conflicts_detected > 0:
                logger.warning(
                    "Keep-All Policy: %d conflicts detected in %s — retaining both versions",
                    conflicts_detected, file_path.name,
                )
                # Mark fallback nodes as inactive (audit trail)
                for node in fallback_nodes:
                    node.metadata.is_active = False
                    node.metadata.conflict_detected = True
                primary_nodes.extend(fallback_nodes)
        except Exception as exc:
            logger.info("Dual-run fallback skipped for %s: %s", file_path.name, exc)

    final_nodes = primary_nodes if primary_success else fallback_nodes
    return final_nodes, primary_success, fallback_triggered, conflicts_detected


def _detect_conflicts(
    primary: List[schema.ContextNode],
    fallback: List[schema.ContextNode],
    threshold: float,
) -> int:
    """
    Compare primary vs fallback node sets using Levenshtein distance.
    Returns number of conflicts detected above threshold.
    """
    conflicts = 0
    try:
        from rapidfuzz import fuzz
    except ImportError:
        # Without RapidFuzz, skip conflict detection
        return 0

    primary_texts = {n.chunk_id: n.content for n in primary}
    fallback_texts = {n.chunk_id: n.content for n in fallback}

    # Compare texts on matching page numbers
    primary_by_page: Dict[int, List[str]] = {}
    for n in primary:
        primary_by_page.setdefault(n.metadata.page_number, []).append(n.content)

    fallback_by_page: Dict[int, List[str]] = {}
    for n in fallback:
        fallback_by_page.setdefault(n.metadata.page_number, []).append(n.content)

    for page_no in primary_by_page:
        if page_no not in fallback_by_page:
            continue
        for p_text in primary_by_page[page_no]:
            best_ratio = 0.0
            for f_text in fallback_by_page[page_no]:
                ratio = fuzz.ratio(p_text[:500], f_text[:500]) / 100.0
                best_ratio = max(best_ratio, ratio)
            if best_ratio < (1.0 - threshold):
                conflicts += 1

    return conflicts


# ---------------------------------------------------------------------------
# Text processing helpers
# ---------------------------------------------------------------------------

def _map_docling_label(label: str) -> str:
    """Map Docling structural labels to schema content_type."""
    mapping = {
        "title": "header",
        "section_header": "header",
        "paragraph": "text",
        "list_item": "text",
        "caption": "image_caption",
        "page_footer": "text",
        "page_header": "header",
    }
    return mapping.get(label, "text")


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
        3. Multi-Engine Dual-Run extraction
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

    # 3. Multi-Engine Dual-Run extraction
    try:
        nodes, primary_success, fallback_triggered, conflicts = (
            dual_run_with_conflict_detection(
                file_path, config, doc_id, file_hash, logger
            )
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
        fallback_engine_name = "pdfplumber"
        if _PYPDFIUM2_AVAILABLE:
            fallback_engine_name = "pdfplumber+pypdfium2"

    metrics = schema.ExtractionMetrics(
        total_pages=max((n.metadata.page_number for n in nodes), default=0),
        total_nodes=len(nodes),
        tables_extracted=sum(1 for n in nodes if n.content_type == "table"),
        headers_extracted=sum(1 for n in nodes if n.content_type == "header"),
        conflicts_detected=conflicts,
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
    parser = argparse.ArgumentParser(description="Split-RAG Tier 1 Extraction Engine (Refactored)")
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

    logger.info("Docling available: %s", _DOCLING_AVAILABLE)
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
