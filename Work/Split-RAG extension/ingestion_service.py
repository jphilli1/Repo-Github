"""
Split-RAG Extension — Phase 1: Deterministic Ingestion & Spatial Metadata Extraction

DocumentIngestionService:
  - Primary engine: IBM Docling (DocumentConverter, CPU-only)
  - Fallback engine: pdfplumber (geometry-based)
  - Outputs: List[ChunkMetadata] with bounding-box provenance

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai
imported directly. Docling manages its own ML internals.
"""

from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from schemas import (
    ChunkMetadata,
    generate_document_id,
    generate_lineage_trace,
    generate_node_id,
)

logger = logging.getLogger("SplitRAG.Ingestion")


# ---------------------------------------------------------------------------
# DocumentIngestionService
# ---------------------------------------------------------------------------

class DocumentIngestionService:
    """Ingests PDFs into validated ChunkMetadata objects with spatial provenance."""

    def __init__(
        self,
        *,
        use_docling: bool = True,
        enable_ocr: bool = False,
        enable_tables: bool = True,
        use_gpu: bool = False,
        entity_scan_pages: int = 20,
        entity_rules: Optional[Dict] = None,
    ) -> None:
        self._use_docling = use_docling
        self._enable_ocr = enable_ocr
        self._enable_tables = enable_tables
        self._use_gpu = use_gpu
        self._entity_scan_pages = entity_scan_pages
        self._entity_rules = entity_rules or {}
        self._docling_available = False

        if self._use_docling:
            self._docling_available = self._probe_docling()

    # ------------------------------------------------------------------
    # Docling availability probe
    # ------------------------------------------------------------------

    @staticmethod
    def _probe_docling() -> bool:
        try:
            from docling.document_converter import DocumentConverter  # noqa: F401
            return True
        except (ImportError, ModuleNotFoundError) as exc:
            logger.warning("Docling not available — will use pdfplumber fallback: %s", exc)
            return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ingest(self, file_path: Path) -> Tuple[List[ChunkMetadata], Dict[str, Optional[str]]]:
        """
        Ingest a PDF and return (chunks, entities).

        Returns:
            chunks  — list of ChunkMetadata with bounding boxes
            entities — dict with keys like 'borrower', 'lender', etc.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_bytes = file_path.read_bytes()
        file_hash = hashlib.md5(file_bytes).hexdigest()
        doc_id = generate_document_id(file_bytes)

        # Attempt primary engine
        chunks: List[ChunkMetadata] = []
        if self._docling_available:
            try:
                chunks = self._ingest_docling(file_path, doc_id, file_hash)
                logger.info(
                    "Docling extracted %d chunks from %s", len(chunks), file_path.name
                )
            except (OSError, RuntimeError, ValueError, ImportError) as exc:
                logger.warning(
                    "Docling failed for %s: %s — falling back to pdfplumber",
                    file_path.name,
                    exc,
                )
                chunks = []

        # Fallback
        if not chunks:
            chunks = self._ingest_pdfplumber(file_path, doc_id, file_hash)
            logger.info(
                "pdfplumber extracted %d chunks from %s", len(chunks), file_path.name
            )

        # Entity anchoring
        entities = self._extract_entities(file_path)

        return chunks, entities

    # ------------------------------------------------------------------
    # Docling extraction
    # ------------------------------------------------------------------

    def _ingest_docling(
        self, file_path: Path, doc_id: str, file_hash: str
    ) -> List[ChunkMetadata]:
        from docling.document_converter import DocumentConverter, PdfFormatOption
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions,
            TableFormerMode,
        )
        from docling.datamodel.base_models import InputFormat

        pipeline_opts = PdfPipelineOptions()
        pipeline_opts.do_ocr = self._enable_ocr
        pipeline_opts.do_table_structure = self._enable_tables
        pipeline_opts.table_structure_options.mode = TableFormerMode.ACCURATE

        if not self._use_gpu:
            pipeline_opts.accelerator_options.device = "cpu"

        converter = DocumentConverter(
            format_options={
                InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_opts)
            }
        )

        result = converter.convert(file_path)
        doc = result.document

        chunks: List[ChunkMetadata] = []
        idx = 0

        # --- text elements ---
        for item in doc.texts:
            ctype = _map_docling_label(item.label)
            page_no = item.prov[0].page_no if item.prov else 1
            bbox_raw = item.prov[0].bbox.as_tuple() if item.prov else None
            bbox_list = [tuple(float(c) for c in bbox_raw)] if bbox_raw else []

            node_id = generate_node_id(file_path.name, page_no, idx, item.text)
            lineage = generate_lineage_trace(
                file_hash,
                page_no,
                list(bbox_raw) if bbox_raw else None,
                "docling",
            )

            chunks.append(
                ChunkMetadata(
                    node_id=node_id,
                    source_file_name=file_path.name,
                    page_number=page_no,
                    chunk_type=ctype,
                    bounding_boxes=bbox_list,
                    raw_text=item.text,
                    reading_order_index=idx,
                    extraction_method="docling",
                    lineage_trace=lineage,
                )
            )
            idx += 1

        # --- tables ---
        for table in doc.tables:
            page_no = table.prov[0].page_no if table.prov else 1
            md = table.export_to_markdown()
            num_rows = table.data.num_rows if hasattr(table.data, "num_rows") else 0
            num_cols = table.data.num_cols if hasattr(table.data, "num_cols") else 0

            node_id = generate_node_id(file_path.name, page_no, idx, md)
            lineage = generate_lineage_trace(file_hash, page_no, None, "docling")

            chunks.append(
                ChunkMetadata(
                    node_id=node_id,
                    source_file_name=file_path.name,
                    page_number=page_no,
                    chunk_type="table",
                    bounding_boxes=[],
                    raw_text=md,
                    reading_order_index=idx,
                    extraction_method="docling",
                    lineage_trace=lineage,
                )
            )
            idx += 1

        return chunks

    # ------------------------------------------------------------------
    # pdfplumber fallback
    # ------------------------------------------------------------------

    def _ingest_pdfplumber(
        self, file_path: Path, doc_id: str, file_hash: str
    ) -> List[ChunkMetadata]:
        import pdfplumber

        chunks: List[ChunkMetadata] = []
        idx = 0

        with pdfplumber.open(file_path) as pdf:
            for page_idx, page in enumerate(pdf.pages):
                page_no = page_idx + 1
                page_width = float(page.width)
                page_height = float(page.height)

                # --- extract words for bbox reconstruction ---
                words = page.extract_words(keep_blank_chars=False) or []

                # Group words into lines by clustering on y-coordinate (top)
                lines = _cluster_words_to_lines(words)

                # Merge adjacent lines into paragraph-level chunks
                paragraphs = _merge_lines_to_paragraphs(lines, page_no)

                for para_text, para_bbox in paragraphs:
                    if len(para_text.strip()) < 5:
                        continue

                    ctype = _guess_chunk_type(para_text)
                    node_id = generate_node_id(file_path.name, page_no, idx, para_text)
                    lineage = generate_lineage_trace(
                        file_hash, page_no, list(para_bbox), "pdfplumber"
                    )

                    chunks.append(
                        ChunkMetadata(
                            node_id=node_id,
                            source_file_name=file_path.name,
                            page_number=page_no,
                            chunk_type=ctype,
                            bounding_boxes=[para_bbox],
                            raw_text=para_text,
                            reading_order_index=idx,
                            extraction_method="pdfplumber",
                            lineage_trace=lineage,
                        )
                    )
                    idx += 1

                # --- tables ---
                tables = page.extract_tables() or []
                for table_data in tables:
                    if not table_data:
                        continue
                    md = _table_to_markdown(table_data)
                    node_id = generate_node_id(file_path.name, page_no, idx, md)
                    lineage = generate_lineage_trace(
                        file_hash, page_no, None, "pdfplumber"
                    )

                    chunks.append(
                        ChunkMetadata(
                            node_id=node_id,
                            source_file_name=file_path.name,
                            page_number=page_no,
                            chunk_type="table",
                            bounding_boxes=[],
                            raw_text=md,
                            reading_order_index=idx,
                            extraction_method="pdfplumber",
                            lineage_trace=lineage,
                        )
                    )
                    idx += 1

        return chunks

    # ------------------------------------------------------------------
    # Entity anchoring (regex-based, from rules.json)
    # ------------------------------------------------------------------

    def _extract_entities(self, file_path: Path) -> Dict[str, Optional[str]]:
        entities: Dict[str, Optional[str]] = {
            "borrower": None,
            "lender": None,
            "guarantor": None,
        }
        rules_entities = self._entity_rules.get("entities", {})
        if not rules_entities:
            return entities

        try:
            import pdfplumber

            text_buf = ""
            with pdfplumber.open(file_path) as pdf:
                limit = min(len(pdf.pages), self._entity_scan_pages)
                for i in range(limit):
                    t = pdf.pages[i].extract_text()
                    if t:
                        text_buf += t + "\n"

            for role in ("borrower", "lender", "guarantor"):
                patterns = rules_entities.get(role, {}).get("patterns", [])
                for pat in patterns:
                    m = re.search(pat, text_buf, re.IGNORECASE | re.MULTILINE)
                    if m:
                        entities[role] = m.group("entity").strip().strip('",.')
                        break
        except (OSError, ValueError, re.error, AttributeError) as exc:
            logger.warning("Entity extraction failed for %s: %s", file_path.name, exc)

        return entities


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _map_docling_label(label: str) -> str:
    mapping = {
        "title": "title",
        "section_header": "header",
        "paragraph": "paragraph",
        "list_item": "list_item",
        "caption": "image_caption",
        "page_footer": "footer",
        "page_header": "header",
    }
    return mapping.get(label, "paragraph")


def _cluster_words_to_lines(
    words: List[Dict],
    y_tolerance: float = 3.0,
) -> List[List[Dict]]:
    """Group pdfplumber word dicts into lines by y-proximity."""
    if not words:
        return []
    sorted_words = sorted(words, key=lambda w: (round(float(w["top"]) / y_tolerance), float(w["x0"])))
    lines: List[List[Dict]] = []
    current_line: List[Dict] = [sorted_words[0]]
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
    lines: List[List[Dict]],
    page_no: int,
    gap_threshold: float = 12.0,
) -> List[Tuple[str, Tuple[float, float, float, float]]]:
    """Merge consecutive lines into paragraph chunks with combined bounding boxes."""
    if not lines:
        return []

    paragraphs: List[Tuple[str, Tuple[float, float, float, float]]] = []
    current_texts: List[str] = []
    x0_min = float("inf")
    y0_min = float("inf")
    x1_max = 0.0
    y1_max = 0.0
    prev_bottom = 0.0

    for line_words in lines:
        line_text = " ".join(w["text"] for w in line_words)
        lx0 = min(float(w["x0"]) for w in line_words)
        ly0 = min(float(w["top"]) for w in line_words)
        lx1 = max(float(w["x1"]) for w in line_words)
        ly1 = max(float(w["bottom"]) for w in line_words)

        if current_texts and (ly0 - prev_bottom) > gap_threshold:
            para_text = " ".join(current_texts)
            paragraphs.append((para_text, (x0_min, y0_min, x1_max, y1_max)))
            current_texts = []
            x0_min = float("inf")
            y0_min = float("inf")
            x1_max = 0.0
            y1_max = 0.0

        current_texts.append(line_text)
        x0_min = min(x0_min, lx0)
        y0_min = min(y0_min, ly0)
        x1_max = max(x1_max, lx1)
        y1_max = max(y1_max, ly1)
        prev_bottom = ly1

    if current_texts:
        paragraphs.append((" ".join(current_texts), (x0_min, y0_min, x1_max, y1_max)))

    return paragraphs


def _guess_chunk_type(text: str) -> str:
    """Simple heuristic to classify a text chunk."""
    stripped = text.strip()
    if len(stripped) < 80 and stripped.isupper():
        return "header"
    if re.match(r"^\d+\.\s", stripped):
        return "header"
    if re.match(r"^[-•●]\s", stripped):
        return "list_item"
    return "paragraph"


def _table_to_markdown(table_data: List[List]) -> str:
    """Convert a pdfplumber table (list of lists) to Markdown."""
    if not table_data:
        return ""
    rows = []
    for row in table_data:
        cells = [str(c) if c is not None else "" for c in row]
        rows.append("| " + " | ".join(cells) + " |")
    if len(rows) >= 1:
        sep = "| " + " | ".join(["---"] * len(table_data[0])) + " |"
        rows.insert(1, sep)
    return "\n".join(rows)
