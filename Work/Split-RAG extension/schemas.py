"""
Split-RAG Extension - Pydantic v2 Data Models
Defines ChunkMetadata, graph node types, and retrieval result schemas.

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import List, Optional, Literal, Dict, Any, Tuple

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# ID Generation Utilities (Deterministic)
# ---------------------------------------------------------------------------

def generate_node_id(source_file: str, page: int, chunk_index: int, raw_text: str) -> str:
    """MD5-based deterministic node ID from position + content."""
    sample = raw_text.strip()[:80]
    composite = f"{source_file}:{page}:{chunk_index}:{sample}"
    return hashlib.md5(composite.encode("utf-8")).hexdigest()


def generate_document_id(file_bytes: bytes) -> str:
    """MD5 hash of raw file bytes — identical files always produce identical IDs."""
    return hashlib.md5(file_bytes).hexdigest()


def generate_lineage_trace(
    source_hash: str,
    page_index: int,
    bbox: Optional[List[float]],
    extraction_method: str,
) -> str:
    """SHA-256 lineage trace for audit (CANON_004)."""
    bbox_str = ",".join(f"{v:.4f}" for v in bbox) if bbox else "none"
    key = f"{source_hash}|{page_index}|{bbox_str}|{extraction_method}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Phase 1 — ChunkMetadata (core ingestion model)
# ---------------------------------------------------------------------------

class ChunkMetadata(BaseModel):
    """
    Strict Pydantic model for every structural element extracted from a document.
    Preserves spatial provenance for UI bounding-box citations.
    """
    node_id: str = Field(..., description="Deterministic MD5 node identifier")
    source_file_name: str = Field(..., description="Original file name")
    page_number: int = Field(..., ge=1, description="1-based page number")
    chunk_type: Literal[
        "title", "header", "paragraph", "table", "table_row",
        "list_item", "image_caption", "kv_pair", "footer",
    ] = Field(..., description="Structural element type")
    bounding_boxes: List[Tuple[float, float, float, float]] = Field(
        default_factory=list,
        description="List of [x0, y0, x1, y1] coordinate tuples",
    )
    raw_text: str = Field(..., description="Extracted text content")
    reading_order_index: int = Field(
        default=0, description="Position in document reading order"
    )
    parent_section_id: Optional[str] = Field(
        default=None, description="ID of the parent section node"
    )
    extraction_method: Literal["docling", "pdfplumber"] = Field(
        default="pdfplumber", description="Engine that produced this chunk"
    )
    lineage_trace: Optional[str] = Field(
        default=None, description="SHA-256 audit hash"
    )
    source_scope: Literal["primary", "corpus"] = Field(
        default="primary", description="Disambiguation scope"
    )
    is_active: bool = Field(
        default=True, description="False for fallback duplicates retained for audit"
    )
    conflict_detected: bool = Field(
        default=False, description="True if Keep-All Policy flagged a conflict"
    )

    @field_validator("node_id")
    @classmethod
    def id_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("node_id cannot be empty")
        return v


# ---------------------------------------------------------------------------
# Graph-level models
# ---------------------------------------------------------------------------

class EntityNode(BaseModel):
    """An entity discovered via rule-based / RapidFuzz matching."""
    entity_id: str
    entity_label: str
    entity_type: str = "keyword"
    matched_score: float = Field(default=100.0, ge=0.0, le=100.0)
    source_chunk_ids: List[str] = Field(default_factory=list)


class GraphEdge(BaseModel):
    """A typed, directed relationship in the Document Knowledge Graph."""
    source_id: str
    target_id: str
    edge_type: Literal[
        "HAS_PAGE", "HAS_SECTION", "HAS_CHILD", "NEXT_CHUNK",
        "CONTAINS_TABLE", "MENTIONED_IN",
    ]
    weight: float = Field(default=1.0, ge=0.0)


# ---------------------------------------------------------------------------
# Retrieval result model
# ---------------------------------------------------------------------------

class RetrievalResult(BaseModel):
    """Single retrieval hit with provenance metadata."""
    node_id: str
    raw_text: str
    score: float
    page_number: int
    chunk_type: str
    bounding_boxes: List[Tuple[float, float, float, float]] = Field(
        default_factory=list
    )
    source_file_name: str = ""
    subgraph_label: Optional[str] = None


# ---------------------------------------------------------------------------
# Document-level container
# ---------------------------------------------------------------------------

class DocumentGraph(BaseModel):
    """Top-level container for a processed document's knowledge graph."""
    document_id: str
    filename: str
    processed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    schema_version: str = "extension-1.0.0"
    borrower_entity: Optional[str] = None
    chunks: List[ChunkMetadata] = Field(default_factory=list)
    entities: List[EntityNode] = Field(default_factory=list)
    edges: List[GraphEdge] = Field(default_factory=list)
    total_pages: int = 0

    def to_json(self) -> str:
        return self.model_dump_json(indent=2)
