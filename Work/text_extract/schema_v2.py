# AI-Native Split-RAG System v2.0 - Data Contract
# Tier 1 & Tier 2 Interface Definition
# CP-001: All functions have explicit return type hints
# CP-003: Path handling via pathlib (where applicable)

import hashlib
import json
from datetime import datetime, timezone
from typing import List, Optional, Literal, Tuple
from pydantic import BaseModel, Field, field_validator

# --- ID Generation Utilities (Deterministic) ---

def generate_document_id(file_bytes: bytes) -> str:
    """
    Generates a deterministic MD5 hash of the file contents.
    Ensures T-001 Determinism: Same file always = same ID.
    """
    return hashlib.md5(file_bytes).hexdigest()

def generate_chunk_id(doc_id: str, page_number: int, chunk_index: int, content_sample: str) -> str:
    """
    Generates a deterministic MD5 hash based on position and content.
    """
    # normalize content sample to avoid minor whitespace jitter affecting ID
    clean_sample = content_sample.strip()[:50]
    composite_key = f"{doc_id}:{page_number}:{chunk_index}:{clean_sample}"
    return hashlib.md5(composite_key.encode('utf-8')).hexdigest()

def generate_lineage_trace(source_file_hash: str, page_index: int, bbox_coords: Optional[List[float]], extraction_method: str) -> str:
    """
    Generates a SHA-256 hash for strict audit trails (CANON_004).
    """
    bbox_str = ",".join(map(str, bbox_coords)) if bbox_coords else "none"
    trace_key = f"{source_file_hash}|{page_index}|{bbox_str}|{extraction_method}"
    return hashlib.sha256(trace_key.encode('utf-8')).hexdigest()

# --- Pydantic Models ---

class NodeMetadata(BaseModel):
    page_number: int = Field(..., description="1-based page number")
    bbox: Optional[List[float]] = Field(default=None, min_length=4, max_length=4)
    table_shape: Optional[List[int]] = Field(default=None, min_length=2, max_length=2, description="[rows, cols]")
    edge_density: float = Field(default=0.0, description="Heuristic for image/content complexity")
    source_scope: Literal["primary", "corpus"] = Field(..., description="Scope for disambiguation")
    extraction_method: Literal["docling", "pdfplumber"]
    conflict_detected: bool = Field(default=False, description="True if Keep-All Policy triggered conflict")
    is_active: bool = Field(default=True, description="False for fallback versions retained for audit")

class ContextNode(BaseModel):
    chunk_id: str = Field(..., pattern=r"^[a-f0-9]{32}$", description="32-char MD5 hash")
    parent_section_id: Optional[str] = None
    content_type: Literal["header", "text", "table", "image_caption", "kv_pair"]
    content: str
    verified_content: bool = False
    metadata: NodeMetadata
    lineage_trace: str = Field(..., pattern=r"^[a-f0-9]{64}$", description="64-char SHA-256 hash")

class ExtractionMetrics(BaseModel):
    total_pages: int
    total_nodes: int
    tables_extracted: int
    headers_extracted: int
    conflicts_detected: int
    extraction_time_seconds: float
    primary_engine_used: bool = True
    fallback_triggered: bool = False

class ContextGraph(BaseModel):
    document_id: str = Field(..., pattern=r"^[a-f0-9]{32}$")
    filename: str
    processed_at: str  # ISO 8601 String
    schema_version: str = "2.0.0"
    borrower_entity: Optional[str] = None
    lender_entity: Optional[str] = None
    guarantor_entity: Optional[str] = None
    nodes: List[ContextNode] = []
    metrics: Optional[ExtractionMetrics] = None

    @classmethod
    def get_current_timestamp(cls) -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_json(self) -> str:
        """Standardized serialization method."""
        return self.model_dump_json(indent=2)