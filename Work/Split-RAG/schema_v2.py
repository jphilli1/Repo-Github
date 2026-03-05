# AI-Native Split-RAG System v2.0 - Data Contract
# Tier 1 & Tier 2 Interface Definition
# CP-001: All functions have explicit return type hints
# CP-003: Path handling via pathlib (where applicable)
#
# ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Literal, Tuple

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


def generate_lineage_trace(source_file_hash: str, page_index: int, bbox_coords: Optional[Tuple[float, ...]], extraction_method: str) -> str:
    """
    Generates a SHA-256 hash for strict audit trails (CANON_004).
    """
    bbox_str = ",".join(map(str, bbox_coords)) if bbox_coords else "none"
    trace_key = f"{source_file_hash}|{page_index}|{bbox_str}|{extraction_method}"
    return hashlib.sha256(trace_key.encode('utf-8')).hexdigest()


# --- Pydantic Models ---

class NodeMetadata(BaseModel):
    page_number: int = Field(..., description="1-based page number")
    bbox: Optional[Tuple[float, float, float, float]] = Field(default=None)
    table_shape: Optional[List[int]] = Field(default=None, min_length=2, max_length=2, description="[rows, cols]")
    cell_bboxes: Optional[List[Tuple[float, float, float, float]]] = Field(default=None, description="Granular bounding boxes for individual table cells")
    edge_density: float = Field(default=0.0, description="Heuristic for image/content complexity")
    source_scope: Literal["primary", "corpus"] = Field(..., description="Scope for disambiguation")
    extraction_method: Literal["pdfplumber", "pypdfium2"]
    conflict_detected: bool = Field(default=False, description="True if Keep-All Policy triggered conflict")
    is_active: bool = Field(default=True, description="False for fallback versions retained for audit")
    section_label: Optional[str] = Field(default=None, description="Normalized section header label (e.g. 'financial_covenants')")
    section_level: int = Field(default=0, description="Header hierarchy depth: 0=none, 1=top-level, 2=sub, 3=sub-sub")
    is_email_block: bool = Field(default=False, description="True if detected as part of an embedded email block")


class ContextNode(BaseModel):
    chunk_id: str = Field(..., pattern=r"^[a-f0-9]{32}$", description="32-char MD5 hash")
    parent_section_id: Optional[str] = None
    content_type: Literal["header", "text", "table", "image_caption", "kv_pair"]
    content: str
    verified_content: bool = False
    metadata: NodeMetadata
    lineage_trace: str = Field(..., pattern=r"^[a-f0-9]{64}$", description="64-char SHA-256 hash")


# --- Extracted Intelligence Models ---

class MetricObservation(BaseModel):
    """A single extracted financial metric observation with provenance."""
    metric_name: str = Field(..., description="Canonical metric name (e.g. 'dscr', 'ltv')")
    raw_value: str = Field(..., description="Original text (e.g. '1.25x', '$50MM')")
    normalized_value: Optional[float] = Field(default=None, description="Numeric value (e.g. 1.25, 50000000.0)")
    unit: Optional[str] = Field(default=None, description="Unit type: 'multiple', 'percent', 'currency'")
    normalized_unit: Optional[str] = Field(default=None, description="Specific unit (e.g. 'currency_usd', 'percent', 'multiple')")
    scale_hint: Optional[str] = Field(default=None, description="Original scale notation (e.g. 'MM', 'B', 'K')")
    source_section: Optional[str] = Field(default=None, description="Section where metric was found")
    page_number: Optional[int] = None
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_chunk_id: Optional[str] = None


class CreditTeamMember(BaseModel):
    """Extracted credit team member with role and provenance."""
    role: str = Field(..., description="Role identifier (e.g. 'relationship_manager', 'credit_officer')")
    name: str
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_chunk_id: Optional[str] = None


class ExtractedEntity(BaseModel):
    """A single extracted entity with full provenance chain."""
    entity_type: str = Field(..., description="Entity type key (e.g. 'borrower', 'property_address')")
    raw_value: str
    normalized_value: Optional[str] = None
    source_section: Optional[str] = None
    page_number: Optional[int] = None
    confidence_score: float = Field(default=0.5, ge=0.0, le=1.0)
    evidence_chunk_id: Optional[str] = None


class ExtractedIntelligence(BaseModel):
    """All structured intelligence extracted from a credit document."""
    document_type: Optional[str] = Field(default=None, description="Classified document type (e.g. 'credit_memo', 'term_sheet')")
    document_type_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    entities: List[ExtractedEntity] = Field(default=[], description="All extracted entities (borrower structure, CRE, collateral, etc.)")
    financial_metrics: List[MetricObservation] = Field(default=[], description="Extracted financial metrics with normalization")
    credit_team: List[CreditTeamMember] = Field(default=[], description="Identified credit team members")
    covenants: List[ExtractedEntity] = Field(default=[], description="Extracted covenant terms")


class ExtractionMetrics(BaseModel):
    total_pages: int
    total_nodes: int
    tables_extracted: int
    headers_extracted: int
    conflicts_detected: int
    extraction_time_seconds: float
    primary_engine_used: bool = True
    fallback_triggered: bool = False
    fallback_engine: Optional[str] = None
    kv_pairs_extracted: int = 0
    entities_extracted: int = 0


class ContextGraph(BaseModel):
    document_id: str = Field(..., pattern=r"^[a-f0-9]{32}$")
    filename: str
    processed_at: str  # ISO 8601 String
    schema_version: str = "2.1.0"
    borrower_entity: Optional[str] = None
    lender_entity: Optional[str] = None
    guarantor_entity: Optional[str] = None
    intelligence: Optional[ExtractedIntelligence] = Field(default=None, description="Structured extraction results")
    nodes: List[ContextNode] = []
    metrics: Optional[ExtractionMetrics] = None

    @classmethod
    def get_current_timestamp(cls) -> str:
        return datetime.now(timezone.utc).isoformat()

    def to_json(self) -> str:
        """Standardized serialization method."""
        return self.model_dump_json(indent=2)
