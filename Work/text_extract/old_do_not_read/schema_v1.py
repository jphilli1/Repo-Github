# [EXACT] — Copy this file verbatim
"""
Split-RAG Document Extractor - Pydantic Schema Definitions
Version: 1.0.0

This module defines the strict data contract for Context Graph JSON output.
All extraction output MUST pass validation against these models.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional, Literal, Dict, Any
from pydantic import (
    BaseModel,
    Field,
    field_validator,
    model_validator,
    ConfigDict,
)
import re


# -------------------------------------------------------------------
# Helper validators
# -------------------------------------------------------------------

_UUID_RE = re.compile(r"^[a-f0-9]{64}$")
_ISO_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def _validate_sha256_hex(v: str) -> str:
    if not isinstance(v, str) or not _UUID_RE.match(v):
        raise ValueError("Must be a 64-char lowercase hex string (sha256).")
    return v


def _validate_iso_utc_z(v: str) -> str:
    if not isinstance(v, str) or not _ISO_DT_RE.match(v):
        raise ValueError("Must be ISO8601 UTC string ending in Z (e.g., 2025-01-01T00:00:00Z).")
    return v


# -------------------------------------------------------------------
# Core schema
# -------------------------------------------------------------------

NodeType = Literal["document", "section", "chunk", "table", "image"]
SourceTier = Literal["tier1_local", "tier2_sandbox"]


class Provenance(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_tier: SourceTier = Field(..., description="Where this node was produced (tier1_local or tier2_sandbox).")
    extractor_version: str = Field(..., description="Extractor version string, e.g. 2.1.0.")
    created_utc: str = Field(..., description="ISO UTC timestamp ending in Z.")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Confidence score 0-1.")
    warnings: List[str] = Field(default_factory=list, description="Any warnings from extraction.")

    @field_validator("created_utc")
    @classmethod
    def _v_created_utc(cls, v: str) -> str:
        return _validate_iso_utc_z(v)


class FileRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    file_path: str = Field(..., description="Relative path from working_dir, using forward slashes.")
    file_name: str = Field(..., description="Base name of file.")
    file_ext: str = Field(..., description="Lowercase extension without dot.")
    file_size_bytes: int = Field(..., ge=0, description="File size in bytes.")
    file_modified_utc: str = Field(..., description="ISO UTC timestamp ending in Z.")
    file_sha256: str = Field(..., description="SHA256 hex digest (lowercase).")

    @field_validator("file_modified_utc")
    @classmethod
    def _v_modified_utc(cls, v: str) -> str:
        return _validate_iso_utc_z(v)

    @field_validator("file_sha256")
    @classmethod
    def _v_sha256(cls, v: str) -> str:
        return _validate_sha256_hex(v)


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid")

    page: Optional[int] = Field(None, ge=1, description="1-indexed page number if applicable.")
    sheet: Optional[str] = Field(None, description="Sheet name if applicable.")
    row_start: Optional[int] = Field(None, ge=1, description="1-indexed row start if applicable.")
    row_end: Optional[int] = Field(None, ge=1, description="1-indexed row end if applicable.")
    col_start: Optional[int] = Field(None, ge=1, description="1-indexed column start if applicable.")
    col_end: Optional[int] = Field(None, ge=1, description="1-indexed column end if applicable.")
    bbox: Optional[List[float]] = Field(
        None,
        description="Bounding box [x0, y0, x1, y1] in source coordinate space if applicable.",
        min_length=4,
        max_length=4,
    )


class TableData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["csv"] = Field("csv", description="Table encoding format.")
    csv_text: str = Field(..., description="CSV-encoded table content.")


class ImageData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: Literal["png"] = Field("png", description="Image encoding format.")
    width: int = Field(..., ge=1)
    height: int = Field(..., ge=1)
    sha256: str = Field(..., description="SHA256 hex digest of image bytes (lowercase).")
    category: Literal["chart", "diagram", "photo", "unknown"] = Field("unknown", description="Heuristic category.")
    edge_density: Optional[float] = Field(None, ge=0.0, le=1.0, description="Edge density metric if computed.")

    @field_validator("sha256")
    @classmethod
    def _v_sha256(cls, v: str) -> str:
        return _validate_sha256_hex(v)


class ChunkText(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(..., description="Chunk text content.")
    char_start: Optional[int] = Field(None, ge=0, description="Start character offset within parent section.")
    char_end: Optional[int] = Field(None, ge=0, description="End character offset within parent section.")
    token_estimate: Optional[int] = Field(None, ge=0, description="Optional heuristic token estimate.")


class ContextNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    node_id: str = Field(..., description="Stable node id (sha256 hex).")
    node_type: NodeType = Field(..., description="Node type.")
    title: Optional[str] = Field(None, description="Optional title for document/section.")
    parent_id: Optional[str] = Field(None, description="Parent node id if any.")
    file_ref: FileRef = Field(..., description="File reference metadata.")
    location: Optional[Location] = Field(None, description="Optional location metadata.")
    provenance: Provenance = Field(..., description="Provenance and scoring.")
    text: Optional[ChunkText] = Field(None, description="Text payload for chunk/section.")
    table: Optional[TableData] = Field(None, description="Table payload for table nodes.")
    image: Optional[ImageData] = Field(None, description="Image payload for image nodes.")
    tags: List[str] = Field(default_factory=list, description="Arbitrary tags.")

    @field_validator("node_id")
    @classmethod
    def _v_node_id(cls, v: str) -> str:
        return _validate_sha256_hex(v)

    @field_validator("parent_id")
    @classmethod
    def _v_parent_id(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return _validate_sha256_hex(v)

    @model_validator(mode="after")
    def _validate_payload(self) -> "ContextNode":
        # Enforce payload rules per node type
        if self.node_type in ("document", "section"):
            # section/document may have title; may optionally have text (some docs)
            return self
        if self.node_type == "chunk":
            if self.text is None:
                raise ValueError("chunk nodes must include text payload.")
            if self.table is not None or self.image is not None:
                raise ValueError("chunk nodes cannot include table/image payload.")
            return self
        if self.node_type == "table":
            if self.table is None:
                raise ValueError("table nodes must include table payload.")
            if self.text is not None or self.image is not None:
                raise ValueError("table nodes cannot include text/image payload.")
            return self
        if self.node_type == "image":
            if self.image is None:
                raise ValueError("image nodes must include image payload.")
            if self.text is not None or self.table is not None:
                raise ValueError("image nodes cannot include text/table payload.")
            return self
        return self


class EdgeType(BaseModel):
    model_config = ConfigDict(extra="forbid")

    edge_id: str = Field(..., description="Stable edge id (sha256 hex).")
    from_id: str = Field(..., description="Source node id.")
    to_id: str = Field(..., description="Target node id.")
    relation: Literal[
        "contains",
        "next",
        "references",
        "derived_from",
        "same_doc",
    ] = Field(..., description="Relationship type.")
    weight: float = Field(1.0, ge=0.0, le=1.0, description="Optional weight 0-1.")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional extra metadata.")

    @field_validator("edge_id", "from_id", "to_id")
    @classmethod
    def _v_ids(cls, v: str) -> str:
        return _validate_sha256_hex(v)


class ContextGraph(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: str = Field(..., description="Schema version.")
    run_id: str = Field(..., description="Run identifier (sha256).")
    created_utc: str = Field(..., description="ISO UTC timestamp ending in Z.")
    nodes: List[ContextNode] = Field(..., description="All nodes.")
    edges: List[EdgeType] = Field(default_factory=list, description="All edges.")
    stats: Dict[str, Any] = Field(default_factory=dict, description="Optional run statistics.")

    @field_validator("run_id")
    @classmethod
    def _v_run_id(cls, v: str) -> str:
        return _validate_sha256_hex(v)

    @field_validator("created_utc")
    @classmethod
    def _v_created_utc(cls, v: str) -> str:
        return _validate_iso_utc_z(v)


# -------------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------------

def utc_now_z() -> str:
    """Return current UTC timestamp as ISO8601 string ending in Z."""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def validate_graph(data: Dict[str, Any]) -> ContextGraph:
    """Validate a dictionary against the ContextGraph schema."""
    return ContextGraph.model_validate(data)


def validate_node(data: Dict[str, Any]) -> ContextNode:
    """Validate a single node dictionary."""
    return ContextNode.model_validate(data)


def get_json_schema() -> Dict[str, Any]:
    """Get JSON Schema representation for documentation."""
    return ContextGraph.model_json_schema()


if __name__ == "__main__":
    import json
    print(json.dumps(get_json_schema(), indent=2))
