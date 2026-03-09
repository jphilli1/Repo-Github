"""Pydantic v2 models for all LDCB registry entities and pipeline data contracts."""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class FolderStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    SKIP = "SKIP"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class DocumentType(str, Enum):
    ANNUAL_REVIEW = "annual_review"
    LOAN_MODIFICATION = "loan_modification"
    CREDIT_MEMO = "credit_memo"
    UNKNOWN = "unknown"


class CandidateDisposition(str, Enum):
    SELECTED = "SELECTED"
    RETAINED_AUDIT = "RETAINED_AUDIT"
    QUARANTINED = "QUARANTINED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"


class QualityGateResult(str, Enum):
    PASS = "PASS"
    FAIL_SIZE = "FAIL_SIZE"
    FAIL_PAGES = "FAIL_PAGES"
    FAIL_DRAFT = "FAIL_DRAFT"
    FAIL_CORRUPTED = "FAIL_CORRUPTED"


# ---------------------------------------------------------------------------
# Deterministic ID helpers
# ---------------------------------------------------------------------------

def generate_document_id(file_path: Path, file_size: int, modified_time: float) -> str:
    """MD5 hash of path + size + mtime for stable document identity."""
    composite = f"{file_path.as_posix()}|{file_size}|{modified_time}"
    return hashlib.md5(composite.encode("utf-8")).hexdigest()


def generate_content_hash(file_path: Path) -> str:
    """MD5 hash of actual file bytes for duplicate detection."""
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def generate_lineage_trace(
    source_path: str,
    file_size: int,
    modified_time: float,
    reason: str,
) -> str:
    """SHA-256 lineage trace from source attributes + decision reason."""
    trace_key = f"{source_path}|{file_size}|{modified_time}|{reason}"
    return hashlib.sha256(trace_key.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Registry models
# ---------------------------------------------------------------------------

class RelationshipRecord(BaseModel):
    """A discovered relationship (client/borrower) folder."""
    relationship_id: str
    raw_folder_name: str
    canonical_name: str
    folder_path: str
    status: FolderStatus = FolderStatus.ACTIVE
    skip_reason: Optional[str] = None
    depth: int = 0
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: str = ""


class LoanRecord(BaseModel):
    """A discovered loan folder under a relationship."""
    loan_id: str
    relationship_id: str
    raw_folder_name: str
    canonical_name: str
    folder_path: str
    status: FolderStatus = FolderStatus.ACTIVE
    skip_reason: Optional[str] = None
    depth: int = 0
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: str = ""


class FileRecord(BaseModel):
    """Raw file discovered during traversal."""
    file_id: str
    file_path: str
    file_name: str
    file_extension: str
    file_size: int
    modified_time: float
    content_hash: Optional[str] = None
    relationship_id: Optional[str] = None
    loan_id: Optional[str] = None
    depth: int = 0
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: str = ""


class CandidateRecord(BaseModel):
    """A file that passed classification and quality gates."""
    candidate_id: str
    file_id: str
    relationship_id: str
    loan_id: str
    relationship_canonical: str
    loan_canonical: str
    document_type: DocumentType = DocumentType.UNKNOWN
    classification_score: float = 0.0
    classification_signals: str = ""
    document_year: Optional[int] = None
    is_draft: bool = False
    draft_markers_found: str = ""
    page_count: Optional[int] = None
    quality_gate: QualityGateResult = QualityGateResult.PASS
    quality_notes: str = ""
    disposition: CandidateDisposition = CandidateDisposition.RETAINED_AUDIT
    source_path: str = ""
    normalized_basename: str = ""
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: str = ""



class SelectionRecord(BaseModel):
    """A candidate that was selected for corpus inclusion."""
    selection_id: str
    candidate_id: str
    relationship_canonical: str
    loan_canonical: str
    document_type: DocumentType
    document_year: int
    rank_in_year: int
    selection_reason: str
    run_id: str = ""
    selected_at: datetime = Field(default_factory=datetime.utcnow)


class CopyRecord(BaseModel):
    """Audit trail for a file copied to the corpus."""
    copy_id: str
    selection_id: str
    candidate_id: str
    source_path: str
    destination_path: str
    content_hash_before: str
    content_hash_after: str = ""
    copy_success: bool = False
    collision_renamed: bool = False
    error_message: Optional[str] = None
    copied_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: str = ""


class ReviewRecord(BaseModel):
    """Record for files/folders sent to REVIEW_REQUIRED or QUARANTINE."""
    review_id: str
    entity_type: str  # "file", "folder", "candidate"
    entity_id: str
    entity_path: str
    reason_code: str
    reason_detail: str
    bucket: str  # "REVIEW_REQUIRED" or "QUARANTINE"
    resolved: bool = False
    resolution_note: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    run_id: str = ""


class FileManifest(BaseModel):
    """Per-file manifest written alongside each copied file."""
    source_path: str
    destination_path: str
    content_hash: str
    document_id: str
    relationship_canonical: str
    loan_canonical: str
    document_type: str
    document_year: int
    classification_score: float
    is_draft: bool
    page_count: Optional[int] = None
    quality_gate: str
    selection_rank: int
    lineage_trace: str
    run_id: str
    copied_at: str


class RunManifest(BaseModel):
    """Per-run summary manifest."""
    run_id: str
    started_at: str
    completed_at: Optional[str] = None
    source_roots: list[str]
    target_root: str
    total_files_discovered: int = 0
    total_candidates: int = 0
    total_selected: int = 0
    total_copied: int = 0
    total_quarantined: int = 0
    total_review_required: int = 0
    config_snapshot: dict = Field(default_factory=dict)
