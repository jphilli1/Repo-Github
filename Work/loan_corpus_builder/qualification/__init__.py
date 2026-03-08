"""Folder qualification engine — identifies relationship and loan folders."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime
from pathlib import Path

from ..config import LDCBConfig, LDCBRules
from ..models import (
    FileRecord,
    FolderStatus,
    LoanRecord,
    RelationshipRecord,
    ReviewRecord,
)

logger = logging.getLogger("ldcb.qualification")


class FolderQualifier:
    """Determine whether folders represent relationships, loans, or skip targets.

    All pattern matching is driven by rules config — no inline business logic.
    """

    __slots__ = (
        "_config",
        "_rules",
        "_skip_folder_tokens",
        "_admin_folder_tokens",
        "_rel_patterns",
        "_loan_patterns",
    )

    def __init__(self, config: LDCBConfig, rules: LDCBRules) -> None:
        self._config = config
        self._rules = rules
        self._skip_folder_tokens = [t.lower() for t in rules.skip_markers_folder]
        self._admin_folder_tokens = [t.lower() for t in rules.skip_markers_admin]
        self._rel_patterns = [re.compile(p) for p in rules.relationship_patterns]
        self._loan_patterns = [re.compile(p) for p in rules.loan_patterns]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def qualify_folder(
        self,
        folder_path: Path,
        depth: int,
        run_id: str,
    ) -> tuple[
        list[RelationshipRecord],
        list[LoanRecord],
        list[ReviewRecord],
    ]:
        """Classify a single folder. Returns discovered relationships, loans, reviews."""
        relationships: list[RelationshipRecord] = []
        loans: list[LoanRecord] = []
        reviews: list[ReviewRecord] = []

        folder_name = folder_path.name
        folder_lower = folder_name.lower()

        # Check skip markers
        status, skip_reason = self._check_skip_status(folder_lower)

        if status == FolderStatus.SKIP:
            logger.debug("Skipping folder (admin): %s — %s", folder_path, skip_reason)
            return relationships, loans, reviews

        # Check relationship patterns
        qcfg = self._config.qualification
        if qcfg.relationship_folder_min_depth <= depth <= qcfg.relationship_folder_max_depth:
            if self._matches_relationship(folder_name):
                rel_id = hashlib.md5(str(folder_path).encode("utf-8")).hexdigest()
                relationships.append(RelationshipRecord(
                    relationship_id=rel_id,
                    raw_folder_name=folder_name,
                    canonical_name=self._normalize_name(folder_name),
                    folder_path=str(folder_path),
                    status=status if status != FolderStatus.SKIP else FolderStatus.INACTIVE,
                    skip_reason=skip_reason,
                    depth=depth,
                    run_id=run_id,
                ))
                logger.info(
                    "Relationship folder qualified: %s (status=%s)", folder_path, status.value
                )

        # Check loan patterns
        if qcfg.loan_folder_min_depth <= depth <= qcfg.loan_folder_max_depth:
            if self._matches_loan(folder_name):
                loan_id = hashlib.md5(str(folder_path).encode("utf-8")).hexdigest()
                # Try to find parent relationship
                rel_id = self._find_parent_relationship_id(folder_path)
                loans.append(LoanRecord(
                    loan_id=loan_id,
                    relationship_id=rel_id or "",
                    raw_folder_name=folder_name,
                    canonical_name=self._normalize_name(folder_name),
                    folder_path=str(folder_path),
                    status=status if status != FolderStatus.SKIP else FolderStatus.INACTIVE,
                    skip_reason=skip_reason,
                    depth=depth,
                    run_id=run_id,
                ))
                logger.info("Loan folder qualified: %s (status=%s)", folder_path, status.value)

        # Inactive folders that are not admin-skip go to review
        if status == FolderStatus.INACTIVE:
            review_id = hashlib.md5(
                f"review|{folder_path}|{skip_reason}".encode("utf-8")
            ).hexdigest()
            reviews.append(ReviewRecord(
                review_id=review_id,
                entity_type="folder",
                entity_id=hashlib.md5(str(folder_path).encode("utf-8")).hexdigest(),
                entity_path=str(folder_path),
                reason_code="STALE_ORPHAN",
                reason_detail=f"Folder matches skip marker: {skip_reason}",
                bucket="REVIEW_REQUIRED",
                run_id=run_id,
            ))

        return relationships, loans, reviews

    def qualify_file_context(
        self,
        file_record: FileRecord,
        relationships: dict[str, RelationshipRecord],
        loans: dict[str, LoanRecord],
    ) -> FileRecord:
        """Enrich a FileRecord with relationship_id and loan_id from qualified folders."""
        fp = Path(file_record.file_path)
        for ancestor in fp.parents:
            ancestor_key = hashlib.md5(str(ancestor).encode("utf-8")).hexdigest()
            if file_record.loan_id is None and ancestor_key in loans:
                file_record.loan_id = ancestor_key
            if file_record.relationship_id is None and ancestor_key in relationships:
                file_record.relationship_id = ancestor_key
        return file_record

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _check_skip_status(self, folder_lower: str) -> tuple[FolderStatus, str | None]:
        """Return (status, reason) based on skip/admin markers."""
        for token in self._admin_folder_tokens:
            if token in folder_lower:
                return FolderStatus.SKIP, f"admin_folder:{token}"

        for token in self._skip_folder_tokens:
            if token in folder_lower:
                return FolderStatus.INACTIVE, f"skip_marker:{token}"

        return FolderStatus.ACTIVE, None

    def _matches_relationship(self, folder_name: str) -> bool:
        return any(p.search(folder_name) for p in self._rel_patterns)

    def _matches_loan(self, folder_name: str) -> bool:
        return any(p.search(folder_name) for p in self._loan_patterns)

    def _find_parent_relationship_id(self, folder_path: Path) -> str | None:
        """Walk up parents looking for a relationship-pattern match."""
        for ancestor in folder_path.parents:
            if self._matches_relationship(ancestor.name):
                return hashlib.md5(str(ancestor).encode("utf-8")).hexdigest()
        return None

    @staticmethod
    def _normalize_name(raw: str) -> str:
        """Basic name normalization: strip numeric prefixes, collapse whitespace."""
        # Remove leading numeric IDs (e.g., "12345 - Acme Corp" -> "Acme Corp")
        cleaned = re.sub(r"^\d+[\s_-]+", "", raw)
        # Collapse separators
        cleaned = re.sub(r"[_-]+", " ", cleaned)
        # Collapse whitespace
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned
