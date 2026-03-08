"""Document classification engine — type identification and quality gating."""

from __future__ import annotations

import logging
import re
from pathlib import Path

from ..config import ClassificationConfig, LDCBConfig, LDCBRules, QualityGateConfig
from ..models import (
    CandidateRecord,
    DocumentType,
    FileRecord,
    QualityGateResult,
    ReviewRecord,
    generate_document_id,
)

logger = logging.getLogger("ldcb.classification")


class DocumentClassifier:
    """Classify files into document types using config-driven pattern matching.

    Classification is a two-pass process:
      1. Filename pattern scoring (cheap)
      2. Folder context boosting (cheap)

    AR weak-evidence rule: 'AR' alone is insufficient without folder context.
    """

    __slots__ = ("_config", "_rules", "_cls_config", "_draft_markers", "_year_patterns")

    def __init__(self, config: LDCBConfig, rules: LDCBRules) -> None:
        self._config = config
        self._rules = rules
        self._cls_config = config.classification
        self._draft_markers = [m.lower() for m in rules.draft_suppression_markers]
        self._year_patterns = [re.compile(p) for p in rules.year_extraction_patterns]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify(
        self,
        file_record: FileRecord,
        relationship_canonical: str,
        loan_canonical: str,
        run_id: str,
    ) -> CandidateRecord | None:
        """Classify a file and return CandidateRecord if above threshold, else None."""
        fname = file_record.file_name
        fpath = Path(file_record.file_path)
        fname_lower = fname.lower()

        best_type = DocumentType.UNKNOWN
        best_score = 0.0
        best_signals: list[str] = []

        for type_key, type_rule in self._rules.document_types.items():
            score, signals = self._score_document_type(
                fname_lower, fpath, type_rule.filename_patterns,
                type_rule.folder_context_patterns, type_rule.weights,
                type_rule.weak_tokens,
            )
            if score > best_score:
                best_score = score
                best_type = DocumentType(type_key) if type_key in DocumentType.__members__.values() else self._resolve_doc_type(type_key)
                best_signals = signals

        # Normalize score to 0-1 range
        normalized_score = min(best_score / 10.0, 1.0) if best_score > 0 else 0.0

        # Check threshold
        if normalized_score < self._cls_config.min_confidence_threshold:
            return None

        # Draft detection
        is_draft, draft_markers = self._detect_draft(fname_lower)

        # Year extraction
        doc_year = self._extract_year(fname, fpath)

        # Build candidate
        candidate_id = generate_document_id(
            fpath, file_record.file_size, file_record.modified_time
        )

        return CandidateRecord(
            candidate_id=candidate_id,
            file_id=file_record.file_id,
            relationship_id=file_record.relationship_id or "",
            loan_id=file_record.loan_id or "",
            relationship_canonical=relationship_canonical,
            loan_canonical=loan_canonical,
            document_type=best_type,
            classification_score=normalized_score,
            classification_signals="; ".join(best_signals),
            document_year=doc_year,
            is_draft=is_draft,
            draft_markers_found=", ".join(draft_markers),
            source_path=str(fpath),
            normalized_basename=self._normalize_basename(fname),
            run_id=run_id,
        )

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score_document_type(
        self,
        fname_lower: str,
        file_path: Path,
        filename_patterns: list[str],
        folder_patterns: list[str],
        weights: dict[str, int],
        weak_tokens: list[str],
    ) -> tuple[float, list[str]]:
        """Score a file against one document type definition."""
        score = 0.0
        signals: list[str] = []

        # Pass 1: filename pattern matching
        filename_hit = False
        weak_hit = False
        for pattern_str in filename_patterns:
            pattern = re.compile(pattern_str)
            if pattern.search(fname_lower):
                # Check if this is a weak token match
                is_weak = any(
                    re.fullmatch(rf"(?i)\b{re.escape(wt)}\b", pattern_str.replace("(?i)\\b", "").replace("\\b", ""))
                    for wt in weak_tokens
                )
                if is_weak:
                    # Check if ONLY the weak token matched
                    weak_hit = True
                    base_score = weights.get("filename_weak_ar", 2)
                    score += base_score
                    signals.append(f"filename_weak:{pattern_str}={base_score}")
                else:
                    filename_hit = True
                    base_score = weights.get("filename_strong", 5)
                    score += base_score
                    signals.append(f"filename_strong:{pattern_str}={base_score}")
                break  # Only count best filename match

        # Pass 2: folder context
        folder_hit = False
        parent_path_lower = str(file_path.parent).lower()
        for pattern_str in folder_patterns:
            pattern = re.compile(pattern_str)
            if pattern.search(parent_path_lower):
                folder_hit = True
                ctx_score = weights.get("folder_context", 3)
                score += ctx_score
                signals.append(f"folder_context:{pattern_str}={ctx_score}")
                break

        # AR weak evidence rule: weak token alone penalized unless folder supports
        if weak_hit and not filename_hit and not folder_hit:
            if self._cls_config.ar_requires_supporting_signal:
                penalty = self._cls_config.ar_weak_evidence_penalty
                score *= penalty
                signals.append(f"ar_weak_penalty:{penalty}")
        elif weak_hit and folder_hit:
            # Boost: weak filename + folder context = strong combined signal
            combined = weights.get("combined_weak_ar_plus_folder", 5)
            score = max(score, combined)
            signals.append(f"combined_weak_plus_folder={combined}")

        return score, signals

    # ------------------------------------------------------------------
    # Draft detection
    # ------------------------------------------------------------------

    def _detect_draft(self, fname_lower: str) -> tuple[bool, list[str]]:
        """Check filename for draft suppression markers."""
        found: list[str] = []
        for marker in self._draft_markers:
            if marker in fname_lower:
                found.append(marker)
        return len(found) > 0, found

    # ------------------------------------------------------------------
    # Year extraction
    # ------------------------------------------------------------------

    def _extract_year(self, filename: str, file_path: Path) -> int | None:
        """Extract document year from filename or parent folder names."""
        # Try filename first
        for pattern in self._year_patterns:
            match = pattern.search(filename)
            if match:
                return int(match.group(1))

        # Try parent folder names (up to 3 levels)
        for i, ancestor in enumerate(file_path.parents):
            if i >= 3:
                break
            for pattern in self._year_patterns:
                match = pattern.search(ancestor.name)
                if match:
                    return int(match.group(1))

        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_doc_type(type_key: str) -> DocumentType:
        """Map config type keys to DocumentType enum."""
        mapping = {
            "annual_review": DocumentType.ANNUAL_REVIEW,
            "loan_modification": DocumentType.LOAN_MODIFICATION,
            "credit_memo": DocumentType.CREDIT_MEMO,
        }
        return mapping.get(type_key, DocumentType.UNKNOWN)

    @staticmethod
    def _normalize_basename(filename: str) -> str:
        """Normalize a filename for deduplication comparison."""
        name = Path(filename).stem.lower()
        # Remove common noise tokens
        name = re.sub(r"[\s_-]+", " ", name)
        name = re.sub(r"\bcopy\s*\d*\b", "", name)
        name = re.sub(r"\(\d+\)", "", name)
        return name.strip()


class QualityGate:
    """Apply quality checks to candidates. Failures go to quarantine."""

    __slots__ = ("_config", "_qg_config", "_rules")

    def __init__(self, config: LDCBConfig, rules: LDCBRules) -> None:
        self._config = config
        self._qg_config = config.quality_gate
        self._rules = rules

    def evaluate(self, candidate: CandidateRecord, file_size: int) -> tuple[QualityGateResult, str]:
        """Return (result, notes) for a candidate."""
        # Size check
        if file_size < self._qg_config.min_meaningful_size_bytes:
            return QualityGateResult.FAIL_SIZE, f"File size {file_size} below minimum {self._qg_config.min_meaningful_size_bytes}"

        # Page count check (if available)
        if candidate.page_count is not None:
            if candidate.page_count < self._qg_config.min_page_count:
                return QualityGateResult.FAIL_PAGES, f"Page count {candidate.page_count} below minimum {self._qg_config.min_page_count}"
            if candidate.page_count > self._qg_config.max_page_count:
                return QualityGateResult.FAIL_PAGES, f"Page count {candidate.page_count} above maximum {self._qg_config.max_page_count}"

        # Draft penalty (does not fail, but adjusts score)
        if candidate.is_draft:
            penalty = self._qg_config.draft_penalty
            adjusted = candidate.classification_score * (1.0 - penalty)
            return QualityGateResult.PASS, f"Draft penalty applied: {candidate.classification_score:.3f} -> {adjusted:.3f}"

        return QualityGateResult.PASS, "All quality checks passed"
