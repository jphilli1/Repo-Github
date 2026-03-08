"""Selection engine — picks best candidates per (relationship, loan, doc_type, year)."""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from ..config import LDCBConfig
from ..models import (
    CandidateDisposition,
    CandidateRecord,
    DocumentType,
    SelectionRecord,
)

logger = logging.getLogger("ldcb.selection")


# ---------------------------------------------------------------------------
# Sort key for candidate ranking
# ---------------------------------------------------------------------------

def _candidate_sort_key(c: CandidateRecord, file_size: int, modified_time: float) -> tuple:
    """Tie-break order:
    non-draft → stronger score → adequate pages → latest mtime → larger size → cleaner name.
    """
    return (
        0 if not c.is_draft else 1,             # non-draft first
        -c.classification_score,                  # higher score first
        -(c.page_count or 0),                     # more pages first
        -modified_time,                           # newer first
        -file_size,                               # larger first
        len(c.normalized_basename),               # shorter name = cleaner
    )


class SelectionEngine:
    """For each (relationship, loan, doc_type) tuple, retain latest N years × top M candidates.

    Business rule:
      1. Find the latest document year available
      2. Retain that year + prior (retain_years - 1) years
      3. Within each retained year, rank and keep top_n
    """

    __slots__ = ("_retain_years", "_top_n")

    def __init__(self, config: LDCBConfig) -> None:
        self._retain_years = config.selection.retain_years
        self._top_n = config.selection.top_n_per_year

    def select(
        self,
        candidates: list[CandidateRecord],
        file_metadata: dict[str, tuple[int, float]],
        run_id: str,
    ) -> tuple[list[SelectionRecord], list[CandidateRecord]]:
        """Run selection on all candidates.

        Args:
            candidates: All qualified candidates.
            file_metadata: Map of candidate_id -> (file_size, modified_time).
            run_id: Current pipeline run ID.

        Returns:
            (selected, updated_candidates) — selections plus candidates with dispositions set.
        """
        selections: list[SelectionRecord] = []

        # Group by (relationship_canonical, loan_canonical, document_type)
        groups: dict[tuple[str, str, DocumentType], list[CandidateRecord]] = defaultdict(list)
        for c in candidates:
            key = (c.relationship_canonical, c.loan_canonical, c.document_type)
            groups[key].append(c)

        for group_key, group_candidates in groups.items():
            rel_canonical, loan_canonical, doc_type = group_key
            group_selections = self._select_group(
                group_candidates, file_metadata, rel_canonical, loan_canonical, doc_type, run_id
            )
            selections.extend(group_selections)

        logger.info(
            "Selection complete: %d candidates -> %d selected",
            len(candidates),
            len(selections),
        )
        return selections, candidates

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _select_group(
        self,
        candidates: list[CandidateRecord],
        file_metadata: dict[str, tuple[int, float]],
        rel_canonical: str,
        loan_canonical: str,
        doc_type: DocumentType,
        run_id: str,
    ) -> list[SelectionRecord]:
        """Select within a single (rel, loan, type) group."""
        selections: list[SelectionRecord] = []

        # Partition by year
        by_year: dict[int, list[CandidateRecord]] = defaultdict(list)
        no_year: list[CandidateRecord] = []

        for c in candidates:
            if c.document_year is not None:
                by_year[c.document_year].append(c)
            else:
                no_year.append(c)

        if not by_year and not no_year:
            return selections

        # Determine retained years
        if by_year:
            latest_year = max(by_year.keys())
            retained_years = set(
                range(latest_year - self._retain_years + 1, latest_year + 1)
            )
        else:
            retained_years = set()

        # Process each retained year
        for year in sorted(retained_years, reverse=True):
            year_candidates = by_year.get(year, [])
            if not year_candidates:
                continue

            # Sort by ranking key
            year_candidates.sort(
                key=lambda c: _candidate_sort_key(
                    c, *file_metadata.get(c.candidate_id, (0, 0.0))
                )
            )

            for rank, c in enumerate(year_candidates):
                if rank < self._top_n:
                    c.disposition = CandidateDisposition.SELECTED
                    sel_id = hashlib.md5(
                        f"{c.candidate_id}|{year}|{rank}".encode("utf-8")
                    ).hexdigest()
                    selections.append(SelectionRecord(
                        selection_id=sel_id,
                        candidate_id=c.candidate_id,
                        relationship_canonical=rel_canonical,
                        loan_canonical=loan_canonical,
                        document_type=doc_type,
                        document_year=year,
                        rank_in_year=rank + 1,
                        selection_reason=f"top_{rank + 1}_of_{len(year_candidates)}_in_{year}",
                        run_id=run_id,
                    ))
                else:
                    c.disposition = CandidateDisposition.RETAINED_AUDIT

        # Non-retained years → RETAINED_AUDIT
        for year, year_cands in by_year.items():
            if year not in retained_years:
                for c in year_cands:
                    c.disposition = CandidateDisposition.RETAINED_AUDIT

        # No-year candidates → REVIEW_REQUIRED
        for c in no_year:
            c.disposition = CandidateDisposition.REVIEW_REQUIRED

        return selections
