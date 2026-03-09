"""Corpus builder / copy engine — copies selected files to canonical destination."""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

from ..config import LDCBConfig
from ..models import (
    CandidateRecord,
    CopyRecord,
    FileManifest,
    SelectionRecord,
    generate_content_hash,
    generate_lineage_trace,
)

logger = logging.getLogger("ldcb.corpus")


class CorpusBuilder:
    """Copy selected files into the canonical corpus structure.

    Target layout:
      <target_root>/<relationship_canonical>/<loan_canonical>/<document_type>/<year>/<file>

    Guarantees:
      - Never mutates source tree
      - Collision-safe renaming
      - Manifest JSON per copied file
      - Content hash verification post-copy
    """

    __slots__ = ("_target_root", "_config", "_manifest_dir")

    def __init__(self, config: LDCBConfig) -> None:
        self._config = config
        self._target_root = Path(config.paths.target_root)
        self._manifest_dir = Path(config.paths.manifest_dir)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        selections: list[SelectionRecord],
        candidates_by_id: dict[str, CandidateRecord],
        run_id: str,
    ) -> list[CopyRecord]:
        """Execute corpus copy for all selections. Returns CopyRecord audit trail."""
        self._target_root.mkdir(parents=True, exist_ok=True)
        self._manifest_dir.mkdir(parents=True, exist_ok=True)

        copy_records: list[CopyRecord] = []

        # GAP-004: tqdm progress bar for copy loop
        sel_iter = selections
        try:
            from tqdm import tqdm
            sel_iter = tqdm(selections, desc="Copying to corpus", unit="file")
        except ImportError:
            pass

        for sel in sel_iter:
            candidate = candidates_by_id.get(sel.candidate_id)
            if candidate is None:
                logger.error("Selection %s references unknown candidate %s", sel.selection_id, sel.candidate_id)
                continue

            record = self._copy_file(sel, candidate, run_id)
            copy_records.append(record)

        logger.info("Corpus build complete: %d files copied", sum(1 for r in copy_records if r.copy_success))
        return copy_records

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _copy_file(
        self,
        selection: SelectionRecord,
        candidate: CandidateRecord,
        run_id: str,
    ) -> CopyRecord:
        """Copy a single file and produce audit records."""
        source = Path(candidate.source_path)
        copy_id = hashlib.md5(
            f"{selection.selection_id}|{source}".encode("utf-8")
        ).hexdigest()

        # Build destination path
        dest_dir = (
            self._target_root
            / selection.relationship_canonical
            / selection.loan_canonical
            / selection.document_type.value
            / str(selection.document_year)
        )
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest_file = dest_dir / source.name
        collision_renamed = False

        # Collision-safe rename
        if dest_file.exists():
            collision_renamed = True
            stem = source.stem
            suffix = source.suffix
            counter = 1
            while dest_file.exists():
                dest_file = dest_dir / f"{stem}_{counter}{suffix}"
                counter += 1
            logger.info("Collision rename: %s -> %s", source.name, dest_file.name)

        # Compute source hash before copy
        try:
            hash_before = generate_content_hash(source)
        except (OSError, PermissionError) as exc:
            logger.error("Cannot read source for hashing: %s — %s", source, exc)
            return CopyRecord(
                copy_id=copy_id,
                selection_id=selection.selection_id,
                candidate_id=candidate.candidate_id,
                source_path=str(source),
                destination_path=str(dest_file),
                content_hash_before="",
                copy_success=False,
                error_message=str(exc),
                run_id=run_id,
            )

        # Execute copy
        try:
            shutil.copy2(str(source), str(dest_file))
        except (OSError, PermissionError, shutil.SameFileError) as exc:
            logger.error("Copy failed: %s -> %s — %s", source, dest_file, exc)
            return CopyRecord(
                copy_id=copy_id,
                selection_id=selection.selection_id,
                candidate_id=candidate.candidate_id,
                source_path=str(source),
                destination_path=str(dest_file),
                content_hash_before=hash_before,
                copy_success=False,
                error_message=str(exc),
                run_id=run_id,
            )

        # Verify copy
        try:
            hash_after = generate_content_hash(dest_file)
        except (OSError, PermissionError) as exc:
            logger.error("Cannot verify copy hash: %s — %s", dest_file, exc)
            hash_after = ""

        copy_success = hash_before == hash_after and hash_after != ""

        if not copy_success:
            logger.error(
                "Hash mismatch after copy: %s (before=%s, after=%s)",
                dest_file, hash_before, hash_after,
            )

        # Write per-file manifest
        if self._config.corpus.generate_manifest:
            self._write_manifest(selection, candidate, hash_before, dest_file, run_id)

        record = CopyRecord(
            copy_id=copy_id,
            selection_id=selection.selection_id,
            candidate_id=candidate.candidate_id,
            source_path=str(source),
            destination_path=str(dest_file),
            content_hash_before=hash_before,
            content_hash_after=hash_after,
            copy_success=copy_success,
            collision_renamed=collision_renamed,
            run_id=run_id,
        )

        logger.info(
            "Copied %s -> %s (verified=%s)", source.name, dest_file, copy_success
        )
        return record

    def _write_manifest(
        self,
        selection: SelectionRecord,
        candidate: CandidateRecord,
        content_hash: str,
        dest_file: Path,
        run_id: str,
    ) -> None:
        """Write a JSON manifest file alongside the copied document."""
        lineage = generate_lineage_trace(
            candidate.source_path,
            0,  # size available from file_metadata if needed
            0.0,  # mtime available from file_metadata if needed
            f"selected:rank={selection.rank_in_year}:year={selection.document_year}",
        )

        manifest = FileManifest(
            source_path=candidate.source_path,
            destination_path=str(dest_file),
            content_hash=content_hash,
            document_id=candidate.candidate_id,
            relationship_canonical=selection.relationship_canonical,
            loan_canonical=selection.loan_canonical,
            document_type=selection.document_type.value,
            document_year=selection.document_year,
            classification_score=candidate.classification_score,
            is_draft=candidate.is_draft,
            page_count=candidate.page_count,
            quality_gate=candidate.quality_gate.value,
            selection_rank=selection.rank_in_year,
            lineage_trace=lineage,
            run_id=run_id,
            copied_at=datetime.utcnow().isoformat(),
        )

        manifest_file = dest_file.with_suffix(dest_file.suffix + ".manifest.json")
        with open(manifest_file, "w", encoding="utf-8") as f:
            json.dump(manifest.model_dump(), f, indent=2, default=str)

        logger.debug("Manifest written: %s", manifest_file)
