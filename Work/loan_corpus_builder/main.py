"""LDCB Pipeline CLI — orchestrates the full discovery-to-corpus workflow.

Usage:
    python -m loan_corpus_builder.main --config data/config.json --rules data/rules.json
    python -m loan_corpus_builder.main --source-root /mnt/share/loans --target-root ./corpus
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from .adapters import ParquetExporter, SQLiteRegistry
from .classification import DocumentClassifier, QualityGate
from .config import LDCBConfig, LDCBRules, load_config, load_rules
from .corpus import CorpusBuilder
from .logging_utils import setup_logging
from .mapping import CanonicalMapper
from .models import (
    CandidateDisposition,
    CandidateRecord,
    FileRecord,
    RelationshipRecord,
    LoanRecord,
    ReviewRecord,
    RunManifest,
    generate_content_hash,
    generate_document_id,
)
from .qualification import FolderQualifier
from .selection import SelectionEngine
from .traversal import TraversalEngine

logger = logging.getLogger("ldcb.main")


def generate_run_id() -> str:
    """Generate a stable run ID from current timestamp."""
    ts = datetime.utcnow().isoformat()
    return hashlib.md5(ts.encode("utf-8")).hexdigest()[:12]


def _try_import_tqdm():
    """Import tqdm if available, else return a no-op passthrough."""
    try:
        from tqdm import tqdm
        return tqdm
    except ImportError:
        return None


def run_pipeline(
    config: LDCBConfig,
    rules: LDCBRules,
    run_id: str,
    source_roots: list[Path] | None = None,
    dry_run: bool = False,
) -> RunManifest:
    """Execute the full LDCB pipeline.

    Stages:
      1. Traversal — discover files via TraversalEngine (BUG-002 fix)
      2. Folder Qualification — identify relationships & loans
      3. Document Classification — type and score each file
      3b. Content-hash deduplication (GAP-002 fix)
      4. Quality Gate — filter unfit candidates
      5. Selection — pick best per (rel, loan, type, year)
      6. Corpus Build — copy selected files (or dry-run summary) (GAP-001 fix)
      7. Persist — write all records to SQLite + manifests
    """
    started_at = datetime.utcnow().isoformat()
    effective_roots = source_roots or [Path(r) for r in config.paths.source_roots]

    logger.info("=== LDCB Pipeline Run %s ===", run_id)
    logger.info("Source roots: %s", effective_roots)
    logger.info("Target root: %s", config.paths.target_root)
    if dry_run:
        logger.info("DRY-RUN mode — no files will be copied")

    # Initialize components
    registry = SQLiteRegistry(Path(config.paths.sqlite_db))
    traversal = TraversalEngine(config)
    qualifier = FolderQualifier(config, rules)

    data_dir = Path(__file__).parent / "data"
    rel_alias_path = data_dir / "relationship_aliases.csv"
    loan_alias_path = data_dir / "loan_aliases.csv"
    mapper = CanonicalMapper(
        config,
        relationship_alias_path=rel_alias_path if rel_alias_path.exists() else None,
        loan_alias_path=loan_alias_path if loan_alias_path.exists() else None,
    )

    classifier = DocumentClassifier(config, rules)
    quality_gate = QualityGate(config, rules)
    selector = SelectionEngine(config)
    builder = CorpusBuilder(config)

    # Counters
    total_files = 0
    total_candidates = 0
    total_quarantined = 0
    total_review = 0

    # Accumulators
    all_candidates: list[CandidateRecord] = []
    all_reviews: list[ReviewRecord] = []
    file_metadata: dict[str, tuple[int, float]] = {}  # candidate_id -> (size, mtime)
    candidates_by_id: dict[str, CandidateRecord] = {}

    # Relationship and loan registries — properly typed (BUG-003 fix)
    relationships: dict[str, RelationshipRecord] = {}
    loans: dict[str, LoanRecord] = {}

    # GAP-005: Incremental run support — load existing file IDs to skip re-processing
    existing_file_ids: set[str] = set()
    try:
        existing_file_ids = registry.get_existing_file_ids(run_id)
        if existing_file_ids:
            logger.info("Incremental run: %d files already registered, will skip", len(existing_file_ids))
    except (OSError, ValueError) as exc:
        logger.debug("Could not load existing file IDs: %s", exc)

    # GAP-004: tqdm progress reporting
    tqdm_cls = _try_import_tqdm()

    # ------------------------------------------------------------------
    # Stage 1: Traversal + Qualification (BUG-002 fix: uses TraversalEngine)
    # ------------------------------------------------------------------
    logger.info("--- Stage 1: Traversal & Qualification ---")

    # Collect all FileRecords from TraversalEngine first, then process
    discovered_files: list[FileRecord] = []

    for root in effective_roots:
        if not root.exists():
            logger.warning("Source root does not exist, skipping: %s", root)
            continue

        for file_rec in traversal.scan(root, run_id):
            # GAP-005: skip files already registered in this run
            if file_rec.file_id in existing_file_ids:
                logger.debug("Skipping already-registered file: %s", file_rec.file_path)
                continue
            discovered_files.append(file_rec)

    # Qualify folders and enrich files — with progress bar (GAP-004)
    qualified_folders: set[str] = set()
    file_iter = discovered_files
    if tqdm_cls is not None:
        file_iter = tqdm_cls(discovered_files, desc="Qualifying & classifying", unit="file")

    for file_rec in file_iter:
        total_files += 1
        fpath = Path(file_rec.file_path)

        # Qualify each ancestor folder (only once per unique folder)
        for ancestor in fpath.parents:
            ancestor_str = str(ancestor)
            if ancestor_str in qualified_folders:
                continue
            qualified_folders.add(ancestor_str)

            # Compute depth relative to whichever source root contains this file
            depth = 0
            for root in effective_roots:
                try:
                    ancestor.relative_to(root)
                    depth = len(ancestor.parts) - len(root.parts)
                    break
                except ValueError:
                    continue

            rels, lns, revs = qualifier.qualify_folder(ancestor, depth, run_id)
            for r in rels:
                relationships[r.relationship_id] = r
                registry.upsert_relationship(r)
            for ln in lns:
                loans[ln.loan_id] = ln
                registry.upsert_loan(ln)
            for rv in revs:
                all_reviews.append(rv)
                registry.upsert_review(rv)

        # Enrich file with relationship/loan context (BUG-003: properly typed dicts)
        file_rec = qualifier.qualify_file_context(file_rec, relationships, loans)
        registry.upsert_file(file_rec)

        # ----------------------------------------------------------
        # Stage 2: Classification
        # ----------------------------------------------------------
        rel_canonical = ""
        loan_canonical = ""

        if file_rec.relationship_id and file_rec.relationship_id in relationships:
            rel_obj = relationships[file_rec.relationship_id]
            rel_canonical = mapper.resolve_relationship(rel_obj.raw_folder_name)

        if file_rec.loan_id and file_rec.loan_id in loans:
            loan_obj = loans[file_rec.loan_id]
            loan_canonical = mapper.resolve_loan(loan_obj.raw_folder_name)

        candidate = classifier.classify(
            file_rec, rel_canonical, loan_canonical, run_id
        )

        if candidate is None:
            # Below classification threshold — quarantine
            review_id = hashlib.md5(
                f"unclassified|{fpath}".encode("utf-8")
            ).hexdigest()
            review = ReviewRecord(
                review_id=review_id,
                entity_type="file",
                entity_id=file_rec.file_id,
                entity_path=str(fpath),
                reason_code="UNCLASSIFIED",
                reason_detail="Below classification confidence threshold",
                bucket="QUARANTINE",
                run_id=run_id,
            )
            all_reviews.append(review)
            registry.upsert_review(review)
            total_quarantined += 1
            continue

        # GAP-003: Populate page count from file metadata
        if candidate.page_count is None:
            candidate.page_count = _extract_page_count(fpath)

        # ----------------------------------------------------------
        # Stage 3: Quality Gate
        # ----------------------------------------------------------
        qg_result, qg_notes = quality_gate.evaluate(candidate, file_rec.file_size)
        candidate.quality_gate = qg_result
        candidate.quality_notes = qg_notes

        if qg_result.value.startswith("FAIL"):
            candidate.disposition = CandidateDisposition.QUARANTINED
            review_id = hashlib.md5(
                f"quality|{fpath}|{qg_result.value}".encode("utf-8")
            ).hexdigest()
            review = ReviewRecord(
                review_id=review_id,
                entity_type="candidate",
                entity_id=candidate.candidate_id,
                entity_path=str(fpath),
                reason_code=qg_result.value,
                reason_detail=qg_notes,
                bucket="QUARANTINE",
                run_id=run_id,
            )
            all_reviews.append(review)
            registry.upsert_review(review)
            total_quarantined += 1

        registry.upsert_candidate(candidate)
        all_candidates.append(candidate)
        file_metadata[candidate.candidate_id] = (file_rec.file_size, file_rec.modified_time)
        candidates_by_id[candidate.candidate_id] = candidate
        total_candidates += 1

    registry.commit()
    logger.info(
        "Discovery complete: %d files, %d candidates, %d quarantined",
        total_files, total_candidates, total_quarantined,
    )

    # ------------------------------------------------------------------
    # Stage 3b: Content-hash deduplication (GAP-002 fix)
    # ------------------------------------------------------------------
    logger.info("--- Stage 3b: Content-Hash Deduplication ---")

    passing = [c for c in all_candidates if not c.quality_gate.value.startswith("FAIL")]
    passing, dedup_reviews = _deduplicate_by_content_hash(
        passing, file_metadata, run_id
    )
    for rv in dedup_reviews:
        all_reviews.append(rv)
        registry.upsert_review(rv)
    total_quarantined += len(dedup_reviews)
    registry.commit()
    logger.info("After dedup: %d passing candidates (%d duplicates removed)", len(passing), len(dedup_reviews))

    # ------------------------------------------------------------------
    # Stage 4: Selection
    # ------------------------------------------------------------------
    logger.info("--- Stage 4: Selection ---")

    selections, updated_candidates = selector.select(passing, file_metadata, run_id)

    for sel in selections:
        registry.upsert_selection(sel)

    # Update candidate dispositions
    for c in updated_candidates:
        registry.upsert_candidate(c)

    # Review-required candidates (no year)
    for c in all_candidates:
        if c.disposition == CandidateDisposition.REVIEW_REQUIRED:
            review_id = hashlib.md5(
                f"no_year|{c.source_path}".encode("utf-8")
            ).hexdigest()
            review = ReviewRecord(
                review_id=review_id,
                entity_type="candidate",
                entity_id=c.candidate_id,
                entity_path=c.source_path,
                reason_code="NO_YEAR",
                reason_detail="Document year could not be determined",
                bucket="REVIEW_REQUIRED",
                run_id=run_id,
            )
            all_reviews.append(review)
            registry.upsert_review(review)
            total_review += 1

    registry.commit()
    logger.info("Selection complete: %d selected from %d passing candidates", len(selections), len(passing))

    # ------------------------------------------------------------------
    # Stage 5: Corpus Build (GAP-001 fix: dry-run gate)
    # ------------------------------------------------------------------
    logger.info("--- Stage 5: Corpus Build ---")

    total_copied = 0

    if dry_run:
        # GAP-001: Emit selection summary instead of copying
        logger.info("DRY-RUN: skipping file copy. Selection summary:")
        for sel in selections:
            cand = candidates_by_id.get(sel.candidate_id)
            src = cand.source_path if cand else "UNKNOWN"
            logger.info(
                "  WOULD COPY: %s -> %s/%s/%s/%d/ (rank %d)",
                src,
                sel.relationship_canonical,
                sel.loan_canonical,
                sel.document_type.value,
                sel.document_year,
                sel.rank_in_year,
            )
        # Print dry-run summary to stdout
        print(f"\nDRY-RUN: {len(selections)} files would be copied. No files were modified.")
    else:
        # Wrap copy loop with progress bar (GAP-004)
        copy_records = builder.build(selections, candidates_by_id, run_id)
        for cr in copy_records:
            registry.upsert_copy(cr)

        total_copied = sum(1 for cr in copy_records if cr.copy_success)
        logger.info("Corpus build: %d files copied successfully", total_copied)

    registry.commit()

    # ------------------------------------------------------------------
    # Stage 6: Finalize
    # ------------------------------------------------------------------
    logger.info("--- Stage 6: Finalize ---")

    manifest = RunManifest(
        run_id=run_id,
        started_at=started_at,
        completed_at=datetime.utcnow().isoformat(),
        source_roots=[str(r) for r in effective_roots],
        target_root=config.paths.target_root,
        total_files_discovered=total_files,
        total_candidates=total_candidates,
        total_selected=len(selections),
        total_copied=total_copied,
        total_quarantined=total_quarantined,
        total_review_required=total_review,
    )
    registry.upsert_run(manifest)
    registry.commit()

    # Write run manifest JSON
    manifest_dir = Path(config.paths.manifest_dir)
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / f"run_{run_id}.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest.model_dump(), f, indent=2, default=str)
    logger.info("Run manifest written: %s", manifest_path)

    # Optional Parquet export
    try:
        exporter = ParquetExporter(Path(config.paths.parquet_dir))
        exported = exporter.export_from_sqlite(Path(config.paths.sqlite_db), run_id)
        if exported:
            logger.info("Parquet exports: %s", [str(p) for p in exported])
    except (ImportError, ValueError) as exc:
        logger.debug("Parquet export skipped: %s", exc)

    # Print summary
    stats = registry.get_run_stats(run_id)
    registry.close()

    logger.info("=== Pipeline Complete ===")
    logger.info("Run ID: %s", run_id)
    logger.info("Files discovered: %d", total_files)
    logger.info("Candidates: %d", total_candidates)
    logger.info("Selected: %d", len(selections))
    logger.info("Copied: %d", total_copied)
    logger.info("Quarantined: %d", total_quarantined)
    logger.info("Review required: %d", total_review)
    logger.info("Registry stats: %s", stats)

    return manifest


# ---------------------------------------------------------------------------
# GAP-002: Content-hash deduplication
# ---------------------------------------------------------------------------

def _deduplicate_by_content_hash(
    candidates: list[CandidateRecord],
    file_metadata: dict[str, tuple[int, float]],
    run_id: str,
) -> tuple[list[CandidateRecord], list[ReviewRecord]]:
    """Remove duplicate files (same content hash) keeping the best-ranked candidate.

    Groups candidates by content hash. Within each group, keeps the candidate
    with the best classification score; remaining duplicates get QUARANTINED
    with a structured review record.
    """
    reviews: list[ReviewRecord] = []

    # Compute content hashes — group candidates by hash
    hash_groups: dict[str, list[CandidateRecord]] = defaultdict(list)
    unhashable: list[CandidateRecord] = []

    for c in candidates:
        try:
            content_hash = generate_content_hash(Path(c.source_path))
            hash_groups[content_hash].append(c)
        except (OSError, PermissionError) as exc:
            logger.debug("Cannot hash %s for dedup: %s", c.source_path, exc)
            unhashable.append(c)

    deduped: list[CandidateRecord] = list(unhashable)

    for content_hash, group in hash_groups.items():
        if len(group) == 1:
            deduped.append(group[0])
            continue

        # Sort: best candidate first (non-draft, higher score, larger file)
        group.sort(key=lambda c: (
            0 if not c.is_draft else 1,
            -c.classification_score,
            -file_metadata.get(c.candidate_id, (0, 0.0))[0],
        ))

        # Keep the best, quarantine the rest
        deduped.append(group[0])
        for dup in group[1:]:
            dup.disposition = CandidateDisposition.QUARANTINED
            review_id = hashlib.md5(
                f"dedup|{content_hash}|{dup.source_path}".encode("utf-8")
            ).hexdigest()
            reviews.append(ReviewRecord(
                review_id=review_id,
                entity_type="candidate",
                entity_id=dup.candidate_id,
                entity_path=dup.source_path,
                reason_code="DUPLICATE_CONTENT",
                reason_detail=f"Content hash {content_hash} matches {group[0].source_path}",
                bucket="QUARANTINE",
                run_id=run_id,
            ))
            logger.info(
                "Dedup: %s is duplicate of %s (hash=%s)",
                dup.source_path, group[0].source_path, content_hash[:12],
            )

    return deduped, reviews


# ---------------------------------------------------------------------------
# GAP-003: Page count extraction
# ---------------------------------------------------------------------------

def _extract_page_count(file_path: Path) -> int | None:
    """Extract page count from PDF or DOCX files using lightweight readers.

    - PDF: uses PyPDF2 (PdfReader) if available
    - DOCX: uses python-docx paragraph count as proxy if available

    Returns None if the library is unavailable or extraction fails.
    """
    ext = file_path.suffix.lower()

    if ext == ".pdf":
        return _extract_pdf_page_count(file_path)
    elif ext in (".docx",):
        return _extract_docx_page_count(file_path)

    return None


def _extract_pdf_page_count(file_path: Path) -> int | None:
    """Extract page count from a PDF using PyPDF2."""
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        try:
            from pypdf import PdfReader  # type: ignore[no-redef]
        except ImportError:
            logger.debug("PyPDF2/pypdf not available — skipping PDF page count for %s", file_path)
            return None

    try:
        reader = PdfReader(str(file_path))
        return len(reader.pages)
    except (OSError, PermissionError, ValueError) as exc:
        logger.debug("Cannot read PDF page count for %s: %s", file_path, exc)
        return None


def _extract_docx_page_count(file_path: Path) -> int | None:
    """Estimate page count from a DOCX using paragraph count as proxy.

    Heuristic: ~25 paragraphs per page is a reasonable average for business docs.
    """
    try:
        from docx import Document  # type: ignore[import-untyped]
    except ImportError:
        logger.debug("python-docx not available — skipping DOCX page count for %s", file_path)
        return None

    try:
        doc = Document(str(file_path))
        para_count = len(doc.paragraphs)
        # Rough estimate: 25 paragraphs per page, minimum 1
        return max(1, para_count // 25)
    except (OSError, PermissionError, ValueError) as exc:
        logger.debug("Cannot read DOCX for %s: %s", file_path, exc)
        return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="ldcb",
        description="Loan Document Corpus Builder — discovery-to-corpus pipeline",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).parent / "data" / "config.json",
        help="Path to config.json",
    )
    parser.add_argument(
        "--rules",
        type=Path,
        default=Path(__file__).parent / "data" / "rules.json",
        help="Path to rules.json",
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        action="append",
        dest="source_roots",
        help="Source root path(s) — overrides config.paths.source_roots",
    )
    parser.add_argument(
        "--target-root",
        type=Path,
        default=None,
        help="Target corpus root — overrides config.paths.target_root",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Explicit run ID (for idempotent reruns)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run traversal + classification without copying files",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override logging level",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    # Load config and rules
    config = load_config(args.config)
    rules = load_rules(args.rules)

    # CLI overrides
    if args.source_roots:
        config.paths.source_roots = [str(r) for r in args.source_roots]
    if args.target_root:
        config.paths.target_root = str(args.target_root)
    if args.log_level:
        config.logging.level = args.log_level

    # Setup logging
    setup_logging(
        log_dir=Path(config.paths.log_dir),
        level=config.logging.level,
        fmt=config.logging.format,
    )

    run_id = args.run_id or generate_run_id()
    logger.info("Starting LDCB pipeline (run_id=%s, dry_run=%s)", run_id, args.dry_run)

    manifest = run_pipeline(config, rules, run_id, dry_run=args.dry_run)

    # Print summary to stdout
    print(f"\n{'='*60}")
    print(f"LDCB Pipeline Complete — Run {manifest.run_id}")
    if args.dry_run:
        print("  *** DRY-RUN — no files were copied ***")
    print(f"{'='*60}")
    print(f"  Files discovered:  {manifest.total_files_discovered}")
    print(f"  Candidates:        {manifest.total_candidates}")
    print(f"  Selected:          {manifest.total_selected}")
    print(f"  Copied:            {manifest.total_copied}")
    print(f"  Quarantined:       {manifest.total_quarantined}")
    print(f"  Review required:   {manifest.total_review_required}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
