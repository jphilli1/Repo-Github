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
import os
import sys
from datetime import datetime
from pathlib import Path

from .adapters import ParquetExporter, SQLiteRegistry
from .classification import DocumentClassifier, QualityGate
from .config import LDCBConfig, load_config, load_rules
from .corpus import CorpusBuilder
from .logging_utils import setup_logging
from .mapping import CanonicalMapper
from .models import (
    CandidateDisposition,
    CandidateRecord,
    FileRecord,
    ReviewRecord,
    RunManifest,
)
from .qualification import FolderQualifier
from .selection import SelectionEngine
from .traversal import TraversalEngine

logger = logging.getLogger("ldcb.main")


def generate_run_id() -> str:
    """Generate a stable run ID from current timestamp."""
    ts = datetime.utcnow().isoformat()
    return hashlib.md5(ts.encode("utf-8")).hexdigest()[:12]


def run_pipeline(
    config: LDCBConfig,
    rules_obj: object,
    run_id: str,
    source_roots: list[Path] | None = None,
) -> RunManifest:
    """Execute the full LDCB pipeline.

    Stages:
      1. Traversal — discover files
      2. Folder Qualification — identify relationships & loans
      3. Document Classification — type and score each file
      4. Quality Gate — filter unfit candidates
      5. Selection — pick best per (rel, loan, type, year)
      6. Corpus Build — copy selected files to canonical layout
      7. Persist — write all records to SQLite + manifests
    """
    from .config import LDCBRules

    rules: LDCBRules = rules_obj  # type: ignore[assignment]

    started_at = datetime.utcnow().isoformat()
    effective_roots = source_roots or [Path(r) for r in config.paths.source_roots]

    logger.info("=== LDCB Pipeline Run %s ===", run_id)
    logger.info("Source roots: %s", effective_roots)
    logger.info("Target root: %s", config.paths.target_root)

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

    # Relationship and loan registries (path hash -> record)
    relationships: dict[str, object] = {}
    loans: dict[str, object] = {}

    # ------------------------------------------------------------------
    # Stage 1: Traversal + Qualification (interleaved for efficiency)
    # ------------------------------------------------------------------
    logger.info("--- Stage 1: Traversal & Qualification ---")

    for root in effective_roots:
        if not root.exists():
            logger.warning("Source root does not exist, skipping: %s", root)
            continue

        root_depth = len(root.parts)

        for dirpath_str, dirnames, filenames in os.walk(
            str(root), topdown=True, followlinks=config.traversal.followlinks
        ):
            dirpath = Path(dirpath_str)
            current_depth = len(dirpath.parts) - root_depth

            if current_depth >= config.traversal.max_depth:
                dirnames.clear()
                continue

            # Qualify this folder
            rels, lns, revs = qualifier.qualify_folder(dirpath, current_depth, run_id)
            for r in rels:
                relationships[r.relationship_id] = r
                registry.upsert_relationship(r)
            for l in lns:
                loans[l.loan_id] = l
                registry.upsert_loan(l)
            for rv in revs:
                all_reviews.append(rv)
                registry.upsert_review(rv)

            # Discover files in this folder
            for fname in filenames:
                file_path = dirpath / fname
                ext = file_path.suffix.lower()

                if ext not in set(e.lower() for e in config.traversal.file_extensions):
                    continue

                try:
                    stat = file_path.stat()
                except (OSError, PermissionError):
                    continue

                size = stat.st_size
                mtime = stat.st_mtime

                if size < config.traversal.min_file_size_bytes or size > config.traversal.max_file_size_bytes:
                    continue

                from .models import generate_document_id
                file_id = generate_document_id(file_path, size, mtime)

                file_rec = FileRecord(
                    file_id=file_id,
                    file_path=str(file_path),
                    file_name=fname,
                    file_extension=ext,
                    file_size=size,
                    modified_time=mtime,
                    depth=current_depth,
                    run_id=run_id,
                )

                # Enrich with relationship/loan context
                file_rec = qualifier.qualify_file_context(file_rec, relationships, loans)
                registry.upsert_file(file_rec)
                total_files += 1

                # ----------------------------------------------------------
                # Stage 2: Classification
                # ----------------------------------------------------------
                rel_canonical = ""
                loan_canonical = ""

                if file_rec.relationship_id and file_rec.relationship_id in relationships:
                    rel_obj = relationships[file_rec.relationship_id]
                    raw_name = getattr(rel_obj, "raw_folder_name", "")
                    rel_canonical = mapper.resolve_relationship(raw_name)

                if file_rec.loan_id and file_rec.loan_id in loans:
                    loan_obj = loans[file_rec.loan_id]
                    raw_name = getattr(loan_obj, "raw_folder_name", "")
                    loan_canonical = mapper.resolve_loan(raw_name)

                candidate = classifier.classify(
                    file_rec, rel_canonical, loan_canonical, run_id
                )

                if candidate is None:
                    # Below classification threshold — quarantine
                    review_id = hashlib.md5(
                        f"unclassified|{file_path}".encode("utf-8")
                    ).hexdigest()
                    review = ReviewRecord(
                        review_id=review_id,
                        entity_type="file",
                        entity_id=file_id,
                        entity_path=str(file_path),
                        reason_code="UNCLASSIFIED",
                        reason_detail="Below classification confidence threshold",
                        bucket="QUARANTINE",
                        run_id=run_id,
                    )
                    all_reviews.append(review)
                    registry.upsert_review(review)
                    total_quarantined += 1
                    continue

                # ----------------------------------------------------------
                # Stage 3: Quality Gate
                # ----------------------------------------------------------
                qg_result, qg_notes = quality_gate.evaluate(candidate, size)
                candidate.quality_gate = qg_result
                candidate.quality_notes = qg_notes

                if qg_result.value.startswith("FAIL"):
                    candidate.disposition = CandidateDisposition.QUARANTINED
                    review_id = hashlib.md5(
                        f"quality|{file_path}|{qg_result.value}".encode("utf-8")
                    ).hexdigest()
                    review = ReviewRecord(
                        review_id=review_id,
                        entity_type="candidate",
                        entity_id=candidate.candidate_id,
                        entity_path=str(file_path),
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
                file_metadata[candidate.candidate_id] = (size, mtime)
                candidates_by_id[candidate.candidate_id] = candidate
                total_candidates += 1

    registry.commit()
    logger.info(
        "Discovery complete: %d files, %d candidates, %d quarantined",
        total_files, total_candidates, total_quarantined,
    )

    # ------------------------------------------------------------------
    # Stage 4: Selection
    # ------------------------------------------------------------------
    logger.info("--- Stage 4: Selection ---")

    # Filter to only passing candidates
    passing = [c for c in all_candidates if not c.quality_gate.value.startswith("FAIL")]
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
    # Stage 5: Corpus Build
    # ------------------------------------------------------------------
    logger.info("--- Stage 5: Corpus Build ---")

    copy_records = builder.build(selections, candidates_by_id, run_id)
    for cr in copy_records:
        registry.upsert_copy(cr)

    total_copied = sum(1 for cr in copy_records if cr.copy_success)
    registry.commit()
    logger.info("Corpus build: %d files copied successfully", total_copied)

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
    logger.info("Starting LDCB pipeline (run_id=%s)", run_id)

    manifest = run_pipeline(config, rules, run_id)

    # Print summary to stdout
    print(f"\n{'='*60}")
    print(f"LDCB Pipeline Complete — Run {manifest.run_id}")
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
