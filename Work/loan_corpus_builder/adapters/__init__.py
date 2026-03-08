"""Data persistence adapters — SQLite registry and Parquet export."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from ..models import (
    CandidateRecord,
    CopyRecord,
    FileRecord,
    LoanRecord,
    RelationshipRecord,
    ReviewRecord,
    RunManifest,
    SelectionRecord,
)

logger = logging.getLogger("ldcb.adapters")


# ---------------------------------------------------------------------------
# SQLite Registry
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS relationship_registry (
    relationship_id TEXT PRIMARY KEY,
    raw_folder_name TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    folder_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    skip_reason TEXT,
    depth INTEGER DEFAULT 0,
    discovered_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS loan_registry (
    loan_id TEXT PRIMARY KEY,
    relationship_id TEXT NOT NULL,
    raw_folder_name TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    folder_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    skip_reason TEXT,
    depth INTEGER DEFAULT 0,
    discovered_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_registry (
    file_id TEXT PRIMARY KEY,
    file_path TEXT NOT NULL,
    file_name TEXT NOT NULL,
    file_extension TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    modified_time REAL NOT NULL,
    content_hash TEXT,
    relationship_id TEXT,
    loan_id TEXT,
    depth INTEGER DEFAULT 0,
    discovered_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS candidate_registry (
    candidate_id TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    relationship_id TEXT NOT NULL,
    loan_id TEXT NOT NULL,
    relationship_canonical TEXT NOT NULL,
    loan_canonical TEXT NOT NULL,
    document_type TEXT NOT NULL DEFAULT 'unknown',
    classification_score REAL DEFAULT 0.0,
    classification_signals TEXT,
    document_year INTEGER,
    is_draft INTEGER DEFAULT 0,
    draft_markers_found TEXT,
    page_count INTEGER,
    quality_gate TEXT DEFAULT 'PASS',
    quality_notes TEXT,
    disposition TEXT DEFAULT 'RETAINED_AUDIT',
    source_path TEXT NOT NULL,
    normalized_basename TEXT,
    discovered_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS selection_registry (
    selection_id TEXT PRIMARY KEY,
    candidate_id TEXT NOT NULL,
    relationship_canonical TEXT NOT NULL,
    loan_canonical TEXT NOT NULL,
    document_type TEXT NOT NULL,
    document_year INTEGER NOT NULL,
    rank_in_year INTEGER NOT NULL,
    selection_reason TEXT,
    run_id TEXT NOT NULL,
    selected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS copy_registry (
    copy_id TEXT PRIMARY KEY,
    selection_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    content_hash_before TEXT NOT NULL,
    content_hash_after TEXT,
    copy_success INTEGER DEFAULT 0,
    collision_renamed INTEGER DEFAULT 0,
    error_message TEXT,
    copied_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS review_registry (
    review_id TEXT PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    entity_path TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_detail TEXT,
    bucket TEXT NOT NULL,
    resolved INTEGER DEFAULT 0,
    resolution_note TEXT,
    created_at TEXT NOT NULL,
    run_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_registry (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    source_roots TEXT NOT NULL,
    target_root TEXT NOT NULL,
    total_files_discovered INTEGER DEFAULT 0,
    total_candidates INTEGER DEFAULT 0,
    total_selected INTEGER DEFAULT 0,
    total_copied INTEGER DEFAULT 0,
    total_quarantined INTEGER DEFAULT 0,
    total_review_required INTEGER DEFAULT 0,
    config_snapshot TEXT
);

CREATE INDEX IF NOT EXISTS idx_file_registry_run ON file_registry(run_id);
CREATE INDEX IF NOT EXISTS idx_candidate_registry_run ON candidate_registry(run_id);
CREATE INDEX IF NOT EXISTS idx_candidate_registry_type ON candidate_registry(document_type);
CREATE INDEX IF NOT EXISTS idx_selection_registry_run ON selection_registry(run_id);
CREATE INDEX IF NOT EXISTS idx_copy_registry_run ON copy_registry(run_id);
CREATE INDEX IF NOT EXISTS idx_review_registry_run ON review_registry(run_id);
"""


class SQLiteRegistry:
    """SQLite-backed operational registry for LDCB pipeline state."""

    __slots__ = ("_db_path", "_conn")

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path))
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        logger.info("SQLite registry initialized at %s", db_path)

    def _init_schema(self) -> None:
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()

    # ------------------------------------------------------------------
    # Upsert methods (INSERT OR REPLACE for idempotent reruns)
    # ------------------------------------------------------------------

    def upsert_relationship(self, rec: RelationshipRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO relationship_registry
               (relationship_id, raw_folder_name, canonical_name, folder_path,
                status, skip_reason, depth, discovered_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.relationship_id, rec.raw_folder_name, rec.canonical_name,
             rec.folder_path, rec.status.value, rec.skip_reason, rec.depth,
             rec.discovered_at.isoformat(), rec.run_id),
        )

    def upsert_loan(self, rec: LoanRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO loan_registry
               (loan_id, relationship_id, raw_folder_name, canonical_name,
                folder_path, status, skip_reason, depth, discovered_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.loan_id, rec.relationship_id, rec.raw_folder_name,
             rec.canonical_name, rec.folder_path, rec.status.value,
             rec.skip_reason, rec.depth, rec.discovered_at.isoformat(), rec.run_id),
        )

    def upsert_file(self, rec: FileRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO file_registry
               (file_id, file_path, file_name, file_extension, file_size,
                modified_time, content_hash, relationship_id, loan_id,
                depth, discovered_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.file_id, rec.file_path, rec.file_name, rec.file_extension,
             rec.file_size, rec.modified_time, rec.content_hash,
             rec.relationship_id, rec.loan_id, rec.depth,
             rec.discovered_at.isoformat(), rec.run_id),
        )

    def upsert_candidate(self, rec: CandidateRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO candidate_registry
               (candidate_id, file_id, relationship_id, loan_id,
                relationship_canonical, loan_canonical, document_type,
                classification_score, classification_signals, document_year,
                is_draft, draft_markers_found, page_count, quality_gate,
                quality_notes, disposition, source_path, normalized_basename,
                discovered_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.candidate_id, rec.file_id, rec.relationship_id, rec.loan_id,
             rec.relationship_canonical, rec.loan_canonical,
             rec.document_type.value, rec.classification_score,
             rec.classification_signals, rec.document_year,
             int(rec.is_draft), rec.draft_markers_found, rec.page_count,
             rec.quality_gate.value, rec.quality_notes, rec.disposition.value,
             rec.source_path, rec.normalized_basename,
             rec.discovered_at.isoformat(), rec.run_id),
        )

    def upsert_selection(self, rec: SelectionRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO selection_registry
               (selection_id, candidate_id, relationship_canonical,
                loan_canonical, document_type, document_year, rank_in_year,
                selection_reason, run_id, selected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.selection_id, rec.candidate_id, rec.relationship_canonical,
             rec.loan_canonical, rec.document_type.value, rec.document_year,
             rec.rank_in_year, rec.selection_reason, rec.run_id,
             rec.selected_at.isoformat()),
        )

    def upsert_copy(self, rec: CopyRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO copy_registry
               (copy_id, selection_id, candidate_id, source_path,
                destination_path, content_hash_before, content_hash_after,
                copy_success, collision_renamed, error_message, copied_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.copy_id, rec.selection_id, rec.candidate_id, rec.source_path,
             rec.destination_path, rec.content_hash_before, rec.content_hash_after,
             int(rec.copy_success), int(rec.collision_renamed),
             rec.error_message, rec.copied_at.isoformat(), rec.run_id),
        )

    def upsert_review(self, rec: ReviewRecord) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO review_registry
               (review_id, entity_type, entity_id, entity_path, reason_code,
                reason_detail, bucket, resolved, resolution_note, created_at, run_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (rec.review_id, rec.entity_type, rec.entity_id, rec.entity_path,
             rec.reason_code, rec.reason_detail, rec.bucket,
             int(rec.resolved), rec.resolution_note,
             rec.created_at.isoformat(), rec.run_id),
        )

    def upsert_run(self, manifest: RunManifest) -> None:
        self._conn.execute(
            """INSERT OR REPLACE INTO run_registry
               (run_id, started_at, completed_at, source_roots, target_root,
                total_files_discovered, total_candidates, total_selected,
                total_copied, total_quarantined, total_review_required,
                config_snapshot)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (manifest.run_id, manifest.started_at, manifest.completed_at,
             json.dumps(manifest.source_roots), manifest.target_root,
             manifest.total_files_discovered, manifest.total_candidates,
             manifest.total_selected, manifest.total_copied,
             manifest.total_quarantined, manifest.total_review_required,
             json.dumps(manifest.config_snapshot, default=str)),
        )

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.commit()
        self._conn.close()
        logger.info("SQLite registry closed")

    # ------------------------------------------------------------------
    # Query helpers (for idempotent reruns)
    # ------------------------------------------------------------------

    def get_existing_file_ids(self, run_id: str) -> set[str]:
        """Return set of file_ids already registered for this run."""
        cursor = self._conn.execute(
            "SELECT file_id FROM file_registry WHERE run_id = ?", (run_id,)
        )
        return {row[0] for row in cursor.fetchall()}

    def get_run_stats(self, run_id: str) -> dict[str, int]:
        """Return summary counts for a run."""
        stats: dict[str, int] = {}
        for table in ("file_registry", "candidate_registry", "selection_registry",
                       "copy_registry", "review_registry"):
            cursor = self._conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE run_id = ?", (run_id,)  # noqa: S608
            )
            stats[table] = cursor.fetchone()[0]
        return stats


# ---------------------------------------------------------------------------
# Parquet exporter (optional, requires pandas)
# ---------------------------------------------------------------------------

class ParquetExporter:
    """Export registry tables to Parquet files for analytics."""

    __slots__ = ("_output_dir",)

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

    def export_from_sqlite(self, db_path: Path, run_id: str) -> list[Path]:
        """Export all registry tables for a run to Parquet."""
        try:
            import pandas as pd
        except ImportError:
            logger.warning("pandas not available, skipping Parquet export")
            return []

        conn = sqlite3.connect(str(db_path))
        exported: list[Path] = []

        tables = [
            "relationship_registry", "loan_registry", "file_registry",
            "candidate_registry", "selection_registry", "copy_registry",
            "review_registry",
        ]

        for table in tables:
            try:
                df = pd.read_sql_query(
                    f"SELECT * FROM {table} WHERE run_id = ?",  # noqa: S608
                    conn,
                    params=(run_id,),
                )
                if df.empty:
                    continue
                out_path = self._output_dir / f"{table}_{run_id}.parquet"
                df.to_parquet(str(out_path), index=False, engine="pyarrow")
                exported.append(out_path)
                logger.info("Exported %s (%d rows) -> %s", table, len(df), out_path)
            except (sqlite3.OperationalError, ValueError) as exc:
                logger.warning("Failed to export %s: %s", table, exc)

        conn.close()
        return exported
