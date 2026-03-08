"""Typed configuration loader for LDCB pipeline."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class PathsConfig:
    source_roots: list[str] = field(default_factory=list)
    target_root: str = "corpus_output"
    sqlite_db: str = "ldcb_registry.db"
    parquet_dir: str = "exports"
    log_dir: str = "logs"
    quarantine_dir: str = "quarantine"
    review_dir: str = "review_required"
    manifest_dir: str = "manifests"


@dataclass(slots=True)
class TraversalConfig:
    max_depth: int = 8
    followlinks: bool = False
    batch_size: int = 500
    file_extensions: list[str] = field(default_factory=lambda: [".pdf", ".docx", ".doc"])
    min_file_size_bytes: int = 512
    max_file_size_bytes: int = 524288000


@dataclass(slots=True)
class QualificationConfig:
    relationship_folder_min_depth: int = 1
    relationship_folder_max_depth: int = 3
    loan_folder_min_depth: int = 2
    loan_folder_max_depth: int = 5


@dataclass(slots=True)
class ClassificationConfig:
    min_confidence_threshold: float = 0.25
    ar_weak_evidence_penalty: float = 0.5
    ar_requires_supporting_signal: bool = True
    max_pages_for_metadata_scan: int = 5


@dataclass(slots=True)
class QualityGateConfig:
    min_page_count: int = 1
    max_page_count: int = 500
    min_meaningful_size_bytes: int = 2048
    draft_penalty: float = 0.3


@dataclass(slots=True)
class SelectionConfig:
    retain_years: int = 3
    top_n_per_year: int = 2


@dataclass(slots=True)
class CorpusConfig:
    copy_method: str = "shutil_copy2"
    collision_rename: bool = True
    preserve_timestamps: bool = True
    generate_manifest: bool = True


@dataclass(slots=True)
class HashingConfig:
    document_id_algorithm: str = "md5"
    lineage_algorithm: str = "sha256"


@dataclass(slots=True)
class LoggingConfig:
    level: str = "INFO"
    format: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    file_rotation_mb: int = 50
    max_backup_count: int = 5


@dataclass(slots=True)
class LDCBConfig:
    paths: PathsConfig = field(default_factory=PathsConfig)
    traversal: TraversalConfig = field(default_factory=TraversalConfig)
    qualification: QualificationConfig = field(default_factory=QualificationConfig)
    classification: ClassificationConfig = field(default_factory=ClassificationConfig)
    quality_gate: QualityGateConfig = field(default_factory=QualityGateConfig)
    selection: SelectionConfig = field(default_factory=SelectionConfig)
    corpus: CorpusConfig = field(default_factory=CorpusConfig)
    hashing: HashingConfig = field(default_factory=HashingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


# ---------------------------------------------------------------------------
# Rules dataclass
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class DocumentTypeRule:
    label: str = ""
    aliases: list[str] = field(default_factory=list)
    filename_patterns: list[str] = field(default_factory=list)
    folder_context_patterns: list[str] = field(default_factory=list)
    weights: dict[str, int] = field(default_factory=dict)
    weak_tokens: list[str] = field(default_factory=list)


@dataclass(slots=True)
class LDCBRules:
    skip_markers_folder: list[str] = field(default_factory=list)
    skip_markers_admin: list[str] = field(default_factory=list)
    draft_suppression_markers: list[str] = field(default_factory=list)
    document_types: dict[str, DocumentTypeRule] = field(default_factory=dict)
    relationship_patterns: list[str] = field(default_factory=list)
    loan_patterns: list[str] = field(default_factory=list)
    year_extraction_patterns: list[str] = field(default_factory=list)
    quarantine_reason_codes: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _safe_get(data: dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Nested dict lookup with default."""
    current = data
    for k in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(k, default)
    return current


def load_config(config_path: Path) -> LDCBConfig:
    """Load and validate config.json into typed LDCBConfig."""
    if not config_path.exists():
        logger.warning("Config file not found at %s, using defaults", config_path)
        return LDCBConfig()

    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    paths_d = data.get("paths", {})
    trav_d = data.get("traversal", {})
    qual_d = data.get("qualification", {})
    cls_d = data.get("classification", {})
    qg_d = data.get("quality_gate", {})
    sel_d = data.get("selection", {})
    corp_d = data.get("corpus", {})
    hash_d = data.get("hashing", {})
    log_d = data.get("logging", {})

    cfg = LDCBConfig(
        paths=PathsConfig(**paths_d),
        traversal=TraversalConfig(**trav_d),
        qualification=QualificationConfig(**qual_d),
        classification=ClassificationConfig(**cls_d),
        quality_gate=QualityGateConfig(**qg_d),
        selection=SelectionConfig(**sel_d),
        corpus=CorpusConfig(**corp_d),
        hashing=HashingConfig(**hash_d),
        logging=LoggingConfig(**log_d),
    )
    logger.info("Loaded config from %s", config_path)
    return cfg


def load_rules(rules_path: Path) -> LDCBRules:
    """Load and validate rules.json into typed LDCBRules."""
    if not rules_path.exists():
        logger.warning("Rules file not found at %s, using defaults", rules_path)
        return LDCBRules()

    with open(rules_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    skip_m = data.get("skip_markers", {})
    doc_types_raw = data.get("document_types", {})

    doc_types: dict[str, DocumentTypeRule] = {}
    for key, val in doc_types_raw.items():
        doc_types[key] = DocumentTypeRule(
            label=val.get("label", ""),
            aliases=val.get("aliases", []),
            filename_patterns=val.get("filename_patterns", []),
            folder_context_patterns=val.get("folder_context_patterns", []),
            weights=val.get("weights", {}),
            weak_tokens=val.get("weak_tokens", []),
        )

    rules = LDCBRules(
        skip_markers_folder=skip_m.get("folder_level", []),
        skip_markers_admin=skip_m.get("admin_folders", []),
        draft_suppression_markers=data.get("draft_suppression_markers", []),
        document_types=doc_types,
        relationship_patterns=data.get("relationship_patterns", []),
        loan_patterns=data.get("loan_patterns", []),
        year_extraction_patterns=data.get("year_extraction_patterns", []),
        quarantine_reason_codes=data.get("quarantine_reason_codes", {}),
    )
    logger.info("Loaded rules from %s", rules_path)
    return rules
