"""Traversal engine — scans network share folder structures."""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Generator

from ..config import LDCBConfig, TraversalConfig
from ..models import FileRecord, generate_document_id

logger = logging.getLogger("ldcb.traversal")

try:
    from tqdm import tqdm as _tqdm
except ImportError:
    _tqdm = None


class TraversalEngine:
    """Walk source roots and yield FileRecord for every qualifying file.

    Cheap filter: extension whitelist + size bounds.  No content inspection.
    """

    __slots__ = ("_config", "_extensions", "_min_size", "_max_size", "_max_depth", "_followlinks")

    def __init__(self, config: LDCBConfig) -> None:
        tc: TraversalConfig = config.traversal
        self._config = config
        self._extensions = set(ext.lower() for ext in tc.file_extensions)
        self._min_size = tc.min_file_size_bytes
        self._max_size = tc.max_file_size_bytes
        self._max_depth = tc.max_depth
        self._followlinks = tc.followlinks

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan(self, source_root: Path, run_id: str) -> Generator[FileRecord, None, None]:
        """Yield FileRecord for each qualifying file under *source_root*."""
        root_depth = len(source_root.parts)
        logger.info("Starting traversal of %s (max_depth=%d)", source_root, self._max_depth)

        for dirpath_str, dirnames, filenames in os.walk(
            str(source_root), topdown=True, followlinks=self._followlinks
        ):
            dirpath = Path(dirpath_str)
            current_depth = len(dirpath.parts) - root_depth

            # Prune beyond max depth
            if current_depth >= self._max_depth:
                dirnames.clear()
                continue

            for fname in filenames:
                file_path = dirpath / fname
                record = self._qualify_file(file_path, current_depth, run_id)
                if record is not None:
                    yield record

    def scan_all(self, run_id: str) -> Generator[FileRecord, None, None]:
        """Scan all configured source roots."""
        for root_str in self._config.paths.source_roots:
            root = Path(root_str)
            if not root.exists():
                logger.warning("Source root does not exist: %s", root)
                continue
            yield from self.scan(root, run_id)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _qualify_file(self, file_path: Path, depth: int, run_id: str) -> FileRecord | None:
        """Apply cheap filters and return FileRecord or None."""
        # Extension check
        ext = file_path.suffix.lower()
        if ext not in self._extensions:
            return None

        # Stat the file (single syscall)
        try:
            stat = file_path.stat()
        except (OSError, PermissionError) as exc:
            logger.debug("Cannot stat %s: %s", file_path, exc)
            return None

        size = stat.st_size
        mtime = stat.st_mtime

        # Size bounds
        if size < self._min_size:
            logger.debug("Skipping undersized file (%d bytes): %s", size, file_path)
            return None
        if size > self._max_size:
            logger.debug("Skipping oversized file (%d bytes): %s", size, file_path)
            return None

        file_id = generate_document_id(file_path, size, mtime)

        return FileRecord(
            file_id=file_id,
            file_path=str(file_path),
            file_name=file_path.name,
            file_extension=ext,
            file_size=size,
            modified_time=mtime,
            depth=depth,
            discovered_at=datetime.utcnow(),
            run_id=run_id,
        )
