"""Structured logging setup for LDCB pipeline."""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    log_dir: Path,
    level: str = "INFO",
    fmt: str = "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    rotation_mb: int = 50,
    backup_count: int = 5,
) -> logging.Logger:
    """Configure root logger with file rotation and console output."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "ldcb_pipeline.log"

    root = logging.getLogger("ldcb")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Avoid duplicate handlers on repeated calls
    if root.handlers:
        return root

    formatter = logging.Formatter(fmt)

    # Rotating file handler
    fh = RotatingFileHandler(
        log_file,
        maxBytes=rotation_mb * 1024 * 1024,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setFormatter(formatter)
    root.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    root.addHandler(ch)

    return root
