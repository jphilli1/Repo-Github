"""Canonical mapping engine — resolves messy folder/file names to normalized entities."""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Optional

from ..config import LDCBConfig

logger = logging.getLogger("ldcb.mapping")


class CanonicalMapper:
    """Map raw relationship and loan folder names to canonical forms.

    Resolution order:
      1. Exact match in alias CSV
      2. Fuzzy match (rapidfuzz) above threshold
      3. Normalized fallback (cleaned raw name)

    Alias CSVs: data/relationship_aliases.csv, data/loan_aliases.csv
    Format: raw_name,canonical_name
    """

    __slots__ = (
        "_relationship_aliases",
        "_loan_aliases",
        "_fuzzy_threshold",
    )

    def __init__(
        self,
        config: LDCBConfig,
        relationship_alias_path: Path | None = None,
        loan_alias_path: Path | None = None,
        fuzzy_threshold: int = 85,
    ) -> None:
        self._fuzzy_threshold = fuzzy_threshold
        self._relationship_aliases = self._load_aliases(relationship_alias_path)
        self._loan_aliases = self._load_aliases(loan_alias_path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def resolve_relationship(self, raw_name: str) -> str:
        """Resolve a raw relationship folder name to canonical form."""
        return self._resolve(raw_name, self._relationship_aliases)

    def resolve_loan(self, raw_name: str) -> str:
        """Resolve a raw loan folder name to canonical form."""
        return self._resolve(raw_name, self._loan_aliases)

    def add_relationship_alias(self, raw: str, canonical: str) -> None:
        """Dynamically register a relationship alias."""
        self._relationship_aliases[raw.lower().strip()] = canonical

    def add_loan_alias(self, raw: str, canonical: str) -> None:
        """Dynamically register a loan alias."""
        self._loan_aliases[raw.lower().strip()] = canonical

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve(self, raw_name: str, aliases: dict[str, str]) -> str:
        """Three-tier resolution: exact → fuzzy → normalized fallback."""
        key = raw_name.lower().strip()

        # Tier 1: exact match
        if key in aliases:
            canonical = aliases[key]
            logger.debug("Exact alias match: '%s' -> '%s'", raw_name, canonical)
            return canonical

        # Tier 2: fuzzy match
        fuzzy_result = self._fuzzy_match(key, aliases)
        if fuzzy_result is not None:
            logger.debug("Fuzzy alias match: '%s' -> '%s'", raw_name, fuzzy_result)
            return fuzzy_result

        # Tier 3: normalized fallback
        normalized = self._normalize(raw_name)
        logger.debug("Normalized fallback: '%s' -> '%s'", raw_name, normalized)
        return normalized

    def _fuzzy_match(self, key: str, aliases: dict[str, str]) -> str | None:
        """Attempt fuzzy matching using rapidfuzz if available."""
        if not aliases:
            return None
        try:
            from rapidfuzz import fuzz, process

            choices = list(aliases.keys())
            result = process.extractOne(
                key, choices, scorer=fuzz.token_sort_ratio, score_cutoff=self._fuzzy_threshold
            )
            if result is not None:
                matched_key, score, _ = result
                return aliases[matched_key]
        except ImportError:
            logger.debug("rapidfuzz not available, skipping fuzzy matching")
        return None

    @staticmethod
    def _load_aliases(alias_path: Path | None) -> dict[str, str]:
        """Load alias CSV into dict[raw_lower, canonical]."""
        aliases: dict[str, str] = {}
        if alias_path is None or not alias_path.exists():
            return aliases

        with open(alias_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                raw = row.get("raw_name", "").lower().strip()
                canonical = row.get("canonical_name", "").strip()
                if raw and canonical:
                    aliases[raw] = canonical

        logger.info("Loaded %d aliases from %s", len(aliases), alias_path)
        return aliases

    @staticmethod
    def _normalize(raw: str) -> str:
        """Normalize a folder name for use as canonical when no alias exists."""
        # Remove leading numeric IDs
        cleaned = re.sub(r"^\d+[\s_-]+", "", raw)
        # Replace separators with spaces
        cleaned = re.sub(r"[_-]+", " ", cleaned)
        # Remove special chars except alphanumeric, spaces, periods
        cleaned = re.sub(r"[^a-zA-Z0-9\s.]", "", cleaned)
        # Title case, collapse whitespace
        cleaned = re.sub(r"\s+", " ", cleaned).strip().title()
        # Filesystem-safe: replace remaining spaces with underscores
        cleaned = cleaned.replace(" ", "_")
        return cleaned
