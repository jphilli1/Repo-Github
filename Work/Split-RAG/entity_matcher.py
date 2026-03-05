"""
Split-RAG Extension — Rule-Based Entity Matching

Provides domain-specific keyword dictionaries and regex patterns for
deterministic entity extraction without LLM inference.

Uses RapidFuzz when available; falls back to exact regex matching.

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("SplitRAG.EntityMatcher")


# ---------------------------------------------------------------------------
# Default keyword dictionaries for financial / legal domain
# ---------------------------------------------------------------------------

DEFAULT_ENTITY_KEYWORDS: Dict[str, List[str]] = {
    "financial_metric": [
        "NPL ratio",
        "net interest margin",
        "capital adequacy",
        "loan-to-value",
        "debt service coverage",
        "return on equity",
        "tier 1 capital",
        "leverage ratio",
        "provision coverage",
        "cost-to-income",
    ],
    "contract_term": [
        "maturity date",
        "interest rate",
        "principal amount",
        "collateral",
        "covenant",
        "default event",
        "prepayment",
        "amortization",
        "guarantee",
        "subordination",
    ],
    "regulatory": [
        "Basel III",
        "Basel IV",
        "Dodd-Frank",
        "Volcker Rule",
        "IFRS 9",
        "CECL",
        "stress test",
        "risk-weighted assets",
        "liquidity coverage",
        "net stable funding",
    ],
    "pricing": [
        "SOFR",
        "LIBOR",
        "spread",
        "basis points",
        "margin",
        "commitment fee",
        "origination fee",
        "prepayment penalty",
        "floor rate",
        "cap rate",
    ],
}


def load_entity_keywords(
    rules_path: Optional[Path] = None,
) -> Dict[str, List[str]]:
    """
    Load entity keyword dictionaries from rules.json or return defaults.

    Expected rules.json structure:
        {
            "entity_keywords": {
                "category_name": ["keyword1", "keyword2", ...],
                ...
            }
        }
    """
    if rules_path and rules_path.exists():
        try:
            with open(rules_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            custom = data.get("entity_keywords", {})
            if custom:
                logger.info("Loaded %d entity categories from %s", len(custom), rules_path)
                return custom
        except Exception as exc:
            logger.warning("Failed to load entity keywords from %s: %s", rules_path, exc)

    logger.info("Using default entity keyword dictionaries (%d categories)", len(DEFAULT_ENTITY_KEYWORDS))
    return DEFAULT_ENTITY_KEYWORDS


def scan_text_for_entities(
    text: str,
    keywords: Dict[str, List[str]],
    fuzzy_threshold: int = 85,
) -> List[Tuple[str, str, float]]:
    """
    Scan a text block against keyword dictionaries.

    Returns list of (category, keyword, score) tuples for matches found.
    """
    results: List[Tuple[str, str, float]] = []
    text_lower = text.lower()

    try:
        from rapidfuzz import fuzz

        for category, kw_list in keywords.items():
            for kw in kw_list:
                score = fuzz.partial_ratio(kw.lower(), text_lower)
                if score >= fuzzy_threshold:
                    results.append((category, kw, float(score)))
    except ImportError:
        # Exact substring fallback
        for category, kw_list in keywords.items():
            for kw in kw_list:
                if kw.lower() in text_lower:
                    results.append((category, kw, 100.0))

    return results


def extract_contract_ids(text: str) -> List[str]:
    """
    Extract potential contract / loan / deal IDs from text using regex patterns.
    """
    patterns = [
        r"(?:Contract|Loan|Deal|Facility)\s*(?:#|No\.?|Number:?)\s*([A-Z0-9\-]{4,20})",
        r"(?:Ref(?:erence)?)\s*(?:#|No\.?|:)\s*([A-Z0-9\-]{4,20})",
    ]
    ids: List[str] = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            ids.append(m.group(1).strip())
    return ids


def extract_monetary_values(text: str) -> List[Tuple[str, str]]:
    """
    Extract monetary amounts and their context from text.
    Returns list of (amount_str, surrounding_context) tuples.
    """
    pattern = r"(\$[\d,]+(?:\.\d{1,2})?(?:\s*(?:million|billion|thousand|MM|M|B|K))?)"
    results: List[Tuple[str, str]] = []
    for m in re.finditer(pattern, text, re.IGNORECASE):
        start = max(0, m.start() - 50)
        end = min(len(text), m.end() + 50)
        context = text[start:end].strip()
        results.append((m.group(1), context))
    return results
