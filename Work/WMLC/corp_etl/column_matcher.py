"""Fuzzy column matching utility.

After normalization, all columns are UPPERCASE_UNDERSCORE.
Strategy: direct match on normalized name -> try alternates -> warn and return None.
"""

import re
import logging

logger = logging.getLogger("wmlc_etl.column_matcher")


def _normalize_name(name):
    """Normalize a column name to UPPERCASE_UNDERSCORE form."""
    return re.sub(r'[^A-Z0-9_]', '', name.upper().replace(' ', '_').replace('-', '_'))


def find_column(df, preferred, alternates=None):
    """Find a column in a DataFrame. All comparisons use normalized form.

    Args:
        df: pandas DataFrame (columns already normalized to UPPERCASE)
        preferred: preferred column name (any case — will be normalized)
        alternates: list of alternate names to try

    Returns:
        Matched column name (str) or None if not found.
    """
    all_names = [preferred] + (alternates or [])
    logger.debug(f"Looking for '{preferred}' in columns: {list(df.columns)[:15]}")

    # Direct match (columns are already normalized)
    normalized_preferred = _normalize_name(preferred)
    if normalized_preferred in df.columns:
        logger.debug(f"Direct match: '{normalized_preferred}'")
        return normalized_preferred

    # Try alternates
    for alt in (alternates or []):
        normalized_alt = _normalize_name(alt)
        if normalized_alt in df.columns:
            logger.debug(f"Alternate match: '{alt}' -> '{normalized_alt}'")
            return normalized_alt

    # Not found
    logger.warning(f"No match found for '{preferred}' (normalized: '{normalized_preferred}') "
                   f"or alternates in {list(df.columns)[:10]}")
    return None


def resolve_column(df, hint_key, column_hints):
    """Resolve a column name using the hints dictionary from config.

    Args:
        df: pandas DataFrame
        hint_key: key into column_hints dict
        column_hints: dict from config.yaml

    Returns:
        Matched column name (str) or None.
    """
    names = column_hints.get(hint_key, [hint_key])
    if isinstance(names, str):
        names = [names]
    if not names:
        return None
    return find_column(df, names[0], names[1:])
