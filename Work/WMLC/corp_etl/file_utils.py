"""Shared file I/O and column normalization utilities for corp_etl."""

import os
import re
import logging
import pandas as pd

logger = logging.getLogger("wmlc_etl.file_utils")


def read_file(path, skip_rows=0):
    """Auto-detect file type by extension and read accordingly.

    Supports .xlsx, .xls, and .csv. All data read as str dtype.

    Args:
        path: file path
        skip_rows: rows to skip before header

    Returns:
        pandas DataFrame (all columns as str)
    """
    ext = os.path.splitext(path)[1].lower()
    logger.info(f"Detected {ext} format, skip_rows={skip_rows}")

    if ext in (".xlsx", ".xls"):
        return pd.read_excel(path, skiprows=skip_rows, dtype=str, engine="openpyxl")
    elif ext == ".csv":
        return pd.read_csv(path, skiprows=skip_rows, dtype=str)
    else:
        raise ValueError(f"Unsupported file type: {ext} for {path}")


def normalize_columns(df):
    """Normalize column names: UPPERCASE, spaces to underscores, strip special chars.

    Args:
        df: pandas DataFrame (modified in place)

    Returns:
        (df, changed) where changed is list of (original, new) tuples for columns that changed.
    """
    original = list(df.columns)
    new_cols = []
    seen = {}

    for col in original:
        normalized = re.sub(r'[^A-Z0-9_]', '', col.upper().replace(' ', '_').replace('-', '_'))
        # Handle duplicates
        if normalized in seen:
            seen[normalized] += 1
            logger.warning(f"Two columns normalized to same name '{normalized}' — "
                           f"appending suffix _{seen[normalized]}")
            normalized = f"{normalized}_{seen[normalized]}"
        else:
            seen[normalized] = 0
        new_cols.append(normalized)

    df.columns = new_cols
    changed = [(orig, new) for orig, new in zip(original, new_cols) if orig != new]

    logger.info(f"Normalized {len(df.columns)} columns, {len(changed)} renamed")
    if changed:
        logger.debug(f"Column renames: {changed}")

    return df, changed


# ── Key formatting utilities ──────────────────────────────────────────────

_NULL_STRINGS = {"", "nan", "none", "nat", "null", "<na>"}


def _is_null_string(val):
    """Check if a string value represents a null/missing value."""
    return str(val).strip().lower() in _NULL_STRINGS


def pad12(val):
    """Zero-pad to 12 digits. Returns None for null/empty/nan values."""
    s = str(val).strip()
    if s.lower() in _NULL_STRINGS:
        return None
    # Strip trailing .0 from float-like strings
    s = re.sub(r'\.0$', '', s)
    # Keep only digits
    digits = "".join(c for c in s if c.isdigit())
    if not digits:
        return None
    return digits.zfill(12)


def strip_dash_pad12(val):
    """Strip dashes, then zero-pad to 12. Returns None for null/empty/nan."""
    s = str(val).strip()
    if s.lower() in _NULL_STRINGS:
        return None
    cleaned = s.replace("-", "")
    # Strip trailing .0
    cleaned = re.sub(r'\.0$', '', cleaned)
    digits = "".join(c for c in cleaned if c.isdigit())
    if not digits:
        return None
    return digits.zfill(12)


def clean_key_column(series, logger_name=""):
    """Clean a key column: convert nan-like strings to None, log stats.

    Args:
        series: pandas Series (string dtype)
        logger_name: column name for logging

    Returns:
        Cleaned pandas Series with real None for null values.
    """
    # Convert padded-nan strings like "000000000nan", "0000000nan00", etc.
    cleaned = series.replace(
        to_replace=[r'(?i)^0*nan.*$', r'(?i)^0*none.*$', r'(?i)^0*nat.*$', r'^\s*$'],
        value=None, regex=True
    )
    nulls = cleaned.isna().sum()
    valid = len(cleaned) - nulls
    if logger_name:
        logger.info(f"{logger_name}: {valid}/{len(cleaned)} non-null values after cleanup")
    return cleaned
