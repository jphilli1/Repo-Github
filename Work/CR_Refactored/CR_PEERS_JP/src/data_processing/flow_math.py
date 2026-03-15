"""
Flow-Variable Math Utilities
==============================

Stateless utility functions for converting YTD cumulative income-statement
variables to discrete quarterly flows and annualized projections.

**Critical rule:** Income metrics must be de-accumulated quarterly via
``ytd_to_discrete()`` before any rolling TTM or annualization is applied.
Never use raw YTD values in reports.

Stock vs Flow Convention
------------------------
- **Stock** (point-in-time): Balances, Delinquency (PD30/PD90), Nonaccrual, ACL
  → Use directly. No TTM prefix.
- **Flow** (cumulative YTD): NCO, Income, Provision, Interest Expense
  → ``ytd_to_discrete()`` → ``.rolling(4).sum()`` for TTM.
  → ``annualize_ytd()`` for Yield / Provision rates.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
#  HTTP retry helper (used by FDIC/FFIEC/FRED fetchers)
# ---------------------------------------------------------------------------

def retry_request(session, method, url, max_attempts=3, backoff_base=2.0, **kwargs):
    """Retry an HTTP request with exponential backoff on Timeouts and 5xx errors.

    Returns the response on success or re-raises the last exception after all
    attempts are exhausted.
    """
    import requests as _req

    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = getattr(session, method)(url, **kwargs)
            if response.status_code >= 500 and attempt < max_attempts:
                wait = backoff_base ** attempt
                logging.warning(
                    f"HTTP {response.status_code} from {url} (attempt {attempt}/{max_attempts}). "
                    f"Retrying in {wait:.0f}s..."
                )
                time.sleep(wait)
                continue
            return response
        except (_req.exceptions.Timeout, _req.exceptions.ConnectionError) as e:
            last_exc = e
            if attempt < max_attempts:
                wait = backoff_base ** attempt
                logging.warning(
                    f"{type(e).__name__} on {url} (attempt {attempt}/{max_attempts}). "
                    f"Retrying in {wait:.0f}s..."
                )
                time.sleep(wait)
            else:
                raise
    raise last_exc  # pragma: no cover


# ---------------------------------------------------------------------------
#  YTD de-accumulation
# ---------------------------------------------------------------------------

def ytd_to_discrete(df: pd.DataFrame, col_name: str) -> pd.Series:
    """
    Convert a YTD cumulative series to discrete quarterly flows.

    Q1: discrete = YTD (accumulation starts fresh each year)
    Q2: discrete = YTD_Q2 - YTD_Q1
    Q3: discrete = YTD_Q3 - YTD_Q2
    Q4: discrete = YTD_Q4 - YTD_Q3

    Groups by CERT to prevent differencing across banks.
    """
    if col_name not in df.columns:
        return pd.Series(0.0, index=df.index)

    if df.empty:
        return pd.Series(0.0, index=df.index)
    q_flows = []
    for cert, group in df.groupby('CERT'):
        group = group.sort_values('REPDTE')
        diffs = group[col_name].diff()

        # Q1: flow IS the YTD value (no prior quarter to subtract)
        is_q1 = group['REPDTE'].dt.quarter == 1
        diffs.loc[is_q1] = group.loc[is_q1, col_name]

        # First record fallback (when diff is NaN due to no prior row)
        diffs = diffs.fillna(group[col_name])
        q_flows.append(diffs)

    return pd.concat(q_flows).reindex(df.index).fillna(0)


# ---------------------------------------------------------------------------
#  YTD annualization
# ---------------------------------------------------------------------------

def annualize_ytd(df: pd.DataFrame, col_name: str) -> pd.Series:
    """
    Annualize a YTD cumulative flow variable: YTD_value * (4.0 / quarter).

    This is the standard banking convention for income-statement items:
    - Q1: YTD * 4 (project 1 quarter to full year)
    - Q2: YTD * 2 (project 2 quarters to full year)
    - Q3: YTD * 4/3
    - Q4: YTD * 1 (full year, no adjustment)

    Use for Yield and Provision rates. Do NOT use for NCO (use TTM instead).
    """
    if col_name not in df.columns:
        return pd.Series(np.nan, index=df.index)

    quarter = df['REPDTE'].dt.quarter
    return df[col_name] * (4.0 / quarter)


# ---------------------------------------------------------------------------
#  FRED frequency inference
# ---------------------------------------------------------------------------

def infer_freq_from_index(idx: pd.DatetimeIndex) -> tuple:
    """Infer FRED series frequency from a DatetimeIndex.

    Returns a tuple of (frequency_name, frequency_code):
      - ("daily", "D")
      - ("monthly", "M")
      - ("quarterly", "Q")

    Defensive: drops NaT, deduplicates, and wraps all heuristics in
    try/except so it never crashes the FRED pipeline.  Falls back to
    ("quarterly", "Q") when inference is uncertain.
    """
    try:
        idx = pd.DatetimeIndex(idx, copy=False)
        idx = idx.dropna().sort_values().unique()
        if len(idx) < 3:
            return ("monthly", "M")  # conservative default

        # Try pandas built-in inference first (most reliable)
        guess = pd.infer_freq(idx)
        if guess:
            g = guess.upper()
            if g.startswith(("Q", "QS")):
                return ("quarterly", "Q")
            if g.startswith(("M", "MS")):
                return ("monthly", "M")
            if g.startswith(("D", "B", "C", "W")):
                return ("daily", "D")

        # Heuristic fallback: median observations per year
        vc = pd.Series(idx.year).value_counts()
        med_per_year = float(vc.median()) if not vc.empty else 0.0
        if med_per_year >= 200:
            return ("daily", "D")
        if 6 <= med_per_year <= 15:
            return ("monthly", "M")
        if 3 <= med_per_year <= 5:
            return ("quarterly", "Q")

        # Last resort: count distinct months per year using a DataFrame
        ym = pd.DataFrame({"year": idx.year, "month": idx.month}).drop_duplicates()
        months_per_year = float(ym.groupby("year")["month"].nunique().median())
        if months_per_year and months_per_year >= 6:
            return ("monthly", "M")
        return ("quarterly", "Q")
    except Exception:
        # Absolute last resort — never crash the FRED pipeline
        return ("quarterly", "Q")
