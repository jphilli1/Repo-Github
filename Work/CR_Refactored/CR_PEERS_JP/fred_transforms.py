#!/usr/bin/env python3
"""
FRED Transforms — Derived Columns, Spreads, Z-Scores, and Regime Flags
=======================================================================

All transformation logic for the expanded FRED ingestion layer.
Functions accept a wide-format DataFrame (DATE index, series IDs as columns)
and return augmented DataFrames with derived columns appended.

Transformation catalogue (registry-driven):
  pct_chg_mom             — month-over-month % change
  pct_chg_qoq_annualized — quarter-over-quarter % change, annualized
  pct_chg_yoy             — year-over-year % change
  z_score_5y              — z-score over trailing 5-year window
  z_score_10y             — z-score over trailing 10-year window
  rolling_3m_avg          — 3-month (or ~13-week) rolling mean
  rolling_12m_avg         — 12-month (or ~52-week) rolling mean

Spread definitions:
  jumbo mortgage rate vs conforming benchmark (MORTGAGE30US)
  large-bank CRE demand vs standards
  large-bank residential growth vs top-100-bank delinquency
  Case-Shiller high-tier vs national index

Regime flags:
  CRE tightening + demand negative
  Jumbo standards tightening + jumbo demand negative
  Resi delinquency rising while high-tier HPI decelerating
  Securities-in-bank-credit falling while broker-dealer margin weakening
"""

from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Core statistical transforms
# ---------------------------------------------------------------------------
def pct_chg_mom(series: pd.Series) -> pd.Series:
    """Month-over-month percent change."""
    return series.pct_change(periods=1) * 100


def pct_chg_qoq_annualized(series: pd.Series) -> pd.Series:
    """Quarter-over-quarter percent change, annualized."""
    qoq = series.pct_change(periods=1)
    return ((1 + qoq) ** 4 - 1) * 100


def pct_chg_yoy(series: pd.Series, periods: int = 12) -> pd.Series:
    """Year-over-year percent change.  Periods default to 12 (monthly)."""
    return series.pct_change(periods=periods) * 100


def z_score(series: pd.Series, window_years: int = 5, freq_periods_per_year: int = 12) -> pd.Series:
    """Rolling z-score over a trailing window."""
    w = window_years * freq_periods_per_year
    roll_mean = series.rolling(window=w, min_periods=max(w // 2, 12)).mean()
    roll_std = series.rolling(window=w, min_periods=max(w // 2, 12)).std()
    return (series - roll_mean) / roll_std.replace(0, np.nan)


def rolling_avg(series: pd.Series, window: int) -> pd.Series:
    """Rolling mean with centered=False."""
    return series.rolling(window=window, min_periods=max(window // 2, 1)).mean()


# ---------------------------------------------------------------------------
# Frequency helpers
# ---------------------------------------------------------------------------
_FREQ_MAP = {
    "D": 252,   # trading days
    "W": 52,
    "M": 12,
    "Q": 4,
    "A": 1,
}


def _periods_per_year(freq: str) -> int:
    return _FREQ_MAP.get(freq.upper(), 12)


def _yoy_periods(freq: str) -> int:
    return _periods_per_year(freq)


def _3m_window(freq: str) -> int:
    ppy = _periods_per_year(freq)
    return max(int(ppy / 4), 1)


def _12m_window(freq: str) -> int:
    return _periods_per_year(freq)


# ---------------------------------------------------------------------------
# Registry-driven transform dispatcher
# ---------------------------------------------------------------------------
def apply_transforms(
    df: pd.DataFrame,
    series_id: str,
    transform_list: List[str],
    freq: str = "M",
) -> pd.DataFrame:
    """
    Apply the list of named transforms to a single series column in *df*.
    New columns are named ``{series_id}__{transform_name}``.
    Returns the augmented DataFrame (modifies in place for efficiency).
    """
    if series_id not in df.columns:
        return df

    col = df[series_id].copy()
    ppy = _periods_per_year(freq)

    for t in transform_list:
        out_col = f"{series_id}__{t}"

        if t == "pct_chg_mom":
            df[out_col] = pct_chg_mom(col)

        elif t == "pct_chg_qoq_annualized":
            df[out_col] = pct_chg_qoq_annualized(col)

        elif t == "pct_chg_yoy":
            df[out_col] = pct_chg_yoy(col, periods=_yoy_periods(freq))

        elif t == "z_score_5y":
            df[out_col] = z_score(col, window_years=5, freq_periods_per_year=ppy)

        elif t == "z_score_10y":
            df[out_col] = z_score(col, window_years=10, freq_periods_per_year=ppy)

        elif t == "rolling_3m_avg":
            df[out_col] = rolling_avg(col, _3m_window(freq))

        elif t == "rolling_12m_avg":
            df[out_col] = rolling_avg(col, _12m_window(freq))

        elif t.startswith("spread_vs_"):
            ref_id = t.replace("spread_vs_", "")
            if ref_id in df.columns:
                df[out_col] = col - df[ref_id]

        # Unknown transforms are silently skipped (logged by caller)

    return df


def apply_all_registry_transforms(
    df: pd.DataFrame,
    registry: List,
) -> pd.DataFrame:
    """
    Walk the full registry and apply each spec's transforms to df.
    The registry is a list of FREDSeriesSpec (or any object with
    series_id, transformations, freq attributes).
    """
    for spec in registry:
        if spec.series_id in df.columns and spec.transformations:
            df = apply_transforms(df, spec.series_id, spec.transformations, spec.freq)
    return df


# ---------------------------------------------------------------------------
# Named spreads
# ---------------------------------------------------------------------------
def compute_spreads(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the four named spreads required by the dashboard.
    Each spread column is prefixed with ``SPREAD__``.
    """
    # 1. Jumbo vs conforming mortgage rate
    if "OBMMIJUMBO30YF" in df.columns and "MORTGAGE30US" in df.columns:
        # Both may be daily; align via forward-fill
        j = df["OBMMIJUMBO30YF"].ffill()
        c = df["MORTGAGE30US"].ffill()
        df["SPREAD__Jumbo_vs_Conforming"] = j - c

    # 2. Large-bank CRE demand vs standards
    #    Positive spread = demand outpacing tightening = expansion signal
    if "SUBLPDRCDNLGNQ" in df.columns and "SUBLPDRCSNLGNQ" in df.columns:
        df["SPREAD__CRE_Demand_vs_Standards_LgBank"] = (
            df["SUBLPDRCDNLGNQ"].ffill() - df["SUBLPDRCSNLGNQ"].ffill()
        )

    # 3. Large-bank resi growth vs top-100-bank delinquency
    if "H8B1221NLGCQG" in df.columns and "DRSFRMT100S" in df.columns:
        df["SPREAD__Resi_Growth_vs_Delinq"] = (
            df["H8B1221NLGCQG"].ffill() - df["DRSFRMT100S"].ffill()
        )

    # 4. Case-Shiller high-tier (NYC proxy) vs national
    ht_col = None
    for cand in ("NYXRHTSA", "LXXRHTSA", "SFXRHTSA"):
        if cand in df.columns:
            ht_col = cand
            break
    if ht_col and "CSUSHPISA" in df.columns:
        # Express as YoY growth differential
        ht_yoy = df[ht_col].pct_change(12) * 100
        nat_yoy = df["CSUSHPISA"].pct_change(12) * 100
        df["SPREAD__HighTier_vs_National_HPI_YoY"] = ht_yoy - nat_yoy

    return df


# ---------------------------------------------------------------------------
# Regime flags
# ---------------------------------------------------------------------------
def compute_regime_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute boolean regime flag columns.
    Each flag column is prefixed with ``REGIME__``.
    """
    # 1. CRE tightening > 0 AND demand < 0
    std_col = _first_present(df, ["SUBLPDRCSNLGNQ", "SUBLPDRCSN"])
    dem_col = _first_present(df, ["SUBLPDRCDNLGNQ", "SUBLPDRCDN"])
    if std_col and dem_col:
        df["REGIME__CRE_Tightening"] = (
            (df[std_col].ffill() > 0) & (df[dem_col].ffill() < 0)
        ).astype(int)

    # 2. Jumbo standards tightening > 0 AND jumbo demand < 0
    j_std = _first_present(df, ["SUBLPDHMSKLGNQ", "SUBLPDHMSKNQ"])
    j_dem = _first_present(df, ["SUBLPDHMDJLGNQ", "SUBLPDHMDJNQ"])
    if j_std and j_dem:
        df["REGIME__Jumbo_Tightening"] = (
            (df[j_std].ffill() > 0) & (df[j_dem].ffill() < 0)
        ).astype(int)

    # 3. Resi delinquency rising while high-tier HPI decelerating
    dq_col = _first_present(df, ["DRSFRMT100S"])
    ht_col = _first_present(df, ["NYXRHTSA", "CSUSHPISA"])
    if dq_col and ht_col:
        dq_rising = df[dq_col].ffill().diff() > 0
        ht_decel = df[ht_col].pct_change(12).ffill().diff() < 0
        df["REGIME__Resi_Stress"] = (dq_rising & ht_decel).astype(int)

    # 4. Securities-in-bank-credit falling while broker-dealer margin weakening
    sbc_col = _first_present(df, ["SBCLCBM027SBOG", "INVEST"])
    bd_col = _first_present(df, ["BOGZ1FU663067005A", "BOGZ1FU664004005Q"])
    if sbc_col and bd_col:
        sbc_falling = df[sbc_col].ffill().pct_change(12) < 0
        bd_weak = df[bd_col].ffill().pct_change(4 if "Q" in bd_col else 12) < 0
        df["REGIME__SBL_Deleveraging"] = (sbc_falling & bd_weak).astype(int)

    return df


def _first_present(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return the first column name from candidates that exists in df."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


# ---------------------------------------------------------------------------
# Master pipeline
# ---------------------------------------------------------------------------
def run_full_transform_pipeline(
    df: pd.DataFrame,
    registry: List,
) -> pd.DataFrame:
    """
    Run all transforms, spreads, and regime flags on the wide-format
    FRED DataFrame.  Returns the augmented DataFrame.
    """
    df = apply_all_registry_transforms(df, registry)
    df = compute_spreads(df)
    df = compute_regime_flags(df)
    return df


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a tiny synthetic df to test
    dates = pd.date_range("2020-01-01", periods=60, freq="MS")
    rng = np.random.default_rng(42)
    test_df = pd.DataFrame({
        "DATE": dates,
        "INVEST": 4000 + rng.normal(0, 50, 60).cumsum(),
        "SBCLCBM027SBOG": 2000 + rng.normal(0, 30, 60).cumsum(),
        "MORTGAGE30US": 3.5 + rng.normal(0, 0.1, 60).cumsum(),
        "OBMMIJUMBO30YF": 3.8 + rng.normal(0, 0.1, 60).cumsum(),
        "CSUSHPISA": 200 + rng.normal(0, 2, 60).cumsum(),
    }).set_index("DATE")

    # Test basic transforms
    test_df = apply_transforms(test_df, "INVEST", ["pct_chg_yoy", "z_score_5y", "rolling_12m_avg"], "M")
    test_df = compute_spreads(test_df)

    print("Columns after transforms:")
    for c in sorted(test_df.columns):
        non_null = test_df[c].notna().sum()
        print(f"  {c}: {non_null} non-null values")
    print("\nTransform pipeline test PASSED")
