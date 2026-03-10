#!/usr/bin/env python3
"""
Case-Shiller Release-Table Discovery
=====================================

Dynamically discovers all series in the three Case-Shiller FRED releases
rather than hardcoding a fixed subset.

Releases covered:
  1. Standard HPI        (release_id=199159) — 23 SA + 23 NSA series
  2. Tiered-Price HPI    (release_id=345173) — 16 markets × 3 tiers × 2 SA/NSA = 96 series
  3. Sales-Pair Counts   (release_id=199159) — 22+ NSA count series

The module fetches the FRED release-tables API, parses each series returned,
classifies it (tier, metro, SA/NSA), and merges the results into the static
seed from fred_series_registry.py.  Series already in the seed are not
duplicated; new ones are added at priority 3 (registry-only) by default.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd

from fred_series_registry import (
    FREDSeriesSpec,
    FRED_EXPANSION_REGISTRY,
    _CS_CAT,
    _CS_SHEET,
    _CS_SEL_SHEET,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# FRED release IDs
# ---------------------------------------------------------------------------
STANDARD_HPI_RELEASE_ID = 199159       # S&P Cotality Case-Shiller Home Price Indices
TIERED_HPI_RELEASE_ID = 345173         # S&P Cotality Case-Shiller HPI by Tiered Price
# Sales-pair counts are part of the standard release (199159)

# Metro abbreviation → full name
METRO_MAP = {
    "AX": "Atlanta", "BX": "Boston", "CH": "Chicago", "CL": "Cleveland",
    "DA": "Dallas", "DN": "Denver", "DT": "Detroit", "LV": "Las Vegas",
    "LX": "Los Angeles", "MI": "Miami", "MN": "Minneapolis", "NY": "New York",
    "PH": "Phoenix", "PO": "Portland", "SD": "San Diego", "SF": "San Francisco",
    "SE": "Seattle", "TA": "Tampa", "WD": "Washington DC", "CH": "Charlotte",
}

# Wealth-market metros that get priority 1-2 for tiered indexes
WEALTH_METROS = {"NY", "LX", "SF", "MI", "WD", "BX", "SD", "SE"}

# ---------------------------------------------------------------------------
# FRED API helpers
# ---------------------------------------------------------------------------
FRED_API_BASE = "https://api.stlouisfed.org/fred"


async def _fetch_release_series(
    session: aiohttp.ClientSession,
    api_key: str,
    release_id: int,
    limit: int = 1000,
) -> List[Dict]:
    """Fetch all series in a FRED release."""
    url = f"{FRED_API_BASE}/release/series"
    params = {
        "release_id": release_id,
        "api_key": api_key,
        "file_type": "json",
        "limit": limit,
    }
    all_series = []
    offset = 0

    while True:
        params["offset"] = offset
        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.warning(f"FRED release/series returned {resp.status} for release {release_id}")
                break
            data = await resp.json()
            serieses = data.get("seriess", [])
            if not serieses:
                break
            all_series.extend(serieses)
            if len(serieses) < limit:
                break
            offset += limit

    return all_series


def _classify_case_shiller_series(raw: Dict) -> Optional[FREDSeriesSpec]:
    """
    Parse a raw FRED series dict from the release API and classify it
    into a FREDSeriesSpec.  Returns None if the series is discontinued
    or not a Case-Shiller index.
    """
    sid = raw.get("id", "")
    title = raw.get("title", "")
    freq = raw.get("frequency_short", "M")
    sa = raw.get("seasonal_adjustment_short", "NSA")
    units = raw.get("units", "Index")
    active = raw.get("observation_end", "2020-01-01") >= "2024-01-01"

    if not active:
        return None

    # Determine subcategory / tier / metro
    subcategory = "Unknown"
    chart_group = "cs_collateral_trend"
    sheet = _CS_SHEET
    priority = 3
    notes = f"Auto-discovered. © S&P Dow Jones Indices LLC. {raw.get('notes', '')[:200]}"

    title_upper = title.upper()

    # Sales-pair count?
    if "SALES PAIR" in title_upper or "RPSNSA" in sid or "RPSN" in sid:
        subcategory = "Sales-Pair Counts"
        chart_group = "cs_sales_pairs"
        priority = 2
        if any(k in sid for k in ("SPCS10", "SPCS20")):
            priority = 1
            sheet = _CS_SEL_SHEET

    # Tiered?
    elif "HIGH" in title_upper and "TIER" in title_upper:
        subcategory = f"High Tier"
        chart_group = "cs_tiered"
        metro_code = sid[:2]
        if metro_code in WEALTH_METROS:
            priority = 1
            sheet = _CS_SEL_SHEET
        else:
            priority = 2
    elif "MIDDLE" in title_upper and "TIER" in title_upper:
        subcategory = f"Middle Tier"
        chart_group = "cs_tiered"
        metro_code = sid[:2]
        if metro_code in WEALTH_METROS:
            priority = 2
            sheet = _CS_SEL_SHEET
        else:
            priority = 3
    elif "LOW" in title_upper and "TIER" in title_upper:
        subcategory = "Low Tier"
        chart_group = "cs_tiered"
        priority = 3

    # Standard HPI (national / composite / metro)
    elif "NATIONAL" in title_upper:
        subcategory = "National"
        priority = 1
        sheet = _CS_SEL_SHEET
    elif "COMPOSITE" in title_upper:
        subcategory = "Composite-10" if "10" in title else "Composite-20"
        priority = 1
        sheet = _CS_SEL_SHEET
    elif "HOME PRICE INDEX" in title_upper:
        # Metro-level standard HPI
        subcategory = "Metro"
        metro_code = sid[:2]
        if metro_code in WEALTH_METROS:
            priority = 2
            sheet = _CS_SEL_SHEET
        else:
            priority = 3

    # Infer metro from title for subcategory enrichment
    for code, name in METRO_MAP.items():
        if name.upper() in title_upper:
            subcategory = f"{subcategory} — {name}"
            break

    transforms = ["pct_chg_yoy"]
    if priority <= 2:
        transforms.extend(["pct_chg_mom", "z_score_5y"])
    if "HIGH TIER" in title_upper and sa == "SA":
        transforms.append("spread_vs_CSUSHPISA")

    return FREDSeriesSpec(
        category=_CS_CAT,
        subcategory=subcategory,
        series_id=sid,
        display_name=title,
        freq=freq,
        units=units,
        seasonal_adjustment=sa,
        priority=priority,
        use_case=f"Case-Shiller collateral: {subcategory}",
        chart_group=chart_group,
        transformations=transforms,
        is_active=True,
        notes=notes,
        sheet=sheet,
        discovery_source=f"release:{raw.get('release_id', 'unknown')}",
    )


# ---------------------------------------------------------------------------
# Main discovery entry point
# ---------------------------------------------------------------------------
async def discover_case_shiller_async(
    api_key: str,
    rate_limit_delay: float = 1.0,
) -> List[FREDSeriesSpec]:
    """
    Fetch all active Case-Shiller series from FRED release tables.
    Merges with the static seed, deduplicating by series_id.
    Returns the combined list.
    """
    existing_ids = {s.series_id for s in FRED_EXPANSION_REGISTRY}
    discovered: List[FREDSeriesSpec] = []

    async with aiohttp.ClientSession() as session:
        for release_id in (STANDARD_HPI_RELEASE_ID, TIERED_HPI_RELEASE_ID):
            logger.info(f"Discovering Case-Shiller series from release {release_id}...")
            raw_series = await _fetch_release_series(session, api_key, release_id)
            logger.info(f"  Found {len(raw_series)} raw series in release {release_id}")

            for raw in raw_series:
                spec = _classify_case_shiller_series(raw)
                if spec is None:
                    continue
                if spec.series_id in existing_ids:
                    continue
                existing_ids.add(spec.series_id)
                discovered.append(spec)

            # Rate limit between releases
            await asyncio.sleep(rate_limit_delay)

    logger.info(f"Case-Shiller discovery: {len(discovered)} new series found")
    return discovered


def discover_case_shiller_sync(api_key: str) -> List[FREDSeriesSpec]:
    """Synchronous wrapper for discover_case_shiller_async."""
    try:
        loop = asyncio.get_running_loop()
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(discover_case_shiller_async(api_key))
    except RuntimeError:
        return asyncio.run(discover_case_shiller_async(api_key))


def merge_discovered_into_registry(
    discovered: List[FREDSeriesSpec],
) -> List[FREDSeriesSpec]:
    """
    Merge newly discovered series into the master registry.
    Static seed entries always take precedence over discovered ones.
    """
    existing_ids = {s.series_id for s in FRED_EXPANSION_REGISTRY}
    merged = list(FRED_EXPANSION_REGISTRY)
    added = 0
    for spec in discovered:
        if spec.series_id not in existing_ids:
            merged.append(spec)
            existing_ids.add(spec.series_id)
            added += 1
    logger.info(f"Merged {added} discovered series into registry (total: {len(merged)})")
    return merged


def discovered_to_dataframe(specs: List[FREDSeriesSpec]) -> pd.DataFrame:
    """Convert discovered specs to a DataFrame for audit / Excel output."""
    rows = [asdict(s) for s in specs]
    df = pd.DataFrame(rows)
    if "transformations" in df.columns:
        df["transformations"] = df["transformations"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else str(x)
        )
    return df


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import sys

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("ERROR: Set FRED_API_KEY environment variable")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)
    discovered = discover_case_shiller_sync(api_key)
    merged = merge_discovered_into_registry(discovered)

    print(f"\nDiscovered {len(discovered)} new Case-Shiller series")
    print(f"Total merged registry: {len(merged)} series")

    df = discovered_to_dataframe(discovered)
    if not df.empty:
        print(f"\nDiscovered series by subcategory:")
        print(df.groupby("subcategory").size().sort_values(ascending=False))
        print(f"\nBy SA/NSA:")
        print(df.groupby("seasonal_adjustment").size())
        print(f"\nBy priority:")
        print(df.groupby("priority").size())
