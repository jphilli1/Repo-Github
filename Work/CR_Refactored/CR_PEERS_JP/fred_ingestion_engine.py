#!/usr/bin/env python3
"""
FRED Ingestion Engine — Registry-Driven Fetch, Transform, and Output
=====================================================================

Orchestrates the full FRED expansion pipeline:
  1. Build fetch list from the registry
  2. Fetch via async HTTP (compatible with upstream FREDDataFetcher pattern)
  3. Run Case-Shiller release-table discovery
  4. Apply transforms, spreads, and regime flags
  5. Validate: duplicates, discontinued, missing metadata, stale releases
  6. Route output into sheet-specific DataFrames

This module integrates with the existing ``DashboardOrchestrator`` in
``MSPBNA CR Normalized.py`` and can also run standalone for testing.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import asdict
from typing import Dict, List, Optional, Tuple

import aiohttp
import numpy as np
import pandas as pd

from fred_series_registry import (
    FREDSeriesSpec,
    FRED_EXPANSION_REGISTRY,
    OUTPUT_SHEET_MAP,
    get_series_ids_to_fetch,
    get_series_by_sheet,
    registry_to_dataframe,
    validate_registry,
)
from fred_transforms import (
    apply_all_registry_transforms,
    compute_spreads,
    compute_regime_flags,
    run_full_transform_pipeline,
)

logger = logging.getLogger(__name__)

FRED_API_BASE = "https://api.stlouisfed.org/fred"


# ---------------------------------------------------------------------------
# Async fetcher (compatible with upstream FREDDataFetcher pattern)
# ---------------------------------------------------------------------------
class FREDExpansionFetcher:
    """
    Async FRED series fetcher for the expansion registry.
    Mirrors the upstream FREDDataFetcher interface but operates on the
    expanded registry.
    """

    def __init__(
        self,
        api_key: str,
        concurrency: int = 3,
        rate_limit_delay: float = 1.0,
        years_back: int = 15,
    ):
        self.api_key = api_key
        self.concurrency = concurrency
        self.rate_limit_delay = rate_limit_delay
        self.years_back = years_back
        self.last_raw_obs_df: Optional[pd.DataFrame] = None
        self._failed_series: List[str] = []

    async def _fetch_single(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        series_id: str,
    ) -> Tuple[str, Optional[pd.DataFrame]]:
        """Fetch observations for a single series."""
        url = f"{FRED_API_BASE}/series/observations"
        start_date = pd.Timestamp.now() - pd.DateOffset(years=self.years_back)
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
            "observation_start": start_date.strftime("%Y-%m-%d"),
            "sort_order": "asc",
        }

        async with semaphore:
            try:
                await asyncio.sleep(self.rate_limit_delay)
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning(f"FRED returned {resp.status} for {series_id}")
                        self._failed_series.append(series_id)
                        return series_id, None
                    data = await resp.json()
                    obs = data.get("observations", [])
                    if not obs:
                        logger.warning(f"No observations for {series_id}")
                        self._failed_series.append(series_id)
                        return series_id, None

                    records = []
                    for o in obs:
                        val = o.get("value", ".")
                        if val == ".":
                            continue
                        try:
                            records.append({
                                "DATE": pd.Timestamp(o["date"]),
                                series_id: float(val),
                            })
                        except (ValueError, TypeError):
                            continue

                    if not records:
                        return series_id, None

                    df = pd.DataFrame(records).set_index("DATE")
                    return series_id, df

            except Exception as e:
                logger.warning(f"Error fetching {series_id}: {e}")
                self._failed_series.append(series_id)
                return series_id, None

    async def _fetch_metadata(
        self,
        session: aiohttp.ClientSession,
        semaphore: asyncio.Semaphore,
        series_id: str,
    ) -> Tuple[str, Dict]:
        """Fetch metadata for a single series."""
        url = f"{FRED_API_BASE}/series"
        params = {
            "series_id": series_id,
            "api_key": self.api_key,
            "file_type": "json",
        }
        async with semaphore:
            try:
                await asyncio.sleep(self.rate_limit_delay * 0.5)
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    if resp.status != 200:
                        return series_id, {}
                    data = await resp.json()
                    serieses = data.get("serieses", [])
                    if not serieses:
                        return series_id, {}
                    s = serieses[0]
                    return series_id, {
                        "frequency": s.get("frequency", ""),
                        "frequency_short": s.get("frequency_short", ""),
                        "units": s.get("units", ""),
                        "seasonal_adjustment_short": s.get("seasonal_adjustment_short", ""),
                        "observation_start": s.get("observation_start", ""),
                        "observation_end": s.get("observation_end", ""),
                        "title": s.get("title", ""),
                        "notes": (s.get("notes", "") or "")[:500],
                    }
            except Exception:
                return series_id, {}

    async def fetch_all(
        self,
        series_ids: List[str],
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str], pd.DataFrame]:
        """
        Fetch all requested series.
        Returns:
          (merged_df, descriptions_df, failed_series, metadata_df)
        """
        self._failed_series = []
        semaphore = asyncio.Semaphore(self.concurrency)

        async with aiohttp.ClientSession() as session:
            # Fetch data and metadata concurrently
            data_tasks = [
                self._fetch_single(session, semaphore, sid)
                for sid in series_ids
            ]
            meta_tasks = [
                self._fetch_metadata(session, semaphore, sid)
                for sid in series_ids
            ]

            data_results = await asyncio.gather(*data_tasks)
            meta_results = await asyncio.gather(*meta_tasks)

        # Merge data into wide-format
        frames = [df for _, df in data_results if df is not None]
        if frames:
            merged = frames[0]
            for f in frames[1:]:
                merged = merged.join(f, how="outer")
            merged = merged.sort_index()
        else:
            merged = pd.DataFrame()

        # Build raw observations (long format) for upstream compat
        raw_parts = []
        for sid, df in data_results:
            if df is not None:
                long = df.reset_index().melt(id_vars=["DATE"], var_name="SeriesID", value_name="VALUE")
                raw_parts.append(long)
        self.last_raw_obs_df = pd.concat(raw_parts, ignore_index=True) if raw_parts else pd.DataFrame()

        # Build metadata DataFrame
        meta_dict = {sid: meta for sid, meta in meta_results if meta}
        meta_df = pd.DataFrame.from_dict(meta_dict, orient="index")
        meta_df.index.name = "SeriesID"
        meta_df = meta_df.reset_index()

        # Build descriptions DataFrame
        desc_rows = []
        registry_map = {s.series_id: s for s in FRED_EXPANSION_REGISTRY}
        for sid in series_ids:
            spec = registry_map.get(sid)
            meta = meta_dict.get(sid, {})
            desc_rows.append({
                "Series ID": sid,
                "Category": spec.category if spec else "",
                "Subcategory": spec.subcategory if spec else "",
                "short": spec.display_name if spec else meta.get("title", sid),
                "long": meta.get("title", spec.display_name if spec else ""),
                "Priority": spec.priority if spec else 3,
                "Sheet": spec.sheet if spec else "",
                "Chart Group": spec.chart_group if spec else "",
                "Use Case": spec.use_case if spec else "",
                "Frequency": meta.get("frequency_short", spec.freq if spec else ""),
                "Units": meta.get("units", spec.units if spec else ""),
                "SA": meta.get("seasonal_adjustment_short", spec.seasonal_adjustment if spec else ""),
                "Discovery": spec.discovery_source if spec else "static",
                "Is Active": spec.is_active if spec else True,
                "Observation End": meta.get("observation_end", ""),
            })
        desc_df = pd.DataFrame(desc_rows)

        logger.info(
            f"Fetched {len(frames)} series successfully, "
            f"{len(self._failed_series)} failed"
        )

        return merged, desc_df, list(self._failed_series), meta_df


# ---------------------------------------------------------------------------
# Validation engine
# ---------------------------------------------------------------------------
def validate_fetched_data(
    df: pd.DataFrame,
    desc_df: pd.DataFrame,
    registry: List[FREDSeriesSpec],
) -> pd.DataFrame:
    """
    Run validation checks and return a DataFrame of issues.
    Checks:
      - Duplicate series IDs in registry
      - Discontinued series (observation_end before 2024)
      - Missing metadata fields
      - Stale releases (no data in last 6 months)
      - Columns in df not in registry
    """
    issues = []

    # Registry-level validation
    reg_issues = validate_registry()
    for issue_type, items in reg_issues.items():
        for item in items:
            issues.append({"check": issue_type, "series_id": "", "detail": item, "severity": "warning"})

    # Discontinued check
    if "Observation End" in desc_df.columns:
        for _, row in desc_df.iterrows():
            obs_end = str(row.get("Observation End", ""))
            if obs_end and obs_end < "2024-01-01":
                issues.append({
                    "check": "discontinued",
                    "series_id": row.get("Series ID", ""),
                    "detail": f"Last observation: {obs_end}",
                    "severity": "error",
                })

    # Stale data check
    if not df.empty:
        cutoff = pd.Timestamp.now() - pd.DateOffset(months=6)
        for col in df.columns:
            if col.startswith("SPREAD__") or col.startswith("REGIME__") or "__" in col:
                continue
            last_valid = df[col].last_valid_index()
            if last_valid is not None and last_valid < cutoff:
                issues.append({
                    "check": "stale_data",
                    "series_id": col,
                    "detail": f"Last valid: {last_valid.strftime('%Y-%m-%d')}",
                    "severity": "warning",
                })

    # Missing in registry
    reg_ids = {s.series_id for s in registry}
    for col in df.columns:
        if col not in reg_ids and not col.startswith(("SPREAD__", "REGIME__")) and "__" not in col:
            issues.append({
                "check": "orphan_column",
                "series_id": col,
                "detail": "Column present in data but not in registry",
                "severity": "info",
            })

    return pd.DataFrame(issues) if issues else pd.DataFrame(
        columns=["check", "series_id", "detail", "severity"]
    )


# ---------------------------------------------------------------------------
# Sheet-level output routing
# ---------------------------------------------------------------------------
def route_to_sheets(
    df: pd.DataFrame,
    registry: List[FREDSeriesSpec],
    desc_df: Optional[pd.DataFrame] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Split the wide-format transformed DataFrame into per-sheet DataFrames.
    Returns a dict of sheet_name → DataFrame ready for Excel write.
    """
    sheets: Dict[str, pd.DataFrame] = {}
    registry_map = {s.series_id: s for s in registry}

    for sheet_name in OUTPUT_SHEET_MAP:
        # Gather base series columns for this sheet
        base_cols = [
            s.series_id for s in registry
            if s.sheet == sheet_name and s.series_id in df.columns
        ]

        # Also gather derived columns (transforms, spreads, regimes)
        derived_cols = []
        for col in df.columns:
            if "__" in col:
                base_id = col.split("__")[0]
                if base_id in [s.series_id for s in registry if s.sheet == sheet_name]:
                    derived_cols.append(col)

        # Special: add SPREAD__ and REGIME__ columns to the most relevant sheet
        for col in df.columns:
            if col.startswith("SPREAD__") or col.startswith("REGIME__"):
                # Route spreads/regimes to the sheet of their primary component
                if "SBL" in col or "Deleveraging" in col:
                    if sheet_name == "FRED_SBL_Backdrop":
                        derived_cols.append(col)
                elif "Jumbo" in col or "Resi" in col:
                    if sheet_name == "FRED_Residential_Jumbo":
                        derived_cols.append(col)
                elif "CRE" in col:
                    if sheet_name == "FRED_CRE":
                        derived_cols.append(col)
                elif "HPI" in col or "HighTier" in col:
                    if sheet_name == "FRED_CaseShiller_Selected":
                        derived_cols.append(col)

        all_cols = sorted(set(base_cols + derived_cols))
        if all_cols:
            sheet_df = df[all_cols].copy()
            sheet_df.index.name = "DATE"
            sheets[sheet_name] = sheet_df

    # Always include the master registry as a metadata sheet
    sheets["FRED_Expansion_Registry"] = registry_to_dataframe()

    # Include descriptions if provided
    if desc_df is not None and not desc_df.empty:
        sheets["FRED_Expansion_Descriptions"] = desc_df

    return sheets


# ---------------------------------------------------------------------------
# Full pipeline orchestrator
# ---------------------------------------------------------------------------
async def run_expansion_pipeline_async(
    api_key: str,
    max_priority: int = 2,
    years_back: int = 15,
    discover_case_shiller: bool = True,
    concurrency: int = 3,
    rate_limit_delay: float = 1.0,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, List[str]]:
    """
    Execute the full FRED expansion pipeline:
      1. Build fetch list from registry
      2. Optionally discover Case-Shiller releases
      3. Fetch all series
      4. Apply transforms, spreads, regime flags
      5. Validate
      6. Route to sheets

    Returns:
      (sheet_dict, validation_df, failed_series)
    """
    # Build registry (optionally with discovery)
    registry = list(FRED_EXPANSION_REGISTRY)

    if discover_case_shiller:
        try:
            from fred_case_shiller_discovery import (
                discover_case_shiller_async,
                merge_discovered_into_registry,
            )
            discovered = await discover_case_shiller_async(api_key, rate_limit_delay)
            registry = merge_discovered_into_registry(discovered)
            logger.info(f"Registry after CS discovery: {len(registry)} series")
        except Exception as e:
            logger.warning(f"Case-Shiller discovery failed, using static seed: {e}")

    # Filter to max_priority
    active_specs = [s for s in registry if s.is_active and s.priority <= max_priority]
    series_ids = list(dict.fromkeys(s.series_id for s in active_specs))

    logger.info(f"Fetching {len(series_ids)} series (priority <= {max_priority})")

    # Fetch
    fetcher = FREDExpansionFetcher(
        api_key=api_key,
        concurrency=concurrency,
        rate_limit_delay=rate_limit_delay,
        years_back=years_back,
    )
    df, desc_df, failed, meta_df = await fetcher.fetch_all(series_ids)

    if df.empty:
        logger.error("No data fetched — pipeline cannot continue")
        return {}, pd.DataFrame(), failed

    logger.info(f"Fetched data shape: {df.shape}")

    # Transform
    df = run_full_transform_pipeline(df, registry)
    logger.info(f"After transforms: {df.shape[1]} columns")

    # Validate
    validation_df = validate_fetched_data(df, desc_df, registry)
    if not validation_df.empty:
        errs = len(validation_df[validation_df["severity"] == "error"])
        warns = len(validation_df[validation_df["severity"] == "warning"])
        logger.info(f"Validation: {errs} errors, {warns} warnings")

    # Route to sheets
    sheets = route_to_sheets(df, registry, desc_df)

    # Add validation sheet
    if not validation_df.empty:
        sheets["FRED_Expansion_Validation"] = validation_df

    return sheets, validation_df, failed


def run_expansion_pipeline_sync(
    api_key: str,
    max_priority: int = 2,
    years_back: int = 15,
    discover_case_shiller: bool = True,
) -> Tuple[Dict[str, pd.DataFrame], pd.DataFrame, List[str]]:
    """Synchronous wrapper for the async pipeline."""
    try:
        loop = asyncio.get_running_loop()
        import nest_asyncio
        nest_asyncio.apply()
        return loop.run_until_complete(
            run_expansion_pipeline_async(
                api_key, max_priority, years_back, discover_case_shiller
            )
        )
    except RuntimeError:
        return asyncio.run(
            run_expansion_pipeline_async(
                api_key, max_priority, years_back, discover_case_shiller
            )
        )


# ---------------------------------------------------------------------------
# Excel writer helper
# ---------------------------------------------------------------------------
def write_expansion_sheets_to_excel(
    sheets: Dict[str, pd.DataFrame],
    file_path: str,
    mode: str = "a",
) -> None:
    """
    Write sheet_dict to an Excel file.
    If mode='a' (append), adds sheets to an existing workbook.
    If mode='w' (write), creates a new workbook.
    """
    kwargs = {"engine": "openpyxl"}
    if mode == "a":
        kwargs["mode"] = "a"
        kwargs["if_sheet_exists"] = "replace"

    with pd.ExcelWriter(file_path, **kwargs) as writer:
        for sheet_name, df in sheets.items():
            # Truncate sheet name to Excel limit
            sn = sheet_name[:31]
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
            df.to_excel(writer, sheet_name=sn, index=False)
            logger.info(f"  Wrote sheet '{sn}': {df.shape}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from pathlib import Path

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("ERROR: Set FRED_API_KEY environment variable")
        sys.exit(1)

    print("=" * 70)
    print("FRED Expansion Pipeline — Standalone Test Run")
    print("=" * 70)

    sheets, validation_df, failed = run_expansion_pipeline_sync(
        api_key, max_priority=2, years_back=15, discover_case_shiller=True,
    )

    print(f"\nSheets generated: {list(sheets.keys())}")
    for name, df in sheets.items():
        print(f"  {name}: {df.shape}")

    if failed:
        print(f"\nFailed series ({len(failed)}): {failed}")

    if not validation_df.empty:
        print(f"\nValidation issues:")
        print(validation_df.to_string(index=False))

    # Optionally write to test file
    out_dir = Path(__file__).parent / "output"
    out_dir.mkdir(exist_ok=True)
    out_path = str(out_dir / "fred_expansion_test.xlsx")
    write_expansion_sheets_to_excel(sheets, out_path, mode="w")
    print(f"\nTest output written to: {out_path}")
