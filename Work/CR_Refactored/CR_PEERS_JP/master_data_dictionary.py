"""
MasterDataDictionary — Single source of truth for Call Report & FDIC metric definitions.

Replaces the fragmented MDRMValidator, MDRMClient, EnhancedDataDictionaryClient,
and the hardcoded FDIC_FFIEC_Series_Key.py with a unified three-tier waterfall:

    Tier 1 (Regulatory Truth) : Federal Reserve MDRM CSV  (downloaded as ZIP, cached 30 days)
    Tier 2 (FDIC API)         : FDIC BankFind Suite API    (schema fetched dynamically)
    Tier 3 (Local / Derived)  : Locally-defined calculated fields (e.g. TTM_NCO_Rate)

No BeautifulSoup scraping.  No XBRL XML parsing.  Just Pandas + requests.
"""

from __future__ import annotations

import io
import logging
import os
import re
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
#  Constants
# ---------------------------------------------------------------------------

_MDRM_ZIP_URL = "https://www.federalreserve.gov/apps/mdrm/pdf/MDRM.zip"
_MDRM_CACHE_MAX_AGE_DAYS = 30

_FDIC_API_BASE = "https://banks.data.fdic.gov/api"
_FDIC_FINANCIALS_ENDPOINT = f"{_FDIC_API_BASE}/financials"

# Call Report mnemonic prefixes (4-letter schedule identifiers).
# Stripping these yields the base item code used in the MDRM master file.
_CR_PREFIXES = re.compile(
    r"^(RIADB|RIADA|RIAD|UBPRE|UBPRM|UBPR|RCFD|RCFN|RCON|RCOA|RCOB|RCOW)"
)

# How many seconds to wait when hitting the FDIC API
_FDIC_REQUEST_TIMEOUT = 30

# Maximum retries for network-bound operations
_MAX_RETRIES = 3


# ---------------------------------------------------------------------------
#  Tier 3 — Local / Derived Metric Definitions
# ---------------------------------------------------------------------------
# These are calculated fields that do not exist in any regulatory download.
# Each entry follows the contract: {code: {short, long}}.

LOCAL_DERIVED_METRICS: Dict[str, Dict[str, str]] = {
    # --- Bank-Level Profitability & Coverage ---
    "Cost_of_Funds": {
        "short": "Cost of Funds",
        "long": "Annualized cost of interest-bearing liabilities "
                "(Quarterly Interest Expense * 4 / Average Interest-Bearing Liabilities).",
    },
    "Allowance_to_Gross_Loans_Rate": {
        "short": "ACL / Total Loans",
        "long": "Total Allowance for Credit Losses as a percentage of Total Gross Loans.",
    },
    "Risk_Adj_Allowance_Coverage": {
        "short": "ACL / (Loans - SBL)",
        "long": "Risk-Adjusted Coverage: Total Allowance divided by "
                "(Total Loans minus Securities-Based Lending).",
    },
    "Nonaccrual_to_Gross_Loans_Rate": {
        "short": "Nonaccrual / Loans",
        "long": "Total nonaccrual loans as a percentage of total gross loans.",
    },
    "TTM_NCO_Rate": {
        "short": "Total NCO Rate",
        "long": "Trailing 12-Month sum of Net Charge-Offs as a percentage of "
                "TTM Average Gross Loans.",
    },
    "TTM_PD30_Rate": {
        "short": "TTM PD 30-89 / Avg Loans",
        "long": "Trailing 12-Month average of loans 30-89 days past due as a "
                "percentage of TTM average gross loans.",
    },
    "TTM_PD90_Rate": {
        "short": "TTM PD 90+ / Avg Loans",
        "long": "Trailing 12-Month average of loans 90+ days past due as a "
                "percentage of TTM average gross loans.",
    },
    "TTM_Past_Due_Rate": {
        "short": "Total TTM PD Rate",
        "long": "Trailing 12-Month average of total loans 30+ days past due as a "
                "percentage of TTM average gross loans.",
    },

    # --- Loan Composition (Portfolio Mix) ---
    "SBL_Composition": {
        "short": "SBL %",
        "long": "Securities-Based Lending as a % of Total Loans.",
    },
    "Fund_Finance_Composition": {
        "short": "Fund Finance %",
        "long": "Loans to Nondepository Financial Institutions (PE/VC Capital Call Lines) "
                "as a % of Total Loans.",
    },
    "Wealth_Resi_Composition": {
        "short": "Wealth Resi %",
        "long": "Wealth Residential (Jumbo 1-4 Family First Liens + HELOCs) as a % of Total Loans.",
    },
    "Corp_CI_Composition": {
        "short": "Corp C&I %",
        "long": "Traditional Commercial & Industrial loans as a % of Total Loans.",
    },
    "CRE_OO_Composition": {
        "short": "CRE Owner-Occ %",
        "long": "Owner-Occupied Nonfarm Nonresidential CRE as a % of Total Loans.",
    },
    "CRE_Investment_Composition": {
        "short": "CRE Invest. %",
        "long": "Investment CRE (Construction, Multifamily, Non-OO Nonfarm) as a % of Total Loans.",
    },
    "Consumer_Auto_Composition": {
        "short": "Auto %",
        "long": "Automobile loans as a % of Total Loans.",
    },
    "Consumer_Other_Composition": {
        "short": "Cons. Other %",
        "long": "Other Consumer loans (Credit Cards, Unsecured, Other Revolving) "
                "as a % of Total Loans.",
    },
    "IDB_CI_Composition": {
        "short": "C&I Comp.",
        "long": "Commercial & Industrial (including Owner-Occupied CRE) loans "
                "as a percentage of total gross loans.",
    },
    "IDB_CRE_Composition": {
        "short": "CRE Comp.",
        "long": "Commercial Real Estate (Construction, Multifamily, Farmland, "
                "Income-Producing) loans as a percentage of total gross loans.",
    },
    "IDB_Consumer_Composition": {
        "short": "Consumer Comp.",
        "long": "Consumer loans as a percentage of total gross loans.",
    },
    "IDB_Resi_Composition": {
        "short": "Residential Comp.",
        "long": "Residential Real Estate (including 1-4 Family Investor) loans "
                "as a percentage of total gross loans.",
    },
    "IDB_Other_Composition": {
        "short": "Other Comp.",
        "long": "Other loans and leases as a percentage of total gross loans.",
    },

    # --- Segment Risk: SBL ---
    "SBL_TTM_NCO_Rate": {
        "short": "SBL NCO Rate",
        "long": "Net Charge-offs for SBL (Estimated from All Other Loans) as a % of SBL Loans.",
    },
    "SBL_TTM_PD30_Rate": {
        "short": "SBL PD 30-89",
        "long": "Past Due 30-89 Days for SBL (Proxy: All Other Loans) as a % of SBL Loans.",
    },
    "SBL_NA_Rate": {
        "short": "SBL Nonaccrual",
        "long": "Nonaccrual Rate for SBL (Proxy: All Other Loans).",
    },

    # --- Segment Risk: Fund Finance ---
    "Fund_Finance_TTM_PD30_Rate": {
        "short": "Fund Fin. PD 30-89",
        "long": "Past Due 30-89 Days for Nondepository Fin. Institutions (Memo Item).",
    },
    "Fund_Finance_NA_Rate": {
        "short": "Fund Fin. Nonaccrual",
        "long": "Nonaccrual Rate for Nondepository Fin. Institutions (Memo Item).",
    },

    # --- Segment Risk: Wealth Residential ---
    "Wealth_Resi_TTM_NCO_Rate": {
        "short": "Wealth Resi NCOs",
        "long": "Net Charge-offs for 1-4 Family & HELOCs as a % of Avg Segment Loans.",
    },
    "Wealth_Resi_TTM_PD30_Rate": {
        "short": "Wealth Resi PD 30-89",
        "long": "Past Due 30-89 Days for 1-4 Family & HELOCs.",
    },
    "Wealth_Resi_NA_Rate": {
        "short": "Wealth Resi Nonaccrual",
        "long": "Nonaccrual Rate for 1-4 Family & HELOCs.",
    },

    # --- Segment Risk: C&I ---
    "Corp_CI_TTM_NCO_Rate": {
        "short": "C&I NCO Rate",
        "long": "Net Charge-offs for C&I Loans as a % of Avg C&I Loans.",
    },
    "Corp_CI_TTM_PD30_Rate": {
        "short": "C&I PD 30-89",
        "long": "Past Due 30-89 Days for C&I Loans.",
    },
    "Corp_CI_NA_Rate": {
        "short": "C&I Nonaccrual",
        "long": "Nonaccrual Rate for C&I Loans.",
    },

    # --- Segment Risk: CRE (Owner-Occupied) ---
    "CRE_OO_TTM_NCO_Rate": {
        "short": "CRE OO NCO Rate",
        "long": "Net Charge-offs for Owner-Occupied CRE as a % of Avg OO CRE Loans.",
    },
    "CRE_OO_TTM_PD30_Rate": {
        "short": "CRE OO PD 30-89",
        "long": "Past Due 30-89 Days for Owner-Occupied CRE.",
    },
    "CRE_OO_NA_Rate": {
        "short": "CRE OO Nonaccrual",
        "long": "Nonaccrual Rate for Owner-Occupied CRE.",
    },

    # --- Segment Risk: CRE (Investment) ---
    "CRE_Investment_TTM_NCO_Rate": {
        "short": "CRE Inv. NCO Rate",
        "long": "Net Charge-offs for Investment CRE (Constr/Multi/Non-OO) "
                "as a % of Avg Inv. CRE Loans.",
    },
    "CRE_Investment_TTM_PD30_Rate": {
        "short": "CRE Inv. PD 30-89",
        "long": "Past Due 30-89 Days for Investment CRE.",
    },
    "CRE_Investment_NA_Rate": {
        "short": "CRE Inv. Nonaccrual",
        "long": "Nonaccrual Rate for Investment CRE.",
    },
    "CRE_Concentration_Capital_Risk": {
        "short": "Inv. CRE / Capital",
        "long": "Total Investment CRE Loans as a percentage of Tier 1 Capital + ACL.",
    },

    # --- Segment Risk: Consumer ---
    "Consumer_Auto_TTM_NCO_Rate": {
        "short": "Auto NCO Rate",
        "long": "Net Charge-offs for Auto Loans as a % of Avg Auto Loans.",
    },
    "Consumer_Other_TTM_NCO_Rate": {
        "short": "Cons. Other NCOs",
        "long": "Net Charge-offs for Credit Cards & Other Consumer Loans "
                "as a % of Avg Segment Loans.",
    },

    # --- Segment Growth Metrics ---
    "Total_Loan_Growth_TTM": {
        "short": "Total Loan Growth",
        "long": "Trailing 12-Month (Year-over-Year) growth rate of Total Gross Loans.",
    },
    "SBL_Growth_TTM": {
        "short": "SBL Growth",
        "long": "TTM growth rate of Securities-Based Lending.",
    },
    "Fund_Finance_Growth_TTM": {
        "short": "Fund Finance Growth",
        "long": "TTM growth rate of Loans to Nondepository Financial Institutions.",
    },
    "Wealth_Resi_Growth_TTM": {
        "short": "Wealth Resi Growth",
        "long": "TTM growth rate of Wealth Residential (1-4 Family + HELOCs).",
    },
    "Corp_CI_Growth_TTM": {
        "short": "C&I Growth",
        "long": "TTM growth rate of Commercial & Industrial portfolio.",
    },
    "CRE_OO_Growth_TTM": {
        "short": "CRE OO Growth",
        "long": "TTM growth rate of Owner-Occupied CRE.",
    },
    "CRE_Investment_Growth_TTM": {
        "short": "CRE Inv. Growth",
        "long": "TTM growth rate of Investment CRE.",
    },
    "Consumer_Auto_Growth_TTM": {
        "short": "Auto Growth",
        "long": "TTM growth rate of Automobile Loans.",
    },
    "Consumer_Other_Growth_TTM": {
        "short": "Cons. Other Growth",
        "long": "TTM growth rate of Other Consumer Loans.",
    },
    "IDB_CRE_Growth_TTM": {
        "short": "TTM CRE Growth",
        "long": "Trailing 12-Month (YoY) growth rate of CRE portfolio.",
    },
    "IDB_CRE_Growth_36M": {
        "short": "36M CRE Growth",
        "long": "36-Month (3-Year) growth rate of CRE portfolio.",
    },

    # --- TTM PD Rates (Granular) ---
    "IDB_CI_TTM_PD30_Rate": {
        "short": "TTM C&I PD 30-89 Rate",
        "long": "TTM avg C&I loans 30-89 days past due as a % of TTM avg C&I loans.",
    },
    "IDB_CI_TTM_PD90_Rate": {
        "short": "TTM C&I PD 90+ Rate",
        "long": "TTM avg C&I loans 90+ days past due as a % of TTM avg C&I loans.",
    },
    "IDB_CRE_TTM_PD30_Rate": {
        "short": "TTM CRE PD 30-89 Rate",
        "long": "TTM avg CRE loans 30-89 days past due as a % of TTM avg CRE loans.",
    },
    "IDB_CRE_TTM_PD90_Rate": {
        "short": "TTM CRE PD 90+ Rate",
        "long": "TTM avg CRE loans 90+ days past due as a % of TTM avg CRE loans.",
    },
    "IDB_Consumer_TTM_PD30_Rate": {
        "short": "TTM Consumer PD 30-89 Rate",
        "long": "TTM avg Consumer loans 30-89 days past due.",
    },
    "IDB_Consumer_TTM_PD90_Rate": {
        "short": "TTM Consumer PD 90+ Rate",
        "long": "TTM avg Consumer loans 90+ days past due.",
    },
    "IDB_Resi_TTM_PD30_Rate": {
        "short": "TTM Resi. PD 30-89 Rate",
        "long": "TTM avg Residential loans 30-89 days past due.",
    },
    "IDB_Resi_TTM_PD90_Rate": {
        "short": "TTM Resi. PD 90+ Rate",
        "long": "TTM avg Residential loans 90+ days past due.",
    },
    "IDB_Other_TTM_PD30_Rate": {
        "short": "TTM Other PD 30-89 Rate",
        "long": "TTM avg Other loans 30-89 days past due.",
    },
    "IDB_Other_TTM_PD90_Rate": {
        "short": "TTM Other PD 90+ Rate",
        "long": "TTM avg Other loans 90+ days past due.",
    },

    # --- Capital Composition ---
    "Total_Capital": {
        "short": "Total Capital",
        "long": "Total risk-based capital available for regulatory purposes.",
    },
    "Common_Stock_Pct": {
        "short": "Common Stock %",
        "long": "Common stock as a percentage of total equity capital.",
    },
    "EQSUR_Pct": {
        "short": "EQSUR %",
        "long": "EQSUR as a percentage of total equity capital.",
    },
    "Retained_Earnings_Pct": {
        "short": "Retained Earnings %",
        "long": "Retained earnings (net of AOCI) as a percentage of total equity.",
    },
    "Preferred_Stock_Pct": {
        "short": "Preferred Stock %",
        "long": "Perpetual preferred stock as a percentage of total equity capital.",
    },
    "CI_to_Capital_Risk": {
        "short": "C&I / Total Capital",
        "long": "Commercial & Industrial loans as a percentage of total capital.",
    },
    "TTM_Common_Stock_Pct": {
        "short": "TTM Common Stock %",
        "long": "Trailing 12-month average common stock % of total equity.",
    },
    "TTM_EQSUR_Pct": {
        "short": "TTM EQSUR %",
        "long": "Trailing 12-month average EQSUR % of total equity.",
    },
    "TTM_Retained_Earnings_Pct": {
        "short": "TTM Retained Earnings %",
        "long": "Trailing 12-month average retained earnings % of total equity.",
    },
    "TTM_Preferred_Stock_Pct": {
        "short": "TTM Preferred Stock %",
        "long": "Trailing 12-month average preferred stock % of total equity.",
    },

    # --- RI-C Segment ACL (FFIEC schedule RI-C disaggregated allowance) ---
    "RIC_CRE_Nonaccrual_Rate": {
        "short": "CRE – Nonaccrual Rate %",
        "long": "Nonaccrual loans divided by Amortized Cost (RI-C CRE segment).",
    },
    "Total_ACL": {
        "short": "Total Allowance for Credit Losses",
        "long": "Sum of on-balance-sheet allowance (LNATRES) and off-balance-sheet "
                "allowance (AOBS).",
    },
    "Loan_Yield_Proxy": {
        "short": "Loan Yield (Proxy)",
        "long": "TTM Interest Income on Loans / TTM Average Gross Loans.",
    },
}


# ---------------------------------------------------------------------------
#  MasterDataDictionary
# ---------------------------------------------------------------------------

class MasterDataDictionary:
    """Unified metric definition resolver with a three-tier waterfall.

    Usage::

        mdd = MasterDataDictionary(cache_dir="./data_cache")
        result = mdd.lookup_metric("RCON2170")
        report = mdd.export_dictionary_report()
    """

    def __init__(self, cache_dir: str = ".data_dictionary_cache") -> None:
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        # Lazy-loaded DataFrames
        self._mdrm_df: Optional[pd.DataFrame] = None
        self._fdic_schema: Optional[Dict[str, Dict[str, str]]] = None

        # Lookup caches (populated on first use, reset if underlying data refreshes)
        self._mdrm_index: Optional[Dict[str, Dict[str, str]]] = None
        self._fdic_index: Optional[Dict[str, Dict[str, str]]] = None

    # ------------------------------------------------------------------
    #  Public API
    # ------------------------------------------------------------------

    def lookup_metric(self, code: str) -> Dict[str, str]:
        """Resolve a metric definition through the three-tier waterfall.

        Returns a dict with keys:
            Metric_Code, Metric_Name, Description, Source_of_Truth, Is_Derived

        If no definition is found anywhere, Source_of_Truth will be
        ``"Not Found"`` and Description will be ``"Definition Not Found"``.
        """
        normalized = self._normalize_key(code)

        # --- Tier 1: Federal Reserve MDRM CSV ---
        result = self._lookup_mdrm(normalized)
        if result is not None:
            return self._format_result(
                code=code,
                name=result.get("name", normalized),
                description=result.get("description", ""),
                source="Tier 1 — Federal Reserve MDRM",
                is_derived=False,
            )

        # If the full code (e.g. RCON2170) failed, try the stripped base item
        base_item = self._strip_prefix(normalized)
        if base_item != normalized:
            result = self._lookup_mdrm(base_item)
            if result is not None:
                return self._format_result(
                    code=code,
                    name=result.get("name", base_item),
                    description=result.get("description", ""),
                    source="Tier 1 — Federal Reserve MDRM (base item)",
                    is_derived=False,
                )

        # --- Tier 2: FDIC BankFind API ---
        result = self._lookup_fdic(normalized)
        if result is not None:
            return self._format_result(
                code=code,
                name=result.get("name", normalized),
                description=result.get("description", ""),
                source="Tier 2 — FDIC BankFind API",
                is_derived=False,
            )

        # Also try base item against FDIC
        if base_item != normalized:
            result = self._lookup_fdic(base_item)
            if result is not None:
                return self._format_result(
                    code=code,
                    name=result.get("name", base_item),
                    description=result.get("description", ""),
                    source="Tier 2 — FDIC BankFind API (base item)",
                    is_derived=False,
                )

        # --- Tier 3: Local / Derived Metrics ---
        result = self._lookup_local(normalized)
        if result is not None:
            return self._format_result(
                code=code,
                name=result.get("name", normalized),
                description=result.get("description", ""),
                source="Tier 3 — Local/Derived",
                is_derived=True,
            )

        # --- Not found ---
        return self._format_result(
            code=code,
            name=normalized,
            description="Definition Not Found",
            source="Not Found",
            is_derived=False,
        )

    def export_dictionary_report(self) -> pd.DataFrame:
        """Build a comprehensive DataFrame of every known metric definition.

        Columns: Metric_Code, Metric_Name, Description, Source_of_Truth, Is_Derived
        """
        rows: List[Dict[str, Any]] = []

        # Tier 1 — MDRM
        mdrm_idx = self._get_mdrm_index()
        for code, info in mdrm_idx.items():
            rows.append({
                "Metric_Code": code,
                "Metric_Name": info.get("name", code),
                "Description": info.get("description", ""),
                "Source_of_Truth": "Tier 1 — Federal Reserve MDRM",
                "Is_Derived": False,
            })

        seen_codes = {r["Metric_Code"] for r in rows}

        # Tier 2 — FDIC (only codes not already covered by Tier 1)
        fdic_idx = self._get_fdic_index()
        for code, info in fdic_idx.items():
            if code.upper() not in seen_codes:
                rows.append({
                    "Metric_Code": code,
                    "Metric_Name": info.get("name", code),
                    "Description": info.get("description", ""),
                    "Source_of_Truth": "Tier 2 — FDIC BankFind API",
                    "Is_Derived": False,
                })
                seen_codes.add(code.upper())

        # Tier 3 — Local / Derived
        for code, info in LOCAL_DERIVED_METRICS.items():
            if code.upper() not in seen_codes:
                rows.append({
                    "Metric_Code": code,
                    "Metric_Name": info.get("short", code),
                    "Description": info.get("long", ""),
                    "Source_of_Truth": "Tier 3 — Local/Derived",
                    "Is_Derived": True,
                })
                seen_codes.add(code.upper())

        df = pd.DataFrame(rows)
        if not df.empty:
            df.sort_values("Metric_Code", inplace=True, ignore_index=True)
        return df

    def get_mdrm_dataframe(self) -> Optional[pd.DataFrame]:
        """Return the raw MDRM DataFrame (all columns, all rows).

        Useful for schedule-level filtering on the ``Reporting Form`` or
        ``Description`` columns (e.g. RC-C, RC-N, RI-B).
        """
        return self._load_mdrm_dataframe()

    # ------------------------------------------------------------------
    #  Tier 1 — Federal Reserve MDRM
    # ------------------------------------------------------------------

    def _get_mdrm_index(self) -> Dict[str, Dict[str, str]]:
        """Return the MDRM lookup dict, loading from cache or downloading.

        The MDRM CSV has columns ``Mnemonic`` (e.g. RCFD, RCON) and
        ``Item Code`` (e.g. 2170).  We index entries by:
          - The composite key  ``Mnemonic + Item Code`` (e.g. RCFD2170)
          - The bare ``Item Code`` alone (e.g. 2170) — first seen wins
          - The ``Item Name`` as an alias (e.g. TOTAL ASSETS)

        This lets callers look up by full MDRM identifier *or* just the
        4-character item number.
        """
        if self._mdrm_index is not None:
            return self._mdrm_index

        df = self._load_mdrm_dataframe()
        index: Dict[str, Dict[str, str]] = {}

        if df is not None and not df.empty:
            col_map = self._detect_mdrm_columns(df)
            mnemonic_col = col_map.get("mnemonic")    # e.g. "Mnemonic"
            item_code_col = col_map.get("item_code")  # e.g. "Item Code"
            name_col = col_map.get("name")             # e.g. "Item Name"
            desc_col = col_map.get("description")      # e.g. "Description"

            for _, row in df.iterrows():
                mnemonic = (
                    str(row.get(mnemonic_col, "")).strip().upper()
                    if mnemonic_col else ""
                )
                item_code = (
                    str(row.get(item_code_col, "")).strip().upper()
                    if item_code_col else ""
                )
                name = str(row.get(name_col, "")).strip() if name_col else ""
                desc = str(row.get(desc_col, "")).strip() if desc_col else name

                if not mnemonic and not item_code:
                    continue

                entry = {"name": name or item_code, "description": desc}

                # Composite key: e.g. RCFD2170
                if mnemonic and item_code:
                    composite = mnemonic + item_code
                    index[composite] = entry

                # Bare mnemonic key (e.g. RCFD) — only if not already set
                if mnemonic and mnemonic not in index:
                    index[mnemonic] = entry

                # Bare item code (e.g. 2170) — first seen wins so the most
                # common reporting form takes precedence
                if item_code and item_code not in index:
                    index[item_code] = entry

        self._mdrm_index = index
        logger.info("MDRM index built with %d entries.", len(index))
        return index

    def _load_mdrm_dataframe(self) -> Optional[pd.DataFrame]:
        """Load the MDRM CSV, using local cache if fresh enough."""
        if self._mdrm_df is not None:
            return self._mdrm_df

        csv_path = self._cache_dir / "MDRM.csv"

        # Check cache freshness
        if csv_path.exists():
            age = datetime.now() - datetime.fromtimestamp(csv_path.stat().st_mtime)
            if age < timedelta(days=_MDRM_CACHE_MAX_AGE_DAYS):
                logger.info("Loading MDRM from cache (%s days old).", age.days)
                try:
                    self._mdrm_df = self._read_mdrm_csv(csv_path)
                    return self._mdrm_df
                except Exception as exc:
                    logger.warning("Cached MDRM CSV is corrupt, re-downloading: %s", exc)

        # Download fresh copy
        self._mdrm_df = self._download_mdrm_zip(csv_path)
        return self._mdrm_df

    @staticmethod
    def _read_mdrm_csv(path: Path) -> pd.DataFrame:
        """Read an MDRM CSV, auto-detecting the optional 'PUBLIC' marker row."""
        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            first_line = fh.readline().strip().strip('"')

        skip = 1 if first_line.upper() in ("PUBLIC", "CONFIDENTIAL") else 0
        return pd.read_csv(path, dtype=str, skiprows=skip, low_memory=False)

    def _download_mdrm_zip(self, csv_dest: Path) -> Optional[pd.DataFrame]:
        """Download the MDRM ZIP from the Federal Reserve and extract the CSV."""
        logger.info("Downloading MDRM ZIP from %s ...", _MDRM_ZIP_URL)
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    _MDRM_ZIP_URL,
                    timeout=60,
                    headers={"User-Agent": "MasterDataDictionary/1.0"},
                )
                resp.raise_for_status()
                break
            except requests.RequestException as exc:
                logger.warning(
                    "MDRM download attempt %d/%d failed: %s",
                    attempt, _MAX_RETRIES, exc,
                )
                if attempt == _MAX_RETRIES:
                    logger.error("All MDRM download attempts exhausted.")
                    return None
                time.sleep(2 ** attempt)

        # Extract the CSV from the ZIP
        try:
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_names = [
                    n for n in zf.namelist()
                    if n.lower().endswith(".csv")
                ]
                if not csv_names:
                    # Fallback: try any non-directory entry
                    csv_names = [n for n in zf.namelist() if not n.endswith("/")]

                if not csv_names:
                    logger.error("MDRM ZIP contains no usable files.")
                    return None

                target_name = csv_names[0]
                logger.info("Extracting '%s' from MDRM ZIP.", target_name)

                raw_bytes = zf.read(target_name)

                # The Fed's MDRM CSV has a quirk: the very first line is a
                # single word "PUBLIC" (a visibility marker), followed by the
                # real header row on line 2.  Detect and skip that marker.
                df = None
                for encoding in ("utf-8", "latin-1", "cp1252"):
                    try:
                        text = raw_bytes.decode(encoding)
                        lines = text.split("\n", 2)
                        skip = 0
                        if lines and lines[0].strip().upper() in ("PUBLIC", "CONFIDENTIAL"):
                            skip = 1
                        df = pd.read_csv(
                            io.StringIO(text),
                            dtype=str,
                            skiprows=skip,
                            low_memory=False,
                        )
                        # Validate we got real columns (not a single "PUBLIC" col)
                        if len(df.columns) > 2:
                            break
                        df = None
                    except (UnicodeDecodeError, pd.errors.ParserError):
                        continue

                if df is None or df.empty:
                    logger.error("Could not parse MDRM CSV with any encoding.")
                    return None

                # Persist cleaned CSV to local cache (no marker row)
                df.to_csv(csv_dest, index=False)
                logger.info("MDRM CSV cached at %s (%d rows).", csv_dest, len(df))
                return df

        except zipfile.BadZipFile:
            logger.error("Downloaded MDRM file is not a valid ZIP.")
            return None

    @staticmethod
    def _detect_mdrm_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """Heuristically map MDRM CSV columns to semantic roles.

        Returns a dict with keys: mnemonic, item_code, name, description.
        """
        cols_lower = {c.strip().lower(): c for c in df.columns}

        def _find(candidates: List[str]) -> Optional[str]:
            # Exact match first
            for cand in candidates:
                if cand.lower() in cols_lower:
                    return cols_lower[cand.lower()]
            # Partial match fallback
            for cand in candidates:
                for col_lower, col_orig in cols_lower.items():
                    if cand.lower() in col_lower:
                        return col_orig
            return None

        return {
            "mnemonic": _find([
                "Mnemonic", "MDRM Mnemonic", "Series Mnemonic",
            ]),
            "item_code": _find([
                "Item Code", "ItemCode", "Item_Code", "Item Number",
            ]),
            "name": _find([
                "Item Name", "Series Name", "Short Definition",
                "Metric Name", "Field Name", "Item_Name",
            ]),
            "description": _find([
                "Description", "Long Definition", "Series Description",
                "Item Description", "Definition",
            ]),
        }

    def _lookup_mdrm(self, code: str) -> Optional[Dict[str, str]]:
        """Look up a single code in the MDRM index."""
        idx = self._get_mdrm_index()
        return idx.get(code.upper())

    # ------------------------------------------------------------------
    #  Tier 2 — FDIC BankFind API (Dynamic Schema)
    # ------------------------------------------------------------------

    def _get_fdic_index(self) -> Dict[str, Dict[str, str]]:
        """Return the FDIC schema dict, fetching dynamically if needed."""
        if self._fdic_index is not None:
            return self._fdic_index

        schema = self._fetch_fdic_schema()
        self._fdic_index = schema if schema else {}
        return self._fdic_index

    def _fetch_fdic_schema(self) -> Dict[str, Dict[str, str]]:
        """Query the FDIC BankFind Suite API to extract field definitions.

        Strategy: Fetch a single row of financials data with all available
        fields, then query the /financials endpoint's field metadata.  The FDIC
        API exposes field titles/descriptions via a dedicated endpoint at
        ``/financials?$describe``.  If that fails, we fall back to parsing the
        YAML documentation files or a minimal metadata probe.
        """
        cache_path = self._cache_dir / "fdic_schema.csv"

        # Use cached schema if less than 30 days old
        if cache_path.exists():
            age = datetime.now() - datetime.fromtimestamp(cache_path.stat().st_mtime)
            if age < timedelta(days=_MDRM_CACHE_MAX_AGE_DAYS):
                logger.info("Loading FDIC schema from cache (%s days old).", age.days)
                try:
                    df = pd.read_csv(cache_path, dtype=str)
                    return {
                        row["code"]: {"name": row.get("name", ""), "description": row.get("description", "")}
                        for _, row in df.iterrows()
                        if row.get("code")
                    }
                except Exception as exc:
                    logger.warning("Cached FDIC schema corrupt, re-fetching: %s", exc)

        schema: Dict[str, Dict[str, str]] = {}

        # --- Approach 1: FDIC financials field_list metadata endpoint ---
        schema = self._fetch_fdic_schema_via_financials()

        # --- Approach 2: Fallback to the summary/locations/institutions endpoints ---
        if not schema:
            schema = self._fetch_fdic_schema_via_summary()

        # Cache results
        if schema:
            rows = [
                {"code": k, "name": v.get("name", ""), "description": v.get("description", "")}
                for k, v in schema.items()
            ]
            pd.DataFrame(rows).to_csv(cache_path, index=False)
            logger.info("FDIC schema cached at %s (%d fields).", cache_path, len(schema))

        return schema

    def _fetch_fdic_schema_via_financials(self) -> Dict[str, Dict[str, str]]:
        """Probe the FDIC /financials endpoint to discover available fields
        and their human-readable descriptions.

        The FDIC BankFind API returns field metadata in the ``meta.labels``
        object of every response.  We issue two requests:

        1. A bare ``limit=1`` to get whatever default fields the API returns.
        2. A targeted request that explicitly asks for common Call Report
           field codes so their labels appear in the metadata even if the
           default response omits them.
        """
        schema: Dict[str, Dict[str, str]] = {}
        headers = {"User-Agent": "MasterDataDictionary/1.0"}

        def _ingest(payload: dict) -> None:
            meta = payload.get("meta", {})
            labels = meta.get("labels", {})
            for field_code, label in labels.items():
                field_upper = field_code.strip().upper()
                label_str = str(label).strip()
                if field_upper not in schema or not schema[field_upper]["description"]:
                    schema[field_upper] = {
                        "name": label_str if label_str else field_upper,
                        "description": label_str if label_str else "",
                    }

            # Also harvest any fields present in the data rows
            for row_obj in payload.get("data", []):
                row_data = row_obj.get("data", row_obj)
                if isinstance(row_data, dict):
                    for field_code in row_data.keys():
                        field_upper = field_code.strip().upper()
                        if field_upper not in schema:
                            schema[field_upper] = {
                                "name": field_upper,
                                "description": "",
                            }

        # --- Request 1: Default fields ---
        try:
            resp = requests.get(
                _FDIC_FINANCIALS_ENDPOINT,
                params={"limit": 1, "sort_by": "REPDTE", "sort_order": "DESC"},
                timeout=_FDIC_REQUEST_TIMEOUT,
                headers=headers,
            )
            resp.raise_for_status()
            _ingest(resp.json())
        except (requests.RequestException, ValueError) as exc:
            logger.warning("FDIC default schema fetch failed: %s", exc)

        # --- Request 2: Explicit common fields ---
        # These are the most commonly used FDIC financial series codes.
        # Requesting them by name forces the API to return their labels.
        _common_fields = (
            "CERT,REPDTE,ASSET,DEP,LIAB,EQ,LNLS,LNLSNET,LNATRES,"
            "LNCI,LNRECONS,LNREMULT,LNRENROW,LNRENROT,LNREAG,"
            "LNRERES,LNRELOC,LNCON,LNCRCD,LNAUTO,LNOTHER,LNAG,LS,"
            "NTLNLS,NCLNLS,P3LNLS,P9LNLS,"
            "ROA,ROE,NIMY,EEFFR,RBCT1CER,RBCRWAJ,RBCT1J,RBCT2,"
            "NTCI,NTRECONS,NTREMULT,NTRERES,NTRELOC,NTCON,"
            "P3CI,P3RECONS,P3LREMUL,P3RENROT,P3RERES,P3RELOC,P3CON,"
            "P9CI,P9RECONS,P9REMULT,P9RENROT,P9RERES,P9RELOC,P9CON,"
            "NACI,NARECONS,NAREMULT,NARENROT,NARERES,NARELOC,NACON,"
            "NONIIAY,ELNATRY,EINTEXP,ELNATR,ILNDOM,ILNFOR,"
            "EDEPDOM,EDEPFOR,RWAJ,RB2LNRES,EQCS,EQSUR,EQUP,"
            "EQCCOMPI,EQPP,OTHBOR,FREPP,AOBS"
        )
        try:
            resp = requests.get(
                _FDIC_FINANCIALS_ENDPOINT,
                params={
                    "fields": _common_fields,
                    "limit": 1,
                    "sort_by": "REPDTE",
                    "sort_order": "DESC",
                },
                timeout=_FDIC_REQUEST_TIMEOUT,
                headers=headers,
            )
            resp.raise_for_status()
            _ingest(resp.json())
        except (requests.RequestException, ValueError) as exc:
            logger.warning("FDIC explicit-fields schema fetch failed: %s", exc)

        logger.info(
            "FDIC financials endpoint returned %d field definitions.", len(schema)
        )
        return schema

    def _fetch_fdic_schema_via_summary(self) -> Dict[str, Dict[str, str]]:
        """Fallback: probe FDIC summary endpoint for available fields."""
        schema: Dict[str, Dict[str, str]] = {}

        # Try multiple FDIC endpoints to gather field metadata
        endpoints = [
            f"{_FDIC_API_BASE}/financials",
            f"{_FDIC_API_BASE}/summary",
        ]

        for endpoint in endpoints:
            try:
                resp = requests.get(
                    endpoint,
                    params={"limit": 1, "sort_by": "REPDTE", "sort_order": "DESC"},
                    timeout=_FDIC_REQUEST_TIMEOUT,
                    headers={"User-Agent": "MasterDataDictionary/1.0"},
                )
                resp.raise_for_status()
                payload = resp.json()

                meta = payload.get("meta", {})
                labels = meta.get("labels", {})
                for field_code, label in labels.items():
                    field_upper = field_code.strip().upper()
                    if field_upper not in schema:
                        schema[field_upper] = {
                            "name": str(label).strip(),
                            "description": str(label).strip(),
                        }

            except (requests.RequestException, ValueError, KeyError) as exc:
                logger.warning("FDIC %s schema probe failed: %s", endpoint, exc)
                continue

        return schema

    def _lookup_fdic(self, code: str) -> Optional[Dict[str, str]]:
        """Look up a single code in the FDIC schema index."""
        idx = self._get_fdic_index()
        result = idx.get(code.upper())
        if result is not None:
            # If the FDIC API echoed the code as its own name (no real label),
            # provide a minimal description noting it's a confirmed FDIC field.
            if not result.get("description") or result["description"] == code.upper():
                result = dict(result)
                result["description"] = (
                    f"FDIC BankFind API field: {code.upper()}. "
                    "See FDIC data documentation for full definition."
                )
        return result

    # ------------------------------------------------------------------
    #  Tier 3 — Local / Derived
    # ------------------------------------------------------------------

    @staticmethod
    def _lookup_local(code: str) -> Optional[Dict[str, str]]:
        """Look up a code in the local derived-metrics registry."""
        entry = LOCAL_DERIVED_METRICS.get(code)
        if entry is None:
            # Try case-insensitive match
            for key, val in LOCAL_DERIVED_METRICS.items():
                if key.upper() == code.upper():
                    entry = val
                    break

        if entry is not None:
            return {
                "name": entry.get("short", code),
                "description": entry.get("long", ""),
            }
        return None

    # ------------------------------------------------------------------
    #  Key Normalization
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_key(code: str) -> str:
        """Normalize a metric code to upper-case, stripped of whitespace."""
        return code.strip().upper()

    @staticmethod
    def _strip_prefix(code: str) -> str:
        """Strip a Call Report mnemonic prefix (RCFD, RCON, RIAD, etc.)
        to yield the base 4-character item code.

        Examples:
            RCON2170 → 2170
            RCFDJ454 → J454
            ASSET    → ASSET   (no prefix to strip)
        """
        match = _CR_PREFIXES.match(code)
        if match:
            return code[match.end():]
        return code

    # ------------------------------------------------------------------
    #  Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _format_result(
        code: str,
        name: str,
        description: str,
        source: str,
        is_derived: bool,
    ) -> Dict[str, Any]:
        return {
            "Metric_Code": code,
            "Metric_Name": name,
            "Description": description,
            "Source_of_Truth": source,
            "Is_Derived": is_derived,
        }

    def clear_cache(self) -> None:
        """Remove all cached files, forcing a fresh download on next use."""
        for path in self._cache_dir.iterdir():
            if path.is_file():
                path.unlink()
        self._mdrm_df = None
        self._mdrm_index = None
        self._fdic_schema = None
        self._fdic_index = None
        logger.info("Data dictionary cache cleared.")

    def refresh(self) -> None:
        """Force re-download of all sources and rebuild indexes."""
        self.clear_cache()
        self._get_mdrm_index()
        self._get_fdic_index()
        logger.info("Data dictionary refreshed from all sources.")
