"""
Case-Shiller ZIP Code Mapper — HUD USPS Crosswalk Enrichment
==============================================================

Maps the 20 regional Case-Shiller metro indexes to ZIP codes using the
HUD USPS ZIP Code Crosswalk API.  Produces standalone Excel reference
sheets that can be used to map internal loan data to Case-Shiller regions.

Explicitly EXCLUDES:
  - U.S. National
  - Composite-10
  - Composite-20

Environment variables:
  HUD_USER_TOKEN              — required; HUD User API access token
  HUD_CROSSWALK_YEAR          — optional; crosswalk vintage year
  HUD_CROSSWALK_QUARTER       — optional; crosswalk vintage quarter (1-4)
  ENABLE_CASE_SHILLER_ZIP_ENRICHMENT — optional; default "true"
"""

import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# CASE-SHILLER METRO → CBSA / CBSA-DIV MAPPING TABLE
# ═══════════════════════════════════════════════════════════════════════════
#
# Each entry maps a Case-Shiller regional metro to its official CBSA code
# and, where the metro is defined at the division level, a CBSA Division
# code.  Judgment calls are documented in the comments column.
#
# Sources:
#   OMB Bulletin 23-01 (CBSA Delineations, July 2023)
#   HUD USPS Crosswalk documentation
#
# Key judgment calls:
#   - New York:   CBSA 35620 covers the full metro; CBSA Div 35614
#                 (NY-Jersey City-White Plains) is the best match for the
#                 S&P CoreLogic index which focuses on the NY metro division.
#   - Los Angeles: CBSA 31080; Div 31084 (LA-Long Beach-Glendale).
#   - Washington:  CBSA 47900; Div 47894 (Washington-Arlington-Alexandria).
#   - Chicago:     CBSA 16980; Div 16974 (Chicago-Naperville-Evanston).
#   - Miami:       CBSA 33100; Div 33124 (Miami-Miami Beach-Kendall).
#   - Detroit:     CBSA 19820; Div 19804 (Detroit-Dearborn-Livonia).
#   - Seattle:     CBSA 42660; Div 42644 (Seattle-Bellevue-Kent).
#
# Metros without divisions use CBSA-level mapping only.

CASE_SHILLER_METRO_MAP: List[Dict[str, Any]] = [
    {
        "case_shiller_region": "Atlanta",
        "cbsa_code": "12060",
        "cbsa_name": "Atlanta-Sandy Springs-Alpharetta, GA",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Boston",
        "cbsa_code": "14460",
        "cbsa_name": "Boston-Cambridge-Newton, MA-NH",
        "cbsadiv_code": "14454",
        "cbsadiv_name": "Boston, MA",
        "mapping_type": "cbsadiv",
        "comments": "Division 14454 best matches S&P index focus area",
    },
    {
        "case_shiller_region": "Charlotte",
        "cbsa_code": "16740",
        "cbsa_name": "Charlotte-Concord-Gastonia, NC-SC",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Chicago",
        "cbsa_code": "16980",
        "cbsa_name": "Chicago-Naperville-Elgin, IL-IN-WI",
        "cbsadiv_code": "16974",
        "cbsadiv_name": "Chicago-Naperville-Evanston, IL",
        "mapping_type": "cbsadiv",
        "comments": "Division 16974 is core Chicago metro",
    },
    {
        "case_shiller_region": "Cleveland",
        "cbsa_code": "17460",
        "cbsa_name": "Cleveland-Elyria, OH",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Dallas",
        "cbsa_code": "19100",
        "cbsa_name": "Dallas-Fort Worth-Arlington, TX",
        "cbsadiv_code": "19124",
        "cbsadiv_name": "Dallas-Plano-Irving, TX",
        "mapping_type": "cbsadiv",
        "comments": "Division 19124 matches S&P Dallas index",
    },
    {
        "case_shiller_region": "Denver",
        "cbsa_code": "19740",
        "cbsa_name": "Denver-Aurora-Lakewood, CO",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Detroit",
        "cbsa_code": "19820",
        "cbsa_name": "Detroit-Warren-Dearborn, MI",
        "cbsadiv_code": "19804",
        "cbsadiv_name": "Detroit-Dearborn-Livonia, MI",
        "mapping_type": "cbsadiv",
        "comments": "Division 19804 is core Detroit metro",
    },
    {
        "case_shiller_region": "Las Vegas",
        "cbsa_code": "29820",
        "cbsa_name": "Las Vegas-Henderson-Paradise, NV",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Los Angeles",
        "cbsa_code": "31080",
        "cbsa_name": "Los Angeles-Long Beach-Anaheim, CA",
        "cbsadiv_code": "31084",
        "cbsadiv_name": "Los Angeles-Long Beach-Glendale, CA",
        "mapping_type": "cbsadiv",
        "comments": "Division 31084 excludes Orange County (Anaheim); matches S&P LA index",
    },
    {
        "case_shiller_region": "Miami",
        "cbsa_code": "33100",
        "cbsa_name": "Miami-Fort Lauderdale-Pompano Beach, FL",
        "cbsadiv_code": "33124",
        "cbsadiv_name": "Miami-Miami Beach-Kendall, FL",
        "mapping_type": "cbsadiv",
        "comments": "Division 33124 is Miami-Dade focus",
    },
    {
        "case_shiller_region": "Minneapolis",
        "cbsa_code": "33460",
        "cbsa_name": "Minneapolis-St. Paul-Bloomington, MN-WI",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "New York",
        "cbsa_code": "35620",
        "cbsa_name": "New York-Newark-Jersey City, NY-NJ-PA",
        "cbsadiv_code": "35614",
        "cbsadiv_name": "New York-Jersey City-White Plains, NY-NJ",
        "mapping_type": "cbsadiv",
        "comments": "Division 35614 covers the core NY metro area the S&P index tracks",
    },
    {
        "case_shiller_region": "Phoenix",
        "cbsa_code": "38060",
        "cbsa_name": "Phoenix-Mesa-Chandler, AZ",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Portland",
        "cbsa_code": "38900",
        "cbsa_name": "Portland-Vancouver-Hillsboro, OR-WA",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "San Diego",
        "cbsa_code": "41740",
        "cbsa_name": "San Diego-Chula Vista-Carlsbad, CA",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "San Francisco",
        "cbsa_code": "41860",
        "cbsa_name": "San Francisco-Oakland-Berkeley, CA",
        "cbsadiv_code": "41884",
        "cbsadiv_name": "San Francisco-San Mateo-Redwood City, CA",
        "mapping_type": "cbsadiv",
        "comments": "Division 41884 is core SF area; S&P index tracks broader metro",
    },
    {
        "case_shiller_region": "Seattle",
        "cbsa_code": "42660",
        "cbsa_name": "Seattle-Tacoma-Bellevue, WA",
        "cbsadiv_code": "42644",
        "cbsadiv_name": "Seattle-Bellevue-Kent, WA",
        "mapping_type": "cbsadiv",
        "comments": "Division 42644 is core Seattle area",
    },
    {
        "case_shiller_region": "Tampa",
        "cbsa_code": "45300",
        "cbsa_name": "Tampa-St. Petersburg-Clearwater, FL",
        "cbsadiv_code": None,
        "cbsadiv_name": None,
        "mapping_type": "cbsa",
        "comments": "Single CBSA, no divisions",
    },
    {
        "case_shiller_region": "Washington",
        "cbsa_code": "47900",
        "cbsa_name": "Washington-Arlington-Alexandria, DC-VA-MD-WV",
        "cbsadiv_code": "47894",
        "cbsadiv_name": "Washington-Arlington-Alexandria, DC-VA-MD-WV",
        "mapping_type": "cbsadiv",
        "comments": "Division 47894 is core DC metro",
    },
]

# Excluded index types — never in ZIP output
_EXCLUDED_INDEX_TYPES = {"U.S. National", "Composite-10", "Composite-20"}

# The 20 valid regional metro names
VALID_CS_METROS = {m["case_shiller_region"] for m in CASE_SHILLER_METRO_MAP}


# ═══════════════════════════════════════════════════════════════════════════
# HUD API CLIENT
# ═══════════════════════════════════════════════════════════════════════════

HUD_API_BASE = "https://www.huduser.gov/hudapi/public/usps"

# type=8 → cbsa-zip, type=9 → cbsadiv-zip
_HUD_TYPE_CBSA = 8
_HUD_TYPE_CBSADIV = 9

_MAX_RETRIES = 3
_RETRY_BACKOFF = [2, 4, 8]


def _get_hud_token() -> Optional[str]:
    """Read HUD API token from environment."""
    return os.getenv("HUD_USER_TOKEN")


def fetch_hud_crosswalk(
    query: str = "All",
    crosswalk_type: int = _HUD_TYPE_CBSA,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    token: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch HUD USPS ZIP Crosswalk data via REST API.

    Parameters
    ----------
    query : str
        Geography query.  "All" for full file.
    crosswalk_type : int
        8 = cbsa-zip, 9 = cbsadiv-zip
    year, quarter : optional ints
        Crosswalk vintage.  Omit for latest available.
    token : str or None
        HUD API bearer token.  Falls back to HUD_USER_TOKEN env var.

    Returns
    -------
    pd.DataFrame  with columns normalised to lowercase
    """
    if not _HAS_REQUESTS:
        raise ImportError("'requests' library is required for HUD API calls")

    tok = token or _get_hud_token()
    if not tok:
        raise EnvironmentError(
            "HUD_USER_TOKEN environment variable is not set. "
            "Register at https://www.huduser.gov/hudapi/public/register "
            "and set HUD_USER_TOKEN=<your-token>."
        )

    params: Dict[str, Any] = {"type": crosswalk_type, "query": query}
    if year is not None:
        params["year"] = year
    if quarter is not None:
        params["quarter"] = quarter

    headers = {"Authorization": f"Bearer {tok}"}
    type_label = "cbsa-zip" if crosswalk_type == _HUD_TYPE_CBSA else "cbsadiv-zip"
    logger.info(f"Fetching HUD crosswalk: type={type_label}, query={query}, year={year}, quarter={quarter}")

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(HUD_API_BASE, params=params, headers=headers, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, dict) and "data" in data:
                    records = data["data"]
                elif isinstance(data, dict) and "results" in data:
                    records = data["results"]
                elif isinstance(data, list):
                    records = data
                else:
                    records = data.get("data", data) if isinstance(data, dict) else []

                if not records:
                    logger.warning(f"HUD API returned empty result for type={type_label}")
                    return pd.DataFrame()

                df = pd.DataFrame(records)
                df.columns = [c.lower().strip() for c in df.columns]
                logger.info(f"HUD crosswalk fetched: {len(df)} rows, type={type_label}")
                return df

            if resp.status_code in (429, 500, 502, 503, 504):
                logger.warning(f"HUD API {resp.status_code}, retry {attempt}/{_MAX_RETRIES}")
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_BACKOFF[attempt - 1])
                    continue
            # Non-retryable error
            logger.error(f"HUD API error {resp.status_code}: {resp.text[:500]}")
            return pd.DataFrame()

        except requests.exceptions.RequestException as e:
            logger.warning(f"HUD API request failed (attempt {attempt}): {e}")
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_BACKOFF[attempt - 1])
                continue
            logger.error(f"HUD API request failed after {_MAX_RETRIES} attempts")
            return pd.DataFrame()

    return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# METRO MAP BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def build_case_shiller_metro_map() -> pd.DataFrame:
    """Return the metro mapping table as a DataFrame."""
    df = pd.DataFrame(CASE_SHILLER_METRO_MAP)
    # Ensure string types for codes
    for col in ["cbsa_code", "cbsadiv_code"]:
        df[col] = df[col].where(df[col].notna(), None)
    return df


# ═══════════════════════════════════════════════════════════════════════════
# ZIP COVERAGE BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_zip(z: Any) -> str:
    """Zero-pad a ZIP code to 5 characters."""
    s = str(z).strip().split(".")[0].split("-")[0]  # handle float or ZIP+4
    return s.zfill(5)


def _find_zip_col(df: pd.DataFrame) -> Optional[str]:
    """Find the ZIP column name in a HUD crosswalk DataFrame."""
    candidates = ["zip", "zip_code", "zipcode", "zip5"]
    for c in candidates:
        if c in df.columns:
            return c
    # Fallback: first column containing 'zip'
    for c in df.columns:
        if "zip" in c:
            return c
    return None


def _find_geo_code_col(df: pd.DataFrame, crosswalk_type: int) -> Optional[str]:
    """Find the geography code column (cbsa or cbsadiv) in a HUD crosswalk DF."""
    if crosswalk_type == _HUD_TYPE_CBSA:
        candidates = ["cbsa", "cbsa_code", "geoid", "cbsacode"]
    else:
        candidates = ["cbsadiv", "cbsadiv_code", "cbsa_div", "cbsadivcode", "geoid"]
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _extract_ratios(row: pd.Series) -> Dict[str, float]:
    """Extract HUD ratio fields from a crosswalk row."""
    out = {}
    for key in ["tot_ratio", "res_ratio", "bus_ratio", "oth_ratio"]:
        val = row.get(key, np.nan)
        try:
            out[key] = float(val) if pd.notna(val) else np.nan
        except (ValueError, TypeError):
            out[key] = np.nan
    return out


def _extract_year_quarter(row: pd.Series) -> Tuple[Optional[int], Optional[int]]:
    """Extract year and quarter from a HUD crosswalk row."""
    year = row.get("year") or row.get("crosswalk_year")
    quarter = row.get("quarter") or row.get("crosswalk_quarter")
    try:
        year = int(year) if pd.notna(year) else None
    except (ValueError, TypeError):
        year = None
    try:
        quarter = int(quarter) if pd.notna(quarter) else None
    except (ValueError, TypeError):
        quarter = None
    return year, quarter


def build_case_shiller_zip_coverage(
    cbsa_xwalk: pd.DataFrame,
    cbsadiv_xwalk: pd.DataFrame,
    metro_map: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Join metro mapping to HUD crosswalk data to produce one row per
    (case_shiller_region, zip_code, geography).

    Parameters
    ----------
    cbsa_xwalk : DataFrame
        HUD crosswalk type=8 (cbsa-zip)
    cbsadiv_xwalk : DataFrame
        HUD crosswalk type=9 (cbsadiv-zip)
    metro_map : DataFrame or None
        Metro mapping table; defaults to built-in CASE_SHILLER_METRO_MAP.

    Returns
    -------
    DataFrame with columns per Part 4 spec.
    """
    if metro_map is None:
        metro_map = build_case_shiller_metro_map()

    rows: List[Dict[str, Any]] = []

    for _, metro in metro_map.iterrows():
        region = metro["case_shiller_region"]
        use_div = metro["mapping_type"] == "cbsadiv" and pd.notna(metro.get("cbsadiv_code"))

        if use_div and not cbsadiv_xwalk.empty:
            # Use CBSA Division crosswalk
            geo_col = _find_geo_code_col(cbsadiv_xwalk, _HUD_TYPE_CBSADIV)
            zip_col = _find_zip_col(cbsadiv_xwalk)
            if geo_col and zip_col:
                target_code = str(metro["cbsadiv_code"]).strip()
                matched = cbsadiv_xwalk[
                    cbsadiv_xwalk[geo_col].astype(str).str.strip() == target_code
                ]
                for _, xrow in matched.iterrows():
                    ratios = _extract_ratios(xrow)
                    yr, qtr = _extract_year_quarter(xrow)
                    rows.append({
                        "case_shiller_region": region,
                        "tier_applicability": "All tiers (High/Middle/Low inherit metro ZIP universe)",
                        "geography_level_used": "cbsadiv",
                        "cbsa_code": metro["cbsa_code"],
                        "cbsa_name": metro["cbsa_name"],
                        "cbsadiv_code": metro["cbsadiv_code"],
                        "cbsadiv_name": metro["cbsadiv_name"],
                        "zip_code": _normalize_zip(xrow[zip_col]),
                        **ratios,
                        "year": yr,
                        "quarter": qtr,
                        "hud_crosswalk_type": "cbsadiv-zip (type=9)",
                        "source_system": "HUD USPS ZIP Crosswalk",
                        "mapping_status": "matched",
                        "notes": metro.get("comments", ""),
                    })

        # If no div mapping or div returned empty, fall back to CBSA
        if not rows or rows[-1].get("case_shiller_region") != region:
            if cbsa_xwalk.empty:
                continue
            geo_col = _find_geo_code_col(cbsa_xwalk, _HUD_TYPE_CBSA)
            zip_col = _find_zip_col(cbsa_xwalk)
            if not geo_col or not zip_col:
                continue
            target_code = str(metro["cbsa_code"]).strip()
            matched = cbsa_xwalk[
                cbsa_xwalk[geo_col].astype(str).str.strip() == target_code
            ]
            for _, xrow in matched.iterrows():
                ratios = _extract_ratios(xrow)
                yr, qtr = _extract_year_quarter(xrow)
                rows.append({
                    "case_shiller_region": region,
                    "tier_applicability": "All tiers (High/Middle/Low inherit metro ZIP universe)",
                    "geography_level_used": "cbsa",
                    "cbsa_code": metro["cbsa_code"],
                    "cbsa_name": metro["cbsa_name"],
                    "cbsadiv_code": metro.get("cbsadiv_code"),
                    "cbsadiv_name": metro.get("cbsadiv_name"),
                    "zip_code": _normalize_zip(xrow[zip_col]),
                    **ratios,
                    "year": yr,
                    "quarter": qtr,
                    "hud_crosswalk_type": "cbsa-zip (type=8)",
                    "source_system": "HUD USPS ZIP Crosswalk",
                    "mapping_status": "matched",
                    "notes": metro.get("comments", ""),
                })

    if not rows:
        logger.warning("No ZIP coverage rows produced — check HUD crosswalk data")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Deduplication
    dedup_cols = ["case_shiller_region", "zip_code", "cbsa_code", "cbsadiv_code", "year", "quarter"]
    existing_dedup = [c for c in dedup_cols if c in df.columns]
    df = df.drop_duplicates(subset=existing_dedup, keep="first")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def summarize_case_shiller_zip_coverage(coverage_df: pd.DataFrame) -> pd.DataFrame:
    """One row per region with aggregate counts."""
    if coverage_df.empty:
        return pd.DataFrame()

    rows = []
    for region, grp in coverage_df.groupby("case_shiller_region"):
        rows.append({
            "case_shiller_region": region,
            "zip_count": grp["zip_code"].nunique(),
            "unique_cbsa_count": grp["cbsa_code"].nunique(),
            "unique_cbsadiv_count": grp["cbsadiv_code"].dropna().nunique(),
            "used_cbsa_mapping": (grp["geography_level_used"] == "cbsa").any(),
            "used_cbsadiv_mapping": (grp["geography_level_used"] == "cbsadiv").any(),
            "year": grp["year"].max(),
            "quarter": grp["quarter"].max(),
        })
    return pd.DataFrame(rows).sort_values("case_shiller_region").reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════════════
# VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

def validate_zip_coverage(
    coverage_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    metro_map_df: pd.DataFrame,
) -> List[str]:
    """Run validation checks on the ZIP coverage output.

    Returns a list of warning/error strings.
    """
    issues: List[str] = []

    if coverage_df.empty:
        issues.append("ERROR: Coverage DataFrame is empty")
        return issues

    # 1. No non-metro regions
    non_metro = set(coverage_df["case_shiller_region"].unique()) - VALID_CS_METROS
    if non_metro:
        issues.append(f"ERROR: Non-metro regions in coverage: {non_metro}")

    # 2. No blank zip_code
    blank_zips = coverage_df["zip_code"].isna() | (coverage_df["zip_code"].astype(str).str.strip() == "")
    if blank_zips.any():
        issues.append(f"ERROR: {blank_zips.sum()} rows with blank zip_code")

    # 3. ZIPs must be 5-char strings
    bad_len = coverage_df["zip_code"].astype(str).str.len() != 5
    if bad_len.any():
        issues.append(f"WARN: {bad_len.sum()} zip_codes not 5 characters")

    # 4. Summary zip_count reconciliation
    if not summary_df.empty:
        for _, srow in summary_df.iterrows():
            region = srow["case_shiller_region"]
            detail_count = coverage_df[
                coverage_df["case_shiller_region"] == region
            ]["zip_code"].nunique()
            if srow["zip_count"] != detail_count:
                issues.append(
                    f"WARN: {region} summary zip_count={srow['zip_count']} "
                    f"!= detail distinct count={detail_count}"
                )

    # 5. No duplicate region definitions in metro map
    dup_regions = metro_map_df["case_shiller_region"].duplicated()
    if dup_regions.any():
        issues.append(f"ERROR: Duplicate regions in metro map: "
                      f"{metro_map_df.loc[dup_regions, 'case_shiller_region'].tolist()}")

    # 6. Warn if any metro has zero ZIPs
    for region in VALID_CS_METROS:
        count = coverage_df[coverage_df["case_shiller_region"] == region]["zip_code"].nunique()
        if count == 0:
            issues.append(f"WARN: {region} returned zero ZIP rows")

    # 7. Warn if ratio columns are all missing
    ratio_cols = ["tot_ratio", "res_ratio", "bus_ratio", "oth_ratio"]
    for col in ratio_cols:
        if col in coverage_df.columns:
            non_null = coverage_df[col].notna().sum()
            if non_null == 0:
                issues.append(f"WARN: {col} is entirely null across all regions")

    return issues


# ═══════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — build_case_shiller_zip_sheets()
# ═══════════════════════════════════════════════════════════════════════════

def build_case_shiller_zip_sheets(
    token: Optional[str] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
) -> Dict[str, pd.DataFrame]:
    """Top-level function that fetches HUD data and produces the 3 output sheets.

    Returns
    -------
    dict with keys:
        "CaseShiller_Zip_Coverage"
        "CaseShiller_Zip_Summary"
        "CaseShiller_Metro_Map_Audit"

    If the HUD token is missing or fetches fail, returns dict with empty DataFrames
    and logs appropriate warnings.
    """
    tok = token or _get_hud_token()

    # Config from env
    if year is None:
        env_year = os.getenv("HUD_CROSSWALK_YEAR")
        year = int(env_year) if env_year else None
    if quarter is None:
        env_qtr = os.getenv("HUD_CROSSWALK_QUARTER")
        quarter = int(env_qtr) if env_qtr else None

    metro_map_df = build_case_shiller_metro_map()

    # Build audit sheet regardless of API availability
    audit_df = metro_map_df.copy()
    audit_df["included_in_zip_output"] = False

    empty_result = {
        "CaseShiller_Zip_Coverage": pd.DataFrame(),
        "CaseShiller_Zip_Summary": pd.DataFrame(),
        "CaseShiller_Metro_Map_Audit": audit_df,
    }

    if not tok:
        logger.warning(
            "HUD_USER_TOKEN not set — skipping Case-Shiller ZIP enrichment. "
            "Register at https://www.huduser.gov/hudapi/public/register"
        )
        audit_df["comments"] = audit_df["comments"].astype(str) + " | SKIPPED: no HUD token"
        return empty_result

    if not _HAS_REQUESTS:
        logger.warning("'requests' library not installed — skipping HUD API calls")
        audit_df["comments"] = audit_df["comments"].astype(str) + " | SKIPPED: requests not installed"
        return empty_result

    # Fetch both crosswalk types
    logger.info("Fetching HUD CBSA-ZIP crosswalk (type=8)...")
    cbsa_xwalk = fetch_hud_crosswalk(
        query="All", crosswalk_type=_HUD_TYPE_CBSA,
        year=year, quarter=quarter, token=tok,
    )

    logger.info("Fetching HUD CBSADIV-ZIP crosswalk (type=9)...")
    cbsadiv_xwalk = fetch_hud_crosswalk(
        query="All", crosswalk_type=_HUD_TYPE_CBSADIV,
        year=year, quarter=quarter, token=tok,
    )

    if cbsa_xwalk.empty and cbsadiv_xwalk.empty:
        logger.error("Both HUD crosswalk fetches returned empty — cannot build ZIP coverage")
        audit_df["comments"] = audit_df["comments"].astype(str) + " | SKIPPED: HUD API returned empty"
        return empty_result

    # Build coverage
    coverage_df = build_case_shiller_zip_coverage(cbsa_xwalk, cbsadiv_xwalk, metro_map_df)
    summary_df = summarize_case_shiller_zip_coverage(coverage_df)

    # Update audit
    covered_regions = set(coverage_df["case_shiller_region"].unique()) if not coverage_df.empty else set()
    audit_df["included_in_zip_output"] = audit_df["case_shiller_region"].isin(covered_regions)

    # Validate
    issues = validate_zip_coverage(coverage_df, summary_df, metro_map_df)
    for issue in issues:
        if issue.startswith("ERROR"):
            logger.error(issue)
        else:
            logger.warning(issue)

    logger.info(
        f"Case-Shiller ZIP coverage complete: "
        f"{len(coverage_df)} coverage rows, "
        f"{len(summary_df)} metros, "
        f"{len(issues)} validation issues"
    )

    return {
        "CaseShiller_Zip_Coverage": coverage_df,
        "CaseShiller_Zip_Summary": summary_df,
        "CaseShiller_Metro_Map_Audit": audit_df,
    }


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    token = os.getenv("HUD_USER_TOKEN")
    if not token:
        print("ERROR: Set HUD_USER_TOKEN environment variable")
        print("Register at: https://www.huduser.gov/hudapi/public/register")
        sys.exit(1)

    sheets = build_case_shiller_zip_sheets(token=token)
    for name, df in sheets.items():
        print(f"\n{name}: {df.shape}")
        if not df.empty:
            print(df.head(3).to_string(index=False))

    # Optionally write to Excel
    out_path = "case_shiller_zip_coverage_test.xlsx"
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            if not df.empty:
                df.to_excel(writer, sheet_name=name[:31], index=False)
    print(f"\nWritten to: {out_path}")

import os
import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# The 20 Case-Shiller regional metros
CASE_SHILLER_METROS = {
    "Atlanta": {"prefix": ["300", "301", "302", "303"]},
    "Boston": {"prefix": ["020", "021", "022", "023", "024"]},
    "Charlotte": {"prefix": ["280", "281", "282"]},
    "Chicago": {"prefix": ["600", "601", "602", "603", "604", "605", "606"]},
    "Cleveland": {"prefix": ["440", "441", "442", "443", "444"]},
    "Dallas": {"prefix": ["750", "751", "752", "753", "754", "755"]},
    "Denver": {"prefix": ["800", "801", "802", "803", "804"]},
    "Detroit": {"prefix": ["480", "481", "482", "483", "484"]},
    "Las Vegas": {"prefix": ["889", "890", "891"]},
    "Los Angeles": {"prefix": ["900", "901", "902", "903", "904", "905", "906", "907", "908", "910", "911", "912", "913", "914", "915", "916", "917", "918"]},
    "Miami": {"prefix": ["330", "331", "332", "333", "334"]},
    "Minneapolis": {"prefix": ["550", "551", "553", "554", "555"]},
    "New York": {"prefix": ["100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117"]},
    "Phoenix": {"prefix": ["850", "851", "852", "853"]},
    "Portland": {"prefix": ["970", "971", "972"]},
    "San Diego": {"prefix": ["919", "920", "921"]},
    "San Francisco": {"prefix": ["940", "941", "942", "943", "944", "945", "946", "947", "948", "949", "950", "951"]},
    "Seattle": {"prefix": ["980", "981", "982", "983", "984"]},
    "Tampa": {"prefix": ["335", "336", "337", "338"]},
    "Washington": {"prefix": ["200", "201", "202", "203", "204", "205", "206", "207", "208", "209", "220", "221"]},
}


def is_zip_enrichment_enabled() -> bool:
    """Check whether Case-Shiller ZIP enrichment is enabled via env flag."""
    val = os.getenv("ENABLE_CASE_SHILLER_ZIP_ENRICHMENT", "true").strip().lower()
    return val in {"1", "true", "yes", "y"}


def map_zip_to_metro(zip_code: str) -> Optional[str]:
    """Map a 5-character ZIP code to a Case-Shiller metro name, or None."""
    if not zip_code or len(str(zip_code)) < 3:
        return None
    prefix = str(zip_code).zfill(5)[:3]
    for metro, info in CASE_SHILLER_METROS.items():
        if prefix in info["prefix"]:
            return metro
    return None


def build_case_shiller_zip_sheets(
    zip_df: Optional[pd.DataFrame] = None,
    fred_cs_df: Optional[pd.DataFrame] = None,
) -> Dict[str, pd.DataFrame]:
    """Build Case-Shiller ZIP enrichment sheets.

    Parameters
    ----------
    zip_df : DataFrame with at minimum a 'ZIP' column (5-char strings)
    fred_cs_df : DataFrame of Case-Shiller FRED time-series (optional)

    Returns
    -------
    Dict of sheet_name -> DataFrame. Empty dict if disabled or no data.
    """
    result: Dict[str, pd.DataFrame] = {}
    audit_comments = []

    if not is_zip_enrichment_enabled():
        audit_comments.append("SKIPPED: disabled by env flag ENABLE_CASE_SHILLER_ZIP_ENRICHMENT")
        logger.info("Case-Shiller ZIP enrichment disabled by env flag.")
        # Return empty audit sheet noting skip
        result["CaseShiller_Metro_Map_Audit"] = pd.DataFrame(
            [{"status": "SKIPPED", "reason": "disabled by env flag"}]
        )
        return result

    if zip_df is None or zip_df.empty:
        logger.info("No ZIP data provided for Case-Shiller enrichment.")
        return result

    # Ensure ZIP is 5-character string
    zip_df = zip_df.copy()
    zip_df['ZIP'] = zip_df['ZIP'].astype(str).str.zfill(5)

    # Map ZIPs to metros
    zip_df['CS_Metro'] = zip_df['ZIP'].apply(map_zip_to_metro)

    # Coverage: which ZIPs mapped, which didn't
    mapped = zip_df[zip_df['CS_Metro'].notna()]
    unmapped = zip_df[zip_df['CS_Metro'].isna()]

    coverage_rows = []
    for metro in sorted(CASE_SHILLER_METROS.keys()):
        metro_zips = mapped[mapped['CS_Metro'] == metro]
        coverage_rows.append({
            'Metro': metro,
            'ZIP_Count': len(metro_zips),
            'ZIPs': ', '.join(sorted(metro_zips['ZIP'].unique())) if not metro_zips.empty else '',
        })

    result["CaseShiller_Zip_Coverage"] = pd.DataFrame(coverage_rows)

    # Summary
    summary = pd.DataFrame([{
        'total_zips': len(zip_df),
        'mapped_zips': len(mapped),
        'unmapped_zips': len(unmapped),
        'zip_count': len(mapped['ZIP'].unique()),
        'metros_covered': mapped['CS_Metro'].nunique(),
        'metros_total': len(CASE_SHILLER_METROS),
    }])
    result["CaseShiller_Zip_Summary"] = summary

    # Audit
    audit_rows = []
    for metro, info in sorted(CASE_SHILLER_METROS.items()):
        audit_rows.append({
            'metro': metro,
            'prefix_count': len(info['prefix']),
            'prefixes': ', '.join(info['prefix']),
        })
    result["CaseShiller_Metro_Map_Audit"] = pd.DataFrame(audit_rows)

    logger.info(
        "Case-Shiller ZIP enrichment: %d/%d ZIPs mapped to %d metros.",
        len(mapped), len(zip_df), mapped['CS_Metro'].nunique()
    )
    return result

