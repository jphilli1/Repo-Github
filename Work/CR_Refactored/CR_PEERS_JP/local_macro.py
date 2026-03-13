"""
Local Macro Data Layer — Geography Spine & MSA-Level Macro Context
===================================================================

Canonical geography + local/MSA macro source layer for MSPBNA
board/risk reporting.  Provides:

  1. A canonical geography spine keyed by CBSA, state, county, and ZIP
  2. Explicit mapping hierarchy with audit trail
  3. BEA GDP, BLS unemployment, Census population source contracts
  4. MSA_Crosswalk_Audit output tracking every mapping decision

Architecture:
  - This module is the SOLE owner of MSA/CBSA geography resolution.
  - case_shiller_zip_mapper.py remains the authority for Case-Shiller
    regional tagging only — it must NOT be reused as a generic CBSA spine.
  - report_generator.py must NOT import this module directly; the data
    flows through the Excel workbook (same pattern as FRED/FDIC data).

Source Policy:
  - GDP:          BEA metro/county → FRED state-level fallback
  - Unemployment: BLS LAUS metro/state
  - Population:   Census metro/state
  - ZIP/CBSA:     HUD USPS crosswalk (type=4 ZIP-to-CBSA)
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
#  Geography Spine — canonical field names
# ---------------------------------------------------------------------------
SPINE_COLUMNS = [
    "cbsa_code",
    "msa_name",
    "state_fips",
    "state_abbrev",
    "county_fips",
    "zip_code",
]

# Mapping methods (for audit trail)
MAP_METHOD_DIRECT_CBSA = "direct_cbsa"
MAP_METHOD_ZIP_TO_CBSA = "zip_to_cbsa"
MAP_METHOD_COUNTY_TO_CBSA = "county_to_cbsa"
MAP_METHOD_STATE_FALLBACK = "state_fallback"
MAP_METHOD_UNMATCHED = "unmatched"

# Quality flags
QUALITY_HIGH = "high"          # Direct CBSA match
QUALITY_MEDIUM = "medium"      # ZIP or county crosswalk
QUALITY_LOW = "low"            # State-level fallback
QUALITY_UNMATCHED = "unmatched" # No resolution

# HUD crosswalk type for ZIP-to-CBSA
_HUD_TYPE_ZIP_TO_CBSA = 4

# Source dataset identifiers
SOURCE_BEA_GDP = "BEA_RegionalGDP"
SOURCE_BLS_LAUS = "BLS_LAUS"
SOURCE_CENSUS_POP = "Census_Population"
SOURCE_HUD_CROSSWALK = "HUD_USPS_Crosswalk"

# ---------------------------------------------------------------------------
#  Top-50 MSA / CBSA Reference Table
# ---------------------------------------------------------------------------
# Canonical CBSA codes for major MSAs relevant to MSPBNA reporting.
# Source: OMB Bulletin (February 2023 delineations).
# This is the authoritative CBSA reference — NOT derived from Case-Shiller.

TOP_MSAS: List[Dict[str, str]] = [
    {"cbsa_code": "35620", "msa_name": "New York-Newark-Jersey City",
     "state_abbrev": "NY", "state_fips": "36"},
    {"cbsa_code": "31080", "msa_name": "Los Angeles-Long Beach-Anaheim",
     "state_abbrev": "CA", "state_fips": "06"},
    {"cbsa_code": "16980", "msa_name": "Chicago-Naperville-Elgin",
     "state_abbrev": "IL", "state_fips": "17"},
    {"cbsa_code": "19100", "msa_name": "Dallas-Fort Worth-Arlington",
     "state_abbrev": "TX", "state_fips": "48"},
    {"cbsa_code": "26420", "msa_name": "Houston-The Woodlands-Sugar Land",
     "state_abbrev": "TX", "state_fips": "48"},
    {"cbsa_code": "47900", "msa_name": "Washington-Arlington-Alexandria",
     "state_abbrev": "DC", "state_fips": "11"},
    {"cbsa_code": "33100", "msa_name": "Miami-Fort Lauderdale-Pompano Beach",
     "state_abbrev": "FL", "state_fips": "12"},
    {"cbsa_code": "37980", "msa_name": "Philadelphia-Camden-Wilmington",
     "state_abbrev": "PA", "state_fips": "42"},
    {"cbsa_code": "12060", "msa_name": "Atlanta-Sandy Springs-Alpharetta",
     "state_abbrev": "GA", "state_fips": "13"},
    {"cbsa_code": "14460", "msa_name": "Boston-Cambridge-Newton",
     "state_abbrev": "MA", "state_fips": "25"},
    {"cbsa_code": "38060", "msa_name": "Phoenix-Mesa-Chandler",
     "state_abbrev": "AZ", "state_fips": "04"},
    {"cbsa_code": "41860", "msa_name": "San Francisco-Oakland-Berkeley",
     "state_abbrev": "CA", "state_fips": "06"},
    {"cbsa_code": "40140", "msa_name": "Riverside-San Bernardino-Ontario",
     "state_abbrev": "CA", "state_fips": "06"},
    {"cbsa_code": "19820", "msa_name": "Detroit-Warren-Dearborn",
     "state_abbrev": "MI", "state_fips": "26"},
    {"cbsa_code": "42660", "msa_name": "Seattle-Tacoma-Bellevue",
     "state_abbrev": "WA", "state_fips": "53"},
    {"cbsa_code": "33460", "msa_name": "Minneapolis-St. Paul-Bloomington",
     "state_abbrev": "MN", "state_fips": "27"},
    {"cbsa_code": "41740", "msa_name": "San Diego-Chula Vista-Carlsbad",
     "state_abbrev": "CA", "state_fips": "06"},
    {"cbsa_code": "45300", "msa_name": "Tampa-St. Petersburg-Clearwater",
     "state_abbrev": "FL", "state_fips": "12"},
    {"cbsa_code": "19740", "msa_name": "Denver-Aurora-Lakewood",
     "state_abbrev": "CO", "state_fips": "08"},
    {"cbsa_code": "41180", "msa_name": "St. Louis",
     "state_abbrev": "MO", "state_fips": "29"},
]

# Fast lookup by CBSA code
_CBSA_LOOKUP: Dict[str, Dict[str, str]] = {m["cbsa_code"]: m for m in TOP_MSAS}


# ---------------------------------------------------------------------------
#  Data classes
# ---------------------------------------------------------------------------
@dataclass
class GeographyMapping:
    """A single geography resolution result."""
    source_geo_type: str      # "cbsa", "zip", "county_fips", "state"
    source_geo_value: str     # The original value
    cbsa_code: Optional[str] = None
    msa_name: Optional[str] = None
    state_fips: Optional[str] = None
    state_abbrev: Optional[str] = None
    county_fips: Optional[str] = None
    zip_code: Optional[str] = None
    mapping_method: str = MAP_METHOD_UNMATCHED
    mapping_weight: float = 1.0
    coverage_pct: float = 0.0
    quality_flag: str = QUALITY_UNMATCHED


@dataclass
class MacroSourceRow:
    """A single local macro data observation with full provenance."""
    cbsa_code: Optional[str] = None
    msa_name: Optional[str] = None
    state_fips: Optional[str] = None
    state_abbrev: Optional[str] = None
    date: Optional[str] = None
    metric_name: str = ""
    value: Optional[float] = None
    source_dataset: str = ""
    source_series_id: str = ""
    source_frequency: str = ""
    data_vintage: str = ""
    load_timestamp: str = ""


# ---------------------------------------------------------------------------
#  Geography Spine Builder
# ---------------------------------------------------------------------------
def build_geography_spine(
    cbsa_codes: Optional[List[str]] = None,
    zip_codes: Optional[List[str]] = None,
    county_fips_codes: Optional[List[str]] = None,
    state_abbrevs: Optional[List[str]] = None,
    hud_token: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Build a canonical geography spine from input geographies.

    Mapping hierarchy (in priority order):
      1. Direct CBSA code match against TOP_MSAS reference table
      2. ZIP → CBSA via HUD USPS crosswalk (type=4) when token available
      3. County FIPS → CBSA via county-CBSA reference
      4. State-level fallback (flagged as low quality)

    Returns:
        Tuple of (spine_df, audit_df):
        - spine_df: canonical geography spine with SPINE_COLUMNS
        - audit_df: MSA_Crosswalk_Audit with full mapping trail
    """
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    audit_rows: List[Dict[str, Any]] = []
    spine_rows: List[Dict[str, Any]] = []

    # --- 1. Direct CBSA resolution ---
    if cbsa_codes:
        for code in cbsa_codes:
            code = str(code).strip().zfill(5)
            ref = _CBSA_LOOKUP.get(code)
            if ref:
                row = {
                    "cbsa_code": code,
                    "msa_name": ref["msa_name"],
                    "state_fips": ref["state_fips"],
                    "state_abbrev": ref["state_abbrev"],
                    "county_fips": None,
                    "zip_code": None,
                }
                spine_rows.append(row)
                audit_rows.append({
                    "source_geo_type": "cbsa",
                    "source_geo_value": code,
                    "target_cbsa_code": code,
                    "target_msa_name": ref["msa_name"],
                    "mapping_method": MAP_METHOD_DIRECT_CBSA,
                    "mapping_weight": 1.0,
                    "coverage_pct": 100.0,
                    "quality_flag": QUALITY_HIGH,
                    "load_timestamp": load_ts,
                })
            else:
                audit_rows.append({
                    "source_geo_type": "cbsa",
                    "source_geo_value": code,
                    "target_cbsa_code": None,
                    "target_msa_name": None,
                    "mapping_method": MAP_METHOD_UNMATCHED,
                    "mapping_weight": 0.0,
                    "coverage_pct": 0.0,
                    "quality_flag": QUALITY_UNMATCHED,
                    "load_timestamp": load_ts,
                })

    # --- 2. ZIP → CBSA via HUD crosswalk ---
    if zip_codes and hud_token:
        zip_cbsa_map = _fetch_hud_zip_to_cbsa(zip_codes, hud_token)
        for zc in zip_codes:
            zc = str(zc).strip().zfill(5)
            match = zip_cbsa_map.get(zc)
            if match:
                cbsa = match.get("cbsa_code", "")
                ref = _CBSA_LOOKUP.get(cbsa, {})
                row = {
                    "cbsa_code": cbsa,
                    "msa_name": ref.get("msa_name", match.get("msa_name", "")),
                    "state_fips": ref.get("state_fips"),
                    "state_abbrev": ref.get("state_abbrev"),
                    "county_fips": None,
                    "zip_code": zc,
                }
                spine_rows.append(row)
                audit_rows.append({
                    "source_geo_type": "zip",
                    "source_geo_value": zc,
                    "target_cbsa_code": cbsa,
                    "target_msa_name": row["msa_name"],
                    "mapping_method": MAP_METHOD_ZIP_TO_CBSA,
                    "mapping_weight": match.get("tot_ratio", 1.0),
                    "coverage_pct": match.get("tot_ratio", 1.0) * 100,
                    "quality_flag": QUALITY_MEDIUM,
                    "load_timestamp": load_ts,
                })
            elif zc not in [r.get("zip_code") for r in spine_rows]:
                audit_rows.append({
                    "source_geo_type": "zip",
                    "source_geo_value": zc,
                    "target_cbsa_code": None,
                    "target_msa_name": None,
                    "mapping_method": MAP_METHOD_UNMATCHED,
                    "mapping_weight": 0.0,
                    "coverage_pct": 0.0,
                    "quality_flag": QUALITY_UNMATCHED,
                    "load_timestamp": load_ts,
                })
    elif zip_codes and not hud_token:
        for zc in zip_codes:
            audit_rows.append({
                "source_geo_type": "zip",
                "source_geo_value": str(zc).strip().zfill(5),
                "target_cbsa_code": None,
                "target_msa_name": None,
                "mapping_method": MAP_METHOD_UNMATCHED,
                "mapping_weight": 0.0,
                "coverage_pct": 0.0,
                "quality_flag": QUALITY_UNMATCHED,
                "load_timestamp": load_ts,
                "notes": "HUD token not available — ZIP-to-CBSA mapping skipped",
            })

    # --- 3. County FIPS → CBSA (via reference) ---
    if county_fips_codes:
        county_cbsa_map = _build_county_to_cbsa_map()
        for fips in county_fips_codes:
            fips = str(fips).strip().zfill(5)
            cbsa = county_cbsa_map.get(fips)
            if cbsa:
                ref = _CBSA_LOOKUP.get(cbsa, {})
                row = {
                    "cbsa_code": cbsa,
                    "msa_name": ref.get("msa_name", ""),
                    "state_fips": fips[:2],
                    "state_abbrev": ref.get("state_abbrev"),
                    "county_fips": fips,
                    "zip_code": None,
                }
                spine_rows.append(row)
                audit_rows.append({
                    "source_geo_type": "county_fips",
                    "source_geo_value": fips,
                    "target_cbsa_code": cbsa,
                    "target_msa_name": ref.get("msa_name", ""),
                    "mapping_method": MAP_METHOD_COUNTY_TO_CBSA,
                    "mapping_weight": 1.0,
                    "coverage_pct": 100.0,
                    "quality_flag": QUALITY_MEDIUM,
                    "load_timestamp": load_ts,
                })
            else:
                audit_rows.append({
                    "source_geo_type": "county_fips",
                    "source_geo_value": fips,
                    "target_cbsa_code": None,
                    "target_msa_name": None,
                    "mapping_method": MAP_METHOD_UNMATCHED,
                    "mapping_weight": 0.0,
                    "coverage_pct": 0.0,
                    "quality_flag": QUALITY_UNMATCHED,
                    "load_timestamp": load_ts,
                })

    # --- 4. State-level fallback ---
    if state_abbrevs:
        for st in state_abbrevs:
            st = st.strip().upper()
            sfips = _STATE_ABBREV_TO_FIPS.get(st)
            row = {
                "cbsa_code": None,
                "msa_name": None,
                "state_fips": sfips,
                "state_abbrev": st,
                "county_fips": None,
                "zip_code": None,
            }
            spine_rows.append(row)
            audit_rows.append({
                "source_geo_type": "state",
                "source_geo_value": st,
                "target_cbsa_code": None,
                "target_msa_name": None,
                "mapping_method": MAP_METHOD_STATE_FALLBACK,
                "mapping_weight": 1.0,
                "coverage_pct": 100.0,
                "quality_flag": QUALITY_LOW,
                "load_timestamp": load_ts,
            })

    spine_df = pd.DataFrame(spine_rows, columns=SPINE_COLUMNS)
    audit_df = pd.DataFrame(audit_rows)
    if audit_df.empty:
        audit_df = pd.DataFrame(columns=[
            "source_geo_type", "source_geo_value", "target_cbsa_code",
            "target_msa_name", "mapping_method", "mapping_weight",
            "coverage_pct", "quality_flag", "load_timestamp",
        ])
    return spine_df, audit_df


# ---------------------------------------------------------------------------
#  County → CBSA reference
# ---------------------------------------------------------------------------
# Major-county → CBSA mappings for the top MSAs (principal counties only).
# This is intentionally separate from Case-Shiller county map.
_COUNTY_TO_CBSA: Dict[str, str] = {
    # New York-Newark-Jersey City
    "36061": "35620", "36047": "35620", "36081": "35620",
    "36005": "35620", "36085": "35620", "34013": "35620",
    "34017": "35620", "34023": "35620", "34025": "35620",
    "34027": "35620", "34029": "35620", "34031": "35620",
    "34035": "35620", "34037": "35620", "34039": "35620",
    # Los Angeles-Long Beach-Anaheim
    "06037": "31080", "06059": "31080",
    # Chicago-Naperville-Elgin
    "17031": "16980", "17043": "16980", "17089": "16980",
    "17093": "16980", "17097": "16980", "17111": "16980",
    "17197": "16980", "18089": "16980",
    # Dallas-Fort Worth-Arlington
    "48113": "19100", "48085": "19100", "48121": "19100",
    "48139": "19100", "48231": "19100", "48251": "19100",
    "48257": "19100", "48397": "19100", "48439": "19100",
    # Houston-The Woodlands-Sugar Land
    "48201": "26420", "48157": "26420", "48039": "26420",
    "48167": "26420", "48291": "26420", "48339": "26420",
    # Washington-Arlington-Alexandria
    "11001": "47900", "24031": "47900", "24033": "47900",
    "51013": "47900", "51059": "47900", "51107": "47900",
    "51153": "47900", "51510": "47900", "51600": "47900",
    "51610": "47900", "51683": "47900", "51685": "47900",
    # Miami-Fort Lauderdale-Pompano Beach
    "12086": "33100", "12011": "33100", "12099": "33100",
    # Philadelphia-Camden-Wilmington
    "42017": "37980", "42029": "37980", "42045": "37980",
    "42091": "37980", "42101": "37980", "34005": "37980",
    "34007": "37980", "34015": "37980", "10003": "37980",
    # Atlanta-Sandy Springs-Alpharetta
    "13121": "12060", "13089": "12060", "13067": "12060",
    "13135": "12060", "13063": "12060",
    # Boston-Cambridge-Newton
    "25025": "14460", "25017": "14460", "25021": "14460",
    "25023": "14460", "25009": "14460",
    # Phoenix-Mesa-Chandler
    "04013": "38060", "04021": "38060",
    # San Francisco-Oakland-Berkeley
    "06001": "41860", "06013": "41860", "06075": "41860",
    "06081": "41860", "06041": "41860",
    # Riverside-San Bernardino-Ontario
    "06065": "40140", "06071": "40140",
    # Detroit-Warren-Dearborn
    "26163": "19820", "26125": "19820", "26099": "19820",
    "26087": "19820", "26093": "19820",
    # Seattle-Tacoma-Bellevue
    "53033": "42660", "53053": "42660", "53061": "42660",
    # Minneapolis-St. Paul-Bloomington
    "27053": "33460", "27123": "33460", "27003": "33460",
    "27037": "33460", "27019": "33460",
    # San Diego-Chula Vista-Carlsbad
    "06073": "41740",
    # Tampa-St. Petersburg-Clearwater
    "12057": "45300", "12103": "45300", "12101": "45300",
    # Denver-Aurora-Lakewood
    "08031": "19740", "08001": "19740", "08005": "19740",
    "08035": "19740", "08059": "19740",
    # St. Louis
    "29189": "41180", "29510": "41180", "17163": "41180",
    "29183": "41180",
}


def _build_county_to_cbsa_map() -> Dict[str, str]:
    """Return the county-to-CBSA mapping.  Kept as a function so it can
    be extended later from HUD or Census data files."""
    return dict(_COUNTY_TO_CBSA)


# ---------------------------------------------------------------------------
#  State FIPS reference
# ---------------------------------------------------------------------------
_STATE_ABBREV_TO_FIPS: Dict[str, str] = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "PR": "72",
    "RI": "44", "SC": "45", "SD": "46", "TN": "47", "TX": "48",
    "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
    "WI": "55", "WY": "56",
}

_FIPS_TO_STATE_ABBREV: Dict[str, str] = {v: k for k, v in _STATE_ABBREV_TO_FIPS.items()}


# ---------------------------------------------------------------------------
#  HUD ZIP → CBSA crosswalk
# ---------------------------------------------------------------------------
def _fetch_hud_zip_to_cbsa(
    zip_codes: List[str],
    hud_token: str,
    batch_size: int = 50,
) -> Dict[str, Dict[str, Any]]:
    """Fetch ZIP-to-CBSA mappings from HUD USPS crosswalk (type=4).

    Returns dict: {zip_code: {"cbsa_code": ..., "msa_name": ..., "tot_ratio": ...}}
    Only returns the highest-ratio CBSA per ZIP.
    """
    try:
        import requests
    except ImportError:
        logging.warning("requests library not available — ZIP-to-CBSA skipped")
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    base_url = "https://www.huduser.gov/hudapi/public/usps"
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {hud_token}",
        "Accept": "application/json",
        "User-Agent": "LocalMacro/1.0 (Python/requests)",
    })

    for zc in zip_codes:
        zc = str(zc).strip().zfill(5)
        try:
            resp = session.get(
                base_url,
                params={"type": _HUD_TYPE_ZIP_TO_CBSA, "query": zc},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                rows = _extract_hud_rows(data)
                if rows:
                    # Pick highest tot_ratio CBSA
                    best = max(rows, key=lambda r: float(r.get("tot_ratio", 0)))
                    cbsa = str(best.get("cbsa", best.get("geoid", ""))).strip()
                    if cbsa:
                        ref = _CBSA_LOOKUP.get(cbsa, {})
                        result[zc] = {
                            "cbsa_code": cbsa,
                            "msa_name": ref.get("msa_name",
                                                best.get("city", "")),
                            "tot_ratio": float(best.get("tot_ratio", 1.0)),
                        }
            else:
                logging.debug(f"HUD ZIP-to-CBSA {zc}: HTTP {resp.status_code}")
        except Exception as e:
            logging.debug(f"HUD ZIP-to-CBSA {zc} failed: {e}")

    return result


def _extract_hud_rows(payload: Any) -> List[Dict]:
    """Extract row dicts from a HUD API response (multiple shapes)."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if "data" in payload and isinstance(payload["data"], list):
            return payload["data"]
        if "results" in payload:
            res = payload["results"]
            if isinstance(res, list):
                # May be wrapper rows with nested results
                flat = []
                for item in res:
                    if isinstance(item, dict) and "results" in item and isinstance(item["results"], list):
                        flat.extend(item["results"])
                    else:
                        flat.append(item)
                return flat
            if isinstance(res, dict):
                for k in ("rows", "data"):
                    if k in res and isinstance(res[k], list):
                        return res[k]
    return []


# ---------------------------------------------------------------------------
#  Macro Data Fetchers (BEA, BLS, Census)
# ---------------------------------------------------------------------------
def fetch_bea_gdp_metro(
    cbsa_codes: List[str],
    bea_api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch BEA Regional GDP for given CBSAs.

    Source: BEA Regional Economic Accounts, Table CAGDP2 (GDP by metro area).
    Returns DataFrame with columns:
      cbsa_code, date, gdp_value, source_dataset, source_series_id,
      source_frequency, data_vintage, load_timestamp
    """
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not bea_api_key:
        bea_api_key = os.getenv("BEA_API_KEY") or os.getenv("BEA_USER_ID")
    if not bea_api_key:
        logging.info("BEA_API_KEY not configured — GDP fetch skipped")
        return _empty_macro_df("gdp_value")

    try:
        import requests
    except ImportError:
        logging.warning("requests library not available — BEA GDP skipped")
        return _empty_macro_df("gdp_value")

    rows = []
    for cbsa in cbsa_codes:
        cbsa = str(cbsa).strip()
        try:
            resp = requests.get(
                "https://apps.bea.gov/api/data/",
                params={
                    "UserID": bea_api_key,
                    "method": "GetData",
                    "datasetname": "Regional",
                    "TableName": "CAGDP2",
                    "LineCode": "1",       # All industries total
                    "GeoFips": cbsa,
                    "Year": "LAST5",
                    "ResultFormat": "JSON",
                },
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                bea_rows = (data.get("BEAAPI", {})
                            .get("Results", {})
                            .get("Data", []))
                for br in bea_rows:
                    val = br.get("DataValue", "").replace(",", "")
                    try:
                        val_f = float(val)
                    except (ValueError, TypeError):
                        continue
                    rows.append({
                        "cbsa_code": cbsa,
                        "date": f"{br.get('TimePeriod', '')}-01-01",
                        "gdp_value": val_f,
                        "source_dataset": SOURCE_BEA_GDP,
                        "source_series_id": f"CAGDP2_{cbsa}",
                        "source_frequency": "annual",
                        "data_vintage": br.get("NoteRef", ""),
                        "load_timestamp": load_ts,
                    })
            else:
                logging.debug(f"BEA GDP {cbsa}: HTTP {resp.status_code}")
        except Exception as e:
            logging.debug(f"BEA GDP {cbsa} failed: {e}")

    if rows:
        return pd.DataFrame(rows)
    return _empty_macro_df("gdp_value")


def fetch_bls_unemployment_metro(
    cbsa_codes: List[str],
) -> pd.DataFrame:
    """Fetch BLS LAUS unemployment rates for given CBSAs.

    Source: BLS Local Area Unemployment Statistics (LAUS).
    Series format: LAUM{state_fips}{cbsa_code}00000003
    Returns DataFrame with unemployment_rate column.
    """
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        import requests
    except ImportError:
        logging.warning("requests library not available — BLS LAUS skipped")
        return _empty_macro_df("unemployment_rate")

    # Build BLS series IDs for metro areas
    # BLS LAUS metro series: LAUMT{state_fips}{cbsa_code}00000003
    # where 03 = unemployment rate
    series_ids = []
    cbsa_to_series = {}
    for cbsa in cbsa_codes:
        cbsa = str(cbsa).strip()
        ref = _CBSA_LOOKUP.get(cbsa, {})
        sfips = ref.get("state_fips", "")
        if sfips:
            sid = f"LAUMT{sfips}{cbsa}00000003"
            series_ids.append(sid)
            cbsa_to_series[sid] = cbsa

    if not series_ids:
        return _empty_macro_df("unemployment_rate")

    rows = []
    try:
        # BLS Public Data API v2 (no key required for small requests)
        resp = requests.post(
            "https://api.bls.gov/publicAPI/v2/timeseries/data/",
            json={
                "seriesid": series_ids[:50],  # BLS limit: 50 per request
                "startyear": str(datetime.now().year - 5),
                "endyear": str(datetime.now().year),
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            for series in data.get("Results", {}).get("series", []):
                sid = series.get("seriesID", "")
                cbsa = cbsa_to_series.get(sid, "")
                for obs in series.get("data", []):
                    yr = obs.get("year", "")
                    period = obs.get("period", "")
                    # BLS period: M01-M12 for monthly
                    if not period.startswith("M"):
                        continue
                    month = period[1:]
                    try:
                        val = float(obs.get("value", ""))
                    except (ValueError, TypeError):
                        continue
                    rows.append({
                        "cbsa_code": cbsa,
                        "date": f"{yr}-{month}-01",
                        "unemployment_rate": val,
                        "source_dataset": SOURCE_BLS_LAUS,
                        "source_series_id": sid,
                        "source_frequency": "monthly",
                        "data_vintage": obs.get("footnotes", [{}])[0].get("text", ""),
                        "load_timestamp": load_ts,
                    })
        else:
            logging.debug(f"BLS LAUS: HTTP {resp.status_code}")
    except Exception as e:
        logging.debug(f"BLS LAUS fetch failed: {e}")

    if rows:
        return pd.DataFrame(rows)
    return _empty_macro_df("unemployment_rate")


def fetch_census_population_metro(
    cbsa_codes: List[str],
    census_api_key: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch Census population estimates for given CBSAs.

    Source: Census Bureau Population Estimates Program (PEP).
    Returns DataFrame with population column.
    """
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if not census_api_key:
        census_api_key = os.getenv("CENSUS_API_KEY")
    if not census_api_key:
        logging.info("CENSUS_API_KEY not configured — population fetch skipped")
        return _empty_macro_df("population")

    try:
        import requests
    except ImportError:
        logging.warning("requests library not available — Census pop skipped")
        return _empty_macro_df("population")

    rows = []
    # Census PEP API for metro areas
    try:
        resp = requests.get(
            "https://api.census.gov/data/2023/pep/population",
            params={
                "get": "POP_2023,NAME",
                "for": "metropolitan statistical area/micropolitan statistical area:*",
                "key": census_api_key,
            },
            timeout=30,
        )
        if resp.status_code == 200:
            data = resp.json()
            if len(data) > 1:
                headers = data[0]
                pop_idx = headers.index("POP_2023") if "POP_2023" in headers else 0
                name_idx = headers.index("NAME") if "NAME" in headers else 1
                geo_idx = len(headers) - 1  # Last column is the FIPS/CBSA

                target_set = set(str(c).strip() for c in cbsa_codes)
                for row in data[1:]:
                    geo_code = str(row[geo_idx]).strip()
                    if geo_code in target_set:
                        try:
                            pop_val = int(row[pop_idx])
                        except (ValueError, TypeError):
                            continue
                        rows.append({
                            "cbsa_code": geo_code,
                            "date": "2023-07-01",
                            "population": pop_val,
                            "source_dataset": SOURCE_CENSUS_POP,
                            "source_series_id": f"PEP_POP_{geo_code}",
                            "source_frequency": "annual",
                            "data_vintage": "2023",
                            "load_timestamp": load_ts,
                        })
        else:
            logging.debug(f"Census PEP: HTTP {resp.status_code}")
    except Exception as e:
        logging.debug(f"Census PEP fetch failed: {e}")

    if rows:
        return pd.DataFrame(rows)
    return _empty_macro_df("population")


def _empty_macro_df(value_col: str) -> pd.DataFrame:
    """Return an empty DataFrame with standard macro columns."""
    return pd.DataFrame(columns=[
        "cbsa_code", "date", value_col,
        "source_dataset", "source_series_id", "source_frequency",
        "data_vintage", "load_timestamp",
    ])


# ---------------------------------------------------------------------------
#  Transformation Policy Registry
# ---------------------------------------------------------------------------
#  Each local macro series has a declared policy controlling how it is
#  aggregated to quarterly frequency and what derived metrics are valid.
#  This replaces the one-size-fits-all "last observation of quarter" pattern.

@dataclass
class TransformPolicy:
    """Metadata-driven transformation policy for a local macro series."""
    series_family: str          # e.g. "gdp", "unemployment", "population", "hpi"
    transform_type: str         # "level", "rate", "flow", "index"
    aggregation_rule: str       # "mean", "last", "sum", "point_in_time"
    date_basis: str             # "period_end", "period_start", "midpoint"
    quarter_offset: int = 0     # 0 = standard, positive = forward lag
    units: str = ""             # "dollars", "pct", "pp", "persons", "index"
    yoy_lag_quarters: int = 4   # Quarters for YoY computation
    per_capita_eligible: bool = False  # Can be divided by population
    notes: str = ""


# Canonical transformation policies — one per series family.
TRANSFORM_POLICIES: Dict[str, TransformPolicy] = {
    "gdp": TransformPolicy(
        series_family="gdp",
        transform_type="level",
        aggregation_rule="sum",           # GDP is a flow — sum within quarter
        date_basis="period_end",
        units="dollars",
        per_capita_eligible=True,         # GDP / population is valid
        notes="BEA annual GDP; aggregate by sum for sub-annual, "
              "normalize to per-capita BEFORE computing growth",
    ),
    "unemployment": TransformPolicy(
        series_family="unemployment",
        transform_type="rate",
        aggregation_rule="mean",          # Average monthly rates in quarter
        date_basis="period_end",
        units="pct",
        per_capita_eligible=False,        # Rate — dividing by pop is meaningless
        notes="BLS LAUS monthly rate; quarterly = mean of 3 months; "
              "changes are in pp (percentage points), not pct",
    ),
    "population": TransformPolicy(
        series_family="population",
        transform_type="level",
        aggregation_rule="point_in_time", # Stock variable — use latest estimate
        date_basis="midpoint",            # Census mid-year estimate
        units="persons",
        per_capita_eligible=False,        # Population / population is 1
        notes="Census PEP annual; point-in-time stock, not a flow",
    ),
    "hpi": TransformPolicy(
        series_family="hpi",
        transform_type="index",
        aggregation_rule="last",          # End-of-quarter index value
        date_basis="period_end",
        units="index",
        per_capita_eligible=False,        # Index / population is meaningless
        notes="Case-Shiller or FHFA HPI; last value per quarter; "
              "YoY from index levels, never divide index by population",
    ),
}


def get_transform_policy(series_family: str) -> TransformPolicy:
    """Look up the transformation policy for a series family.

    Raises KeyError if the family is not registered.
    """
    return TRANSFORM_POLICIES[series_family]


# ---------------------------------------------------------------------------
#  Math Helpers — Per-Capita Normalization & YoY
# ---------------------------------------------------------------------------
#  HARD RULES (enforced by these functions):
#    1. Never divide a growth rate by population.
#    2. Normalize levels first, then compute growth from the normalized level.
#    3. GDP growth stays in %.
#    4. Unemployment changes are in pp (percentage points), not %.

def validate_population(population: pd.Series) -> pd.Series:
    """Validate population series: zero/negative values hard-fail.

    Returns the validated series unchanged. Raises ValueError on bad data.
    NaN values are preserved (they propagate as NaN in downstream math).
    """
    non_null = population.dropna()
    if (non_null <= 0).any():
        bad = non_null[non_null <= 0]
        raise ValueError(
            f"Population contains zero or negative values at indices: "
            f"{bad.index.tolist()[:5]}. This is invalid — cannot normalize."
        )
    return population


def compute_real_gdp_per_capita(
    gdp_level: pd.Series,
    population: pd.Series,
) -> pd.Series:
    """Compute real GDP per capita from level data.

    Formula: real_gdp_per_capita = real_gdp_level / population

    Both inputs must be aligned (same index). Population is validated:
    zero/negative → ValueError, NaN → propagates as NaN (flagged, not zero-filled).
    """
    validate_population(population)
    # NaN population → NaN result (not zero-fill)
    return gdp_level / population


def compute_real_gdp_per_100k(
    gdp_level: pd.Series,
    population: pd.Series,
) -> pd.Series:
    """Compute real GDP per 100,000 population from level data.

    Formula: real_gdp_per_100k = real_gdp_level / population * 100_000

    Equivalent to compute_real_gdp_per_capita() * 100_000.
    """
    return compute_real_gdp_per_capita(gdp_level, population) * 100_000


def compute_yoy_from_level(
    series: pd.Series,
    lag_periods: int = 4,
) -> pd.Series:
    """Compute year-over-year percent change from a LEVEL series.

    Formula: yoy_pct = series / lag(series, lag_periods) - 1

    Uses lag_periods=4 for quarterly data (4 quarters = 1 year).
    Returns result in decimal form (0.05 = 5%). Multiply by 100 for %.

    This function must ONLY be applied to levels (GDP, GDP-per-capita,
    population), NEVER to rates or growth rates.
    """
    lagged = series.shift(lag_periods)
    return series / lagged - 1


def compute_unemployment_change_pp(
    unemployment_rate: pd.Series,
    lag_periods: int = 4,
) -> pd.Series:
    """Compute unemployment rate change in percentage points (pp).

    Formula: change_pp = current_rate - lagged_rate

    NOT a percent change — this is the arithmetic difference.
    A move from 4.0% to 4.5% is +0.5 pp, NOT +12.5%.
    """
    return unemployment_rate - unemployment_rate.shift(lag_periods)


def aggregate_to_quarter(
    series: pd.Series,
    aggregation_rule: str,
) -> pd.Series:
    """Aggregate a time series to quarterly frequency using the declared rule.

    Aggregation rules:
      - "mean":          Average of observations within the quarter
                         (correct for rates like unemployment)
      - "last":          Last observation of the quarter
                         (correct for indices like HPI)
      - "sum":           Sum of observations within the quarter
                         (correct for flows like GDP components)
      - "point_in_time": Same as "last" — use the latest available estimate
                         (correct for stock variables like population)

    Input series must have a DatetimeIndex. Output is quarter-end indexed.
    """
    if series.empty:
        return series
    if not isinstance(series.index, pd.DatetimeIndex):
        raise TypeError(
            f"aggregate_to_quarter requires DatetimeIndex, got {type(series.index)}"
        )

    if aggregation_rule == "mean":
        return series.resample("QE").mean().dropna()
    elif aggregation_rule == "sum":
        return series.resample("QE").sum().dropna()
    elif aggregation_rule in ("last", "point_in_time"):
        return series.resample("QE").last().dropna()
    else:
        raise ValueError(f"Unknown aggregation_rule: {aggregation_rule!r}")


# ---------------------------------------------------------------------------
#  Derived Metrics Builder
# ---------------------------------------------------------------------------
def build_derived_metrics(
    gdp_df: pd.DataFrame,
    unemp_df: pd.DataFrame,
    pop_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build derived per-capita metrics and aligned quarterly transforms.

    Applies the transformation policy registry to produce:
      - real_gdp_per_capita, real_gdp_per_100k
      - real_gdp_per_100k_yoy_pct (YoY from normalized level, NOT rate/pop)
      - unemployment_rate_quarterly (mean of monthly rates)
      - unemployment_change_pp (arithmetic difference, NOT percent change)

    Returns a DataFrame with cbsa_code, date, and derived metric columns.
    Empty inputs → empty output (no synthetic data).
    """
    if gdp_df.empty and unemp_df.empty:
        return pd.DataFrame(columns=[
            "cbsa_code", "date", "metric_name", "value", "units",
            "transform_type", "aggregation_rule",
        ])

    derived_rows: List[Dict[str, Any]] = []
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- GDP per-capita normalization ---
    gdp_policy = TRANSFORM_POLICIES["gdp"]
    pop_policy = TRANSFORM_POLICIES["population"]

    if not gdp_df.empty and not pop_df.empty:
        # Join GDP and population by cbsa_code
        # Population may have fewer years — forward-fill within each CBSA
        for cbsa in gdp_df["cbsa_code"].unique():
            gdp_slice = gdp_df[gdp_df["cbsa_code"] == cbsa].copy()
            pop_slice = pop_df[pop_df["cbsa_code"] == cbsa].copy()
            if pop_slice.empty:
                # Missing population → flagged NaN, not zero-fill
                for _, row in gdp_slice.iterrows():
                    derived_rows.append({
                        "cbsa_code": cbsa,
                        "date": row.get("date", ""),
                        "metric_name": "real_gdp_per_capita",
                        "value": np.nan,
                        "units": "dollars_per_person",
                        "transform_type": gdp_policy.transform_type,
                        "aggregation_rule": gdp_policy.aggregation_rule,
                        "population_flag": "missing",
                        "load_timestamp": load_ts,
                    })
                    derived_rows.append({
                        "cbsa_code": cbsa,
                        "date": row.get("date", ""),
                        "metric_name": "real_gdp_per_100k",
                        "value": np.nan,
                        "units": "dollars_per_100k",
                        "transform_type": gdp_policy.transform_type,
                        "aggregation_rule": gdp_policy.aggregation_rule,
                        "population_flag": "missing",
                        "load_timestamp": load_ts,
                    })
                continue

            # Build aligned series for per-capita computation
            gdp_vals = pd.Series(
                gdp_slice["gdp_value"].values if "gdp_value" in gdp_slice.columns
                else gdp_slice["value"].values,
                index=range(len(gdp_slice)),
                dtype=float,
            )
            # Use the latest available population for each GDP year
            # (population is annual stock — forward-fill for missing years)
            latest_pop = pop_slice["population"].values[-1] if "population" in pop_slice.columns else pop_slice["value"].values[-1]

            try:
                pop_series = pd.Series(
                    [float(latest_pop)] * len(gdp_slice), dtype=float
                )
                validate_population(pop_series)
                per_cap = compute_real_gdp_per_capita(gdp_vals, pop_series)
                per_100k = compute_real_gdp_per_100k(gdp_vals, pop_series)
            except ValueError as e:
                logging.warning(f"[local_macro] Population validation failed for {cbsa}: {e}")
                per_cap = pd.Series([np.nan] * len(gdp_slice))
                per_100k = pd.Series([np.nan] * len(gdp_slice))

            dates = gdp_slice["date"].tolist()
            for i, dt in enumerate(dates):
                derived_rows.append({
                    "cbsa_code": cbsa,
                    "date": dt,
                    "metric_name": "real_gdp_per_capita",
                    "value": per_cap.iloc[i] if i < len(per_cap) else np.nan,
                    "units": "dollars_per_person",
                    "transform_type": gdp_policy.transform_type,
                    "aggregation_rule": gdp_policy.aggregation_rule,
                    "population_flag": "ok",
                    "load_timestamp": load_ts,
                })
                derived_rows.append({
                    "cbsa_code": cbsa,
                    "date": dt,
                    "metric_name": "real_gdp_per_100k",
                    "value": per_100k.iloc[i] if i < len(per_100k) else np.nan,
                    "units": "dollars_per_100k",
                    "transform_type": gdp_policy.transform_type,
                    "aggregation_rule": gdp_policy.aggregation_rule,
                    "population_flag": "ok",
                    "load_timestamp": load_ts,
                })

            # YoY from normalized level (NOT from raw growth rate)
            if len(per_100k) >= 2:
                yoy = compute_yoy_from_level(per_100k, lag_periods=1)
                # lag_periods=1 because BEA GDP is annual; each row is 1 year apart
                for i, dt in enumerate(dates):
                    yoy_val = yoy.iloc[i] if i < len(yoy) else np.nan
                    derived_rows.append({
                        "cbsa_code": cbsa,
                        "date": dt,
                        "metric_name": "real_gdp_per_100k_yoy_pct",
                        "value": yoy_val * 100 if pd.notna(yoy_val) else np.nan,
                        "units": "pct",
                        "transform_type": gdp_policy.transform_type,
                        "aggregation_rule": gdp_policy.aggregation_rule,
                        "population_flag": "ok",
                        "load_timestamp": load_ts,
                    })

    # --- Unemployment: quarterly mean and pp change ---
    unemp_policy = TRANSFORM_POLICIES["unemployment"]
    if not unemp_df.empty:
        rate_col = "unemployment_rate" if "unemployment_rate" in unemp_df.columns else "value"
        for cbsa in unemp_df["cbsa_code"].unique():
            u_slice = unemp_df[unemp_df["cbsa_code"] == cbsa].copy()
            u_slice["_date"] = pd.to_datetime(u_slice["date"], errors="coerce")
            u_slice = u_slice.dropna(subset=["_date"]).sort_values("_date")
            if u_slice.empty:
                continue

            rate_series = pd.Series(
                u_slice[rate_col].values, index=u_slice["_date"], dtype=float
            )

            # Quarterly aggregation: mean of monthly rates (per policy)
            q_rate = aggregate_to_quarter(rate_series, unemp_policy.aggregation_rule)

            for dt, val in q_rate.items():
                derived_rows.append({
                    "cbsa_code": cbsa,
                    "date": dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt),
                    "metric_name": "unemployment_rate_quarterly",
                    "value": val,
                    "units": "pct",
                    "transform_type": unemp_policy.transform_type,
                    "aggregation_rule": unemp_policy.aggregation_rule,
                    "load_timestamp": load_ts,
                })

            # Unemployment change in pp (lag 4 quarters = 1 year)
            pp_change = compute_unemployment_change_pp(q_rate, lag_periods=4)
            for dt, val in pp_change.items():
                if pd.notna(val):
                    derived_rows.append({
                        "cbsa_code": cbsa,
                        "date": dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt),
                        "metric_name": "unemployment_change_pp",
                        "value": val,
                        "units": "pp",
                        "transform_type": "derived",
                        "aggregation_rule": unemp_policy.aggregation_rule,
                        "load_timestamp": load_ts,
                    })

    if derived_rows:
        return pd.DataFrame(derived_rows)
    return pd.DataFrame(columns=[
        "cbsa_code", "date", "metric_name", "value", "units",
        "transform_type", "aggregation_rule",
    ])


# ---------------------------------------------------------------------------
#  Board-Ready Output Sheets
# ---------------------------------------------------------------------------
#  Required columns for board/risk consumption (per spec):
#    as_of_date, geo_level, msa_name, cbsa_code, state_abbrev, state_fips,
#    county_fips, zip_code, mapping_method, mapping_weight, coverage_pct,
#    source_dataset, source_series_id, source_frequency, real_gdp_level,
#    real_gdp_yoy_pct, population, population_yoy_pct, real_gdp_per_capita,
#    real_gdp_per_100k, real_gdp_per_100k_yoy_pct, unemployment_rate,
#    unemployment_yoy_pp, unemployment_qoq_pp, hpi_yoy_pct, hpi_qoq_pct,
#    portfolio_balance, portfolio_share, macro_stress_flag,
#    data_vintage, load_timestamp

BOARD_COLUMNS = [
    "as_of_date", "geo_level", "msa_name", "cbsa_code", "state_abbrev",
    "state_fips", "county_fips", "zip_code", "mapping_method",
    "mapping_weight", "coverage_pct", "source_dataset", "source_series_id",
    "source_frequency", "real_gdp_level", "real_gdp_yoy_pct", "population",
    "population_yoy_pct", "real_gdp_per_capita", "real_gdp_per_100k",
    "real_gdp_per_100k_yoy_pct", "unemployment_rate", "unemployment_yoy_pp",
    "unemployment_qoq_pp", "hpi_yoy_pct", "hpi_qoq_pct",
    "portfolio_balance", "portfolio_share", "macro_stress_flag",
    "data_vintage", "load_timestamp",
]


def _build_board_row_template(
    spine_row: Dict[str, Any],
    audit_row: Optional[Dict[str, Any]] = None,
    load_ts: str = "",
) -> Dict[str, Any]:
    """Create a board-column template from spine + audit data."""
    row: Dict[str, Any] = {col: None for col in BOARD_COLUMNS}
    row["as_of_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row["load_timestamp"] = load_ts or datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    # Geography from spine
    row["cbsa_code"] = spine_row.get("cbsa_code")
    row["msa_name"] = spine_row.get("msa_name")
    row["state_abbrev"] = spine_row.get("state_abbrev")
    row["state_fips"] = spine_row.get("state_fips")
    row["county_fips"] = spine_row.get("county_fips")
    row["zip_code"] = spine_row.get("zip_code")
    # Geo level classification
    if spine_row.get("cbsa_code"):
        row["geo_level"] = "msa"
    elif spine_row.get("county_fips"):
        row["geo_level"] = "county"
    elif spine_row.get("state_fips"):
        row["geo_level"] = "state"
    else:
        row["geo_level"] = "unknown"
    # Audit trail
    if audit_row:
        row["mapping_method"] = audit_row.get("mapping_method")
        row["mapping_weight"] = audit_row.get("mapping_weight")
        row["coverage_pct"] = audit_row.get("coverage_pct")
    return row


def build_local_macro_latest(
    gdp_df: pd.DataFrame,
    unemp_df: pd.DataFrame,
    pop_df: pd.DataFrame,
    derived_df: pd.DataFrame,
    spine_df: pd.DataFrame,
    audit_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build Local_Macro_Latest: one row per CBSA with latest-period values.

    Pivots the derived metrics into wide format with board-ready columns.
    Only includes the most recent observation per metric per CBSA.
    """
    load_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build audit lookup: cbsa_code → first audit row
    audit_lookup: Dict[str, Dict] = {}
    if not audit_df.empty and "target_cbsa_code" in audit_df.columns:
        for _, arow in audit_df.iterrows():
            cbsa = arow.get("target_cbsa_code")
            if cbsa and cbsa not in audit_lookup:
                audit_lookup[cbsa] = arow.to_dict()

    board_rows: List[Dict[str, Any]] = []

    # Process each CBSA in spine
    for _, spine_row in spine_df.drop_duplicates(subset=["cbsa_code"]).iterrows():
        cbsa = spine_row.get("cbsa_code")
        if not cbsa:
            continue

        row = _build_board_row_template(
            spine_row.to_dict(),
            audit_lookup.get(cbsa),
            load_ts,
        )

        # Pull latest GDP level
        if not gdp_df.empty:
            g = gdp_df[gdp_df["cbsa_code"] == cbsa]
            if not g.empty:
                val_col = "gdp_value" if "gdp_value" in g.columns else "value"
                latest = g.sort_values("date").iloc[-1]
                row["real_gdp_level"] = latest.get(val_col)
                row["source_dataset"] = latest.get("source_dataset")
                row["source_series_id"] = latest.get("source_series_id")
                row["source_frequency"] = latest.get("source_frequency")
                row["data_vintage"] = latest.get("data_vintage")

        # Pull latest population
        if not pop_df.empty:
            p = pop_df[pop_df["cbsa_code"] == cbsa]
            if not p.empty:
                val_col = "population" if "population" in p.columns else "value"
                latest = p.sort_values("date").iloc[-1]
                row["population"] = latest.get(val_col)

        # Pull derived metrics (already computed per-capita, YoY, etc.)
        if not derived_df.empty:
            d = derived_df[derived_df["cbsa_code"] == cbsa]
            _fill_from_derived(row, d, "real_gdp_per_capita", "real_gdp_per_capita")
            _fill_from_derived(row, d, "real_gdp_per_100k", "real_gdp_per_100k")
            _fill_from_derived(row, d, "real_gdp_per_100k_yoy_pct",
                               "real_gdp_per_100k_yoy_pct")
            _fill_from_derived(row, d, "unemployment_rate_quarterly",
                               "unemployment_rate")
            _fill_from_derived(row, d, "unemployment_change_pp",
                               "unemployment_yoy_pp")

        # Compute GDP YoY from raw levels if available
        if not gdp_df.empty:
            g = gdp_df[gdp_df["cbsa_code"] == cbsa].sort_values("date")
            if len(g) >= 2:
                val_col = "gdp_value" if "gdp_value" in g.columns else "value"
                vals = g[val_col].astype(float)
                yoy = compute_yoy_from_level(vals.reset_index(drop=True),
                                             lag_periods=1)
                last_yoy = yoy.iloc[-1]
                if pd.notna(last_yoy):
                    row["real_gdp_yoy_pct"] = last_yoy * 100

        # Population YoY — compute from raw levels
        if not pop_df.empty:
            p = pop_df[pop_df["cbsa_code"] == cbsa].sort_values("date")
            if len(p) >= 2:
                val_col = "population" if "population" in p.columns else "value"
                vals = p[val_col].astype(float)
                yoy = compute_yoy_from_level(vals.reset_index(drop=True),
                                             lag_periods=1)
                last_yoy = yoy.iloc[-1]
                if pd.notna(last_yoy):
                    row["population_yoy_pct"] = last_yoy * 100

        # Macro stress flag: flag if unemployment rising AND GDP declining
        unemp_rising = (row.get("unemployment_yoy_pp") or 0) > 0.5
        gdp_declining = (row.get("real_gdp_yoy_pct") or 0) < 0
        if unemp_rising and gdp_declining:
            row["macro_stress_flag"] = "STRESS"
        elif unemp_rising or gdp_declining:
            row["macro_stress_flag"] = "WATCH"
        else:
            row["macro_stress_flag"] = "OK"

        board_rows.append(row)

    if board_rows:
        return pd.DataFrame(board_rows, columns=BOARD_COLUMNS)
    return pd.DataFrame(columns=BOARD_COLUMNS)


def _fill_from_derived(
    row: Dict[str, Any],
    derived_slice: pd.DataFrame,
    metric_name: str,
    target_col: str,
) -> None:
    """Fill a board row column from the latest derived metric value."""
    if derived_slice.empty:
        return
    match = derived_slice[derived_slice["metric_name"] == metric_name]
    if match.empty:
        return
    latest = match.sort_values("date").iloc[-1]
    val = latest.get("value")
    if pd.notna(val):
        row[target_col] = val


def build_msa_board_panel(
    latest_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build MSA_Board_Panel: compact summary for board presentations.

    Takes the Local_Macro_Latest DataFrame and produces a presentation-ready
    panel with key columns, sorted by GDP level descending.
    """
    if latest_df.empty:
        return pd.DataFrame(columns=[
            "msa_name", "cbsa_code", "state_abbrev", "geo_level",
            "real_gdp_level", "real_gdp_yoy_pct", "real_gdp_per_100k",
            "real_gdp_per_100k_yoy_pct", "population", "population_yoy_pct",
            "unemployment_rate", "unemployment_yoy_pp",
            "macro_stress_flag", "mapping_method", "as_of_date",
        ])

    panel_cols = [
        "msa_name", "cbsa_code", "state_abbrev", "geo_level",
        "real_gdp_level", "real_gdp_yoy_pct", "real_gdp_per_100k",
        "real_gdp_per_100k_yoy_pct", "population", "population_yoy_pct",
        "unemployment_rate", "unemployment_yoy_pp",
        "macro_stress_flag", "mapping_method", "as_of_date",
    ]

    # Select only columns that exist
    available = [c for c in panel_cols if c in latest_df.columns]
    panel = latest_df[available].copy()

    # Sort by GDP level descending (largest MSAs first)
    if "real_gdp_level" in panel.columns:
        panel = panel.sort_values("real_gdp_level", ascending=False,
                                  na_position="last")

    return panel.reset_index(drop=True)


def build_skip_audit(
    reason: str,
    context: str = "",
) -> pd.DataFrame:
    """Build a single-row audit DataFrame explaining why local macro was skipped.

    Written to the workbook so downstream consumers know the omission was
    intentional, not a silent failure.
    """
    return pd.DataFrame([{
        "sheet_name": "Local_Macro_Skip_Audit",
        "skip_reason": reason,
        "context": context,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }])


# ---------------------------------------------------------------------------
#  Orchestrator — run full local macro pipeline
# ---------------------------------------------------------------------------
def run_local_macro_pipeline(
    cbsa_codes: Optional[List[str]] = None,
    zip_codes: Optional[List[str]] = None,
    county_fips_codes: Optional[List[str]] = None,
    state_abbrevs: Optional[List[str]] = None,
    hud_token: Optional[str] = None,
    bea_api_key: Optional[str] = None,
    census_api_key: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """Run the full local macro pipeline.

    Uses the geography spine to resolve all input geographies to CBSAs,
    then fetches GDP, unemployment, and population data.

    Returns dict suitable for unpacking into write_excel_output(**kwargs):
      {
        "Local_Macro_Raw":       raw API responses with provenance
        "Local_Macro_Derived":   per-capita, YoY, quarterly transforms
        "Local_Macro_Mapped":    spine-joined data with geography context
        "Local_Macro_Latest":    one row per CBSA, latest values, board columns
        "MSA_Board_Panel":       compact board-presentation panel
        "MSA_Crosswalk_Audit":   full mapping audit trail
      }
    """
    logging.info("[local_macro] Building geography spine...")

    # Use defaults if no inputs provided
    if not any([cbsa_codes, zip_codes, county_fips_codes, state_abbrevs]):
        cbsa_codes = [m["cbsa_code"] for m in TOP_MSAS]

    spine_df, audit_df = build_geography_spine(
        cbsa_codes=cbsa_codes,
        zip_codes=zip_codes,
        county_fips_codes=county_fips_codes,
        state_abbrevs=state_abbrevs,
        hud_token=hud_token,
    )

    # Get unique resolved CBSAs for API calls
    resolved_cbsas = spine_df["cbsa_code"].dropna().unique().tolist()
    logging.info(f"[local_macro] Resolved {len(resolved_cbsas)} unique CBSAs")

    # Fetch macro data
    logging.info("[local_macro] Fetching BEA GDP...")
    gdp_df = fetch_bea_gdp_metro(resolved_cbsas, bea_api_key=bea_api_key)

    logging.info("[local_macro] Fetching BLS unemployment...")
    unemp_df = fetch_bls_unemployment_metro(resolved_cbsas)

    logging.info("[local_macro] Fetching Census population...")
    pop_df = fetch_census_population_metro(resolved_cbsas, census_api_key=census_api_key)

    # Build raw output (all API results with provenance)
    raw_parts = []
    for label, df, val_col in [
        ("GDP", gdp_df, "gdp_value"),
        ("Unemployment", unemp_df, "unemployment_rate"),
        ("Population", pop_df, "population"),
    ]:
        if not df.empty:
            part = df.copy()
            part["metric_name"] = label
            if val_col in part.columns:
                part = part.rename(columns={val_col: "value"})
            raw_parts.append(part)

    if raw_parts:
        raw_df = pd.concat(raw_parts, ignore_index=True)
    else:
        raw_df = pd.DataFrame(columns=[
            "cbsa_code", "date", "value", "metric_name",
            "source_dataset", "source_series_id", "source_frequency",
            "data_vintage", "load_timestamp",
        ])

    # Build derived per-capita metrics and quarterly transforms
    logging.info("[local_macro] Computing derived metrics...")
    derived_df = build_derived_metrics(gdp_df, unemp_df, pop_df)

    # Build mapped output (join raw + derived to spine for geography context)
    combined_raw = raw_df
    if not derived_df.empty:
        # Append derived metrics to raw (they have provenance metadata too)
        raw_parts_derived = []
        if not raw_df.empty:
            raw_parts_derived.append(raw_df)
        raw_parts_derived.append(derived_df)
        combined_raw = pd.concat(raw_parts_derived, ignore_index=True)

    if not combined_raw.empty and not spine_df.empty:
        mapped_df = combined_raw.merge(
            spine_df.drop_duplicates(subset=["cbsa_code"]),
            on="cbsa_code",
            how="left",
        )
    else:
        mapped_df = combined_raw.copy()
        for col in SPINE_COLUMNS:
            if col not in mapped_df.columns:
                mapped_df[col] = None

    # Build board-ready output sheets
    logging.info("[local_macro] Building board-ready sheets...")
    latest_df = build_local_macro_latest(
        gdp_df, unemp_df, pop_df, derived_df, spine_df, audit_df,
    )
    board_panel_df = build_msa_board_panel(latest_df)

    result = {
        "Local_Macro_Raw": raw_df,
        "Local_Macro_Derived": derived_df,
        "Local_Macro_Mapped": mapped_df,
        "Local_Macro_Latest": latest_df,
        "MSA_Board_Panel": board_panel_df,
        "MSA_Crosswalk_Audit": audit_df,
    }

    # Log summary
    for name, df in result.items():
        logging.info(f"[local_macro] {name}: {len(df)} rows")

    return result
