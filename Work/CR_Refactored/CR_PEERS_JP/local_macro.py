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
        "Local_Macro_Mapped":    spine-joined data with geography context
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

    # Build mapped output (join raw to spine for geography context)
    if not raw_df.empty and not spine_df.empty:
        mapped_df = raw_df.merge(
            spine_df.drop_duplicates(subset=["cbsa_code"]),
            on="cbsa_code",
            how="left",
        )
    else:
        mapped_df = raw_df.copy()
        for col in SPINE_COLUMNS:
            if col not in mapped_df.columns:
                mapped_df[col] = None

    result = {
        "Local_Macro_Raw": raw_df,
        "Local_Macro_Mapped": mapped_df,
        "MSA_Crosswalk_Audit": audit_df,
    }

    # Log summary
    for name, df in result.items():
        logging.info(f"[local_macro] {name}: {len(df)} rows")

    return result
