"""
Case-Shiller ZIP Code Mapper — County-Level FIPS to ZIP Enrichment
==================================================================

Maps the 20 regional Case-Shiller metro indexes to ZIP codes using
county-level FIPS codes (per S&P CoreLogic methodology) joined to the
HUD USPS County-to-ZIP Crosswalk API (type=7).

The county definitions are hardcoded from the official S&P Case-Shiller
"Index Geography" table, ensuring mathematically exact region boundaries
rather than CBSA/CBSA-Division approximations.

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
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

try:
    from dotenv import load_dotenv as _load_dotenv
    _HAS_DOTENV = True
except ImportError:
    _load_dotenv = None
    _HAS_DOTENV = False

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Enrichment status codes — distinguish skipped vs failed vs success
# ---------------------------------------------------------------------------
ENRICH_SKIPPED_DISABLED = "SKIPPED_DISABLED"
ENRICH_SKIPPED_NO_TOKEN = "SKIPPED_NO_TOKEN"
ENRICH_SKIPPED_NO_REQUESTS = "SKIPPED_NO_REQUESTS"
ENRICH_FAILED_TOKEN_AUTH = "FAILED_TOKEN_AUTH"
ENRICH_FAILED_HTTP = "FAILED_HTTP"
ENRICH_FAILED_PARSE = "FAILED_PARSE"
ENRICH_FAILED_EMPTY_RESPONSE = "FAILED_EMPTY_RESPONSE"
ENRICH_SUCCESS_NO_MATCHES = "SUCCESS_NO_MATCHES"
ENRICH_SUCCESS_NO_ZIPS = "SUCCESS_NO_ZIPS"
ENRICH_SUCCESS_WITH_ZIPS = "SUCCESS_WITH_ZIPS"


def is_zip_enrichment_enabled() -> bool:
    """Check whether Case-Shiller ZIP enrichment is enabled via env flag."""
    val = os.getenv("ENABLE_CASE_SHILLER_ZIP_ENRICHMENT", "true").strip().lower()
    return val in {"1", "true", "yes", "y"}


# ---------------------------------------------------------------------------
# HUD token resolver — multi-source with diagnostics
# ---------------------------------------------------------------------------
def resolve_hud_token(
    explicit_token: Optional[str] = None,
    script_dir: Optional[str] = None,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Resolve HUD_USER_TOKEN from multiple sources with diagnostics.

    Resolution order:
        1. explicit_token argument (if provided)
        2. os.getenv("HUD_USER_TOKEN")
        3. .env in script_dir
        4. .env in current working directory

    Returns
    -------
    (token_or_none, diagnostics_dict)

    The diagnostics dict includes:
        token_found, source_used, token_length, token_prefix_masked,
        dotenv_available, paths_checked, current_working_directory,
        script_directory, process_executable, process_pid
    """
    paths_checked: List[str] = []
    source_used = "missing"
    token: Optional[str] = None
    cwd = os.getcwd()
    s_dir = script_dir or str(Path(__file__).parent)

    # 1. Explicit argument
    if explicit_token:
        token = explicit_token
        source_used = "explicit_argument"
    else:
        # 2. Environment variable
        env_tok = os.getenv("HUD_USER_TOKEN")
        paths_checked.append("os.getenv('HUD_USER_TOKEN')")
        if env_tok:
            token = env_tok
            source_used = "environment_variable"
        else:
            # 3. .env in script directory
            env_script = Path(s_dir) / ".env"
            paths_checked.append(str(env_script))
            if _HAS_DOTENV and env_script.exists():
                _load_dotenv(str(env_script), override=False)
                env_tok = os.getenv("HUD_USER_TOKEN")
                if env_tok:
                    token = env_tok
                    source_used = "dotenv_script_dir"
            # 4. .env in cwd (if different)
            env_cwd = Path(cwd) / ".env"
            if str(env_cwd) != str(env_script):
                paths_checked.append(str(env_cwd))
                if not token and _HAS_DOTENV and env_cwd.exists():
                    _load_dotenv(str(env_cwd), override=False)
                    env_tok = os.getenv("HUD_USER_TOKEN")
                    if env_tok:
                        token = env_tok
                        source_used = "dotenv_cwd"

    diag: Dict[str, Any] = {
        "token_found": token is not None,
        "source_used": source_used,
        "token_length": len(token) if token else 0,
        "token_prefix_masked": (token[:6] + "***") if token and len(token) >= 6 else ("***" if token else None),
        "dotenv_available": _HAS_DOTENV,
        "paths_checked": paths_checked,
        "current_working_directory": cwd,
        "script_directory": s_dir,
        "process_executable": sys.executable,
        "process_pid": os.getpid(),
    }
    return token, diag

# ═══════════════════════════════════════════════════════════════════════════
# CASE-SHILLER COUNTY-LEVEL FIPS MAP (S&P CoreLogic Methodology)
# ═══════════════════════════════════════════════════════════════════════════
#
# Each entry maps a Case-Shiller regional metro to its constituent counties
# using official 5-digit US FIPS codes.  Sourced from the "Index Geography"
# table of the S&P CoreLogic Case-Shiller Home Price Indices Methodology PDF.
#
# This replaces the former CBSA/CBSA-Division approximation approach with
# exact county-level definitions as specified by S&P.

CASE_SHILLER_COUNTY_MAP: List[Dict[str, str]] = [
    # ── Composite 10 ──────────────────────────────────────────────────────
    # Boston
    {"case_shiller_region": "Boston", "fips": "25005", "county": "Bristol", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "25009", "county": "Essex", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "25017", "county": "Middlesex", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "25021", "county": "Norfolk", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "25023", "county": "Plymouth", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "25025", "county": "Suffolk", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "25027", "county": "Worcester", "state": "MA"},
    {"case_shiller_region": "Boston", "fips": "33011", "county": "Hillsborough", "state": "NH"},
    {"case_shiller_region": "Boston", "fips": "33015", "county": "Rockingham", "state": "NH"},
    {"case_shiller_region": "Boston", "fips": "33017", "county": "Strafford", "state": "NH"},
    # Chicago
    {"case_shiller_region": "Chicago", "fips": "17031", "county": "Cook", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17037", "county": "DeKalb", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17043", "county": "DuPage", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17063", "county": "Grundy", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17089", "county": "Kane", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17093", "county": "Kendall", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17097", "county": "Lake", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17111", "county": "McHenry", "state": "IL"},
    {"case_shiller_region": "Chicago", "fips": "17197", "county": "Will", "state": "IL"},
    # Denver
    {"case_shiller_region": "Denver", "fips": "08001", "county": "Adams", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08005", "county": "Arapahoe", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08014", "county": "Broomfield", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08019", "county": "Clear Creek", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08031", "county": "Denver", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08035", "county": "Douglas", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08039", "county": "Elbert", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08047", "county": "Gilpin", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08059", "county": "Jefferson", "state": "CO"},
    {"case_shiller_region": "Denver", "fips": "08093", "county": "Park", "state": "CO"},
    # Las Vegas
    {"case_shiller_region": "Las Vegas", "fips": "32003", "county": "Clark", "state": "NV"},
    # Los Angeles
    {"case_shiller_region": "Los Angeles", "fips": "06037", "county": "Los Angeles", "state": "CA"},
    {"case_shiller_region": "Los Angeles", "fips": "06059", "county": "Orange", "state": "CA"},
    # Miami
    {"case_shiller_region": "Miami", "fips": "12011", "county": "Broward", "state": "FL"},
    {"case_shiller_region": "Miami", "fips": "12086", "county": "Miami-Dade", "state": "FL"},
    {"case_shiller_region": "Miami", "fips": "12099", "county": "Palm Beach", "state": "FL"},
    # New York
    {"case_shiller_region": "New York", "fips": "36005", "county": "Bronx", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36027", "county": "Dutchess", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36047", "county": "Kings", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36059", "county": "Nassau", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36061", "county": "New York", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36071", "county": "Orange", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36079", "county": "Putnam", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36081", "county": "Queens", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36085", "county": "Richmond", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36087", "county": "Rockland", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36103", "county": "Suffolk", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "36119", "county": "Westchester", "state": "NY"},
    {"case_shiller_region": "New York", "fips": "34003", "county": "Bergen", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34013", "county": "Essex", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34017", "county": "Hudson", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34019", "county": "Hunterdon", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34023", "county": "Middlesex", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34025", "county": "Monmouth", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34027", "county": "Morris", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34029", "county": "Ocean", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34031", "county": "Passaic", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34035", "county": "Somerset", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34037", "county": "Sussex", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "34039", "county": "Union", "state": "NJ"},
    {"case_shiller_region": "New York", "fips": "42103", "county": "Pike", "state": "PA"},
    # San Diego
    {"case_shiller_region": "San Diego", "fips": "06073", "county": "San Diego", "state": "CA"},
    # San Francisco
    {"case_shiller_region": "San Francisco", "fips": "06001", "county": "Alameda", "state": "CA"},
    {"case_shiller_region": "San Francisco", "fips": "06013", "county": "Contra Costa", "state": "CA"},
    {"case_shiller_region": "San Francisco", "fips": "06041", "county": "Marin", "state": "CA"},
    {"case_shiller_region": "San Francisco", "fips": "06075", "county": "San Francisco", "state": "CA"},
    {"case_shiller_region": "San Francisco", "fips": "06081", "county": "San Mateo", "state": "CA"},
    # Washington
    {"case_shiller_region": "Washington", "fips": "11001", "county": "District of Columbia", "state": "DC"},
    {"case_shiller_region": "Washington", "fips": "24009", "county": "Calvert", "state": "MD"},
    {"case_shiller_region": "Washington", "fips": "24017", "county": "Charles", "state": "MD"},
    {"case_shiller_region": "Washington", "fips": "24021", "county": "Frederick", "state": "MD"},
    {"case_shiller_region": "Washington", "fips": "24031", "county": "Montgomery", "state": "MD"},
    {"case_shiller_region": "Washington", "fips": "24033", "county": "Prince George's", "state": "MD"},
    {"case_shiller_region": "Washington", "fips": "51013", "county": "Arlington", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51043", "county": "Clarke", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51047", "county": "Culpeper", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51059", "county": "Fairfax", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51061", "county": "Fauquier", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51107", "county": "Loudoun", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51153", "county": "Prince William", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51157", "county": "Rappahannock", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51177", "county": "Spotsylvania", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51179", "county": "Stafford", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51187", "county": "Warren", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51510", "county": "Alexandria City", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51600", "county": "Fairfax City", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51610", "county": "Falls Church City", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51630", "county": "Fredericksburg City", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51683", "county": "Manassas City", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "51685", "county": "Manassas Park City", "state": "VA"},
    {"case_shiller_region": "Washington", "fips": "54037", "county": "Jefferson", "state": "WV"},

    # ── Composite 20 (additional 10 metros) ───────────────────────────────
    # Atlanta
    {"case_shiller_region": "Atlanta", "fips": "13013", "county": "Barrow", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13015", "county": "Bartow", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13045", "county": "Carroll", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13057", "county": "Cherokee", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13063", "county": "Clayton", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13067", "county": "Cobb", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13077", "county": "Coweta", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13085", "county": "Dawson", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13089", "county": "DeKalb", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13097", "county": "Douglas", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13113", "county": "Fayette", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13117", "county": "Forsyth", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13121", "county": "Fulton", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13135", "county": "Gwinnett", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13143", "county": "Haralson", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13149", "county": "Heard", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13151", "county": "Henry", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13159", "county": "Jasper", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13171", "county": "Lamar", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13199", "county": "Meriwether", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13217", "county": "Newton", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13223", "county": "Paulding", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13227", "county": "Pickens", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13231", "county": "Pike", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13247", "county": "Rockdale", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13255", "county": "Spalding", "state": "GA"},
    {"case_shiller_region": "Atlanta", "fips": "13297", "county": "Walton", "state": "GA"},
    # Charlotte
    {"case_shiller_region": "Charlotte", "fips": "37025", "county": "Cabarrus", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "37071", "county": "Gaston", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "37097", "county": "Iredell", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "37109", "county": "Lincoln", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "37119", "county": "Mecklenburg", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "37159", "county": "Rowan", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "37179", "county": "Union", "state": "NC"},
    {"case_shiller_region": "Charlotte", "fips": "45023", "county": "Chester", "state": "SC"},
    {"case_shiller_region": "Charlotte", "fips": "45057", "county": "Lancaster", "state": "SC"},
    {"case_shiller_region": "Charlotte", "fips": "45091", "county": "York", "state": "SC"},
    # Cleveland
    {"case_shiller_region": "Cleveland", "fips": "39035", "county": "Cuyahoga", "state": "OH"},
    {"case_shiller_region": "Cleveland", "fips": "39055", "county": "Geauga", "state": "OH"},
    {"case_shiller_region": "Cleveland", "fips": "39085", "county": "Lake", "state": "OH"},
    {"case_shiller_region": "Cleveland", "fips": "39093", "county": "Lorain", "state": "OH"},
    {"case_shiller_region": "Cleveland", "fips": "39103", "county": "Medina", "state": "OH"},
    # Dallas
    {"case_shiller_region": "Dallas", "fips": "48085", "county": "Collin", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48113", "county": "Dallas", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48119", "county": "Delta", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48121", "county": "Denton", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48139", "county": "Ellis", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48231", "county": "Hunt", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48251", "county": "Johnson", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48257", "county": "Kaufman", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48367", "county": "Parker", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48397", "county": "Rockwall", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48439", "county": "Tarrant", "state": "TX"},
    {"case_shiller_region": "Dallas", "fips": "48497", "county": "Wise", "state": "TX"},
    # Detroit
    {"case_shiller_region": "Detroit", "fips": "26087", "county": "Lapeer", "state": "MI"},
    {"case_shiller_region": "Detroit", "fips": "26093", "county": "Livingston", "state": "MI"},
    {"case_shiller_region": "Detroit", "fips": "26099", "county": "Macomb", "state": "MI"},
    {"case_shiller_region": "Detroit", "fips": "26125", "county": "Oakland", "state": "MI"},
    {"case_shiller_region": "Detroit", "fips": "26147", "county": "St. Clair", "state": "MI"},
    {"case_shiller_region": "Detroit", "fips": "26163", "county": "Wayne", "state": "MI"},
    # Minneapolis
    {"case_shiller_region": "Minneapolis", "fips": "27003", "county": "Anoka", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27019", "county": "Carver", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27025", "county": "Chisago", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27037", "county": "Dakota", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27053", "county": "Hennepin", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27059", "county": "Isanti", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27123", "county": "Ramsey", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27139", "county": "Scott", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27141", "county": "Sherburne", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27163", "county": "Washington", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "27171", "county": "Wright", "state": "MN"},
    {"case_shiller_region": "Minneapolis", "fips": "55093", "county": "Pierce", "state": "WI"},
    {"case_shiller_region": "Minneapolis", "fips": "55109", "county": "St. Croix", "state": "WI"},
    # Phoenix
    {"case_shiller_region": "Phoenix", "fips": "04013", "county": "Maricopa", "state": "AZ"},
    {"case_shiller_region": "Phoenix", "fips": "04021", "county": "Pinal", "state": "AZ"},
    # Portland
    {"case_shiller_region": "Portland", "fips": "41005", "county": "Clackamas", "state": "OR"},
    {"case_shiller_region": "Portland", "fips": "41009", "county": "Columbia", "state": "OR"},
    {"case_shiller_region": "Portland", "fips": "41051", "county": "Multnomah", "state": "OR"},
    {"case_shiller_region": "Portland", "fips": "41067", "county": "Washington", "state": "OR"},
    {"case_shiller_region": "Portland", "fips": "41071", "county": "Yamhill", "state": "OR"},
    {"case_shiller_region": "Portland", "fips": "53011", "county": "Clark", "state": "WA"},
    {"case_shiller_region": "Portland", "fips": "53059", "county": "Skamania", "state": "WA"},
    # Seattle
    {"case_shiller_region": "Seattle", "fips": "53033", "county": "King", "state": "WA"},
    {"case_shiller_region": "Seattle", "fips": "53053", "county": "Pierce", "state": "WA"},
    {"case_shiller_region": "Seattle", "fips": "53061", "county": "Snohomish", "state": "WA"},
    # Tampa
    {"case_shiller_region": "Tampa", "fips": "12053", "county": "Hernando", "state": "FL"},
    {"case_shiller_region": "Tampa", "fips": "12057", "county": "Hillsborough", "state": "FL"},
    {"case_shiller_region": "Tampa", "fips": "12101", "county": "Pasco", "state": "FL"},
    {"case_shiller_region": "Tampa", "fips": "12103", "county": "Pinellas", "state": "FL"},
]

# Excluded index types — never in ZIP output
_EXCLUDED_INDEX_TYPES = {"U.S. National", "Composite-10", "Composite-20"}

# The 20 valid regional metro names (derived from county map)
VALID_CS_METROS = {m["case_shiller_region"] for m in CASE_SHILLER_COUNTY_MAP}

# Lookup: FIPS → county map entry (for fast joins)
_FIPS_TO_COUNTY = {m["fips"]: m for m in CASE_SHILLER_COUNTY_MAP}


# ═══════════════════════════════════════════════════════════════════════════
# HUD API CLIENT
# ═══════════════════════════════════════════════════════════════════════════

HUD_API_BASE = "https://www.huduser.gov/hudapi/public/usps"

# type=7 → county-zip crosswalk
_HUD_TYPE_COUNTY_ZIP = 7

_MAX_RETRIES = 3
_RETRY_BACKOFF = [2, 4, 8]


def _get_hud_token() -> Optional[str]:
    """Read HUD API token from environment."""
    return os.getenv("HUD_USER_TOKEN")


# ---------------------------------------------------------------------------
# HUD response parsing helpers
# ---------------------------------------------------------------------------

def _describe_payload(payload: Any) -> Dict[str, Any]:
    """Return concise, log-safe diagnostics about a HUD API payload shape."""
    info: Dict[str, Any] = {"payload_type": type(payload).__name__}
    if isinstance(payload, dict):
        info["top_level_keys"] = sorted(payload.keys())
        for k, v in payload.items():
            if isinstance(v, (list, dict)):
                info[f"{k}_type"] = type(v).__name__
                if isinstance(v, dict):
                    info[f"{k}_keys_sample"] = sorted(v.keys())[:10]
                elif isinstance(v, list) and v:
                    info[f"{k}_len"] = len(v)
                    first = v[0]
                    info[f"{k}_first_type"] = type(first).__name__
                    if isinstance(first, dict):
                        info[f"{k}_first_keys"] = sorted(first.keys())[:10]
    elif isinstance(payload, list):
        info["list_len"] = len(payload)
        if payload:
            first = payload[0]
            info["first_type"] = type(first).__name__
            if isinstance(first, dict):
                info["first_keys"] = sorted(first.keys())[:10]
    return info


def extract_hud_result_rows(payload: Any) -> List[Dict]:
    """Extract the flat list of row dicts from a HUD API response payload.

    Handles at least these documented shapes:
      A) payload is already a list of row dicts
      B) payload = {"results": [row, row, ...]}
      C) payload = {"results": {"rows": [row, ...]}}
      D) payload = {"results": {"data": [row, ...]}}
      E) payload = {"data": [row, row, ...]}

    Returns an empty list (never raises) if no row list can be found.
    """
    # Shape A — already a flat list
    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        logger.warning(
            f"HUD payload is not dict or list (type={type(payload).__name__}). "
            f"Returning empty rows."
        )
        return []

    # Try "data" key first (Shape E) — common HUD v2 format
    candidate = payload.get("data")
    if isinstance(candidate, list):
        return candidate

    # Try "results" key (Shapes B/C/D)
    candidate = payload.get("results")
    if isinstance(candidate, list):
        return candidate  # Shape B

    if isinstance(candidate, dict):
        # Shape C — {"results": {"rows": [...]}}
        inner = candidate.get("rows")
        if isinstance(inner, list):
            return inner
        # Shape D — {"results": {"data": [...]}}
        inner = candidate.get("data")
        if isinstance(inner, list):
            return inner
        # Fallback: find the first list-valued key inside results
        for k, v in candidate.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                logger.info(f"HUD results: extracted rows from nested key '{k}' ({len(v)} rows)")
                return v

    # Last resort: scan all top-level keys for a list of dicts
    for k, v in payload.items():
        if k in ("data", "results"):
            continue
        if isinstance(v, list) and v and isinstance(v[0], dict):
            logger.info(f"HUD payload: extracted rows from top-level key '{k}' ({len(v)} rows)")
            return v

    logger.warning(f"Could not extract row list from HUD payload. Shape: {_describe_payload(payload)}")
    return []


# Wrapper-level metadata keys — these appear on HUD wrapper rows, not on
# actual crosswalk data rows.
_HUD_WRAPPER_KEYS = frozenset({"year", "quarter", "input", "crosswalk_type", "results"})


def flatten_hud_rows(rows: List[Dict]) -> List[Dict]:
    """Flatten wrapper rows that contain nested ``results`` lists.

    The HUD county-to-ZIP API (type=7) can return a shape like::

        [
          {"year": 2025, "quarter": 4, "input": "06037",
           "crosswalk_type": "7", "results": [
             {"zip": "90001", "county": "06037", "res_ratio": 0.85, ...},
             {"zip": "90002", "county": "06037", "res_ratio": 0.72, ...}
           ]},
          ...
        ]

    Each element is a *wrapper row* whose ``results`` key holds the actual
    crosswalk data rows.  This function detects that pattern and explodes the
    nested lists, propagating parent metadata (year, quarter, input,
    crosswalk_type) onto each child row where the child does not already
    carry that key.

    If the rows are already flat data rows (no nested ``results``), they are
    returned unchanged.
    """
    if not rows:
        return rows

    # Quick check: does the first row look like a wrapper?
    first = rows[0]
    if not isinstance(first, dict):
        return rows

    first_keys = set(first.keys())

    # A row is a wrapper if it has a "results" key whose value is a list
    # AND its other keys are a subset of the known wrapper keys.
    has_nested_results = (
        "results" in first_keys
        and isinstance(first.get("results"), list)
        and (first_keys - {"results"}).issubset(_HUD_WRAPPER_KEYS)
    )

    if not has_nested_results:
        # Already flat data rows — return as-is
        return rows

    # Flatten: explode each wrapper row's "results" into individual data rows
    flat: List[Dict] = []
    n_wrappers = 0
    for wrapper in rows:
        if not isinstance(wrapper, dict):
            continue

        inner = wrapper.get("results")
        if not isinstance(inner, list):
            # Not a wrapper row — keep it
            flat.append(wrapper)
            continue

        n_wrappers += 1
        # Parent metadata to propagate
        parent_meta = {k: v for k, v in wrapper.items() if k != "results"}

        for child in inner:
            if not isinstance(child, dict):
                continue
            # Merge parent metadata into child (child values take precedence)
            merged = {**parent_meta, **child}
            flat.append(merged)

    logger.info(
        f"flatten_hud_rows: exploded {n_wrappers} wrapper rows → "
        f"{len(flat)} flat data rows"
    )
    return flat


# Canonical column name mappings for HUD crosswalk outputs
_HUD_COLUMN_MAP = {
    # ZIP variants
    "zip": "zip", "zip_code": "zip", "zipcode": "zip", "zip5": "zip",
    # County FIPS variants
    "county": "county_fips", "county_fips": "county_fips", "geoid": "county_fips",
    "county_geoid": "county_fips", "countyfips": "county_fips", "fips": "county_fips",
    # Ratio variants
    "residential_ratio": "res_ratio", "res_ratio": "res_ratio",
    "business_ratio": "bus_ratio", "bus_ratio": "bus_ratio",
    "other_ratio": "oth_ratio", "oth_ratio": "oth_ratio",
    "total_ratio": "tot_ratio", "tot_ratio": "tot_ratio",
}


def canonicalize_hud_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename HUD crosswalk columns to canonical names and zero-pad codes.

    Canonical fields: zip, county_fips, res_ratio, bus_ratio, oth_ratio, tot_ratio.
    ZIP → 5-char zero-padded.  County FIPS → 5-char zero-padded.
    """
    if df.empty:
        return df

    df = df.copy()
    # Lowercase and strip all column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Rename to canonical
    rename_map = {}
    for col in df.columns:
        canonical = _HUD_COLUMN_MAP.get(col)
        if canonical and canonical != col:
            rename_map[col] = canonical
    if rename_map:
        df = df.rename(columns=rename_map)

    # Zero-pad ZIP to 5 chars
    if "zip" in df.columns:
        df["zip"] = df["zip"].astype(str).str.strip().str.split(".", n=1).str[0].str.split("-", n=1).str[0].str.zfill(5)

    # Zero-pad county FIPS to 5 chars
    if "county_fips" in df.columns:
        df["county_fips"] = df["county_fips"].astype(str).str.strip().str.split(".", n=1).str[0].str.zfill(5)

    return df


def fetch_hud_crosswalk(
    query: str = "All",
    crosswalk_type: int = _HUD_TYPE_COUNTY_ZIP,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    token: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch HUD USPS ZIP Crosswalk data via REST API.

    Parameters
    ----------
    query : str
        Geography query — a 5-digit county FIPS code or "All".
    crosswalk_type : int
        7 = county-zip (default).
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
    logger.info(f"Fetching HUD crosswalk: type=county-zip(7), query={query}, year={year}, quarter={quarter}")

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(HUD_API_BASE, params=params, headers=headers, timeout=120)
            if resp.status_code == 200:
                data = resp.json()

                # Step 1: Extract row list from top-level payload shape
                records = extract_hud_result_rows(data)
                n_before_flatten = len(records)

                # Step 2: Flatten wrapper rows with nested "results" lists
                records = flatten_hud_rows(records)
                n_after_flatten = len(records)

                # Step 3: Log payload diagnostics
                diag = _describe_payload(data)
                diag["extracted_row_count_before_flatten"] = n_before_flatten
                diag["extracted_row_count_after_flatten"] = n_after_flatten
                if records and isinstance(records[0], dict):
                    diag["first_row_keys"] = sorted(records[0].keys())
                    diag["has_zip_column"] = any("zip" in k.lower() for k in records[0].keys())
                    diag["has_fips_column"] = bool(
                        set(k.lower() for k in records[0].keys()) &
                        {"county", "county_fips", "geoid", "countyfips", "fips"}
                    )
                logger.info(f"HUD response parsed: query={query}, diagnostics={diag}")

                if not records:
                    logger.warning(f"HUD API returned empty/unparseable result for query={query}")
                    return pd.DataFrame()

                # Step 4: Normalize nested dicts into flat DataFrame columns
                try:
                    df = pd.json_normalize(records, sep="_")
                except Exception:
                    df = pd.DataFrame(records)

                # Step 5: Canonicalize column names and zero-pad codes
                df = canonicalize_hud_columns(df)

                # Verify we have real crosswalk columns, not just wrapper metadata
                actual_cols = set(df.columns)
                has_zip_col = any("zip" in c for c in actual_cols)
                has_fips_col = bool(actual_cols & {"county", "county_fips", "geoid", "countyfips", "fips"})

                if actual_cols.issubset(_HUD_WRAPPER_KEYS) or (not has_zip_col and not has_fips_col):
                    logger.error(
                        f"HUD response flattened to wrapper-only columns {sorted(actual_cols)} — "
                        f"payload contains nested results that could not be fully flattened. "
                        f"rows_before_flatten={n_before_flatten}, "
                        f"rows_after_flatten={n_after_flatten}, "
                        f"has_zip={has_zip_col}, has_fips={has_fips_col}, "
                        f"payload_shape={_describe_payload(data)}"
                    )
                    return pd.DataFrame()

                logger.info(
                    f"HUD crosswalk fetched: {len(df)} rows, "
                    f"columns={sorted(df.columns)}, query={query}, "
                    f"has_zip={has_zip_col}, has_fips={has_fips_col}"
                )
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
# COUNTY MAP BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def build_case_shiller_county_map() -> pd.DataFrame:
    """Return the county-level FIPS mapping table as a DataFrame."""
    return pd.DataFrame(CASE_SHILLER_COUNTY_MAP)


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


def _find_county_fips_col(df: pd.DataFrame) -> Optional[str]:
    """Find the county FIPS code column in a HUD type=7 crosswalk DF."""
    candidates = ["county", "geoid", "county_fips", "countyfips", "fips"]
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
    county_xwalk: pd.DataFrame,
    county_map_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Join the S&P county FIPS map to HUD county-to-ZIP crosswalk data.

    Produces one row per (case_shiller_region, zip_code, county_fips).

    Parameters
    ----------
    county_xwalk : DataFrame
        HUD crosswalk type=7 (county-zip) — combined results for all
        relevant county FIPS codes.
    county_map_df : DataFrame or None
        County-level FIPS mapping; defaults to built-in CASE_SHILLER_COUNTY_MAP.

    Returns
    -------
    DataFrame with columns: case_shiller_region, zip_code, county_fips,
        county_name, state, tot_ratio, res_ratio, bus_ratio, oth_ratio,
        year, quarter, source_system.
    """
    if county_map_df is None:
        county_map_df = build_case_shiller_county_map()

    if county_xwalk.empty:
        logger.warning("County crosswalk DataFrame is empty — no ZIP coverage")
        return pd.DataFrame()

    # Find the county FIPS and ZIP columns in the HUD data
    fips_col = _find_county_fips_col(county_xwalk)
    zip_col = _find_zip_col(county_xwalk)

    if not fips_col or not zip_col:
        logger.error(
            f"Cannot find required columns in HUD data. "
            f"Available: {sorted(county_xwalk.columns.tolist())}. "
            f"Need county FIPS col (found: {fips_col}) and ZIP col (found: {zip_col}). "
            f"Shape: {county_xwalk.shape}. "
            f"First row keys: {list(county_xwalk.iloc[0].index) if len(county_xwalk) else 'N/A'}"
        )
        return pd.DataFrame()

    # Normalize the FIPS codes in the HUD data to 5-digit zero-padded strings
    county_xwalk = county_xwalk.copy()
    county_xwalk["_fips_norm"] = (
        county_xwalk[fips_col]
        .astype(str)
        .str.strip()
        .str.split(".", n=1).str[0]
        .str.zfill(5)
    )

    # Build lookup set of FIPS codes from our county map
    map_fips_set = set(county_map_df["fips"].values)

    # Filter HUD data to only rows matching our county FIPS codes
    matched_xwalk = county_xwalk[county_xwalk["_fips_norm"].isin(map_fips_set)].copy()

    # Structured diagnostics
    n_total = len(county_xwalk)
    n_unique_fips_hud = county_xwalk["_fips_norm"].nunique()
    n_matched = len(matched_xwalk)
    n_unique_fips_map = len(map_fips_set)
    matched_fips = set(matched_xwalk["_fips_norm"].unique()) if not matched_xwalk.empty else set()
    unmatched_fips = map_fips_set - matched_fips
    logger.info(
        f"HUD coverage join: total_hud_rows={n_total}, "
        f"distinct_hud_fips={n_unique_fips_hud}, "
        f"map_fips_count={n_unique_fips_map}, "
        f"matched_rows={n_matched}, "
        f"unmatched_county_count={len(unmatched_fips)}"
    )

    if matched_xwalk.empty:
        logger.warning(
            f"No HUD crosswalk rows matched any S&P county FIPS codes. "
            f"HUD FIPS sample: {sorted(county_xwalk['_fips_norm'].unique())[:5]}, "
            f"Map FIPS sample: {sorted(list(map_fips_set))[:5]}"
        )
        return pd.DataFrame()

    # Build FIPS → county map info lookup
    fips_info = {}
    for _, row in county_map_df.iterrows():
        fips_info[row["fips"]] = {
            "case_shiller_region": row["case_shiller_region"],
            "county_name": row["county"],
            "state": row["state"],
        }

    rows: List[Dict[str, Any]] = []
    for _, xrow in matched_xwalk.iterrows():
        fips_code = xrow["_fips_norm"]
        info = fips_info.get(fips_code)
        if not info:
            continue

        ratios = _extract_ratios(xrow)
        yr, qtr = _extract_year_quarter(xrow)

        rows.append({
            "case_shiller_region": info["case_shiller_region"],
            "zip_code": _normalize_zip(xrow[zip_col]),
            "county_fips": fips_code,
            "county_name": info["county_name"],
            "state": info["state"],
            **ratios,
            "year": yr,
            "quarter": qtr,
            "source_system": "HUD USPS County-ZIP Crosswalk (type=7)",
        })

    if not rows:
        logger.warning("No ZIP coverage rows produced after join")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Deduplication
    dedup_cols = ["case_shiller_region", "zip_code", "county_fips", "year", "quarter"]
    existing_dedup = [c for c in dedup_cols if c in df.columns]
    df = df.drop_duplicates(subset=existing_dedup, keep="first")

    return df


# ═══════════════════════════════════════════════════════════════════════════
# SUMMARY BUILDER
# ═══════════════════════════════════════════════════════════════════════════

def summarize_case_shiller_zip_coverage(coverage_df: pd.DataFrame) -> pd.DataFrame:
    """One row per region with aggregate ZIP and county counts."""
    if coverage_df.empty:
        return pd.DataFrame()

    rows = []
    for region, grp in coverage_df.groupby("case_shiller_region"):
        rows.append({
            "case_shiller_region": region,
            "zip_count": grp["zip_code"].nunique(),
            "unique_county_count": grp["county_fips"].nunique(),
            "counties": ", ".join(sorted(grp["county_name"].unique())),
            "states": ", ".join(sorted(grp["state"].unique())),
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
    county_map_df: pd.DataFrame,
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

    # 5. All FIPS codes in county map should be valid 5-digit strings
    bad_fips = county_map_df["fips"].astype(str).str.len() != 5
    if bad_fips.any():
        issues.append(f"ERROR: {bad_fips.sum()} FIPS codes in county map are not 5 characters")

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
    hud_user_token: Optional[str] = None,
    year: Optional[int] = None,
    quarter: Optional[int] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """Top-level function that fetches HUD data and produces the 3 output sheets.

    Parameters
    ----------
    hud_user_token : str, optional
        Explicit HUD API token.  Takes precedence over env / .env discovery.
    year, quarter : int, optional
        HUD crosswalk vintage.  Defaults to env vars or latest.
    token : str, optional
        Backward-compatible alias for ``hud_user_token``.

    Returns
    -------
    dict with keys:
        "CaseShiller_Zip_Coverage"   — pd.DataFrame
        "CaseShiller_Zip_Summary"    — pd.DataFrame
        "CaseShiller_County_Map_Audit" — pd.DataFrame
        "enrichment_status"          — str (one of ENRICH_* constants)
        "token_diagnostics"          — dict from resolve_hud_token()
    """
    county_map_df = build_case_shiller_county_map()

    # Build audit sheet from county map
    audit_df = county_map_df.copy()
    audit_df["included_in_zip_output"] = False
    audit_df["comments"] = "S&P CoreLogic official county definition"

    def _result(status: str, cov=pd.DataFrame(), summ=pd.DataFrame(),
                diag: Optional[Dict] = None) -> Dict[str, Any]:
        return {
            "CaseShiller_Zip_Coverage": cov,
            "CaseShiller_Zip_Summary": summ,
            "CaseShiller_County_Map_Audit": audit_df,
            "enrichment_status": status,
            "token_diagnostics": diag or {},
        }

    # --- ENV TOGGLE CHECK ---
    if not is_zip_enrichment_enabled():
        logger.info("Case-Shiller ZIP enrichment disabled by env flag.")
        audit_df["comments"] = audit_df["comments"] + " | SKIPPED: disabled by env flag"
        return _result(ENRICH_SKIPPED_DISABLED)

    # --- TOKEN RESOLUTION ---
    explicit = hud_user_token or token  # backward compat
    tok, diag = resolve_hud_token(explicit_token=explicit)

    if diag["token_found"]:
        logger.info(
            f"HUD token resolved: source={diag['source_used']}, "
            f"length={diag['token_length']}, prefix={diag['token_prefix_masked']}"
        )
    else:
        logger.warning(
            "HUD_USER_TOKEN not visible to current Python process — "
            "skipping Case-Shiller ZIP enrichment. "
            f"source_used={diag['source_used']}, "
            f"paths_checked={diag['paths_checked']}, "
            f"cwd={diag['current_working_directory']}, "
            f"script_dir={diag['script_directory']}, "
            f"executable={diag['process_executable']}, "
            f"pid={diag['process_pid']}. "
            "Register at https://www.huduser.gov/hudapi/public/register"
        )
        audit_df["comments"] = audit_df["comments"] + f" | {ENRICH_SKIPPED_NO_TOKEN}"
        return _result(ENRICH_SKIPPED_NO_TOKEN, diag=diag)

    if not _HAS_REQUESTS:
        logger.warning("'requests' library not installed — skipping HUD API calls")
        audit_df["comments"] = audit_df["comments"] + f" | {ENRICH_SKIPPED_NO_REQUESTS}"
        return _result(ENRICH_SKIPPED_NO_REQUESTS, diag=diag)

    # Config from env
    if year is None:
        env_year = os.getenv("HUD_CROSSWALK_YEAR")
        year = int(env_year) if env_year else None
    if quarter is None:
        env_qtr = os.getenv("HUD_CROSSWALK_QUARTER")
        quarter = int(env_qtr) if env_qtr else None

    # Fetch county-to-ZIP crosswalk for each unique FIPS code
    unique_fips = sorted(set(county_map_df["fips"].values))
    logger.info(f"Fetching HUD county-ZIP crosswalk (type=7) for {len(unique_fips)} counties...")

    all_xwalk_frames = []
    http_errors = []
    auth_failed = False
    for fips_code in unique_fips:
        try:
            xwalk = fetch_hud_crosswalk(
                query=fips_code, crosswalk_type=_HUD_TYPE_COUNTY_ZIP,
                year=year, quarter=quarter, token=tok,
            )
            if not xwalk.empty:
                all_xwalk_frames.append(xwalk)
        except EnvironmentError:
            raise  # token missing — should not happen since we checked above
        except Exception as e:
            err_str = str(e).lower()
            if "401" in err_str or "403" in err_str or "unauthorized" in err_str:
                auth_failed = True
                logger.error(f"HUD API auth failure for FIPS {fips_code}: {e}")
                break
            http_errors.append((fips_code, str(e)))

    if auth_failed:
        logger.error(
            f"HUD API token authentication failed (source={diag['source_used']}, "
            f"prefix={diag['token_prefix_masked']}). Token may be expired or invalid."
        )
        audit_df["comments"] = audit_df["comments"] + f" | {ENRICH_FAILED_TOKEN_AUTH}"
        return _result(ENRICH_FAILED_TOKEN_AUTH, diag=diag)

    if http_errors and not all_xwalk_frames:
        logger.error(
            f"All HUD API calls failed ({len(http_errors)} errors). "
            f"First error: {http_errors[0][1]}"
        )
        audit_df["comments"] = audit_df["comments"] + f" | {ENRICH_FAILED_HTTP}"
        return _result(ENRICH_FAILED_HTTP, diag=diag)

    if not all_xwalk_frames:
        logger.error("All HUD county-ZIP crosswalk fetches returned empty — cannot build ZIP coverage")
        audit_df["comments"] = audit_df["comments"] + f" | {ENRICH_FAILED_EMPTY_RESPONSE}"
        return _result(ENRICH_FAILED_EMPTY_RESPONSE, diag=diag)

    combined_xwalk = pd.concat(all_xwalk_frames, ignore_index=True)
    logger.info(f"Combined HUD crosswalk: {len(combined_xwalk)} total rows from {len(all_xwalk_frames)} counties")

    # --- PARSE VALIDATION ---
    # Verify combined frame has usable crosswalk columns, not just wrapper metadata
    actual_cols = set(combined_xwalk.columns)
    has_zip = any("zip" in c for c in actual_cols)
    has_fips = bool(actual_cols & {"county", "county_fips", "geoid", "countyfips", "fips"})

    if (not has_zip and not has_fips) or actual_cols.issubset(_HUD_WRAPPER_KEYS):
        parse_diag = {
            "columns_found": sorted(actual_cols),
            "row_count": len(combined_xwalk),
            "has_zip_column": has_zip,
            "has_fips_column": has_fips,
        }
        logger.error(
            f"HUD response parsed but lacks usable ZIP/FIPS columns. "
            f"This indicates the response payload was not flattened correctly. "
            f"Diagnostics: {parse_diag}"
        )
        audit_df["comments"] = audit_df["comments"] + f" | {ENRICH_FAILED_PARSE}"
        return _result(ENRICH_FAILED_PARSE, diag=diag)

    # Build coverage
    coverage_df = build_case_shiller_zip_coverage(combined_xwalk, county_map_df)
    summary_df = summarize_case_shiller_zip_coverage(coverage_df)

    # Update audit
    covered_fips = set(coverage_df["county_fips"].unique()) if not coverage_df.empty else set()
    audit_df["included_in_zip_output"] = audit_df["fips"].isin(covered_fips)

    # Validate
    issues = validate_zip_coverage(coverage_df, summary_df, county_map_df)
    for issue in issues:
        if issue.startswith("ERROR"):
            logger.error(issue)
        else:
            logger.warning(issue)

    # Determine final status
    if coverage_df.empty:
        # Distinguish: HUD rows existed but nothing matched county map
        if len(combined_xwalk) > 0 and has_fips:
            status = ENRICH_SUCCESS_NO_MATCHES
            logger.warning(
                f"HUD returned {len(combined_xwalk)} rows but none matched "
                f"S&P county FIPS codes — zero coverage rows produced"
            )
        else:
            status = ENRICH_SUCCESS_NO_ZIPS
            logger.warning("HUD API responded but produced zero ZIP coverage rows")
    else:
        status = ENRICH_SUCCESS_WITH_ZIPS
        # Downstream write validation
        zip_coverage_count = len(coverage_df)
        zip_summary_count = len(summary_df)
        metro_count = coverage_df["case_shiller_region"].nunique() if "case_shiller_region" in coverage_df.columns else 0
        logger.info(
            f"Case-Shiller ZIP coverage complete: "
            f"zip_coverage_rows={zip_coverage_count}, "
            f"zip_summary_rows={zip_summary_count}, "
            f"metro_count={metro_count}, "
            f"validation_issues={len(issues)}"
        )

    return _result(status, cov=coverage_df, summ=summary_df, diag=diag)


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


# ═══════════════════════════════════════════════════════════════════════════
# BACKWARD-COMPATIBLE HELPERS (ZIP-prefix based metro lookup)
# ═══════════════════════════════════════════════════════════════════════════

# The 20 Case-Shiller regional metros with ZIP-prefix mapping
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


def map_zip_to_metro(zip_code: str) -> Optional[str]:
    """Map a 5-character ZIP code to a Case-Shiller metro name, or None."""
    if not zip_code or len(str(zip_code)) < 3:
        return None
    prefix = str(zip_code).zfill(5)[:3]
    for metro, info in CASE_SHILLER_METROS.items():
        if prefix in info["prefix"]:
            return metro
    return None
