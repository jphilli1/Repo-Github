"""
Case-Shiller ZIP Code Enrichment Module.

Maps ZIP codes to Case-Shiller regional metro areas and builds
coverage/summary sheets for the Excel dashboard.

Controlled by ENABLE_CASE_SHILLER_ZIP_ENRICHMENT env variable.
"""

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
