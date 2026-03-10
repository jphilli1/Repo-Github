"""
FRED Case-Shiller Discovery Module.

Maps Case-Shiller metro areas to their FRED series IDs for automated
data retrieval. Uses distinct metro codes to avoid key collisions.
"""

import logging
from typing import Dict

logger = logging.getLogger(__name__)

# Case-Shiller Home Price Index FRED series IDs for the 20 regional metros.
# Each key is a unique metro code (no duplicates).
# Metro naming matches case_shiller_zip_mapper.py exactly.
CASE_SHILLER_SERIES: Dict[str, Dict[str, str]] = {
    "ATL": {
        "metro": "Atlanta",
        "series_id": "ATXRNSA",
        "description": "S&P/Case-Shiller GA-Atlanta Home Price Index",
    },
    "BOS": {
        "metro": "Boston",
        "series_id": "BOXRNSA",
        "description": "S&P/Case-Shiller MA-Boston Home Price Index",
    },
    "CLT": {
        "metro": "Charlotte",
        "series_id": "CRXRNSA",
        "description": "S&P/Case-Shiller NC-Charlotte Home Price Index",
    },
    "CHI": {
        "metro": "Chicago",
        "series_id": "CHXRNSA",
        "description": "S&P/Case-Shiller IL-Chicago Home Price Index",
    },
    "CLE": {
        "metro": "Cleveland",
        "series_id": "LEXRNSA",
        "description": "S&P/Case-Shiller OH-Cleveland Home Price Index",
    },
    "DAL": {
        "metro": "Dallas",
        "series_id": "DAXRNSA",
        "description": "S&P/Case-Shiller TX-Dallas Home Price Index",
    },
    "DEN": {
        "metro": "Denver",
        "series_id": "DNXRNSA",
        "description": "S&P/Case-Shiller CO-Denver Home Price Index",
    },
    "DET": {
        "metro": "Detroit",
        "series_id": "DEXRNSA",
        "description": "S&P/Case-Shiller MI-Detroit Home Price Index",
    },
    "LVG": {
        "metro": "Las Vegas",
        "series_id": "LVXRNSA",
        "description": "S&P/Case-Shiller NV-Las Vegas Home Price Index",
    },
    "LAX": {
        "metro": "Los Angeles",
        "series_id": "LXXRNSA",
        "description": "S&P/Case-Shiller CA-Los Angeles Home Price Index",
    },
    "MIA": {
        "metro": "Miami",
        "series_id": "MIXRNSA",
        "description": "S&P/Case-Shiller FL-Miami Home Price Index",
    },
    "MIN": {
        "metro": "Minneapolis",
        "series_id": "MNXRNSA",
        "description": "S&P/Case-Shiller MN-Minneapolis Home Price Index",
    },
    "NYC": {
        "metro": "New York",
        "series_id": "NYXRNSA",
        "description": "S&P/Case-Shiller NY-New York Home Price Index",
    },
    "PHX": {
        "metro": "Phoenix",
        "series_id": "PHXRNSA",
        "description": "S&P/Case-Shiller AZ-Phoenix Home Price Index",
    },
    "PDX": {
        "metro": "Portland",
        "series_id": "POXRNSA",
        "description": "S&P/Case-Shiller OR-Portland Home Price Index",
    },
    "SDG": {
        "metro": "San Diego",
        "series_id": "SDXRNSA",
        "description": "S&P/Case-Shiller CA-San Diego Home Price Index",
    },
    "SFO": {
        "metro": "San Francisco",
        "series_id": "SFXRNSA",
        "description": "S&P/Case-Shiller CA-San Francisco Home Price Index",
    },
    "SEA": {
        "metro": "Seattle",
        "series_id": "SEXRNSA",
        "description": "S&P/Case-Shiller WA-Seattle Home Price Index",
    },
    "TPA": {
        "metro": "Tampa",
        "series_id": "TPXRNSA",
        "description": "S&P/Case-Shiller FL-Tampa Home Price Index",
    },
    "WDC": {
        "metro": "Washington",
        "series_id": "WDXRNSA",
        "description": "S&P/Case-Shiller DC-Washington Home Price Index",
    },
}


def get_all_cs_series_ids():
    """Return list of all Case-Shiller FRED series IDs."""
    return [v["series_id"] for v in CASE_SHILLER_SERIES.values()]


def get_metro_to_series_map():
    """Return dict mapping metro name -> FRED series ID."""
    return {v["metro"]: v["series_id"] for v in CASE_SHILLER_SERIES.values()}


def get_series_to_metro_map():
    """Return dict mapping FRED series ID -> metro name."""
    return {v["series_id"]: v["metro"] for v in CASE_SHILLER_SERIES.values()}
