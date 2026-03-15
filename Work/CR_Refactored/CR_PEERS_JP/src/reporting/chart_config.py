"""
Chart Configuration & Display Label Resolution
=================================================

Centralized chart configuration for the MSPBNA reporting engine.
Contains:
  - ``CHART_PALETTE`` — single color system for all charts
  - Color convenience aliases (``_C_MSPBNA``, ``_C_WEALTH``, etc.)
  - Composite CERT regime dicts (active, normalized, legacy)
  - Ticker/label maps
  - ``resolve_display_label()`` — canonical entity label resolver
  - ``CHART_COLORS`` — CERT-keyed color map
  - Formatting utilities (``_fmt_percent``, ``_fmt_money_billions``, etc.)
  - ``_METRIC_FORMAT_TYPE`` — NPL coverage x-multiple registry

All chart functions in ``report_generator.py`` MUST use these constants.
No arbitrary per-chart color choices.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


# ==================================================================================
# CENTRALIZED CHART PALETTE — single color system for all charts
# ==================================================================================

CHART_PALETTE = {
    "mspbna":         "#F7A81B",   # Gold — subject bank
    "wealth_peers":   "#7B2D8E",   # Purple — Wealth Peers / Core PB composite
    "all_peers":      "#5B9BD5",   # Blue — All Peers composite
    "peer_cloud":     "#8FA8C8",   # Muted blue-gray — individual peer dots
    "guide":          "#6B7B8D",   # Slate-gray — averages / crosshair guides
    "range_all":      "#D0D0D0",   # Light gray — range bands (All Peers)
    "range_wealth":   "#B8A0C8",   # Muted purple-gray — range bands (Wealth Peers)
    "text":           "#2B2B2B",   # Dark — titles, axis labels
    "grid":           "#D0D0D0",   # Grid lines
}

# Convenience aliases
_C_MSPBNA       = CHART_PALETTE["mspbna"]
_C_WEALTH       = CHART_PALETTE["wealth_peers"]
_C_ALL_PEERS    = CHART_PALETTE["all_peers"]
_C_PEER_CLOUD   = CHART_PALETTE["peer_cloud"]
_C_GUIDE        = CHART_PALETTE["guide"]
_C_RANGE_ALL    = CHART_PALETTE["range_all"]
_C_RANGE_WEALTH = CHART_PALETTE["range_wealth"]
_C_TEXT         = CHART_PALETTE["text"]


# ==================================================================================
# COMPOSITE CERT REGIME
# ==================================================================================

ACTIVE_STANDARD_COMPOSITES: Dict[str, int] = {
    "core_pb": 90001,
    "all_peers": 90003,
}
ACTIVE_NORMALIZED_COMPOSITES: Dict[str, int] = {
    "core_pb": 90004,
    "all_peers": 90006,
}
INACTIVE_LEGACY_COMPOSITES = {90002, 90005, 99998, 99999}

ALL_COMPOSITE_CERTS = (
    set(ACTIVE_STANDARD_COMPOSITES.values())
    | set(ACTIVE_NORMALIZED_COMPOSITES.values())
    | INACTIVE_LEGACY_COMPOSITES
    | {88888}
)

_STANDARD_COMPOSITES = set(ACTIVE_STANDARD_COMPOSITES.values())
_NORMALIZED_COMPOSITES = set(ACTIVE_NORMALIZED_COMPOSITES.values())

# Individual member CERTs for football-field peer range computation
_SUBJECT_BANK_CERT_DEFAULT = int(os.getenv("MSPBNA_CERT", "34221"))
_WEALTH_MEMBER_CERTS = [33124, 57565]
_ALL_PEERS_MEMBER_CERTS = [32992, 33124, 57565, 628, 3511, 7213, 3510]


# ==================================================================================
# DISPLAY LABEL RESOLVER
# ==================================================================================

_TICKER_MAP = {
    "GOLDMAN": "GS",
    "UBS": "UBS",
    "JPMORGAN": "JPM",
    "BANK OF AMERICA": "BAC",
    "CITIBANK": "C",
    "CITI ": "C",
    "WELLS FARGO": "WFC",
}

_COMPOSITE_LABELS = {
    90001: "Wealth Peers",
    90003: "All Peers",
    90004: "Wealth Peers",
    90006: "All Peers",
    88888: "MS Combined",
}

_SUBJECT_CERT = int(os.getenv("MSPBNA_CERT", "34221"))
_MSBNA_CERT = int(os.getenv("MSBNA_CERT", "32992"))


def resolve_display_label(cert: int, name: Optional[str] = None, *,
                          subject_cert: int = _SUBJECT_CERT) -> str:
    """
    Returns standardized display labels for all charts/tables.

    Rules:
    - subject bank -> "MSPBNA"
    - MSBNA -> "MSBNA"
    - individual peers -> ticker symbols (GS, UBS, JPM, BAC, C, WFC)
    - active composites -> descriptive labels (Wealth Peers, All Peers, MS Combined)
    - unknown individuals -> cleaned NAME fallback (not raw CERT)
    """
    if cert == subject_cert:
        return "MSPBNA"
    if cert == _MSBNA_CERT:
        return "MSBNA"

    if cert in _COMPOSITE_LABELS:
        return _COMPOSITE_LABELS[cert]

    if name:
        name_upper = name.upper()
        for pattern, ticker in _TICKER_MAP.items():
            if pattern in name_upper:
                return ticker

    if name:
        cleaned = str(name).title()
        for suffix in [" National Association", " N.A.", ", National Association"]:
            cleaned = cleaned.replace(suffix, "")
        return cleaned.strip()

    return f"CERT {cert}"


# ==================================================================================
# CHART COLOR SYSTEM
# ==================================================================================

CHART_COLORS = {
    "subject":       CHART_PALETTE["mspbna"],
    "wealth_peers":  CHART_PALETTE["wealth_peers"],
    "all_peers":     CHART_PALETTE["all_peers"],
    "peer_cloud":    CHART_PALETTE["peer_cloud"],
    "guide":         CHART_PALETTE["guide"],
}


def _build_cert_color_map(subject_cert: int) -> Dict[int, str]:
    return {
        subject_cert: CHART_COLORS["subject"],
        90001: CHART_COLORS["wealth_peers"],
        90003: CHART_COLORS["all_peers"],
        90004: CHART_COLORS["wealth_peers"],
        90006: CHART_COLORS["all_peers"],
    }


# ==================================================================================
# METRIC FORMAT TYPE REGISTRY
# ==================================================================================
# NPL coverage metrics (ACL / nonaccrual) are formatted as x-multiples (e.g., 1.23x).
# All other metrics default to percent format.

_METRIC_FORMAT_TYPE: Dict[str, str] = {
    "RIC_CRE_Risk_Adj_Coverage": "x",
    "RIC_Resi_Risk_Adj_Coverage": "x",
    "RIC_Comm_Risk_Adj_Coverage": "x",
}


# ==================================================================================
# FORMATTING UTILITIES
# ==================================================================================

def _fmt_money_millions(val: float) -> str:
    """Format value in millions ($X,XXXM)."""
    if pd.isna(val):
        return "N/A"
    return f"${val / 1e3:,.0f}M"


def _fmt_money_millions_with_sign(diff: float) -> str:
    """Format signed diff in millions."""
    if pd.isna(diff):
        return "N/A"
    return f"{'+' if diff > 0 else ''}{_fmt_money_millions(diff)}"


def _fmt_percent_auto(val: float) -> str:
    """Auto-detect decimal vs pct-point scale."""
    if pd.isna(val):
        return "N/A"
    if abs(val) < 1.0:
        return f"{val * 100:.2f}%"
    return f"{val:.2f}%"


def _fmt_money_billions(val: float) -> str:
    """Format FDIC $K -> $XB / $XM / $XK."""
    if pd.isna(val):
        return "N/A"
    abs_val = abs(val)
    if abs_val >= 1e6:
        return f"${val / 1e6:,.1f}B"
    if abs_val >= 1e3:
        return f"${val / 1e3:,.0f}M"
    return f"${val:,.0f}K"


def _fmt_money_billions_diff(diff: float) -> str:
    """Format signed diff in billions."""
    if pd.isna(diff):
        return "N/A"
    return f"{'+' if diff > 0 else ''}{_fmt_money_billions(diff)}"


def _fmt_multiple(val: float) -> str:
    """Format x-multiple (e.g., 1.23x)."""
    if pd.isna(val):
        return "N/A"
    return f"{val:.2f}x"


def _fmt_multiple_diff(diff: float) -> str:
    """Format signed x-diff (+/-0.50x)."""
    if pd.isna(diff):
        return "N/A"
    return f"{diff:+.2f}x"


def _fmt_percent(val: float, ref_val: float = None) -> str:
    """Format % with auto-scale detection."""
    if pd.isna(val):
        return "N/A"
    if abs(val) < 1.0:
        return f"{val * 100:.2f}%"
    return f"{val:.2f}%"


def _fmt_percent_diff(diff: float, ref_val: float = None) -> str:
    """Format %-point diff with ref-value scale detection."""
    if pd.isna(diff):
        return "N/A"
    if ref_val is not None and not pd.isna(ref_val):
        scale = abs(ref_val)
    else:
        scale = abs(diff)
    if scale < 1.0:
        return f"{diff * 100:+.2f}%"
    return f"{diff:+.2f}%"


def _fmt_call_report_date(dt) -> str:
    """Format date for table headers (Month DD, YYYY)."""
    try:
        return dt.strftime("%B %d, %Y")
    except Exception:
        return str(dt)
