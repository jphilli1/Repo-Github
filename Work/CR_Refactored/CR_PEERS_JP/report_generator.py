#!/usr/bin/env python3
"""
MSPBNA Performance Report Generator
====================================

A self-contained reporting engine that automatically locates the latest processed
Excel file and generates professional charts and HTML tables for credit risk analysis.

Usage:
    python report_generator.py

Output:
    - Normalized credit deterioration chart (PNG) -> Peers/charts/
    - Standard & normalized scatter plots (PNG)   -> Peers/scatter/
    - Standard & normalized HTML tables           -> Peers/tables/
"""

import os
import re
import glob
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

warnings.filterwarnings(
    "ignore",
    message="This figure includes Axes that are not compatible with tight_layout",
    category=UserWarning,
)
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from matplotlib.gridspec import GridSpec

try:
    from dotenv import load_dotenv
    _env_path = Path(__file__).parent.resolve() / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

# Dual-mode rendering architecture — see rendering_mode.py
# rendering_mode.py is the SINGLE canonical home for:
#   RenderMode, ArtifactAvailability, ArtifactCapability, ArtifactManifest,
#   ARTIFACT_REGISTRY, should_produce(), is_artifact_available()
from rendering_mode import (
    RenderMode, select_mode,
    ArtifactAvailability, ArtifactCapability,
    ArtifactManifest, ArtifactStatus, ARTIFACT_REGISTRY,
    should_produce, is_artifact_available,
)

# Metric dependency and consumer mapping is centrally managed by metric_registry.py.
# The REPORT_CONSUMER_MAP dict maps each metric code to the list of downstream
# charts/tables that consume it.  This enables impact analysis when a metric
# fails upstream validation.
try:
    from metric_registry import REPORT_CONSUMER_MAP, run_semantic_validation
    _HAS_SEMANTIC_VALIDATION = True
except ImportError:
    REPORT_CONSUMER_MAP = {}
    _HAS_SEMANTIC_VALIDATION = False

# Executive chart generators — YoY heatmap, KRI bullet, sparkline table
try:
    from executive_charts import (
        generate_yoy_heatmap,
        generate_kri_bullet_chart,
        generate_sparkline_table,
        plot_cumulative_growth_loans_vs_acl,
        prepare_cumulative_growth_data,
        BULLET_METRICS_NORMALIZED_RATES,
        BULLET_METRICS_NORMALIZED_COMPOSITION,
        BULLET_METRICS_STANDARD_RATES,
        BULLET_METRICS_STANDARD_COVERAGE,
        SPARKLINE_METRICS_STANDARD,
        SPARKLINE_METRICS_NORMALIZED,
    )
    _HAS_EXECUTIVE_CHARTS = True
except ImportError:
    _HAS_EXECUTIVE_CHARTS = False


# Set style for better-looking charts
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")


# ==================================================================================
# CENTRALIZED CHART PALETTE — single color system for all charts
# ==================================================================================
# All chart functions MUST use these constants. No arbitrary per-chart color choices.

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

# Convenience aliases used in chart functions
_C_MSPBNA       = CHART_PALETTE["mspbna"]
_C_WEALTH       = CHART_PALETTE["wealth_peers"]
_C_ALL_PEERS    = CHART_PALETTE["all_peers"]
_C_PEER_CLOUD   = CHART_PALETTE["peer_cloud"]
_C_GUIDE        = CHART_PALETTE["guide"]
_C_RANGE_ALL    = CHART_PALETTE["range_all"]
_C_RANGE_WEALTH = CHART_PALETTE["range_wealth"]
_C_TEXT         = CHART_PALETTE["text"]


# ==================================================================================
# CHART ANNOTATION HELPER — label anti-overlap engine
# ==================================================================================

class ChartAnnotationHelper:
    """Shared label placement engine for all chart types.

    Priority tiers:
      1. MSPBNA (always labeled)
      2. Wealth Peers (always labeled when plotted)
      3. All Peers (labeled only when explicitly plotted)
      4. Individual peers (labeled only for outliers, edge-of-range, or specifically selected)

    Rules:
      - Use ticker abbreviations only (MSPBNA, GS, UBS, C, JPM, BAC, WFC, MS)
      - Endpoint labels preferred on time-series charts
      - Value labels on bar/line combos are sparse and strategic
      - Leader lines allowed on scatter plots for displaced labels
    """

    # Priority tiers for label placement
    TIER_SUBJECT = 1    # MSPBNA — always label
    TIER_WEALTH = 2     # Wealth Peers — always label when plotted
    TIER_ALL_PEERS = 3  # All Peers — label only if explicitly plotted
    TIER_INDIVIDUAL = 4 # Individual peers — outlier/edge only

    def __init__(self, ax, tag_size: int = 12):
        self.ax = ax
        self.tag_size = tag_size
        self._placed_rects = []  # list of (x0, y0, x1, y1) in display coords

    def _rect_for(self, x_data, y_data, xpx, ypx, text, pad=6):
        """Compute bounding rect in display coords for a label at (x_data, y_data) + pixel offset."""
        xd, yd = self.ax.transData.transform((x_data, y_data))
        cx, cy = xd + xpx, yd + ypx
        w = max(40.0, 0.62 * self.tag_size * len(text)) + 2 * pad
        h = self.tag_size * 1.7 + 2 * pad
        return (cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)

    @staticmethod
    def _rects_overlap(a, b):
        ax0, ay0, ax1, ay1 = a
        bx0, by0, bx1, by1 = b
        return not (ax1 < bx0 or bx1 < ax0 or ay1 < by0 or by1 < ay0)

    def _can_place(self, rect):
        return all(not self._rects_overlap(rect, r) for r in self._placed_rects)

    def place_label(self, x, y, text, tier=4, color="black", box=True,
                    preferred_directions=None, fontweight="bold"):
        """Place a label near (x, y) avoiding collisions with previously placed labels.

        Parameters
        ----------
        x, y : float
            Data coordinates for the anchor point.
        text : str
            Label text (should be a ticker abbreviation).
        tier : int
            Priority tier (1=highest). Higher-priority labels get first placement.
        color : str
            Text color.
        box : bool
            Whether to draw a background box.
        preferred_directions : list of (dx, dy) tuples, optional
            Custom offset candidates in pixels.
        fontweight : str
            Font weight for the annotation.

        Returns
        -------
        bool
            True if label was placed successfully, False if skipped due to crowding.
        """
        if preferred_directions is None:
            preferred_directions = [
                (10, 12), (12, -12), (-10, 12), (-12, -12),
                (16, 0), (-16, 0), (0, 16), (0, -16),
                (22, 14), (-22, 14), (22, -14), (-22, -14),
                (30, 0), (-30, 0), (0, 30), (0, -30),
            ]

        for dx, dy in preferred_directions:
            rect = self._rect_for(x, y, dx, dy, text)
            if self._can_place(rect):
                bbox_props = (dict(boxstyle="round,pad=0.25", fc="white", ec="black",
                                   lw=0.6, alpha=0.95) if box else None)
                arrow_props = (dict(arrowstyle="->", lw=1.0, color="black") if box else None)
                self.ax.annotate(
                    text, xy=(x, y), xytext=(dx, dy), textcoords="offset points",
                    fontsize=self.tag_size, fontweight=fontweight, color=color,
                    bbox=bbox_props, arrowprops=arrow_props, va="center", zorder=10,
                )
                self._placed_rects.append(rect)
                return True

        # Fallback: place at first candidate regardless of overlap (for tier 1-2 only)
        if tier <= 2 and preferred_directions:
            dx, dy = preferred_directions[0]
            rect = self._rect_for(x, y, dx, dy, text)
            bbox_props = (dict(boxstyle="round,pad=0.25", fc="white", ec="black",
                               lw=0.6, alpha=0.95) if box else None)
            arrow_props = (dict(arrowstyle="->", lw=1.0, color="black") if box else None)
            self.ax.annotate(
                text, xy=(x, y), xytext=(dx, dy), textcoords="offset points",
                fontsize=self.tag_size, fontweight=fontweight, color=color,
                bbox=bbox_props, arrowprops=arrow_props, va="center", zorder=10,
            )
            self._placed_rects.append(rect)
            return True
        return False

    def place_endpoint_label(self, x, y, text, color="black"):
        """Place a right-aligned endpoint label on a time-series chart.

        Prefers rightward placement with leader line.
        """
        return self.place_label(
            x, y, text, tier=self.TIER_SUBJECT, color=color, box=True,
            preferred_directions=[(14, 0), (14, 10), (14, -10), (20, 0), (20, 12), (20, -12)],
        )

    def register_fixed_rect(self, x0, y0, x1, y1):
        """Register a fixed rectangle (e.g., from bar labels) to avoid collisions."""
        self._placed_rects.append((x0, y0, x1, y1))


# ==================================================================================
# REPORT CONTEXT — lightweight carrier for generate_reports() helpers
# ==================================================================================

@dataclass
class _ReportContext:
    """Internal context passed to ``_produce_table`` / ``_produce_chart``."""
    mode: RenderMode
    manifest: ArtifactManifest
    base_stem: str
    suppressed_charts: frozenset = field(default_factory=frozenset)


# ==================================================================================
# CONFIGURATION AND UTILITY FUNCTIONS
# ==================================================================================

def find_latest_excel_file(directory: str = "output") -> Optional[str]:
    """Return newest Bank_Performance_Dashboard_*.xlsx. Creates dir safely; falls back to CWD on failure."""
    try:
        base = Path(__file__).parent.resolve()
    except Exception:
        base = Path.cwd().resolve()

    out = (base / directory)
    try:
        out.mkdir(parents=True, exist_ok=True)
        search_root = out
    except OSError as e:
        print(f"WARNING: cannot create/access '{out}': {e}. Falling back to working directory.")
        search_root = Path.cwd()

    def newest(glob_root: Path) -> Optional[str]:
        files = []
        for p in glob_root.glob("Bank_Performance_Dashboard_*.xlsx"):
            try:
                files.append((p.stat().st_mtime, p))
            except OSError:
                pass
        if not files:
            for p in glob_root.glob("**/Bank_Performance_Dashboard_*.xlsx"):
                try:
                    files.append((p.stat().st_mtime, p))
                except OSError:
                    pass
        return str(sorted(files, key=lambda t: t[0], reverse=True)[0][1]) if files else None

    return newest(search_root)


def load_config() -> Dict[str, Any]:
    """
    Loads configuration from environment variables.
    MSPBNA_CERT and MSBNA_CERT are loaded dynamically via os.getenv.
    The subject bank is set to MSPBNA_CERT.
    """
    mspbna_cert = int(os.getenv("MSPBNA_CERT", "34221"))
    msbna_cert = int(os.getenv("MSBNA_CERT", "32992"))
    return {
        'subject_bank_cert': mspbna_cert,
        'mspbna_cert': mspbna_cert,
        'msbna_cert': msbna_cert,
        'output_dir': 'output'
    }


def get_diff_class(diff_str: str) -> str:
    """
    Determines CSS class based on difference value for color coding.
    """
    if diff_str == "N/A":
        return "neutral"

    try:
        diff_value = float(diff_str.replace('%', '').replace('+', ''))

        if diff_value > 0.05:
            return "positive"
        elif diff_value < -0.05:
            return "negative"
        else:
            return "neutral"
    except:
        return "neutral"


# ==================================================================================
# PREFLIGHT VALIDATION
# ==================================================================================

# All known composite/alias CERTs — must never appear as peer dots in scatter plots
ACTIVE_STANDARD_COMPOSITES = {
    "core_pb": 90001,
    "all_peers": 90003,
}
ACTIVE_NORMALIZED_COMPOSITES = {
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

# Plotted peer-average composites by chart path (derived from canonical dicts)
_STANDARD_COMPOSITES = set(ACTIVE_STANDARD_COMPOSITES.values())
_NORMALIZED_COMPOSITES = set(ACTIVE_NORMALIZED_COMPOSITES.values())

# Individual member CERTs for football-field peer range computation
# These match the PEER_GROUPS definitions in MSPBNA_CR_Normalized.py
_SUBJECT_BANK_CERT_DEFAULT = int(os.getenv("MSPBNA_CERT", "34221"))
_WEALTH_MEMBER_CERTS = [33124, 57565]             # GS, UBS (excludes subject)
_ALL_PEERS_MEMBER_CERTS = [32992, 33124, 57565, 628, 3511, 7213, 3510]  # MSBNA + all


# ==================================================================================
# CENTRALIZED DISPLAY LABEL RESOLVER
# ==================================================================================
# All charts and tables MUST use this resolver for entity labels.
# Individual banks → ticker symbols.  Composites → descriptive labels.

# Ticker lookup: bank NAME substring → ticker symbol
_TICKER_MAP = {
    "GOLDMAN": "GS",
    "UBS": "UBS",
    "JPMORGAN": "JPM",
    "BANK OF AMERICA": "BAC",
    "CITIBANK": "C",
    "CITI ": "C",
    "WELLS FARGO": "WFC",
}

# Composite CERT → display label
_COMPOSITE_LABELS = {
    90001: "Wealth Peers",
    90003: "All Peers",
    90004: "Wealth Peers",
    90006: "All Peers",
    88888: "MS Combined",
}

_SUBJECT_CERT = int(os.getenv("MSPBNA_CERT", "34221"))
_MSBNA_CERT = int(os.getenv("MSBNA_CERT", "32992"))


# ==================================================================================
# CENTRALIZED CHART COLOR SYSTEM
# ==================================================================================
# Single source of truth for all chart entity colors.  Every chart function must
# reference these constants instead of defining inline hex strings.

CHART_COLORS = {
    "subject":       "#F7A81B",   # MSPBNA — gold
    "wealth_peers":  "#9C6FB6",   # Wealth Peers (Core PB) — purple
    "all_peers":     "#5B9BD5",   # All Peers composite — blue
    "peer_cloud":    "#A8B8C8",   # Individual peer dots in scatters — muted blue-gray
    "guide":         "#6B7B8D",   # Reference / guide lines — slate-gray
}

# Convenience aliases keyed by CERT number (for chart functions that iterate entities)
def _build_cert_color_map(subject_cert: int) -> Dict[int, str]:
    return {
        subject_cert: CHART_COLORS["subject"],
        90001: CHART_COLORS["wealth_peers"],
        90003: CHART_COLORS["all_peers"],
        90004: CHART_COLORS["wealth_peers"],
        90006: CHART_COLORS["all_peers"],
    }


def resolve_display_label(cert: int, name: Optional[str] = None, *,
                          subject_cert: int = _SUBJECT_CERT) -> str:
    """
    Returns standardized display labels for all charts/tables.

    Rules:
    - subject bank → "MSPBNA"
    - MSBNA → "MSBNA"
    - individual peers → ticker symbols (GS, UBS, JPM, BAC, C, WFC)
    - active composites → descriptive labels (Wealth Peers, All Peers, MS Combined)
    - unknown individuals → cleaned NAME fallback (not raw CERT)
    """
    if cert == subject_cert:
        return "MSPBNA"
    if cert == _MSBNA_CERT:
        return "MSBNA"

    # Composite labels
    if cert in _COMPOSITE_LABELS:
        return _COMPOSITE_LABELS[cert]

    # Ticker resolution from NAME
    if name:
        name_upper = name.upper()
        for pattern, ticker in _TICKER_MAP.items():
            if pattern in name_upper:
                return ticker

    # Fallback: clean bank name (strip "National Association", "N.A.", etc.)
    if name:
        cleaned = str(name).title()
        for suffix in [" National Association", " N.A.", ", National Association"]:
            cleaned = cleaned.replace(suffix, "")
        return cleaned.strip()

    return f"CERT {cert}"


def validate_composite_cert_regime(proc_df: pd.DataFrame) -> Dict[str, Any]:
    """Confirms active composites are present and legacy composites are not used
    for active artifact construction.

    Parameters
    ----------
    proc_df : pd.DataFrame
        Processed DataFrame with a ``CERT`` column.

    Returns
    -------
    dict
        - valid: bool — True if all active composites are present
        - active_present: set of active CERTs found in data
        - active_missing: set of active CERTs NOT found in data
        - legacy_present: set of legacy CERTs found (informational)
        - warnings: list[str]
        - errors: list[str]
    """
    certs_in_data = set(proc_df["CERT"].unique()) if not proc_df.empty else set()
    active_certs = set(ACTIVE_STANDARD_COMPOSITES.values()) | set(ACTIVE_NORMALIZED_COMPOSITES.values())
    active_present = certs_in_data & active_certs
    active_missing = active_certs - certs_in_data
    legacy_present = certs_in_data & INACTIVE_LEGACY_COMPOSITES

    warnings_list = []
    errors_list = []

    for cert in sorted(active_missing):
        errors_list.append(f"Active composite CERT {cert} is missing from data — charts/tables requiring it will be skipped")

    for cert in sorted(legacy_present):
        warnings_list.append(f"Legacy composite CERT {cert} found in data — must NOT be used for chart/table selection")

    return {
        "valid": len(errors_list) == 0,
        "active_present": active_present,
        "active_missing": active_missing,
        "legacy_present": legacy_present,
        "warnings": warnings_list,
        "errors": errors_list,
    }

# Readable artifact names for suppressed_charts
_ARTIFACT_NAMES = {
    "normalized_credit_chart": "Normalized credit deterioration chart",
    "normalized_scatter_nco_vs_nonaccrual": "Normalized scatter: NCO vs Nonaccrual",
    "standard_scatter_nco_vs_npl": "Standard scatter: NCO vs NPL",
    "standard_scatter_pd_vs_npl": "Standard scatter: PD vs NPL",
}


# ==================================================================================
# SHARED LABEL-PLACEMENT / COLLISION-AVOIDANCE HELPER
# ==================================================================================

class LabelPlacer:
    """Pixel-coordinate collision-avoidance engine for chart annotations.

    Usage:
        placer = LabelPlacer(ax)
        placer.place(x, y, "MSPBNA", priority=1, color="black", box=True)
        placer.place(px, py, "Wealth Peers", priority=2, color="#9C6FB6")
    """

    # Default candidate offsets (dx_px, dy_px) tried in priority order
    _DEFAULT_CANDIDATES = [
        (10, 12), (12, -12), (-10, 12), (-12, -12),
        (16, 0), (-16, 0), (0, 16), (0, -16),
        (22, 18), (-22, 18), (22, -18), (-22, -18),
        (30, 0), (-30, 0), (0, 30), (0, -30),
    ]
    _INLINE_CANDIDATES = [
        (12, 0), (18, 0), (-42, 0), (-60, 0),
        (12, 8), (-42, 8), (12, -8), (-42, -8),
    ]

    def __init__(self, ax, tag_size: int = 12, x_threshold: float = 58.0,
                 y_threshold: float = 22.0):
        self.ax = ax
        self.tag_size = tag_size
        self.x_threshold = x_threshold
        self.y_threshold = y_threshold
        self._placed: List[Tuple[float, float]] = []  # screen coords of placed labels

    def _screen(self, x_data: float, y_data: float) -> Tuple[float, float]:
        return self.ax.transData.transform((x_data, y_data))

    def _overlaps(self, sx: float, sy: float) -> bool:
        return any(abs(sx - ox) < self.x_threshold and abs(sy - oy) < self.y_threshold
                   for ox, oy in self._placed)

    def _pick_offset(self, x_data: float, y_data: float,
                     inline: bool = False) -> Tuple[int, int]:
        candidates = self._INLINE_CANDIDATES if inline else self._DEFAULT_CANDIDATES
        sx, sy = self._screen(x_data, y_data)
        for dx, dy in candidates:
            if not self._overlaps(sx + dx, sy + dy):
                self._placed.append((sx + dx, sy + dy))
                return dx, dy
        self._placed.append((sx, sy))
        return candidates[0]

    def place(self, x_data: float, y_data: float, text: str, *,
              priority: int = 5, color: str = "black", box: bool = True,
              inline: bool = False, fontsize: Optional[int] = None,
              fontweight: str = "bold") -> None:
        """Place a label near (x_data, y_data) with collision avoidance.

        Parameters
        ----------
        priority : int
            Lower numbers are placed first (better positions). Call in order.
        inline : bool
            If True, prefer horizontal-only offsets (for guide-line labels).
        """
        dx, dy = self._pick_offset(x_data, y_data, inline=inline)
        fs = fontsize or self.tag_size
        bbox_props = (dict(boxstyle="round,pad=0.25", fc="white", ec="black",
                           lw=0.6, alpha=0.95) if box else None)
        arrow_props = (dict(arrowstyle="->", lw=1.0, color="black") if box else None)
        self.ax.annotate(
            text, xy=(x_data, y_data), xytext=(dx, dy),
            textcoords="offset points", fontsize=fs, fontweight=fontweight,
            color=color, bbox=bbox_props, arrowprops=arrow_props, va="center",
        )

    def reserve(self, x_data: float, y_data: float) -> None:
        """Reserve a screen position to prevent future labels from overlapping."""
        sx, sy = self._screen(x_data, y_data)
        self._placed.append((sx, sy))


def validate_output_inputs(
    proc_df: pd.DataFrame,
    rolling_df: pd.DataFrame,
    subject_cert: int,
) -> Dict[str, Any]:
    """Preflight validation before generating reports.

    Returns a dict with keys:
        - valid: bool — True if all critical checks pass
        - warnings: list[str] — non-blocking issues
        - errors: list[str] — blocking issues
        - suppressed_charts: list[str] — charts that should be suppressed/annotated
        - semantic_report: DataFrame — semantic validation results (if available)
    """
    warnings = []
    errors = []
    suppressed = []

    # 1. Subject bank exists
    if subject_cert not in proc_df["CERT"].values:
        errors.append(f"Subject bank CERT {subject_cert} not found in data")

    # 2. Check normalized metrics are not all-zero or all-NaN
    for metric in ["Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Gross_Loans"]:
        if metric in proc_df.columns:
            vals = pd.to_numeric(proc_df[metric], errors="coerce")
            if vals.isna().all():
                warnings.append(f"{metric} is entirely NaN — normalized charts will be empty")
                suppressed.append("normalized_credit_chart")
            elif (vals == 0).all():
                warnings.append(f"{metric} is entirely zero — normalized charts may be misleading")

    # 3. Check severity columns for material failures on subject bank
    #    AND on plotted peer-average composites. Both are blocking.
    #    Scoped to the LATEST plotted period only — historical failures are
    #    informational but not blocking for current-period charts/tables.
    sev_cols = ["_Norm_NCO_Severity", "_Norm_NA_Severity", "_Norm_Loans_Severity"]
    blocking_certs = {subject_cert} | _NORMALIZED_COMPOSITES  # 90004, 90006

    # Determine latest plotted period
    latest_repdte = None
    if "REPDTE" in proc_df.columns:
        latest_repdte = proc_df["REPDTE"].max()

    for sev_col in sev_cols:
        if sev_col not in proc_df.columns:
            continue

        # Scope to latest period for blocking checks
        if latest_repdte is not None:
            latest_df = proc_df[proc_df["REPDTE"] == latest_repdte]
        else:
            latest_df = proc_df

        material_latest = (latest_df[sev_col] == "material_nan")
        if not material_latest.any():
            continue

        # Check blocking CERTs at latest period
        for cert in blocking_certs:
            cert_material = latest_df.loc[
                (latest_df["CERT"] == cert) & material_latest
            ]
            if not cert_material.empty:
                label = f"subject bank (CERT {cert})" if cert == subject_cert else f"peer-average composite (CERT {cert})"
                errors.append(
                    f"BLOCKING: {sev_col} — {label} has material over-exclusion "
                    f"at latest period ({latest_repdte})"
                )
                suppressed.append("normalized_credit_chart")
                suppressed.append("normalized_scatter_nco_vs_nonaccrual")

        # Non-blocking CERTs at latest period get warnings
        non_blocking = latest_df.loc[
            material_latest & ~latest_df["CERT"].isin(blocking_certs)
        ]
        if not non_blocking.empty:
            warnings.append(
                f"{sev_col}: {len(non_blocking)} non-critical peer rows "
                f"have material over-exclusion at latest period"
            )

    # 3b. Historical material failures: informational warnings only
    for sev_col in sev_cols:
        if sev_col not in proc_df.columns:
            continue
        all_material = (proc_df[sev_col] == "material_nan")
        if all_material.any():
            hist_count = all_material.sum()
            warnings.append(f"{sev_col}: {hist_count} total historical material rows (informational)")

    # 4. Validate required peer-average composite CERTs exist
    proc_certs = set(proc_df["CERT"].unique()) if "CERT" in proc_df.columns else set()
    rolling_certs = set(rolling_df["CERT"].unique()) if "CERT" in rolling_df.columns else set()

    # Standard flow composites
    for cert in _STANDARD_COMPOSITES:
        if cert not in proc_certs and cert not in rolling_certs:
            errors.append(
                f"BLOCKING: Standard composite CERT {cert} missing from data — "
                f"standard charts/scatter cannot plot peer average"
            )
            suppressed.append("standard_scatter_nco_vs_npl")
            suppressed.append("standard_scatter_pd_vs_npl")

    # Normalized flow composites
    for cert in _NORMALIZED_COMPOSITES:
        if cert not in proc_certs and cert not in rolling_certs:
            errors.append(
                f"BLOCKING: Normalized composite CERT {cert} missing from data — "
                f"normalized charts/scatter cannot plot peer average"
            )
            suppressed.append("normalized_credit_chart")
            suppressed.append("normalized_scatter_nco_vs_nonaccrual")

    # 5. Check composites don't contaminate peer scatter
    if "CERT" in rolling_df.columns:
        real_peers = rolling_certs - ALL_COMPOSITE_CERTS - {subject_cert}
        if len(real_peers) == 0:
            errors.append("No real peer banks found in rolling 8Q data — scatter plots impossible")

    # 6. Run semantic validation if available
    semantic_report = pd.DataFrame()
    if _HAS_SEMANTIC_VALIDATION:
        try:
            semantic_report = run_semantic_validation(proc_df)
            high_sev = semantic_report[semantic_report.get("Severity", pd.Series()) == "high"] if not semantic_report.empty else pd.DataFrame()
            if not high_sev.empty:
                for _, row in high_sev.head(5).iterrows():
                    warnings.append(f"Semantic: {row.get('Rule', '?')} — {row.get('Detail', '?')}")
        except Exception as e:
            warnings.append(f"Semantic validation failed: {e}")

    # Deduplicate suppressed list
    suppressed = list(dict.fromkeys(suppressed))

    valid = len(errors) == 0
    return {
        "valid": valid,
        "warnings": warnings,
        "errors": errors,
        "suppressed_charts": suppressed,
        "semantic_report": semantic_report,
    }


# ==================================================================================
# CHART GENERATION FUNCTIONS
# ==================================================================================

def create_credit_deterioration_chart_v3(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    show_both_peer_groups: bool = True
) -> Tuple[Optional[plt.Figure], Optional[plt.Axes]]:
    """
    Creates a chart comparing TTM NCO Rate and NPL-to-Gross Loans Rate
    with golden yellow for subject bank and improved labeling strategy.
    """
    entities_to_plot = [subject_bank_cert]

    _cc = _build_cert_color_map(subject_bank_cert)
    if show_both_peer_groups:
        entities_to_plot.extend([90003, 90001])
        colors = _cc
        entity_names = {
            subject_bank_cert: resolve_display_label(subject_bank_cert, subject_cert=subject_bank_cert),
            90003: resolve_display_label(90003),
            90001: resolve_display_label(90001),
        }
    else:
        entities_to_plot.append(90003)
        colors = _cc
        entity_names = {
            subject_bank_cert: resolve_display_label(subject_bank_cert, subject_cert=subject_bank_cert),
            90003: resolve_display_label(90003),
        }

    chart_df = proc_df_with_peers[proc_df_with_peers['CERT'].isin(entities_to_plot)].copy()

    if chart_df.empty:
        print("No data found for charting")
        return None, None

    chart_df = chart_df.sort_values('REPDTE')
    chart_df = chart_df[chart_df['REPDTE'] >= '2019-10-01']

    if chart_df.empty:
        print("No data found after Q3 2019")
        return None, None

    chart_df['Quarter'] = chart_df['REPDTE'].dt.quarter
    chart_df['Year'] = chart_df['REPDTE'].dt.year
    chart_df['Period_Label'] = 'Q' + chart_df['Quarter'].astype(str) + '-' + (chart_df['Year'] % 100).astype(str).str.zfill(2)

    subject_data = chart_df[chart_df['CERT'] == subject_bank_cert].reset_index(drop=True)
    num_periods = len(subject_data)

    show_label_mask = [False] * num_periods
    for i in range(num_periods - 1, -1, -4):
        show_label_mask[i] = True

    for i, row in subject_data.iterrows():
        quarter = row['Quarter']
        if quarter in [2, 4]:
            show_label_mask[i] = True

    chart_df['Show_Label'] = False
    for cert in entities_to_plot:
        cert_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if len(cert_data) == len(show_label_mask):
            cert_indices = chart_df[chart_df['CERT'] == cert].index
            chart_df.loc[cert_indices, 'Show_Label'] = show_label_mask

    fig, ax = plt.subplots(figsize=(16, 8))
    x_positions = np.arange(len(subject_data))
    num_entities = len(entities_to_plot)
    bar_width = 0.8 / num_entities
    bar_positions = {}

    for i, cert in enumerate(entities_to_plot):
        offset = (i - (num_entities - 1) / 2) * bar_width
        bar_positions[cert] = x_positions + offset

    for cert in entities_to_plot:
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            label = entity_names.get(cert, f"CERT {cert}")
            nco_rate = entity_data['TTM_NCO_Rate'].fillna(0) / 100
            bars = ax.bar(bar_positions[cert], nco_rate, alpha=0.7, color=colors.get(cert, "#7F8C8D"),
                         label=f'{label} TTM NCO Rate', width=bar_width)

    ax2 = ax.twinx()

    line_styles = ['-', '--', '-.']
    for i, cert in enumerate(entities_to_plot):
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            label = entity_names.get(cert, f"CERT {cert}")
            npl_rate = entity_data['NPL_to_Gross_Loans_Rate'].fillna(0) / 100
            line_style = line_styles[i % len(line_styles)]
            ax2.plot(x_positions, npl_rate, color=colors.get(cert, "#7F8C8D"),
                    linestyle=line_style, marker='o', linewidth=2.5,
                    label=f'{label} NPL-to-Book', markersize=5)

    ax.set_xlabel('Reporting Period', fontsize=12, fontweight='bold')
    ax.set_ylabel('TTM NCO Rate', fontsize=12, fontweight='bold', color='black')
    ax2.set_ylabel('NPL-to-Gross Loans Rate', fontsize=12, fontweight='bold', color='black')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.2%}'.format(y)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.2%}'.format(y)))

    ax.set_xticks(x_positions)
    labels = []
    for i, (_, row) in enumerate(subject_data.iterrows()):
        if show_label_mask[i]:
            labels.append(row['Period_Label'])
        else:
            labels.append('')
    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=10)

    for cert in entities_to_plot:
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            nco_rate = entity_data['TTM_NCO_Rate'].fillna(0) / 100
            entity_show_labels = entity_data['Show_Label'].values if 'Show_Label' in entity_data.columns else [False] * len(entity_data)

            for i, rate in enumerate(nco_rate):
                if i < len(entity_show_labels) and entity_show_labels[i] and not np.isnan(rate) and rate > 0:
                    ax.text(bar_positions[cert][i], rate + 0.0005, f'{rate:.2%}',
                           ha='center', va='bottom', fontsize=8, fontweight='bold')

            if cert == subject_bank_cert:
                npl_rate = entity_data['NPL_to_Gross_Loans_Rate'].fillna(0) / 100
                for i, rate in enumerate(npl_rate):
                    if i < len(entity_show_labels) and entity_show_labels[i] and not np.isnan(rate) and rate > 0:
                        ax2.text(i, rate + 0.0005, f'{rate:.2%}',
                                ha='center', va='bottom', fontsize=8, fontweight='bold',
                                bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True, fancybox=True)

    title = 'Credit Deterioration Analysis\nTTM NCO Rate (bars) vs NPL-to-Book (lines)'
    if show_both_peer_groups:
        title += '\n(F&V = Flagstar & Valley excluded from green series)'
    plt.title(title, fontsize=14, fontweight='bold', pad=20)

    plt.tight_layout()
    ax.grid(True, alpha=0.3)
    ax2.grid(False)
    plt.subplots_adjust(bottom=0.15)

    return fig, ax


# ==================================================================================
# HTML TABLE GENERATION FUNCTIONS
# ==================================================================================
CURRENCY_MM_CODES = {"ASSET", "LNLS"}

def _fmt_money_millions(v: float, decimals: int = 0) -> str:
    """Return $X,XXXM using the value already in millions."""
    if pd.isna(v):
        return "N/A"
    fmt = f"${abs(v):,.{decimals}f}M" if decimals else f"${abs(v):,.0f}M"
    return fmt

def _fmt_money_millions_with_sign(diff: float, decimals: int = 0) -> str:
    """Return +/-$X,XXXM (sign before the $)."""
    if pd.isna(diff):
        return "N/A"
    sign = "+" if diff >= 0 else "-"
    base = _fmt_money_millions(abs(diff), decimals=decimals)
    return f"{sign}{base}"

def _fmt_percent_auto(v: float) -> str:
    """If value looks like decimal (e.g., 0.009) convert to %, otherwise assume already pct pts."""
    if pd.isna(v):
        return "N/A"
    try:
        f = float(v)
    except Exception:
        return "N/A"
    if abs(f) < 1.0:
        f *= 100.0
    return f"{f:.2f}%"


def _fmt_money_billions(v: float) -> str:
    """Format a value in FDIC $-thousands as a clean dollar string.

    FDIC data stores dollar amounts in thousands ($K).
    - 254706000 ($K) = $254.7 Billion → display as $254.7B
    - 1500000 ($K) = $1.5 Billion → display as $1.5B
    - 500000 ($K) = $500 Million → display as $500.0M
    - 1500 ($K) = $1.5 Million → display as $1.5M
    """
    if pd.isna(v):
        return "N/A"
    v = float(v)
    av = abs(v)
    if av >= 1e6:            # >= $1B (in $K units)
        return f"${v/1e6:,.1f}B"
    if av >= 1e3:            # >= $1M (in $K units)
        return f"${v/1e3:,.1f}M"
    return f"${v:,.0f}K"

def _fmt_money_billions_diff(v: float) -> str:
    """Format a diff value as +/-$X.XB."""
    if pd.isna(v):
        return "N/A"
    sign = "+" if v >= 0 else "-"
    base = _fmt_money_billions(abs(v))
    return f"{sign}{base}"

def _fmt_multiple(v: float) -> str:
    """Format a value as Xx multiplier."""
    if pd.isna(v):
        return "N/A"
    return f"{float(v):.2f}x"

def _fmt_multiple_diff(v: float) -> str:
    """Format a diff value as +/-Xx."""
    if pd.isna(v):
        return "N/A"
    return f"{float(v):+.2f}x"

# ---------------------------------------------------------------------------
# Metric format type registry — semantic formatting based on denominator type
# ---------------------------------------------------------------------------
# "x"   → x-multiple (NPL coverage: ACL / NPL, typically 0.5x-5x)
# "pct" → percent    (loan coverage: ACL / Loans, share: seg / total)
#
# MAINTENANCE RULE: Any NEW NPL coverage metric (i.e., denominator is a
# nonperforming-loan base like Nonaccrual or Past-Due) that should display
# as an x-multiple MUST be added here explicitly.  Metrics NOT listed
# default to percent formatting.  Do NOT add loan-coverage or share metrics
# to x-format — only NPL/nonaccrual coverage ratios belong here.
# ---------------------------------------------------------------------------
_METRIC_FORMAT_TYPE: Dict[str, str] = {
    # NPL coverage metrics: display as x-multiples (ACL / Nonaccrual)
    "RIC_CRE_Risk_Adj_Coverage": "x",        # CRE ACL / CRE Nonaccrual
    "RIC_Resi_Risk_Adj_Coverage": "x",        # Resi ACL / Resi Nonaccrual
    "RIC_Comm_Risk_Adj_Coverage": "x",        # C&I ACL / C&I Nonaccrual
    # Everything else (loan coverage, share, composition) → percent (default).
    # DO NOT add here: RIC_CRE_ACL_Coverage, RIC_Resi_ACL_Coverage (loan coverage → %),
    #   RIC_CRE_ACL_Share, RIC_Resi_ACL_Share, Norm_CRE_ACL_Share (share of ACL → %).
}

def _fmt_percent(v: float) -> str:
    """Format a value as percentage. Auto-detects decimal vs pct-point scale."""
    if pd.isna(v):
        return "N/A"
    v = float(v)
    if abs(v) < 1.0:
        v *= 100.0
    return f"{v:.2f}%"


def _fmt_percent_diff(diff: float, ref_value: float = None) -> str:
    """Format a percentage-point delta.

    Uses ``ref_value`` (one of the original source values) to decide scale:
    - If ref_value is in decimal form (abs < 1.0), multiply diff by 100.
    - If ref_value is already in pct-point form (abs >= 1.0), display diff as-is.
    This prevents 0.75 ppt deltas from being inflated to 75%.
    """
    if pd.isna(diff):
        return "N/A"
    diff = float(diff)
    if ref_value is not None and not pd.isna(ref_value):
        if abs(float(ref_value)) < 1.0:
            diff *= 100.0
    else:
        # Fallback: if no ref_value, use the same heuristic as _fmt_percent
        if abs(diff) < 1.0:
            diff *= 100.0
    return f"{diff:+.2f}%"

def _fmt_call_report_date(d) -> str:
    """Format a report date for display in table headers."""
    if hasattr(d, 'strftime'):
        return d.strftime('%B %d, %Y')
    return str(d)


def generate_html_email_table_dynamic(df: pd.DataFrame, report_date: datetime,
                                      table_type: str, title: Optional[str] = None,
                                      is_normalized: bool = False) -> str:
    cols = df.columns.tolist()
    date_str = _fmt_call_report_date(report_date)

    if title is None:
        title = "Executive Credit Summary" if table_type == "summary" else "Detailed Peer Analysis"
    max_width = "1200px" if table_type == "summary" else "1600px"

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{
            background-color: transparent; padding: 20px; max-width: {max_width}; margin: 0 auto; text-align: center;
        }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 0 auto; font-size: 11px; background-color: transparent; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; background-color: transparent; }}

        .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 180px; background-color: transparent; }}
        .subject-value {{ background-color: #E6F3FF !important; font-weight: bold; color: #002F6C; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .msbna-value {{ background-color: rgba(76, 120, 168, 0.08) !important; font-weight: 600; color: #444; }}

        .bad-trend {{ color: #d32f2f; font-weight: bold; }}
        .good-trend {{ color: #388e3c; font-weight: bold; }}
        .neutral-trend {{ color: #757575; font-weight: bold; }}

        .footnote {{ font-size: 10px; color: #666; margin-top: 20px; border-top: 1px solid #ccc; padding-top: 10px; text-align: left; }}
    </style></head><body>

    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{date_str}</p>
        <table><thead><tr>"""

    for c in cols: html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"

    for _, row in df.iterrows():
        metric = row['Metric']
        html += "<tr>"
        for c in cols:
            val = row[c]
            cls = ""
            if c == "Metric": val = f"<b>{val}</b>"; cls = 'class="metric-name"'
            elif c == "MSPBNA": cls = 'class="subject-value"'
            elif c == "MSBNA": cls = 'class="msbna-value"'

            # Trend Coloring Logic based on original files
            if "Diff" in c and "N/A" not in str(val):
                try:
                    num = float(str(val).replace('+','').replace('%','').replace('x','').replace('$','').replace('B','').replace(',',''))
                    if abs(num) < 0.001:
                        cls = 'class="neutral-trend"'
                    else:
                        is_risk = any(k in metric for k in ['Nonaccrual', 'NCO', 'Delinq', 'Risk'])
                        is_safe = any(k in metric for k in ['Coverage', 'Ratio', 'Equity', 'ROA', 'ROE', 'Yield', 'Margin'])
                        if is_risk: cls = 'class="bad-trend"' if num > 0 else 'class="good-trend"'
                        elif is_safe: cls = 'class="good-trend"' if num > 0 else 'class="bad-trend"'
                        else: cls = 'class="neutral-trend"'
                except: pass

            html += f"<td {cls}>{val}</td>"
        html += "</tr>"

    norm_footnote = ""
    if is_normalized:
        norm_footnote = '<p style="font-size: 10px; color: #555; text-align: left;">* Normalized for comparison</p>'
    html += f"""</tbody></table>
    {norm_footnote}
    <div class="footnote">
        <p><b>Methodology:</b></p>
        <p><b>Risk-Adj ACL Ratio:</b> Total ACL / (Gross Loans - SBL). Removes low-risk SBL to show coverage on core credit.</p>
        <p><b>CRE NPL Coverage:</b> CRE-Specific ACL / CRE Nonaccrual Loans.</p>
    </div></div></body></html>"""

    return html


def generate_credit_metrics_email_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    is_normalized: bool = False,
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Wealth-focused executive summary table.
    Columns: Metric | MSPBNA | <peer tickers> | Wealth Peers | Delta MSPBNA vs Wealth Peers

    Individual banks use ticker symbols via resolve_display_label().
    'Wealth Peers' = Core PB composite (90001 standard, 90004 normalized).
    Does NOT include MS or All Peers — those belong in the detailed peer table.
    """
    if is_normalized:
        metric_map = {
            "ASSET": ("Total Assets ($B)", 'B'),
            "Norm_Gross_Loans": ("Gross Loans ($B)*", 'B'),
            "Norm_Risk_Adj_Allowance_Coverage": ("Risk-Adj ACL Ratio (%)*", '%'),
            "Norm_ACL_Coverage": ("ACL Ratio (%)*", '%'),
            "Norm_Nonaccrual_Rate": ("Nonaccrual Rate (%)*", '%'),
            "Norm_NCO_Rate": ("NCO Rate (%)*", '%'),
            "Norm_Delinquency_Rate": ("Delinquency Rate (%)*", '%'),
            "Norm_SBL_Composition": ("SBL % of Loans*", '%'),
            "Norm_Wealth_Resi_Composition": ("Wealth Resi %*", '%'),
            "Norm_CRE_Investment_Composition": ("CRE % of Loans*", '%'),
            "Norm_CRE_ACL_Share": ("CRE % of ACL*", '%'),
            "Norm_CRE_ACL_Coverage": ("CRE ACL Ratio (%)*", '%'),
            "Norm_Exclusion_Pct": ("Excluded Loans (%)*", '%'),
            "Norm_Loan_Yield": ("Loan Yield (%)*", '%'),
            "Norm_Loss_Adj_Yield": ("Loss-Adj Yield (%)*", '%'),
        }
    else:
        metric_map = {
            "ASSET": ("Total Assets ($B)", 'B'),
            "LNLS": ("Total Loans ($B)", 'B'),
            "Risk_Adj_Allowance_Coverage": ("Risk-Adj ACL Ratio (%)", '%'),
            "Allowance_to_Gross_Loans_Rate": ("Headline ACL Ratio (%)", '%'),
            "Nonaccrual_to_Gross_Loans_Rate": ("Nonaccrual Rate (%)", '%'),
            "TTM_NCO_Rate": ("NCO Rate (TTM) (%)", '%'),
            "Past_Due_Rate": ("Delinquency Rate (%)", '%'),
            "SBL_Composition": ("SBL % of Loans", '%'),
            "RIC_Resi_Loan_Share": ("Resi % of Loans", '%'),
            "RIC_CRE_Loan_Share": ("CRE % of Loans", '%'),
            "RIC_CRE_ACL_Share": ("CRE % of ACL", '%'),
            "RIC_CRE_ACL_Coverage": ("CRE ACL Ratio (%)", '%'),
            "RIC_CRE_Risk_Adj_Coverage": ("CRE NPL Coverage (x)", 'x'),
            "RIC_CRE_Nonaccrual_Rate": ("% of CRE in Nonaccrual", '%'),
            "RIC_CRE_NCO_Rate": ("CRE NCO Rate (TTM)", '%'),
            "Liquidity_Ratio": ("Liquidity Ratio (%)", '%'),
            "HQLA_Ratio": ("HQLA Ratio (%)", '%'),
            "Loans_to_Deposits": ("Loans to Deposits (%)", '%'),
            "ROA": ("ROA (%)", '%'),
            "ROE": ("ROE (%)", '%'),
            "NIMY": ("Net Interest Margin (%)", '%'),
            "EEFFR": ("Efficiency Ratio (%)", '%'),
            "Loan_Yield_Proxy": ("Loan Yield (%)", '%'),
            "Provision_to_Loans_Rate": ("Provision Rate (%)", '%'),
        }

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Wealth Peers = Core PB composite (90001 standard, 90004 normalized)
    wealth_peers_cert = ACTIVE_NORMALIZED_COMPOSITES["core_pb"] if is_normalized else ACTIVE_STANDARD_COMPOSITES["core_pb"]

    # Identify wealth peer constituents (Goldman, UBS) via resolver
    peer_certs = []  # (cert, label)
    for c in latest_data["CERT"].unique():
        if c in ALL_COMPOSITE_CERTS or c == subject_bank_cert:
            continue
        raw_name = str(latest_data[latest_data["CERT"] == c]["NAME"].iloc[0])
        label = resolve_display_label(c, raw_name, subject_cert=subject_bank_cert)
        if label in ("GS", "UBS"):
            peer_certs.append((c, label))

    try:
        subj = latest_data[latest_data["CERT"] == subject_bank_cert].iloc[0]
    except IndexError:
        return None, None

    def _safe_row(cert):
        if cert is None:
            return None
        sl = latest_data[latest_data["CERT"] == cert]
        return sl.iloc[0] if not sl.empty else None

    # Build ordered column map: MSPBNA, peer tickers, Wealth Peers
    col_entities = [("MSPBNA", subj)]
    for pc, lbl in sorted(peer_certs, key=lambda x: x[1]):
        col_entities.append((lbl, _safe_row(pc)))
    wealth_label = resolve_display_label(wealth_peers_cert)
    col_entities.append((wealth_label, _safe_row(wealth_peers_cert)))

    rows = []
    for code, (disp, fmt) in metric_map.items():
        if code not in subj.index:
            continue

        vals = {}
        for lbl, entity_row in col_entities:
            vals[lbl] = entity_row.get(code, np.nan) if entity_row is not None else np.nan

        # Dead-metric suppression
        if all(pd.isna(v) or (isinstance(v, (int, float)) and abs(v) < 1e-12) for v in vals.values()):
            continue

        v_subj = vals["MSPBNA"]
        v_wealth = vals[wealth_label]
        delta = v_subj - v_wealth if pd.notna(v_subj) and pd.notna(v_wealth) else np.nan

        row = {"Metric": disp}
        for lbl, _ in col_entities:
            v = vals[lbl]
            if fmt == 'B':
                row[lbl] = _fmt_money_billions(v)
            elif fmt == 'x':
                row[lbl] = _fmt_multiple(v)
            else:
                row[lbl] = _fmt_percent(v)

        if fmt == 'B':
            row["Delta MSPBNA vs Wealth Peers"] = _fmt_money_billions_diff(delta)
        elif fmt == 'x':
            row["Delta MSPBNA vs Wealth Peers"] = _fmt_multiple_diff(delta)
        else:
            row["Delta MSPBNA vs Wealth Peers"] = _fmt_percent_diff(delta, v_subj)
        rows.append(row)

    df = pd.DataFrame(rows)
    norm_str = "Normalized" if is_normalized else "Standard"
    html = generate_html_email_table_dynamic(df, latest_date, table_type="summary",
                                             title=f"Executive Summary ({norm_str})",
                                             is_normalized=is_normalized)
    return html, df


def generate_flexible_html_table(
    proc_df_with_peers: pd.DataFrame,
    metrics_to_display: Dict[str, str],
    title: str,
    subject_bank_cert: int,
    peer_certs: Optional[List[int]] = None,
    col_names: Optional[Dict[str, str]] = None,
    ms_combined_cert: Optional[int] = None,
    report_view: str = "ALL_BANKS",
) -> str:
    """
    Generates a flexible HTML table with user-defined metrics and styling.
    """
    if proc_df_with_peers.empty or not metrics_to_display:
        return "<p>No data or metrics provided for table generation.</p>"

    latest_date = proc_df_with_peers['REPDTE'].max()
    latest_data = proc_df_with_peers[proc_df_with_peers['REPDTE'] == latest_date]

    if peer_certs is None:
        peer_certs = [ACTIVE_STANDARD_COMPOSITES["core_pb"], ACTIVE_STANDARD_COMPOSITES["all_peers"]]

    certs_to_include = [subject_bank_cert] + list(peer_certs)

    # Exclude MS_COMBINED_CERT from All Banks listings when REPORT_VIEW == MSPBNA_WEALTH_NORM
    if ms_combined_cert is not None and report_view == "MSPBNA_WEALTH_NORM":
        certs_to_include = [c for c in certs_to_include if c != ms_combined_cert]

    if col_names is None:
        col_names = {
            'Latest': 'Latest Value',
            '8Q_Avg': '8-Qtr Average'
        }

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 0; background-color: #f3f4f6; }}
            .email-container {{
                background-color: white;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                max-width: 1200px;
                margin: 20px auto;
            }}
            .header {{ text-align: center; margin-bottom: 20px; }}
            .header h2 {{ margin: 0; font-size: 20px; color: #2d3748; }}
            .header p {{ margin: 5px 0 0 0; color: #718096; font-size: 12px; }}
            table {{
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                overflow: hidden;
                border-radius: 8px;
            }}
            .table-header {{ background-color: #f7a81b; color: #ffffff; font-weight: bold; }}
            .sub-header {{ background-color: #f7a81b; color: #ffffff; font-weight: bold; text-align: center; }}
            th, td {{ padding: 12px 15px; text-align: left; border: 1px solid #e2e8f0; }}
            th {{ font-size: 14px; text-transform: uppercase; letter-spacing: 0.05em; }}
            .metric-name {{ font-weight: normal; color: #2d3748; }}
            .value-cell {{ text-align: center; }}
            tr:nth-child(even) {{ background-color: #f7fafc; }}
            tr:hover {{ background-color: #edf2f7; }}
            .mspbna-row {{ background-color: #fff3cd; font-weight: 600; }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div class="header">
                <h2>{title}</h2>
                <p>Report Date: {latest_date.strftime('%B %d, %Y')}</p>
            </div>
            <table>
                <thead>
                    <tr class="table-header">
                        <th>Bank Name</th>
                        <th>HQ State</th>
    """

    for display_name in metrics_to_display.values():
        html += f'<th class="value-cell">{display_name}</th>'

    html += """
                    </tr>
                </thead>
                <tbody>
    """

    for cert in certs_to_include:
        latest_row = latest_data[latest_data['CERT'] == cert]
        if latest_row.empty:
            continue

        latest_row = latest_row.iloc[0]
        name = latest_row.get('NAME', 'N/A')
        hq = latest_row.get('HQ_STATE', 'N/A')

        row_class = "mspbna-row" if cert == subject_bank_cert else ""

        html += f"""
                    <tr class="{row_class}">
                        <td class="metric-name">{name}</td>
                        <td>{hq}</td>
        """

        for metric_code in metrics_to_display.keys():
            value = latest_row.get(metric_code, np.nan)
            formatted_value = f"{value:.2f}%" if not pd.isna(value) else "N/A"
            html += f'<td class="value-cell">{formatted_value}</td>'

        html += "</tr>"

    html += """
                </tbody>
            </table>
        </div>
    </body>
    </html>
    """
    return html


# ==================================================================================
# DETAILED / LEGACY HTML TABLE GENERATORS
# ==================================================================================

_TABLE_CSS = """
<style>
body{font-family:Arial,sans-serif;margin:20px;background:#f5f5f5}
.tbl-container{background:#fff;padding:24px;border-radius:8px;box-shadow:0 2px 4px rgba(0,0,0,.1);max-width:1300px;margin:0 auto}
.tbl-container h2{text-align:center;color:#2c3e50;margin:0 0 4px}
.tbl-container p.sub{text-align:center;color:#7f8c8d;font-size:13px;margin:0 0 18px}
table{width:100%;border-collapse:collapse;font-size:12px}
th{background:#34495e;color:#fff;padding:10px 8px;text-align:center;border:1px solid #2c3e50}
td{padding:8px;text-align:center;border:1px solid #bdc3c7}
tr:nth-child(even){background:#f8f9fa}
tr:hover{background:#e3f2fd}
.metric-name{text-align:left!important;font-weight:500;color:#2c3e50}
.mspbna-value{background:#fff3cd;font-weight:600;color:#856404}
.mspbna-row{background:#fff3cd;font-weight:600}
.good-trend{color:#388e3c;font-weight:600}
.bad-trend{color:#d32f2f;font-weight:600}
.neutral{color:#5d4037}
</style>
"""


def _safe_val(row, col, default=np.nan):
    """Safely retrieve a value from a Series, returning default if missing."""
    if col in row.index:
        return row[col]
    return default


def _trend_class(diff_val) -> str:
    """Return CSS class for a numeric diff: negative is good (green), positive is bad (red)."""
    if pd.isna(diff_val):
        return "neutral"
    try:
        v = float(diff_val)
        if v < -0.005:
            return "good-trend"
        elif v > 0.005:
            return "bad-trend"
        return "neutral"
    except (ValueError, TypeError):
        return "neutral"


def generate_detailed_peer_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    is_normalized: bool = False,
) -> Optional[str]:
    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date].copy()

    msbna_cert = _MSBNA_CERT

    # 1. Individual peers excluding Subject, MSBNA, and Composites
    individual_peer_certs = [
        c for c in latest_data["CERT"].unique()
        if c not in ALL_COMPOSITE_CERTS and c not in (subject_bank_cert, msbna_cert)
    ]

    col_mapping = {
        subject_bank_cert: resolve_display_label(subject_bank_cert, subject_cert=subject_bank_cert),
        msbna_cert: resolve_display_label(msbna_cert),
    }
    for c in individual_peer_certs:
        raw_name = str(latest_data[latest_data["CERT"] == c]["NAME"].iloc[0])
        col_mapping[c] = resolve_display_label(c, raw_name, subject_cert=subject_bank_cert)

    peer_avg_cert = ACTIVE_NORMALIZED_COMPOSITES["all_peers"] if is_normalized else ACTIVE_STANDARD_COMPOSITES["all_peers"]
    col_mapping[peer_avg_cert] = resolve_display_label(peer_avg_cert)

    # ORDER: MSPBNA, MSBNA on the LEFT
    ordered_certs = [subject_bank_cert, msbna_cert] + individual_peer_certs + [peer_avg_cert]

    if is_normalized:
        title = "Detailed Peer Analysis (Normalized)"
        metrics = [
            ("Total Assets ($B)", "ASSET", "B"),
            ("Norm Gross Loans ($B)", "Norm_Gross_Loans", "B"),
            ("Norm Risk-Adj ACL Ratio (%)", "Norm_Risk_Adj_Allowance_Coverage", "%"),
            ("Norm ACL Ratio (%)", "Norm_ACL_Coverage", "%"),
            ("Norm Nonaccrual Rate (%)", "Norm_Nonaccrual_Rate", "%"),
            ("Norm NCO Rate (%)", "Norm_NCO_Rate", "%"),
            ("Norm SBL % of Loans", "Norm_SBL_Composition", "%"),
            ("Norm Resi % of Loans", "Norm_Wealth_Resi_Composition", "%"),
            ("Norm CRE % of Loans", "Norm_CRE_Investment_Composition", "%"),
            ("Norm CRE % of ACL", "Norm_CRE_ACL_Share", "%"),
            ("Norm CRE ACL Ratio (%)", "Norm_CRE_ACL_Coverage", "%"),
        ]
    else:
        title = "Detailed Peer Analysis (Standard)"
        metrics = [
            ("Total Assets ($B)", "ASSET", "B"),
            ("Total Loans ($B)", "LNLS", "B"),
            ("Risk-Adj ACL Ratio (%)", "Risk_Adj_Allowance_Coverage", "%"),
            ("Headline ACL Ratio (%)", "Allowance_to_Gross_Loans_Rate", "%"),
            ("Nonaccrual Rate (%)", "Nonaccrual_to_Gross_Loans_Rate", "%"),
            ("NCO Rate (TTM) (%)", "TTM_NCO_Rate", "%"),
            ("SBL % of Loans", "SBL_Composition", "%"),
            ("Resi % of Loans", "RIC_Resi_Loan_Share", "%"),
            ("CRE % of Loans", "RIC_CRE_Loan_Share", "%"),
            ("CRE % of ACL", "RIC_CRE_ACL_Share", "%"),
            ("CRE ACL Ratio (%)", "RIC_CRE_ACL_Coverage", "%"),
            ("CRE NPL Coverage (x)", "RIC_CRE_Risk_Adj_Coverage", "x"),
            ("% of CRE in Nonaccrual", "RIC_CRE_Nonaccrual_Rate", "%"),
            ("CRE NCO Rate (TTM)", "RIC_CRE_NCO_Rate", "%"),
        ]

    return _build_dynamic_peer_html(
        title, ordered_certs, col_mapping, metrics,
        latest_data, subject_bank_cert, peer_avg_cert, latest_date,
        is_normalized=is_normalized,
    )


def generate_core_pb_peer_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    is_normalized: bool = False,
) -> Optional[str]:
    """
    Wealth-focused peer table: MSPBNA | <peer tickers> | Wealth Peers.
    No MS. Uses resolve_display_label() for ticker-style names.
    'Wealth Peers' = Core PB composite (90001 std, 90004 norm).
    """
    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date].copy()

    # Identify Core PB constituents via resolver
    core_pb_certs = []
    col_mapping = {subject_bank_cert: resolve_display_label(subject_bank_cert, subject_cert=subject_bank_cert)}
    for c in latest_data["CERT"].unique():
        if c in ALL_COMPOSITE_CERTS or c == subject_bank_cert:
            continue
        raw_name = str(latest_data[latest_data["CERT"] == c]["NAME"].iloc[0])
        label = resolve_display_label(c, raw_name, subject_cert=subject_bank_cert)
        if label in ("GS", "UBS"):
            core_pb_certs.append(c)
            col_mapping[c] = label

    peer_avg_cert = ACTIVE_NORMALIZED_COMPOSITES["core_pb"] if is_normalized else ACTIVE_STANDARD_COMPOSITES["core_pb"]
    col_mapping[peer_avg_cert] = resolve_display_label(peer_avg_cert)

    # ORDER: MSPBNA, peer tickers, Wealth Peers (no MS)
    ordered_certs = [subject_bank_cert] + core_pb_certs + [peer_avg_cert]

    if is_normalized:
        title = "Wealth Peer Analysis (Normalized)"
        metrics = [
            ("Total Assets ($B)", "ASSET", "B"),
            ("Norm Gross Loans ($B)", "Norm_Gross_Loans", "B"),
            ("Norm Risk-Adj ACL Ratio (%)", "Norm_Risk_Adj_Allowance_Coverage", "%"),
            ("Norm ACL Ratio (%)", "Norm_ACL_Coverage", "%"),
            ("Norm Nonaccrual Rate (%)", "Norm_Nonaccrual_Rate", "%"),
            ("Norm NCO Rate (%)", "Norm_NCO_Rate", "%"),
            ("Norm SBL % of Loans", "Norm_SBL_Composition", "%"),
            ("Norm Resi % of Loans", "Norm_Wealth_Resi_Composition", "%"),
            ("Norm CRE % of Loans", "Norm_CRE_Investment_Composition", "%"),
            ("Norm CRE % of ACL", "Norm_CRE_ACL_Share", "%"),
            ("Norm CRE ACL Ratio (%)", "Norm_CRE_ACL_Coverage", "%"),
        ]
    else:
        title = "Wealth Peer Analysis (Standard)"
        metrics = [
            ("Total Assets ($B)", "ASSET", "B"),
            ("Total Loans ($B)", "LNLS", "B"),
            ("Risk-Adj ACL Ratio (%)", "Risk_Adj_Allowance_Coverage", "%"),
            ("Headline ACL Ratio (%)", "Allowance_to_Gross_Loans_Rate", "%"),
            ("Nonaccrual Rate (%)", "Nonaccrual_to_Gross_Loans_Rate", "%"),
            ("NCO Rate (TTM) (%)", "TTM_NCO_Rate", "%"),
            ("SBL % of Loans", "SBL_Composition", "%"),
            ("Resi % of Loans", "RIC_Resi_Loan_Share", "%"),
            ("CRE % of Loans", "RIC_CRE_Loan_Share", "%"),
            ("CRE % of ACL", "RIC_CRE_ACL_Share", "%"),
            ("CRE ACL Ratio (%)", "RIC_CRE_ACL_Coverage", "%"),
            ("CRE NPL Coverage (x)", "RIC_CRE_Risk_Adj_Coverage", "x"),
            ("% of CRE in Nonaccrual", "RIC_CRE_Nonaccrual_Rate", "%"),
            ("CRE NCO Rate (TTM)", "RIC_CRE_NCO_Rate", "%"),
        ]

    return _build_dynamic_peer_html(
        title, ordered_certs, col_mapping, metrics,
        latest_data, subject_bank_cert, peer_avg_cert, latest_date,
        is_normalized=is_normalized,
    )


def _normalize_display_name(disp: str) -> str:
    """Replace 'Norm ' prefix with trailing asterisk for normalized metrics."""
    if disp.startswith("Norm "):
        return disp[5:] + "*"
    return disp


def _build_dynamic_peer_html(title, ordered_certs, col_mapping, metrics,
                             latest_data, subject_cert, avg_cert, latest_date,
                             is_normalized: bool = False):
    """Shared HTML builder for detailed peer and core PB tables."""
    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{ background-color: transparent; padding: 20px; max-width: 1600px; margin: 0 auto; text-align: center; }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 0 auto; font-size: 11px; background-color: transparent; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; background-color: transparent; }}
        .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 180px; background-color: transparent; }}
        .subject-value {{ background-color: #E6F3FF !important; font-weight: bold; color: #002F6C; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .msbna-value {{ background-color: #F4F7F9 !important; font-weight: 600; color: #444; }}
        .bad-trend {{ color: #d32f2f; font-weight: bold; }}
        .good-trend {{ color: #388e3c; font-weight: bold; }}
        .neutral-trend {{ color: #757575; font-weight: bold; }}
    </style></head><body>
    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{latest_date.strftime('%B %d, %Y')}</p>
        <table><thead><tr><th>Metric</th>"""

    for c in ordered_certs:
        html += f"<th>{col_mapping[c]}</th>"
    html += f"<th>Diff Vs {col_mapping[avg_cert]}</th></tr></thead><tbody>"

    for disp, code, fmt in metrics:
        # Normalize display: Norm prefix → trailing asterisk
        disp_clean = _normalize_display_name(disp) if is_normalized else disp
        html += f'<tr><td class="metric-name"><b>{disp_clean}</b></td>'
        subj_val, avg_val = np.nan, np.nan

        for c in ordered_certs:
            row_slice = latest_data[latest_data["CERT"] == c]
            val = row_slice.iloc[0].get(code, np.nan) if not row_slice.empty else np.nan
            if c == subject_cert:
                subj_val = val
            if c == avg_cert:
                avg_val = val

            if pd.isna(val):
                f_v = "N/A"
            elif fmt == "x":
                f_v = f"{val:.2f}x"
            elif fmt == "B":
                f_v = f"${val / 1e6:,.1f}B"
            else:
                f_v = f"{val * 100:.2f}%" if abs(val) < 1.0 else f"{val:.2f}%"

            cls = ('class="subject-value"' if c == subject_cert
                   else ('class="msbna-value"' if c == _MSBNA_CERT else ''))
            html += f"<td {cls}>{f_v}</td>"

        diff = subj_val - avg_val if pd.notna(subj_val) and pd.notna(avg_val) else np.nan
        if pd.isna(diff):
            f_diff, diff_cls = "N/A", "neutral-trend"
        else:
            if fmt == "x":
                f_diff = f"{diff:+.2f}x"
            elif fmt == "B":
                f_diff = f"{diff / 1e6:+,.1f}B"
            else:
                f_diff = f"{diff * 100:+.2f}%" if abs(diff) < 1.0 else f"{diff:+.2f}%"

            is_risk = any(k in disp for k in ['Nonaccrual', 'NCO', 'Delinq', 'Risk', 'Past Due'])
            is_safe = any(k in disp for k in ['Coverage', 'Ratio', 'Equity', 'ROA', 'ROE', 'Yield', 'Margin', 'Assets'])

            if abs(diff) < 0.001:
                diff_cls = "neutral-trend"
            elif is_risk:
                diff_cls = "bad-trend" if diff > 0 else "good-trend"
            elif is_safe:
                diff_cls = "good-trend" if diff > 0 else "bad-trend"
            else:
                diff_cls = "neutral-trend"

        html += f'<td class="{diff_cls}">{f_diff}</td></tr>'

    footnote = ""
    if is_normalized:
        footnote = '<p style="font-size: 10px; color: #555; text-align: left;">* Normalized for comparison</p>'
    html += f"</tbody></table>{footnote}</div></body></html>"
    return html


def generate_normalized_comparison_table(
    df: pd.DataFrame,
    subject_cert: int,
) -> Optional[str]:
    """
    DEPRECATED — This function produced a mixed standard-vs-normalized side-by-side
    table, which violates the single-regime-per-artifact rule. Use the separate
    standard and normalized table generators instead:
      - generate_executive_summary (is_normalized=False / True)
      - generate_detailed_peer_table (is_normalized=False / True)
      - generate_core_pb_peer_table (is_normalized=False / True)

    Retained as a compatibility stub. Will be removed in a future release.

    Raises
    ------
    NotImplementedError
        Always. This function is deprecated and must not be called.
    """
    raise NotImplementedError(
        "generate_normalized_comparison_table() is deprecated. "
        "Use separate standard/normalized table generators instead. "
        "See report_generator.py docstring for alternatives."
    )


def generate_ratio_components_table(proc_df_with_peers: pd.DataFrame,
                                    subject_bank_cert: int = 34221,
                                    is_normalized: bool = False) -> Optional[str]:
    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date].copy()

    # Synthesize _Total_Past_Due for standard delinquency row
    if 'TopHouse_PD30' in latest_data.columns and 'TopHouse_PD90' in latest_data.columns:
        latest_data['_Total_Past_Due'] = latest_data['TopHouse_PD30'].fillna(0) + latest_data['TopHouse_PD90'].fillna(0)
    elif '_Total_Past_Due' not in latest_data.columns:
        latest_data['_Total_Past_Due'] = 0.0

    try:
        subj = latest_data[latest_data["CERT"] == subject_bank_cert].iloc[0]
    except IndexError:
        return None  # Subject bank is strictly required

    # Select the appropriate peer composite based on normalized vs standard mode
    peer_cert = ACTIVE_NORMALIZED_COMPOSITES["all_peers"] if is_normalized else ACTIVE_STANDARD_COMPOSITES["all_peers"]
    peer_slice = latest_data[latest_data["CERT"] == peer_cert]
    peer = peer_slice.iloc[0] if not peer_slice.empty else pd.Series(dtype=float)

    # Synthesize _Norm_Total_Past_Due for normalized delinquency row
    if 'Norm_PD30' in latest_data.columns and 'Norm_PD90' in latest_data.columns:
        latest_data = latest_data.copy()
        latest_data['_Norm_Total_Past_Due'] = latest_data['Norm_PD30'].fillna(0) + latest_data['Norm_PD90'].fillna(0)
    if '_Norm_Total_Past_Due' not in latest_data.columns:
        latest_data = latest_data.copy()
        latest_data['_Norm_Total_Past_Due'] = 0.0

    # Re-pick subject/peer after adding synthetic column
    try:
        subj = latest_data[latest_data["CERT"] == subject_bank_cert].iloc[0]
    except IndexError:
        return None
    peer_slice = latest_data[latest_data["CERT"] == peer_cert]
    peer = peer_slice.iloc[0] if not peer_slice.empty else pd.Series(dtype=float)

    # Pre-compute synthetic columns that don't exist in the upstream DataFrame
    # but are needed for the ratio components numerator/denominator display.
    def _synth(row):
        """Add computed intermediates to a Series for display purposes."""
        r = row.copy()
        # Risk-Adj denominator (Gross Loans minus SBL)
        gl = r.get('Gross_Loans', r.get('LNLS', np.nan))
        sbl = r.get('SBL_Balance', 0) if pd.notna(r.get('SBL_Balance', np.nan)) else 0
        r['_Risk_Adj_Gross_Loans'] = (gl - sbl) if pd.notna(gl) else np.nan
        # Normalized Risk-Adj denominator (Norm Gross Loans minus SBL)
        norm_gl = r.get('Norm_Gross_Loans', np.nan)
        r['Norm_Risk_Adj_Gross_Loans'] = (norm_gl - sbl) if pd.notna(norm_gl) else np.nan
        # Total Past Due (TopHouse PD30 + PD90)
        pd30 = r.get('TopHouse_PD30', r.get('P3LNLS', 0))
        pd90 = r.get('TopHouse_PD90', r.get('P9LNLS', 0))
        pd30 = pd30 if pd.notna(pd30) else 0
        pd90 = pd90 if pd.notna(pd90) else 0
        r['_Total_Past_Due'] = pd30 + pd90
        # CRE delinquency numerator
        r['_RIC_CRE_Delinq'] = (r.get('RIC_CRE_PD30', 0) or 0) + (r.get('RIC_CRE_PD90', 0) or 0)
        # Resi delinquency numerator
        r['_RIC_Resi_Delinq'] = (r.get('RIC_Resi_PD30', 0) or 0) + (r.get('RIC_Resi_PD90', 0) or 0)
        return r

    subj = _synth(subj)
    peer = _synth(peer)

    if is_normalized:
        title = "Ratio Components Analysis (Normalized)"
        metrics = [
            ("NCO Rate*", "Total NCO*", "Norm_Total_NCO", "Gross Loans*", "Norm_Gross_Loans", "Norm_NCO_Rate"),
            ("Nonaccrual Rate*", "Total Nonaccrual*", "Norm_Total_Nonaccrual", "Gross Loans*", "Norm_Gross_Loans", "Norm_Nonaccrual_Rate"),
            ("ACL Ratio*", "ACL Balance*", "Norm_ACL_Balance", "Gross Loans*", "Norm_Gross_Loans", "Norm_ACL_Coverage"),
            ("Risk-Adj ACL*", "ACL Balance*", "Norm_ACL_Balance", "Gross Loans* - SBL", "Norm_Risk_Adj_Gross_Loans", "Norm_Risk_Adj_Allowance_Coverage"),
            ("Delinquency Rate*", "PD30 + PD90*", "_Norm_Total_Past_Due", "Gross Loans*", "Norm_Gross_Loans", "Norm_Delinquency_Rate"),
            ("SBL %*", "SBL Balance", "SBL_Balance", "Gross Loans*", "Norm_Gross_Loans", "Norm_SBL_Composition"),
            ("Wealth Resi %*", "Wealth Resi Bal.", "Wealth_Resi_Balance", "Gross Loans*", "Norm_Gross_Loans", "Norm_Wealth_Resi_Composition"),
            ("CRE % (Ex-ADC)*", "CRE Inv. Pure Bal.", "CRE_Investment_Pure_Balance", "Gross Loans*", "Norm_Gross_Loans", "Norm_CRE_Investment_Composition"),
            ("CRE % of ACL*", "CRE ACL", "RIC_CRE_ACL", "ACL Balance*", "Norm_ACL_Balance", "Norm_CRE_ACL_Share"),
            ("Resi ACL Coverage*", "Resi ACL", "RIC_Resi_ACL", "Wealth Resi Bal.", "Wealth_Resi_Balance", "Norm_Resi_ACL_Coverage"),
            ("Excluded % of Total*", "Excluded Balance", "Excluded_Balance", "Gross Loans", "LNLS", "Norm_Exclusion_Pct")
        ]
        footnote = '<p style="font-size: 10px; color: #555; text-align: left;">* Normalized for comparison</p><p><b>Normalized Metrics:</b> Exclude C&I, NDFI (Fund Finance), ADC (Construction), Credit Cards, Auto, Ag loans.</p>'
    else:
        title = "Ratio Components Analysis (Standard)"
        metrics = [
            ("NCO Rate (TTM)", "Total NCO TTM", "Total_NCO_TTM", "Gross Loans", "LNLS", "TTM_NCO_Rate"),
            ("Nonaccrual Rate", "Total Nonaccrual", "Total_Nonaccrual", "Gross Loans", "LNLS", "Nonaccrual_to_Gross_Loans_Rate"),
            ("Headline ACL Ratio", "Total ACL", "Total_ACL", "Gross Loans", "LNLS", "Allowance_to_Gross_Loans_Rate"),
            ("Risk-Adj ACL Ratio", "Total ACL", "Total_ACL", "Gross Loans - SBL Balance", "_Risk_Adj_Gross_Loans", "Risk_Adj_Allowance_Coverage"),
            ("Delinquency Rate (30+)", "TopHouse PD30 + TopHouse PD90", "_Total_Past_Due", "Gross Loans", "LNLS", "Past_Due_Rate"),
            ("SBL % of Loans", "SBL Balance", "SBL_Balance", "Gross Loans", "LNLS", "SBL_Composition"),
            ("Resi % of Loans", "Resi Cost Basis", "RIC_Resi_Cost", "Gross Loans", "LNLS", "RIC_Resi_Loan_Share"),
            ("CRE % of Loans", "CRE Inv. Pure Bal.", "CRE_Investment_Pure_Balance", "Gross Loans", "LNLS", "RIC_CRE_Loan_Share"),
            ("Fund Finance %", "Fund Finance Bal.", "Fund_Finance_Balance", "Gross Loans", "LNLS", "Fund_Finance_Composition"),
            ("CRE % of ACL", "CRE ACL", "RIC_CRE_ACL", "Total ACL", "Total_ACL", "RIC_CRE_ACL_Share"),
            ("CRE ACL Coverage", "CRE ACL", "RIC_CRE_ACL", "CRE Cost Basis", "RIC_CRE_Cost", "RIC_CRE_ACL_Coverage"),
            ("CRE NPL Coverage", "CRE ACL", "RIC_CRE_ACL", "CRE Nonaccrual", "RIC_CRE_Nonaccrual", "RIC_CRE_Risk_Adj_Coverage"),
            ("CRE Nonaccrual Rate", "CRE Nonaccrual", "RIC_CRE_Nonaccrual", "CRE Cost Basis", "RIC_CRE_Cost", "RIC_CRE_Nonaccrual_Rate"),
            ("CRE NCO Rate", "CRE NCO TTM", "RIC_CRE_NCO_TTM", "CRE Cost Basis", "RIC_CRE_Cost", "RIC_CRE_NCO_Rate"),
            ("CRE Delinquency Rate", "CRE PD30 + PD90", "_RIC_CRE_Delinq", "CRE Cost Basis", "RIC_CRE_Cost", "RIC_CRE_Delinquency_Rate"),
            ("Resi % of ACL", "Resi ACL", "RIC_Resi_ACL", "Total ACL", "Total_ACL", "RIC_Resi_ACL_Share"),
            ("Resi ACL Coverage", "Resi ACL", "RIC_Resi_ACL", "Resi Cost Basis", "RIC_Resi_Cost", "RIC_Resi_ACL_Coverage"),
            ("Resi NPL Coverage", "Resi ACL", "RIC_Resi_ACL", "Resi Nonaccrual", "RIC_Resi_Nonaccrual", "RIC_Resi_Risk_Adj_Coverage"),
            ("Resi Nonaccrual Rate", "Resi Nonaccrual", "RIC_Resi_Nonaccrual", "Resi Cost Basis", "RIC_Resi_Cost", "RIC_Resi_Nonaccrual_Rate"),
            ("Resi NCO Rate", "Resi NCO TTM", "RIC_Resi_NCO_TTM", "Resi Cost Basis", "RIC_Resi_Cost", "RIC_Resi_NCO_Rate"),
            ("Resi Delinquency Rate", "Resi PD30 + PD90", "_RIC_Resi_Delinq", "Resi Cost Basis", "RIC_Resi_Cost", "RIC_Resi_Delinquency_Rate")
        ]
        footnote = "<p>Standard metrics based on Call Report totals.</p>"

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{ background-color: transparent; padding: 20px; max-width: 1600px; margin: 0 auto; }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 10px; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; text-align: center; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; }}
        .ratio-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 150px; background-color: #f8f9fa; }}
        .formula-col {{ text-align: left !important; font-size: 9px; color: #666; font-style: italic; }}
        .value-col {{ background-color: rgba(0, 47, 108, 0.05); font-weight: 600; }}
        .ratio-col {{ background-color: rgba(0, 47, 108, 0.12); font-weight: bold; color: #002F6C; }}
        .peer-col {{ background-color: #FFF9E6; }}
    </style></head><body>
    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{latest_date.strftime('%B %d, %Y')}</p>
        <table><thead><tr>
            <th>Ratio Name</th><th>Numerator</th><th>MSPBNA Num ($)</th><th>Denominator</th><th>MSPBNA Denom ($)</th><th>MSPBNA Ratio</th><th>Peer Avg Ratio</th>
        </tr></thead><tbody>
    """

    for disp, num_lbl, num_col, den_lbl, den_col, rat_col in metrics:
        v_num = subj.get(num_col, np.nan)
        v_den = subj.get(den_col, np.nan)
        v_rat = subj.get(rat_col, np.nan)
        p_rat = peer.get(rat_col, np.nan)

        # Suppress flatlined metrics: skip if both ratios are zero/NaN
        if all(pd.isna(v) or (isinstance(v, (int, float)) and abs(v) < 1e-12) for v in [v_rat, p_rat]):
            continue

        f_num = "N/A" if pd.isna(v_num) else _fmt_money_billions(v_num)
        f_den = "N/A" if pd.isna(v_den) else _fmt_money_billions(v_den)
        # Format based on semantic type: NPL coverage → x-multiple, everything else → %
        fmt_type = _METRIC_FORMAT_TYPE.get(rat_col, "pct")
        if fmt_type == "x":
            f_rat = _fmt_multiple(v_rat)
            f_prat = _fmt_multiple(p_rat)
        else:
            f_rat = _fmt_percent(v_rat)
            f_prat = _fmt_percent(p_rat)

        html += f"""<tr>
            <td class="ratio-name">{disp}</td>
            <td class="formula-col">{num_lbl}</td>
            <td class="value-col">{f_num}</td>
            <td class="formula-col">{den_lbl}</td>
            <td class="value-col">{f_den}</td>
            <td class="ratio-col">{f_rat}</td>
            <td class="peer-col">{f_prat}</td>
        </tr>"""

    html += f"</tbody></table><div class=\"footnote\">{footnote}</div></div></body></html>"
    return html


def generate_segment_focus_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    segment_name: str = "CRE",
    is_normalized: bool = False,
) -> Optional[str]:
    """
    Wealth-focused segment table.
    Columns: Metric | MSPBNA | <peer tickers> | Wealth Peers | Delta vs Wealth Peers

    Uses resolve_display_label() for ticker-style names.
    'Wealth Peers' = Core PB composite (90001 standard, 90004 normalized).
    """
    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Wealth Peers = Core PB composite
    wealth_peers_cert = ACTIVE_NORMALIZED_COMPOSITES["core_pb"] if is_normalized else ACTIVE_STANDARD_COMPOSITES["core_pb"]

    # Identify wealth peer constituents via resolver
    col_certs = {"MSPBNA": subject_bank_cert}
    for c in latest_data["CERT"].unique():
        if c in ALL_COMPOSITE_CERTS or c == subject_bank_cert:
            continue
        raw_name = str(latest_data[latest_data["CERT"] == c]["NAME"].iloc[0])
        label = resolve_display_label(c, raw_name, subject_cert=subject_bank_cert)
        if label in ("GS", "UBS"):
            col_certs[label] = c
    col_certs[resolve_display_label(wealth_peers_cert)] = wealth_peers_cert

    row_data = {
        k: latest_data[latest_data["CERT"] == v].iloc[0]
        if v in latest_data["CERT"].values else pd.Series()
        for k, v in col_certs.items()
    }

    if is_normalized:
        title = f"{segment_name} Segment Analysis (Normalized)"
        common = [
            ("Norm_Gross_Loans", "Total Loans ($B)*", "B", False),
            ("Norm_SBL_Composition", "SBL % of Loans*", "%", False),
            ("Norm_Wealth_Resi_Composition", "Resi % of Loans*", "%", False),
            ("Norm_CRE_Investment_Composition", "CRE % of Loans*", "%", False),
            ("Norm_Risk_Adj_Allowance_Coverage", "Risk-Adj ACL Ratio (%)*", "%", True),
            ("Norm_Nonaccrual_Rate", "Nonaccrual Rate (%)*", "%", False),
            ("Norm_NCO_Rate", "NCO Rate (%)*", "%", False),
            ("Norm_Delinquency_Rate", "Delinquency Rate (%)*", "%", False),
        ]
        if segment_name == "CRE":
            metrics = common + [
                ("RIC_CRE_Cost", "CRE Balance ($B)", "B", False),
                ("Norm_CRE_ACL_Share", "CRE % of ACL*", "%", False),
                ("Norm_CRE_ACL_Coverage", "CRE ACL Ratio (%)*", "%", True),
                ("RIC_CRE_Risk_Adj_Coverage", "CRE NPL Coverage (x)", "x", True),
                ("RIC_CRE_Nonaccrual_Rate", "% of CRE in Nonaccrual", "%", False),
                ("RIC_CRE_NCO_Rate", "CRE NCO Rate (TTM)", "%", False),
                ("RIC_CRE_Delinquency_Rate", "CRE Delinquency Rate (%)", "%", False),
            ]
        else:
            metrics = common + [
                ("RIC_Resi_Cost", "Resi Balance ($B)*", "B", False),
                ("Norm_Resi_ACL_Share", "Resi % of ACL*", "%", False),
                ("Norm_Resi_ACL_Coverage", "Resi ACL Ratio (%)*", "%", True),
                ("RIC_Resi_Risk_Adj_Coverage", "Resi NPL Coverage (x)", "x", True),
                ("RIC_Resi_Nonaccrual_Rate", "% of Resi in Nonaccrual*", "%", False),
                ("RIC_Resi_NCO_Rate", "Resi NCO Rate (TTM)*", "%", False),
                ("RIC_Resi_Delinquency_Rate", "Resi Delinquency Rate (%)*", "%", False),
            ]
    else:
        title = f"{segment_name} Segment Analysis (Standard)"
        common = [
            ("LNLS", "Total Loans ($B)*", "B", False),
            ("SBL_Composition", "SBL % of Loans*", "%", False),
            ("RIC_Resi_Loan_Share", "Resi % of Loans*", "%", False),
            ("RIC_CRE_Loan_Share", "CRE % of Loans*", "%", False),
            ("Risk_Adj_Allowance_Coverage", "Risk-Adj ACL Ratio (%)*", "%", True),
            ("Nonaccrual_to_Gross_Loans_Rate", "Nonaccrual Rate (%)*", "%", False),
            ("TTM_NCO_Rate", "NCO Rate (TTM) (%)*", "%", False),
            ("Past_Due_Rate", "Delinquency Rate (%)*", "%", False),
        ]
        if segment_name == "CRE":
            metrics = common + [
                ("RIC_CRE_Cost", "CRE Balance ($B)", "B", False),
                ("RIC_CRE_ACL_Share", "CRE % of ACL*", "%", False),
                ("RIC_CRE_ACL_Coverage", "CRE ACL Ratio (%)*", "%", True),
                ("RIC_CRE_Risk_Adj_Coverage", "CRE NPL Coverage (x)", "x", True),
                ("RIC_CRE_Nonaccrual_Rate", "% of CRE in Nonaccrual", "%", False),
                ("RIC_CRE_NCO_Rate", "CRE NCO Rate (TTM)", "%", False),
                ("RIC_CRE_Delinquency_Rate", "CRE Delinquency Rate (%)", "%", False),
            ]
        else:
            metrics = common + [
                ("RIC_Resi_Cost", "Resi Balance ($B)*", "B", False),
                ("RIC_Resi_ACL_Share", "Resi % of ACL*", "%", False),
                ("RIC_Resi_ACL_Coverage", "Resi ACL Ratio (%)*", "%", True),
                ("RIC_Resi_Risk_Adj_Coverage", "Resi NPL Coverage (x)", "x", True),
                ("RIC_Resi_Nonaccrual_Rate", "% of Resi in Nonaccrual*", "%", False),
                ("RIC_Resi_NCO_Rate", "Resi NCO Rate (TTM)*", "%", False),
                ("RIC_Resi_Delinquency_Rate", "Resi Delinquency Rate (%)*", "%", False),
            ]

    # Build dynamic header from col_certs
    col_headers = "".join(f"<th>{lbl}</th>" for lbl in col_certs.keys())

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{ background-color: transparent; padding: 20px; max-width: 1400px; margin: 0 auto; text-align: center; }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 0 auto; font-size: 11px; background-color: transparent; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; background-color: transparent; }}
        .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 180px; background-color: transparent; }}
        .mspbna-good {{ background-color: rgba(56, 142, 60, 0.15) !important; color: #2E7D32; font-weight: bold; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .mspbna-bad {{ background-color: rgba(211, 47, 47, 0.15) !important; color: #C62828; font-weight: bold; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .mspbna-neutral {{ background-color: rgba(0, 47, 108, 0.08) !important; font-weight: bold; color: #002F6C; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .good-trend {{ color: #388e3c; font-weight: bold; }}
        .bad-trend {{ color: #d32f2f; font-weight: bold; }}
        .neutral-trend {{ color: #795548; font-weight: normal; }}
    </style></head><body>
    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{latest_date.strftime('%B %d, %Y')}</p>
        <table><thead><tr>
            <th>Metric</th>{col_headers}<th>Delta vs Wealth Peers</th>
        </tr></thead><tbody>
    """

    for code, disp, fmt, higher_is_better in metrics:
        vals = {k: row_data[k].get(code, np.nan) for k in col_certs}
        v_subj = vals.get("MSPBNA", np.nan)
        wealth_label = resolve_display_label(wealth_peers_cert)
        v_wealth = vals.get(wealth_label, np.nan)
        diff = v_subj - v_wealth if pd.notna(v_subj) and pd.notna(v_wealth) else np.nan

        def fmt_val(v, fmt_type):
            if pd.isna(v): return "N/A"
            if fmt_type == "x": return f"{v:.2f}x"
            if fmt_type == "B": return f"${v / 1e6:.1f}B"
            return f"{v * 100:.2f}%" if abs(v) < 1.0 else f"{v:.2f}%"

        if pd.isna(diff):
            f_diff, diff_cls = "N/A", "neutral-trend"
        else:
            if fmt == "x":
                f_diff = f"{diff:+.2f}x"
            elif fmt == "B":
                f_diff = f"{diff / 1e6:+.1f}B"
            else:
                f_diff = f"{diff * 100:+.2f}%" if abs(diff) < 1.0 else f"{diff:+.2f}%"

            if abs(diff) < 0.0001:
                diff_cls = "neutral-trend"
            else:
                is_good = (diff > 0) if higher_is_better else (diff < 0)
                diff_cls = "good-trend" if is_good else "bad-trend"

        subj_cls = f"mspbna-{diff_cls.split('-')[0]}" if diff_cls != "neutral-trend" else "mspbna-neutral"

        html += f'<tr><td class="metric-name"><b>{disp}</b></td>'
        for lbl in col_certs.keys():
            v = vals[lbl]
            f_v = fmt_val(v, fmt)
            if lbl == "MSPBNA":
                html += f'<td class="{subj_cls}">{f_v}</td>'
            else:
                html += f'<td>{f_v}</td>'
        html += f'<td class="{diff_cls}">{f_diff}</td></tr>'

    html += "</tbody></table></div></body></html>"
    return html


# ==================================================================================
# MAIN REPORT GENERATION FUNCTION
# ==================================================================================

def _ensure_dir(p: Path) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def _to_decimal(series: pd.Series) -> pd.Series:
    """Robustly convert ambiguous % series to decimal for plotting."""
    s = pd.to_numeric(series, errors="coerce")
    med = s.dropna().abs().median()
    if pd.isna(med):
        return s
    return s/100.0 if med >= 0.01 else s

def _fmt_pct(x: float) -> str:
    return f"{x:.2%}"

def _place_titles(fig: plt.Figure, title: str, subtitle: str | None, title_size: int, subtitle_size: int):
    fig.text(0.5, 0.97, title, ha="center", va="top", fontsize=title_size, fontweight="bold", color="#2B2B2B")
    if subtitle:
        fig.text(0.5, 0.935, subtitle, ha="center", va="top", fontsize=subtitle_size, color="#6E6E6E")

def _load_fred_tables(xlsx_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    with pd.ExcelFile(xlsx_path) as xls:
        for cand in ["FRED_Data", "FRED_data", "FRED", "fred"]:
            if cand in xls.sheet_names:
                fred = pd.read_excel(xls, sheet_name=cand)
                break
        else:
            raise FileNotFoundError("FRED data sheet not found (expected one of: FRED_Data/FRED_data/FRED).")
        for cand in ["FRED_Descriptions", "FRED_Dictionary", "FRED Meta"]:
            if cand in xls.sheet_names:
                desc = pd.read_excel(xls, sheet_name=cand)
                break
        else:
            raise FileNotFoundError("FRED_Descriptions sheet not found.")
    fred.columns = [c.strip() for c in fred.columns]
    desc.columns = [c.strip() for c in desc.columns]
    id_col   = next((c for c in fred.columns if c.lower().startswith("series")), None)
    date_col = next((c for c in fred.columns if "date" in c.lower()), None)
    val_col  = next((c for c in fred.columns if "value" in c.lower()), None)

    # Detect wide-format: if there's a date-like column but no Series* or value* column,
    # the sheet is pivoted (date + one column per series).
    if not id_col and date_col and not val_col:
        # Wide format detected — melt to long format
        series_cols = [c for c in fred.columns if c != date_col]
        fred = fred.melt(id_vars=[date_col], value_vars=series_cols,
                         var_name="SeriesID", value_name="VALUE")
        fred = fred.rename(columns={date_col: "DATE"})
        fred["DATE"] = pd.to_datetime(fred["DATE"], errors="coerce")
        fred = fred.dropna(subset=["DATE"])
    elif all([id_col, date_col, val_col]):
        # Already in long format — rename as before
        fred = fred.rename(columns={id_col: "SeriesID", date_col: "DATE", val_col: "VALUE"})
        fred["DATE"] = pd.to_datetime(fred["DATE"], errors="coerce")
    else:
        raise ValueError("FRED Data columns not recognized. Need either wide format "
                         "(date + series columns) or long format (Series*, *date*, *value* columns).")
    sid_col = next((c for c in desc.columns if c.lower().startswith("series")), None)
    short_col = next((c for c in desc.columns if "short" in c.lower()), None)
    if not all([sid_col, short_col]):
        raise ValueError("FRED_Descriptions needs 'Series ID' and 'Short Name' columns.")
    desc = desc.rename(columns={sid_col:"SeriesID", short_col:"ShortName"})
    return fred, desc


def build_fred_macro_table(xlsx_path: str, short_names: list[str]) -> tuple[str, pd.DataFrame]:
    fred, desc = _load_fred_tables(xlsx_path)
    sel = desc[desc["ShortName"].isin(short_names)].copy()
    if sel.empty:
        raise ValueError("None of the requested Short Names were found in FRED_Descriptions.")
    fred_sel = fred.merge(sel[["SeriesID","ShortName"]], on="SeriesID", how="inner")
    if fred_sel.empty:
        raise ValueError("No FRED data for the selected Short Names.")
    today = fred_sel["DATE"].max()
    curr_year = today.year
    rows = []
    for sname, g in fred_sel.groupby("ShortName"):
        g = g.sort_values("DATE")
        g_curr = g[g["DATE"].dt.year == curr_year].dropna(subset=["VALUE"])
        g_curr = g_curr[g_curr["VALUE"] != 0]
        if g_curr.empty:
            continue
        latest_row = g_curr.iloc[-1]
        g_prev = g[g["DATE"].dt.year == (curr_year-1)].dropna(subset=["VALUE"])
        prev_row = g_prev.iloc[-1] if not g_prev.empty else None
        med = g["VALUE"].abs().median()
        def _fmt(v):
            if pd.isna(v): return "N/A"
            return f"{v:.2f}%" if med >= 0.5 and med <= 1000 else f"{v:.2f}"
        rows.append({
            "Macro Indicator": sname,
            "Latest Value (As Of Date)": f"{_fmt(latest_row['VALUE'])} (as of {latest_row['DATE']:%m-%d})",
            f"Last {curr_year-1} Print": f"{_fmt(prev_row['VALUE'])} (as of {prev_row['DATE']:%m-%d})" if prev_row is not None else "N/A"
        })
    out = pd.DataFrame(rows).sort_values("Macro Indicator")
    html = [
        "<html><head><meta charset='utf-8'><style>",
        "body{font-family:Arial,Helvetica,sans-serif}",
        "table{border-collapse:collapse;font-size:12px}",
        "th,td{border:1px solid #d0d0d0;padding:6px 8px}",
        "th{background:#2f4b7c;color:#fff;text-align:left}",
        "tr:nth-child(even){background:#f8f9fb}",
        "</style></head><body>",
        "<table><thead><tr>",
        "<th>Macro Indicator</th><th>Latest Value (As Of Date)</th><th>Last Year Print</th>",
        "</tr></thead><tbody>"
    ]
    for _, r in out.iterrows():
        html += [f"<tr><td>{r['Macro Indicator']}</td>",
                 f"<td>{r['Latest Value (As Of Date)']}</td>",
                 f"<td>{r[f'Last {curr_year-1} Print']}</td></tr>"]
    html += ["</tbody></table></body></html>"]
    return "\n".join(html), out


# ==================================================================================
# FRED EXPANSION — FIRST-WAVE CHART FUNCTIONS
# ==================================================================================

def _fred_chart_style(ax, title: str, ylabel: str = ""):
    """Apply consistent economist-style formatting to FRED overlay charts."""
    ax.set_title(title, fontsize=14, fontweight="bold", color="#2B2B2B", pad=12)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=11)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis="y", alpha=0.3, linewidth=0.5)
    ax.legend(fontsize=9, framealpha=0.7)


def plot_sbl_backdrop(
    fred_df: pd.DataFrame,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """
    SBL Backdrop: Large-bank securities-in-bank-credit vs
    broker-dealer margin-loan proxy.
    """
    sbc_col = next((c for c in ["SBCLCBM027SBOG", "INVEST"] if c in fred_df.columns), None)
    bd_col = next((c for c in ["BOGZ1FU663067005A", "BOGZ1FU664004005Q"] if c in fred_df.columns), None)
    if not sbc_col:
        return None

    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax1.plot(fred_df.index, fred_df[sbc_col], color="#4C78A8", linewidth=2,
             label="Large-Bank Securities in Bank Credit")
    ax1.set_ylabel("Bil. of $", color="#4C78A8", fontsize=11)
    ax1.tick_params(axis="y", labelcolor="#4C78A8")

    if bd_col:
        ax2 = ax1.twinx()
        ax2.plot(fred_df.index, fred_df[bd_col], color="#F7A81B", linewidth=1.5,
                 alpha=0.8, label="Broker-Dealer Margin Proxy")
        ax2.set_ylabel("Mil. of $", color="#F7A81B", fontsize=11)
        ax2.tick_params(axis="y", labelcolor="#F7A81B")
        ax2.spines["top"].set_visible(False)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=9, loc="upper left")

    _fred_chart_style(ax1, "SBL Market Backdrop: Securities Inventory vs Broker-Dealer Leverage")

    # Regime shading
    if "REGIME__SBL_Deleveraging" in fred_df.columns:
        regime = fred_df["REGIME__SBL_Deleveraging"].reindex(fred_df.index).fillna(0)
        ax1.fill_between(fred_df.index, ax1.get_ylim()[0], ax1.get_ylim()[1],
                         where=regime > 0, alpha=0.08, color="red", label="_regime")

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


def plot_jumbo_conditions(
    fred_df: pd.DataFrame,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """
    Jumbo Mortgage Conditions: rate + demand + tightening standards.
    """
    rate_col = "OBMMIJUMBO30YF" if "OBMMIJUMBO30YF" in fred_df.columns else None
    dem_col = next((c for c in ["SUBLPDHMDJLGNQ", "SUBLPDHMDJNQ"] if c in fred_df.columns), None)
    std_col = next((c for c in ["SUBLPDHMSKLGNQ", "SUBLPDHMSKNQ"] if c in fred_df.columns), None)
    if not any([rate_col, dem_col, std_col]):
        return None

    fig, axes = plt.subplots(1 if not dem_col and not std_col else 2, 1,
                             figsize=(14, 8 if dem_col or std_col else 5),
                             sharex=True)
    if not isinstance(axes, np.ndarray):
        axes = [axes]

    # Panel 1: Jumbo rate (+ conforming for spread context)
    ax = axes[0]
    if rate_col:
        ax.plot(fred_df.index, fred_df[rate_col], color="#d32f2f", linewidth=2,
                label="30Y Jumbo Rate")
    if "MORTGAGE30US" in fred_df.columns:
        ax.plot(fred_df.index, fred_df["MORTGAGE30US"], color="#4C78A8",
                linewidth=1.5, alpha=0.7, label="30Y Conforming Rate")
    if "SPREAD__Jumbo_vs_Conforming" in fred_df.columns:
        ax_sp = ax.twinx()
        ax_sp.fill_between(fred_df.index,
                           fred_df["SPREAD__Jumbo_vs_Conforming"].fillna(0),
                           alpha=0.15, color="#F7A81B", label="Jumbo Spread")
        ax_sp.set_ylabel("Spread (ppt)", fontsize=9, color="#F7A81B")
        ax_sp.spines["top"].set_visible(False)
    _fred_chart_style(ax, "Jumbo Mortgage Rate & Spread", "%")

    # Panel 2: SLOOS demand + standards
    if len(axes) > 1 and (dem_col or std_col):
        ax2 = axes[1]
        if dem_col:
            ax2.plot(fred_df.index, fred_df[dem_col].ffill(), color="#388e3c",
                     linewidth=2, label="Jumbo Demand (SLOOS)")
        if std_col:
            ax2.plot(fred_df.index, fred_df[std_col].ffill(), color="#d32f2f",
                     linewidth=2, label="Jumbo Standards (SLOOS)")
        ax2.axhline(0, color="gray", linewidth=0.5, linestyle="--")
        _fred_chart_style(ax2, "Jumbo Credit Appetite (SLOOS)", "Net % of Banks")

        if "REGIME__Jumbo_Tightening" in fred_df.columns:
            regime = fred_df["REGIME__Jumbo_Tightening"].reindex(fred_df.index).fillna(0)
            ax2.fill_between(fred_df.index, ax2.get_ylim()[0], ax2.get_ylim()[1],
                             where=regime > 0, alpha=0.08, color="red")

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


def plot_resi_credit_cycle(
    fred_df: pd.DataFrame,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """
    Residential Credit Cycle: top-100-bank delinquency / charge-off
    vs large-bank residential balance growth.
    """
    dq_col = "DRSFRMT100S" if "DRSFRMT100S" in fred_df.columns else None
    co_col = "CORSFRMT100S" if "CORSFRMT100S" in fred_df.columns else None
    growth_col = next((c for c in ["H8B1221NLGCQG", "RRELCBM027SBOG__pct_chg_yoy"]
                       if c in fred_df.columns), None)
    if not dq_col:
        return None

    fig, ax1 = plt.subplots(figsize=(14, 6))

    # Delinquency / charge-off on left axis
    if dq_col:
        ax1.plot(fred_df.index, fred_df[dq_col], color="#d32f2f", linewidth=2,
                 label="Resi Delinquency (Top 100)")
    if co_col:
        ax1.plot(fred_df.index, fred_df[co_col], color="#ff7043", linewidth=1.5,
                 alpha=0.8, label="Resi Charge-Off (Top 100)")
    ax1.set_ylabel("Rate (%)", color="#d32f2f", fontsize=11)

    # Growth on right axis
    if growth_col:
        ax2 = ax1.twinx()
        ax2.plot(fred_df.index, fred_df[growth_col], color="#4C78A8", linewidth=1.5,
                 label="Large-Bank Resi Growth")
        ax2.set_ylabel("Growth (%)", color="#4C78A8", fontsize=11)
        ax2.tick_params(axis="y", labelcolor="#4C78A8")
        ax2.spines["top"].set_visible(False)
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=9, loc="upper left")

    _fred_chart_style(ax1, "Residential Credit Cycle: Delinquency vs Balance Growth")

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


def plot_cre_cycle(
    fred_df: pd.DataFrame,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """
    CRE Cycle: large-bank CRE growth vs standards vs delinquency / charge-off.
    Three-panel layout: balances, standards/demand, credit quality.
    """
    growth_col = next((c for c in ["H8B3219NLGCMG", "CRELCBM027NBOG__pct_chg_yoy"]
                       if c in fred_df.columns), None)
    std_col = next((c for c in ["SUBLPDRCSNLGNQ", "SUBLPDRCSN"] if c in fred_df.columns), None)
    dem_col = next((c for c in ["SUBLPDRCDNLGNQ", "SUBLPDRCDN"] if c in fred_df.columns), None)
    dq_col = "DRCRELEXFT100S" if "DRCRELEXFT100S" in fred_df.columns else None
    co_col = "CORCREXFT100S" if "CORCREXFT100S" in fred_df.columns else None
    price_col = "COMREPUSQ159N" if "COMREPUSQ159N" in fred_df.columns else None

    panels = sum([bool(growth_col), bool(std_col or dem_col), bool(dq_col or co_col)])
    if panels == 0:
        return None

    fig, axes = plt.subplots(max(panels, 1), 1, figsize=(14, 4 * max(panels, 1)),
                             sharex=True, squeeze=False)
    axes = axes.flatten()
    panel_idx = 0

    # Panel 1: CRE growth
    if growth_col:
        ax = axes[panel_idx]
        ax.plot(fred_df.index, fred_df[growth_col].ffill(), color="#4C78A8",
                linewidth=2, label="CRE Loan Growth (Large Banks)")
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
        _fred_chart_style(ax, "CRE Loan Growth", "Annualized %")
        panel_idx += 1

    # Panel 2: Standards vs demand
    if std_col or dem_col:
        ax = axes[panel_idx]
        if std_col:
            ax.plot(fred_df.index, fred_df[std_col].ffill(), color="#d32f2f",
                    linewidth=2, label="CRE Standards (Tightening)")
        if dem_col:
            ax.plot(fred_df.index, fred_df[dem_col].ffill(), color="#388e3c",
                    linewidth=2, label="CRE Demand")
        ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
        _fred_chart_style(ax, "CRE SLOOS: Standards vs Demand", "Net % of Banks")
        if "REGIME__CRE_Tightening" in fred_df.columns:
            regime = fred_df["REGIME__CRE_Tightening"].reindex(fred_df.index).fillna(0)
            ax.fill_between(fred_df.index, ax.get_ylim()[0], ax.get_ylim()[1],
                            where=regime > 0, alpha=0.08, color="red")
        panel_idx += 1

    # Panel 3: Delinquency / charge-off / CRE prices
    if dq_col or co_col:
        ax = axes[panel_idx]
        if dq_col:
            ax.plot(fred_df.index, fred_df[dq_col], color="#d32f2f",
                    linewidth=2, label="CRE Delinquency (Top 100)")
        if co_col:
            ax.plot(fred_df.index, fred_df[co_col], color="#ff7043",
                    linewidth=1.5, alpha=0.8, label="CRE Charge-Off (Top 100)")
        if price_col:
            ax2 = ax.twinx()
            ax2.plot(fred_df.index, fred_df[price_col], color="#9C6FB6",
                     linewidth=1.5, alpha=0.7, label="CRE Price YoY %")
            ax2.set_ylabel("YoY %", color="#9C6FB6", fontsize=9)
            ax2.spines["top"].set_visible(False)
        _fred_chart_style(ax, "CRE Credit Quality & Collateral Value", "Rate (%)")

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


def plot_cs_collateral_panel(
    fred_df: pd.DataFrame,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """
    Private-Bank Collateral Panel: high-tier Case-Shiller metros vs
    national / 20-city composite + metro sales-pair counts.
    """
    # High-tier metros
    ht_cols = [c for c in ["NYXRHTSA", "LXXRHTSA", "SFXRHTSA", "MIXRHTSA", "WDXRHTSA"]
               if c in fred_df.columns]
    nat_col = "CSUSHPISA" if "CSUSHPISA" in fred_df.columns else None
    comp_col = "SPCS20RSA" if "SPCS20RSA" in fred_df.columns else None
    spc_col = next((c for c in ["SPCS20RPSNSA", "SPCS10RPSNSA"] if c in fred_df.columns), None)

    if not nat_col and not ht_cols:
        return None

    fig, axes = plt.subplots(2 if spc_col else 1, 1,
                             figsize=(14, 9 if spc_col else 5),
                             sharex=True)
    if not isinstance(axes, np.ndarray):
        axes = [axes]

    # Panel 1: HPI levels (YoY changes)
    ax = axes[0]
    colors = ["#d32f2f", "#4C78A8", "#388e3c", "#F7A81B", "#9C6FB6"]
    metro_names = {"NYXRHTSA": "NYC High", "LXXRHTSA": "LA High", "SFXRHTSA": "SF High",
                   "MIXRHTSA": "Miami High", "WDXRHTSA": "DC High"}

    for i, col in enumerate(ht_cols[:5]):
        yoy = fred_df[col].pct_change(12) * 100
        ax.plot(fred_df.index, yoy, color=colors[i % len(colors)],
                linewidth=1.5, label=metro_names.get(col, col))

    if nat_col:
        nat_yoy = fred_df[nat_col].pct_change(12) * 100
        ax.plot(fred_df.index, nat_yoy, color="black", linewidth=2.5,
                linestyle="--", label="National", alpha=0.7)

    ax.axhline(0, color="gray", linewidth=0.5, linestyle="--")
    _fred_chart_style(ax, "Case-Shiller: High-Tier Metros vs National (YoY %)", "YoY %")

    # Panel 2: Sales-pair counts
    if spc_col and len(axes) > 1:
        ax2 = axes[1]
        ax2.bar(fred_df.index, fred_df[spc_col].fillna(0),
                width=25, color="#4C78A8", alpha=0.6, label="Sales Pair Count")
        _fred_chart_style(ax2, "Market Liquidity: Sales Pair Counts", "Count")

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


# ---------- ARTIFACT PRODUCTION HELPERS ----------
# These use the canonical API from rendering_mode.py (should_produce,
# ArtifactManifest.record_generated / record_skipped / record_failed).

def _produce_table(ctx: _ReportContext, artifact_name: str, csv_log,
                   generator_fn, out_dir: Path, *args, **kwargs) -> None:
    """Produce an HTML table artifact with mode-check + manifest recording."""
    if not should_produce(artifact_name, ctx.mode, ctx.manifest, ctx.suppressed_charts):
        return  # skip already recorded by should_produce()
    cap = ARTIFACT_REGISTRY.get(artifact_name)
    suffix = cap.filename_suffix if cap else f"_{artifact_name}.html"
    try:
        result = generator_fn(*args, **kwargs)
        html = result[0] if isinstance(result, tuple) else result
        if html:
            path = str(out_dir / f"{ctx.base_stem}{suffix}")
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            ctx.manifest.record_generated(artifact_name, path)
            csv_log.log_file_written(path, phase="tables", component=artifact_name)
            print(f"  {artifact_name} saved: {path}")
        else:
            ctx.manifest.record_failed(artifact_name, "generator returned empty")
    except Exception as exc:
        ctx.manifest.record_failed(artifact_name, str(exc)[:200])
        print(f"  [{artifact_name}] FAILED: {exc}")


def _produce_chart(ctx: _ReportContext, artifact_name: str, csv_log,
                   chart_fn, out_dir: Path, *args, **kwargs) -> None:
    """Produce a matplotlib chart artifact with mode-check + manifest recording."""
    if not should_produce(artifact_name, ctx.mode, ctx.manifest, ctx.suppressed_charts):
        return  # skip already recorded by should_produce()
    cap = ARTIFACT_REGISTRY.get(artifact_name)
    suffix = cap.filename_suffix if cap else f"_{artifact_name}.png"
    category = cap.category if cap else "chart"
    try:
        path = str(out_dir / f"{ctx.base_stem}{suffix}")
        kwargs["save_path"] = path
        result = chart_fn(*args, **kwargs)
        if result is not None:
            ctx.manifest.record_generated(artifact_name, path)
            csv_log.log_file_written(path, phase=category, component=artifact_name)
            print(f"  {artifact_name} saved: {path}")
        else:
            ctx.manifest.record_failed(artifact_name, "chart function returned None")
    except Exception as exc:
        ctx.manifest.record_failed(artifact_name, str(exc)[:200])
        print(f"  [{artifact_name}] FAILED: {exc}")


def generate_reports(
    # data window & figure sizing
    start_date: str = "2023-01-01",
    credit_figsize: Tuple[float, float] = (18.0, 7.0),
    scatter_size: float = 9.0,
    # typography
    title_size: int = 20,
    subtitle_size: int = 13,
    axis_label_size: int = 16,
    tick_size: int = 14,
    tag_size: int = 12,
    legend_fontsize: int = 14,
    # scatter outliers
    outlier_topn: int = 2,
    # Optional custom title/subtitle. If None, sensible defaults are used.
    credit_title: Optional[str] = None,
    credit_subtitle: Optional[str] = "F&V excluded from Peers (Ex. F&V)",
    # Macro table: pass Short Names; we'll map to Series ID via FRED_Descriptions
    fred_short_names: Optional[List[str]] = None,
    # Dual-mode rendering: "full_local" (default) or "corp_safe"
    render_mode: Optional[str] = None,
) -> Optional[ArtifactManifest]:
    """
    End-to-end report runner with dual-mode architecture.

    Parameters
    ----------
    render_mode : str, optional
        "full_local" (default) — all artifacts using matplotlib/seaborn.
        "corp_safe" — HTML tables only; matplotlib charts are skipped gracefully.
        If None, resolved via select_mode(): REPORT_MODE env var (canonical),
        then REPORT_RENDER_MODE (backward-compatible alias), then full_local.

    Returns
    -------
    ArtifactManifest or None
        Manifest of all artifact outcomes for the run.  None only on early abort.
    """
    # ---- Dual-mode rendering (canonical: rendering_mode.select_mode) ----
    mode = select_mode(render_mode)
    manifest = ArtifactManifest(mode)
    print(f"Render mode: {mode.value}")

    # ---- B7: MS Combined Entity ----
    REPORT_VIEW = os.getenv("REPORT_VIEW", "ALL_BANKS")
    MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))

    if fred_short_names is None:
        fred_short_names = [
            "10Y Treasury",
            "All Loans Delinquency Rate",
            "Consumer Sentiment",
            "Fed Funds",
            "Market Volatility (VIX)",
            "Unemployment",
            "Yield Curve (T10Y2Y)",
        ]

    from logging_utils import setup_csv_logging
    csv_log = setup_csv_logging("report_generator", log_dir="logs")

    print("=" * 80)
    print("MSPBNA PERFORMANCE REPORT GENERATOR")
    print(f"  Mode: {mode.value}")
    print(f"  Available artifacts: {sum(1 for c in ARTIFACT_REGISTRY.values() if c.is_available(mode))} of {len(ARTIFACT_REGISTRY)}")
    print("=" * 80)

    cfg = load_config()
    subject_bank_cert = cfg["subject_bank_cert"]

    # 1) Find latest processed Excel and set output roots
    excel_file = find_latest_excel_file(cfg["output_dir"])
    if not excel_file:
        print("ERROR: No Excel files found in output/. Run the pipeline first.")
        return manifest

    output_root = Path(excel_file).parent.resolve()

    # ---- B1: Output routing ----
    peers_root = output_root / "Peers"
    charts_dir = peers_root / "charts"
    scatter_dir = peers_root / "scatter"
    tables_dir = peers_root / "tables"
    charts_dir.mkdir(parents=True, exist_ok=True)
    scatter_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found latest file: {excel_file}")
    print(f"File created: {datetime.fromtimestamp(os.path.getmtime(excel_file)).strftime('%Y-%m-%d')}")
    csv_log.info(f"Source workbook: {excel_file}",
                 event_type="FILE_DISCOVERED", phase="startup", component="excel_discovery",
                 context={"excel_file": str(excel_file), "mode": mode.value})

    # base already contains the date (e.g. "Bank_Performance_Dashboard_20260311")
    # so we do NOT append a second date suffix to artifact filenames.
    base = Path(excel_file).stem

    try:
        # ------------------------------------------------------------------
        # PHASE 1: WORKBOOK INGESTION
        # ------------------------------------------------------------------
        print("\nLoading data from Excel sheets...")
        with pd.ExcelFile(excel_file) as xls:
            proc_df_with_peers = pd.read_excel(xls, sheet_name="FDIC_Data")
            proc_df_with_peers["REPDTE"] = pd.to_datetime(proc_df_with_peers["REPDTE"])

            roll_sheet = next((s for s in xls.sheet_names if s.lower().startswith("averages_8q")), None)
            if not roll_sheet:
                raise FileNotFoundError("8Q average sheet not found (expects sheet name starting with 'Averages_8Q').")
            rolling8q_df = pd.read_excel(xls, sheet_name=roll_sheet)
            if "CERT" in rolling8q_df.columns:
                rolling8q_df["CERT"] = pd.to_numeric(rolling8q_df["CERT"], errors="coerce").astype("Int64")

            metric_descriptions = pd.read_excel(xls, sheet_name="FDIC_Metric_Descriptions") \
                if "FDIC_Metric_Descriptions" in xls.sheet_names else None

        if metric_descriptions is not None:
            print(f"Loaded {len(metric_descriptions)} metric descriptions")
        print(f"Loaded FDIC data: {len(proc_df_with_peers)} records across {proc_df_with_peers['CERT'].nunique()} banks")
        csv_log.log_df_shape("proc_df_with_peers", len(proc_df_with_peers),
                             len(proc_df_with_peers.columns),
                             phase="sheet_load", component="FDIC_Data")
        csv_log.log_df_shape("rolling8q_df", len(rolling8q_df),
                             len(rolling8q_df.columns),
                             phase="sheet_load", component="Averages_8Q")

        # ------------------------------------------------------------------
        # PHASE 2: PREFLIGHT VALIDATION
        # ------------------------------------------------------------------
        preflight = validate_output_inputs(proc_df_with_peers, rolling8q_df, subject_bank_cert)
        if preflight["warnings"]:
            print(f"\n  PREFLIGHT WARNINGS ({len(preflight['warnings'])}):")
            for w in preflight["warnings"]:
                print(f"    [!] {w}")
                csv_log.warning(w, event_type="PRECHECK_WARN", phase="preflight")
        if preflight["errors"]:
            print(f"\n  PREFLIGHT ERRORS ({len(preflight['errors'])}):")
            for e in preflight["errors"]:
                print(f"    [X] {e}")
                csv_log.error(e, event_type="PRECHECK_FAIL", phase="preflight")
            print("  Aborting report generation due to preflight errors.")
            return manifest
        suppressed_charts = frozenset(preflight.get("suppressed_charts", []))

        # Build context for _produce_table / _produce_chart helpers
        ctx = _ReportContext(
            mode=mode,
            manifest=manifest,
            base_stem=base,
            suppressed_charts=suppressed_charts,
        )

        # ------------------------------------------------------------------
        # PHASE 3: HTML TABLES (both modes)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING HTML TABLES")
        print("-" * 60)

        # Executive Summary — Wealth-focused
        for is_norm in [False, True]:
            norm_str = "normalized" if is_norm else "standard"
            _produce_table(ctx, f"executive_summary_{norm_str}", csv_log,
                           generate_credit_metrics_email_table, tables_dir,
                           proc_df_with_peers, subject_bank_cert, is_normalized=is_norm)

        # Detailed Peer, Core PB, Ratio Components, Segment tables
        for is_norm in [False, True]:
            norm_str = "normalized" if is_norm else "standard"

            _produce_table(ctx, f"detailed_peer_table_{norm_str}", csv_log,
                           generate_detailed_peer_table, tables_dir,
                           proc_df_with_peers, subject_bank_cert, is_normalized=is_norm)

            _produce_table(ctx, f"core_pb_peer_table_{norm_str}", csv_log,
                           generate_core_pb_peer_table, tables_dir,
                           proc_df_with_peers, subject_bank_cert, is_normalized=is_norm)

            _produce_table(ctx, f"ratio_components_{norm_str}", csv_log,
                           generate_ratio_components_table, tables_dir,
                           proc_df_with_peers, subject_bank_cert, is_normalized=is_norm)

            _produce_table(ctx, f"cre_segment_{norm_str}", csv_log,
                           generate_segment_focus_table, tables_dir,
                           proc_df_with_peers, subject_bank_cert,
                           segment_name="CRE", is_normalized=is_norm)

            _produce_table(ctx, f"resi_segment_{norm_str}", csv_log,
                           generate_segment_focus_table, tables_dir,
                           proc_df_with_peers, subject_bank_cert,
                           segment_name="Resi", is_normalized=is_norm)

        # FRED macro table
        _produce_table(ctx, "fred_table", csv_log,
                       build_fred_macro_table, tables_dir,
                       excel_file, list(fred_short_names))

        # ------------------------------------------------------------------
        # PHASE 4: CREDIT DETERIORATION CHARTS (full_local only)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING CHARTS")
        print("-" * 60)

        _produce_chart(ctx, "standard_credit_chart", csv_log,
                       create_credit_deterioration_chart_ppt, charts_dir,
                       proc_df_with_peers=proc_df_with_peers,
                       subject_bank_cert=subject_bank_cert,
                       start_date=start_date,
                       bar_metric="TTM_NCO_Rate",
                       line_metric="NPL_to_Gross_Loans_Rate",
                       bar_entities=[subject_bank_cert, ACTIVE_STANDARD_COMPOSITES["all_peers"], ACTIVE_STANDARD_COMPOSITES["core_pb"]],
                       line_entities=[subject_bank_cert, ACTIVE_STANDARD_COMPOSITES["all_peers"], ACTIVE_STANDARD_COMPOSITES["core_pb"]],
                       custom_title="TTM NCO Rate (bars) vs NPL to Gross Loans Rate (lines)")

        # Normalized credit chart
        norm_line_metric = "Norm_Nonaccrual_Rate"
        if norm_line_metric not in proc_df_with_peers.columns:
            norm_line_metric = "Norm_NPL_to_Gross_Loans_Rate"

        _produce_chart(ctx, "normalized_credit_chart", csv_log,
                       create_credit_deterioration_chart_ppt, charts_dir,
                       proc_df_with_peers=proc_df_with_peers,
                       subject_bank_cert=subject_bank_cert,
                       start_date=start_date,
                       bar_metric="Norm_NCO_Rate",
                       line_metric=norm_line_metric,
                       bar_entities=[subject_bank_cert, ACTIVE_NORMALIZED_COMPOSITES["all_peers"], ACTIVE_NORMALIZED_COMPOSITES["core_pb"]],
                       line_entities=[subject_bank_cert, ACTIVE_NORMALIZED_COMPOSITES["all_peers"], ACTIVE_NORMALIZED_COMPOSITES["core_pb"]],
                       figsize=credit_figsize,
                       title_size=title_size,
                       axis_label_size=axis_label_size,
                       tick_size=tick_size,
                       tag_size=tag_size,
                       legend_fontsize=legend_fontsize,
                       economist_style=True,
                       custom_title=credit_title or "Norm NCO Rate (bars) vs Norm Nonaccrual Rate (lines)")

        # ------------------------------------------------------------------
        # PHASE 5: SCATTER PLOTS (full_local only)
        # ------------------------------------------------------------------
        # Ensure numeric columns for scatter
        for c in ["NPL_to_Gross_Loans_Rate", "TTM_NCO_Rate", "Past_Due_Rate",
                   "Norm_Nonaccrual_Rate", "Norm_NCO_Rate"]:
            if c in rolling8q_df.columns:
                rolling8q_df[c] = pd.to_numeric(rolling8q_df[c], errors="coerce")

        scatter_common = dict(
            show_peers_avg_label=True,
            show_mspbna_label=True,
            identify_outliers=True,
            outliers_topn=outlier_topn,
            figsize=(scatter_size, scatter_size),
            title_size=16,
            axis_label_size=12,
            tick_size=tick_size,
            tag_size=tag_size,
            economist_style=True,
            transparent_bg=True,
            square_axes=True,
        )

        _produce_chart(ctx, "scatter_nco_vs_npl", csv_log,
                       plot_scatter_dynamic, scatter_dir,
                       df=rolling8q_df,
                       x_col="NPL_to_Gross_Loans_Rate",
                       y_col="TTM_NCO_Rate",
                       subject_cert=subject_bank_cert,
                       peer_avg_cert_primary=ACTIVE_STANDARD_COMPOSITES["all_peers"],
                       peer_avg_cert_alt=ACTIVE_STANDARD_COMPOSITES["core_pb"],
                       wealth_peer_cert=ACTIVE_STANDARD_COMPOSITES["core_pb"],
                       use_alt_peer_avg=False,
                       **scatter_common)

        _produce_chart(ctx, "scatter_pd_vs_npl", csv_log,
                       plot_scatter_dynamic, scatter_dir,
                       df=rolling8q_df,
                       x_col="NPL_to_Gross_Loans_Rate",
                       y_col="Past_Due_Rate",
                       subject_cert=subject_bank_cert,
                       peer_avg_cert_primary=ACTIVE_STANDARD_COMPOSITES["all_peers"],
                       peer_avg_cert_alt=ACTIVE_STANDARD_COMPOSITES["core_pb"],
                       wealth_peer_cert=ACTIVE_STANDARD_COMPOSITES["core_pb"],
                       use_alt_peer_avg=False,
                       **scatter_common)

        _produce_chart(ctx, "scatter_norm_nco_vs_nonaccrual", csv_log,
                       plot_scatter_dynamic, scatter_dir,
                       df=rolling8q_df,
                       x_col="Norm_Nonaccrual_Rate",
                       y_col="Norm_NCO_Rate",
                       subject_cert=subject_bank_cert,
                       peer_avg_cert_primary=ACTIVE_NORMALIZED_COMPOSITES["all_peers"],
                       peer_avg_cert_alt=ACTIVE_NORMALIZED_COMPOSITES["core_pb"],
                       wealth_peer_cert=ACTIVE_NORMALIZED_COMPOSITES["core_pb"],
                       use_alt_peer_avg=False,
                       **scatter_common)

        # CRE segment scatter — NCO Rate vs Nonaccrual Rate
        _produce_chart(ctx, "scatter_cre_nco_vs_nonaccrual", csv_log,
                       plot_scatter_dynamic, scatter_dir,
                       df=rolling8q_df,
                       x_col="RIC_CRE_Nonaccrual_Rate",
                       y_col="RIC_CRE_NCO_Rate",
                       subject_cert=subject_bank_cert,
                       peer_avg_cert_primary=ACTIVE_STANDARD_COMPOSITES["all_peers"],
                       peer_avg_cert_alt=ACTIVE_STANDARD_COMPOSITES["core_pb"],
                       wealth_peer_cert=ACTIVE_STANDARD_COMPOSITES["core_pb"],
                       use_alt_peer_avg=False,
                       **scatter_common)

        # Normalized bankwide — ACL Coverage vs Delinquency Rate
        _produce_chart(ctx, "scatter_norm_acl_vs_delinquency", csv_log,
                       plot_scatter_dynamic, scatter_dir,
                       df=rolling8q_df,
                       x_col="Norm_Delinquency_Rate",
                       y_col="Norm_ACL_Coverage",
                       subject_cert=subject_bank_cert,
                       peer_avg_cert_primary=ACTIVE_NORMALIZED_COMPOSITES["all_peers"],
                       peer_avg_cert_alt=ACTIVE_NORMALIZED_COMPOSITES["core_pb"],
                       wealth_peer_cert=ACTIVE_NORMALIZED_COMPOSITES["core_pb"],
                       use_alt_peer_avg=False,
                       **scatter_common)

        # ------------------------------------------------------------------
        # PHASE 6: SEGMENT & ROADMAP CHARTS (full_local only)
        # ------------------------------------------------------------------
        _produce_chart(ctx, "portfolio_mix", csv_log,
                       plot_portfolio_mix, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "problem_asset_attribution", csv_log,
                       plot_problem_asset_attribution, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "reserve_risk_allocation", csv_log,
                       plot_reserve_risk_allocation, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "migration_ladder", csv_log,
                       plot_migration_ladder, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "years_of_reserves", csv_log,
                       plot_years_of_reserves, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "growth_vs_deterioration", csv_log,
                       plot_growth_vs_deterioration, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "growth_vs_deterioration_bookwide", csv_log,
                       plot_growth_vs_deterioration_bookwide, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "risk_adjusted_return", csv_log,
                       plot_risk_adjusted_return, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "concentration_vs_capital", csv_log,
                       plot_concentration_vs_capital, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        _produce_chart(ctx, "liquidity_overlay", csv_log,
                       plot_liquidity_overlay, charts_dir,
                       proc_df_with_peers, subject_bank_cert)

        # Macro correlation heatmap (HTML — BOTH modes)
        art_name = "macro_corr_heatmap_lag1"
        if should_produce(art_name, mode, manifest, suppressed_charts):
            try:
                cap = ARTIFACT_REGISTRY.get(art_name)
                suffix = cap.filename_suffix if cap else f"_{art_name}.html"
                save = str(tables_dir / f"{base}{suffix}")
                html = generate_macro_corr_heatmap(
                    proc_df_with_peers, subject_bank_cert, excel_file,
                    save_path=save,
                )
                if html is not None:
                    manifest.record_generated(art_name, save)
                    csv_log.log_file_written(save, phase="table", component=art_name)
                    print(f"  {art_name} saved: {save}")
                else:
                    manifest.record_failed(art_name, "generator returned None")
            except Exception as exc:
                manifest.record_failed(art_name, str(exc)[:200])
                print(f"  [{art_name}] FAILED: {exc}")

        # Macro overlay — credit stress (PNG — full_local only)
        _produce_chart(ctx, "macro_overlay_credit_stress", csv_log,
                       plot_macro_overlay_credit_stress, charts_dir,
                       proc_df_with_peers, subject_bank_cert, excel_file)

        # Macro overlay — rates & housing (PNG — full_local only)
        _produce_chart(ctx, "macro_overlay_rates_housing", csv_log,
                       plot_macro_overlay_rates_housing, charts_dir,
                       proc_df_with_peers, subject_bank_cert, excel_file)

        # MSA macro panel — geographic macro context (full_local only)
        art_name = "msa_macro_panel"
        if should_produce(art_name, mode, manifest, suppressed_charts):
            try:
                from corp_overlay import select_top_msas, build_msa_macro_panel
                # Build synthetic placeholder data for top MSAs
                # In production, these would come from BEA/Census/Case-Shiller APIs
                top_msas = ["New York", "Los Angeles", "San Francisco", "Miami", "Chicago"]
                _synth_dates = pd.date_range("2020-01-01", periods=20, freq="QS")
                _rng = np.random.RandomState(42)
                _synth_rows = []
                for msa in top_msas:
                    for d in _synth_dates:
                        _synth_rows.append({
                            "msa": msa, "date": d,
                            "hpi_yoy_pct": _rng.normal(5.0, 3.0),
                            "gdp_yoy_pct": _rng.normal(2.5, 1.5),
                            "unemp_rate_chg_pp": _rng.normal(0.0, 0.3),
                        })
                synth_df = pd.DataFrame(_synth_rows)
                save = str(charts_dir / f"{base}_{art_name}.png")
                fig = build_msa_macro_panel(
                    msas=top_msas,
                    case_shiller_df=synth_df[["msa", "date", "hpi_yoy_pct"]],
                    bea_gdp_df=synth_df[["msa", "date", "gdp_yoy_pct"]],
                    unemployment_df=synth_df[["msa", "date", "unemp_rate_chg_pp"]],
                    save_path=save,
                )
                if fig is not None:
                    manifest.record_generated(art_name, save)
                    csv_log.log_file_written(save, phase="chart", component=art_name)
                    print(f"  {art_name} saved: {save}")
                else:
                    manifest.record_failed(art_name, "generator returned None")
            except ImportError:
                manifest.record_failed(art_name, "corp_overlay module not available")
                print(f"  [{art_name}] SKIPPED: corp_overlay not importable")
            except Exception as exc:
                manifest.record_failed(art_name, str(exc)[:200])
                print(f"  [{art_name}] FAILED: {exc}")

        # ------------------------------------------------------------------
        # PHASE 7: FRED EXPANSION CHARTS (full_local only)
        # ------------------------------------------------------------------
        fred_chart_names = ["sbl_backdrop", "jumbo_conditions", "resi_credit_cycle",
                            "cre_cycle", "cs_collateral_panel"]
        fred_chart_fns = [plot_sbl_backdrop, plot_jumbo_conditions, plot_resi_credit_cycle,
                          plot_cre_cycle, plot_cs_collateral_panel]

        # Only attempt loading FRED data if any FRED charts are available in this mode
        any_fred_available = any(is_artifact_available(n, mode, suppressed_charts) for n in fred_chart_names)
        if any_fred_available:
            try:
                fred_expansion_df = None
                with pd.ExcelFile(excel_file) as xls:
                    for sheet_cand in ["FRED_SBL_Backdrop", "FRED_Residential_Jumbo",
                                       "FRED_CRE", "FRED_CaseShiller_Selected"]:
                        if sheet_cand in xls.sheet_names:
                            _df = pd.read_excel(xls, sheet_name=sheet_cand)
                            if "DATE" in _df.columns:
                                _df["DATE"] = pd.to_datetime(_df["DATE"])
                                _df = _df.set_index("DATE")
                            if fred_expansion_df is None:
                                fred_expansion_df = _df
                            else:
                                fred_expansion_df = fred_expansion_df.join(_df, how="outer")

                if fred_expansion_df is not None and not fred_expansion_df.empty:
                    for fname, ffn in zip(fred_chart_names, fred_chart_fns):
                        _produce_chart(ctx, fname, csv_log, ffn, charts_dir, fred_expansion_df)
                else:
                    for fname in fred_chart_names:
                        manifest.record_skipped(fname, "FRED expansion sheets not found")
                    print("  No FRED expansion sheets found — run fred_ingestion_engine.py first")
            except Exception as e:
                for fname in fred_chart_names:
                    manifest.record_failed(fname, str(e)[:200])
                print(f"  Skipped FRED expansion charts: {e}")

        # ------------------------------------------------------------------
        # PHASE 8: EXECUTIVE CHARTS (YoY Heatmap, KRI Bullet, Sparkline)
        # ------------------------------------------------------------------
        if _HAS_EXECUTIVE_CHARTS:
            print("\n" + "-" * 60)
            print("GENERATING EXECUTIVE CHARTS")
            print("-" * 60)

            # YoY Heatmap — 4 variants: Standard/Normalized × Wealth/All Peers
            _heatmap_specs = [
                (False, "wealth",   ACTIVE_STANDARD_COMPOSITES["core_pb"],   "Wealth Peers"),
                (False, "allpeers", ACTIVE_STANDARD_COMPOSITES["all_peers"], "All Peers"),
                (True,  "wealth",   ACTIVE_NORMALIZED_COMPOSITES["core_pb"], "Wealth Peers"),
                (True,  "allpeers", ACTIVE_NORMALIZED_COMPOSITES["all_peers"], "All Peers"),
            ]
            for is_norm, pg_suffix, peer_cert, peer_label in _heatmap_specs:
                norm_str = "normalized" if is_norm else "standard"
                art_name = f"yoy_heatmap_{norm_str}_{pg_suffix}"
                if should_produce(art_name, mode, manifest, suppressed_charts):
                    try:
                        save = str(tables_dir / f"{base}_{art_name}.html")
                        html = generate_yoy_heatmap(
                            proc_df_with_peers, subject_bank_cert, peer_cert,
                            is_normalized=is_norm, save_path=save,
                            peer_label=peer_label,
                        )
                        if html:
                            manifest.record_generated(art_name, save)
                            print(f"  Generated: {art_name}")
                            csv_log.log_file_written(save, phase="executive_charts",
                                                     component=art_name)
                        else:
                            manifest.record_failed(art_name, "insufficient data")
                    except Exception as exc:
                        manifest.record_failed(art_name, str(exc))
                        print(f"  Failed {art_name}: {exc}")

            # KRI Football-Field Charts — split by unit family
            # Standard: 2 charts (% rates + x-multiple coverage)
            # Normalized: 2 charts (rates + composition)
            _bullet_specs = [
                # Standard family
                ("kri_bullet_standard", BULLET_METRICS_STANDARD_RATES,
                 "Key Risk Indicators — MSPBNA vs Peer Range (Standard Rates)",
                 False, ACTIVE_STANDARD_COMPOSITES),
                ("kri_bullet_standard_coverage", BULLET_METRICS_STANDARD_COVERAGE,
                 "Key Risk Indicators — MSPBNA vs Peer Range (Standard Coverage)",
                 False, ACTIVE_STANDARD_COMPOSITES),
                # Normalized family
                ("kri_bullet_normalized_rates", BULLET_METRICS_NORMALIZED_RATES,
                 "Key Risk Indicators — MSPBNA vs Peer Range (Normalized Rates)",
                 True, ACTIVE_NORMALIZED_COMPOSITES),
                ("kri_bullet_normalized_composition", BULLET_METRICS_NORMALIZED_COMPOSITION,
                 "Key Risk Indicators — MSPBNA vs Peer Range (Normalized Composition)",
                 True, ACTIVE_NORMALIZED_COMPOSITES),
            ]
            for art_name, metric_list, chart_title, is_norm, composites in _bullet_specs:
                if should_produce(art_name, mode, manifest, suppressed_charts):
                    try:
                        save = str(charts_dir / f"{base}_{art_name}.png")
                        fig = generate_kri_bullet_chart(
                            proc_df_with_peers, subject_bank_cert,
                            wealth_cert=composites["core_pb"],
                            all_peers_cert=composites["all_peers"],
                            metrics=metric_list,
                            is_normalized=is_norm,
                            save_path=save,
                            title_override=chart_title,
                            wealth_member_certs=_WEALTH_MEMBER_CERTS,
                            all_peers_member_certs=_ALL_PEERS_MEMBER_CERTS,
                        )
                        if fig is not None:
                            manifest.record_generated(art_name, save)
                            print(f"  Generated: {art_name}")
                            csv_log.log_file_written(save, phase="executive_charts",
                                                     component=art_name)
                        else:
                            manifest.record_failed(art_name, "insufficient data")
                    except Exception as exc:
                        manifest.record_failed(art_name, str(exc))
                        print(f"  Failed {art_name}: {exc}")

            # Sparkline Summary Tables — 4 variants: Standard/Normalized × Wealth/All Peers
            _sparkline_specs = [
                ("standard", "wealth",   SPARKLINE_METRICS_STANDARD,
                 ACTIVE_STANDARD_COMPOSITES["core_pb"],
                 ACTIVE_NORMALIZED_COMPOSITES["core_pb"], "Wealth Peers"),
                ("standard", "allpeers", SPARKLINE_METRICS_STANDARD,
                 ACTIVE_STANDARD_COMPOSITES["all_peers"],
                 ACTIVE_NORMALIZED_COMPOSITES["all_peers"], "All Peers"),
                ("normalized", "wealth", SPARKLINE_METRICS_NORMALIZED,
                 ACTIVE_NORMALIZED_COMPOSITES["core_pb"],
                 ACTIVE_NORMALIZED_COMPOSITES["core_pb"], "Wealth Peers"),
                ("normalized", "allpeers", SPARKLINE_METRICS_NORMALIZED,
                 ACTIVE_NORMALIZED_COMPOSITES["all_peers"],
                 ACTIVE_NORMALIZED_COMPOSITES["all_peers"], "All Peers"),
            ]
            for norm_str, pg_suffix, metric_list, p_cert, np_cert, p_label in _sparkline_specs:
                art_name = f"sparkline_{norm_str}_{pg_suffix}"
                if should_produce(art_name, mode, manifest, suppressed_charts):
                    try:
                        save = str(tables_dir / f"{base}_{art_name}.html")
                        html = generate_sparkline_table(
                            proc_df_with_peers, subject_bank_cert,
                            peer_cert=p_cert,
                            norm_peer_cert=np_cert,
                            metrics=metric_list,
                            save_path=save,
                            peer_label=p_label,
                        )
                        if html:
                            manifest.record_generated(art_name, save)
                            print(f"  Generated: {art_name}")
                            csv_log.log_file_written(save, phase="executive_charts",
                                                     component=art_name)
                        else:
                            manifest.record_failed(art_name, "insufficient data")
                    except Exception as exc:
                        manifest.record_failed(art_name, str(exc))
                        print(f"  Failed {art_name}: {exc}")

            # Cumulative Growth: Target Loans vs CRE ACL — 2 variants
            _cumul_specs = [
                ("cumul_growth_loans_vs_acl_wealth",
                 ACTIVE_STANDARD_COMPOSITES["core_pb"], "Wealth Peers"),
                ("cumul_growth_loans_vs_acl_allpeers",
                 ACTIVE_STANDARD_COMPOSITES["all_peers"], "All Peers"),
            ]
            for art_name, peer_cert, p_label in _cumul_specs:
                if should_produce(art_name, mode, manifest, suppressed_charts):
                    try:
                        save = str(charts_dir / f"{base}_{art_name}.png")
                        result = plot_cumulative_growth_loans_vs_acl(
                            proc_df_with_peers,
                            subject_cert=subject_bank_cert,
                            peer_cert=peer_cert,
                            peer_label=p_label,
                            save_path=save,
                        )
                        if result:
                            manifest.record_generated(art_name, save)
                            print(f"  Generated: {art_name}")
                            csv_log.log_file_written(save, phase="executive_charts",
                                                     component=art_name)
                        else:
                            manifest.record_failed(art_name, "insufficient data")
                    except Exception as exc:
                        manifest.record_failed(art_name, str(exc))
                        print(f"  Failed {art_name}: {exc}")
        else:
            print("\n  Executive charts module not available — skipping")
        print("\n" + "=" * 80)
        print("REPORT GENERATION COMPLETE")
        print("=" * 80)
        print(f"Source file: {excel_file}")
        print(f"Mode: {mode.value}")
        print(f"Charts directory: {charts_dir}")
        print(f"Scatter directory: {scatter_dir}")
        print(f"Tables directory: {tables_dir}")
        print(f"Subject bank CERT: {subject_bank_cert}")
        print(f"Report view: {REPORT_VIEW}")
        print(f"Render mode: {mode.value}")

        # Print artifact manifest summary
        print("\n" + "-" * 60)
        print("ARTIFACT MANIFEST")
        print("-" * 60)
        print(manifest.summary_table())

        return manifest

    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
        try:
            csv_log.log_exception(exc=e, phase="generate_reports", component="main")
        except Exception:
            pass
        return manifest
    finally:
        plt.close("all")
        csv_log.shutdown()

    return manifest


def create_credit_deterioration_chart_ppt(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    start_date: str = "2023-01-01",
    bar_metric: str = "TTM_NCO_Rate",
    line_metric: str = "NPL_to_Gross_Loans_Rate",
    bar_entities: Optional[List[int]] = None,
    line_entities: Optional[List[int]] = None,
    figsize: Tuple[float, float] = (16, 7.5),
    title_size: int = 24,
    axis_label_size: int = 15,
    tick_size: int = 13,
    tag_size: int = 12,
    legend_fontsize: int = 13,
    economist_style: bool = True,
    custom_title: Optional[str] = None,
    save_path: Optional[str] = None,
) -> Tuple[Optional[plt.Figure], Optional[plt.Axes]]:
    if proc_df_with_peers.empty:
        return None, None

    _cc = _build_cert_color_map(subject_bank_cert)
    GOLD, BLUE, PURPLE = _C_MSPBNA, _C_ALL_PEERS, _C_WEALTH
    default_entities = [subject_bank_cert, 90003, 90001]
    bar_entities  = bar_entities  or default_entities
    line_entities = line_entities or list(bar_entities)
    names  = {subject_bank_cert: resolve_display_label(subject_bank_cert, subject_cert=subject_bank_cert),
              90003: resolve_display_label(90003), 90001: resolve_display_label(90001),
              90006: resolve_display_label(90006), 90004: resolve_display_label(90004)}
    colors = _cc

    all_requested = set(bar_entities + line_entities)
    df = proc_df_with_peers.loc[
        proc_df_with_peers["CERT"].isin(all_requested)
    ].copy()
    df = df[df["REPDTE"] >= pd.to_datetime(start_date)].sort_values(["REPDTE","CERT"])
    if df.empty:
        return None, None

    # Filter out CERTs that are entirely missing from data (composite suppressed by coverage rules)
    available_certs = set(df["CERT"].unique())
    for cert in all_requested - available_certs:
        print(f"    [WARNING] CERT {cert} not found in data — skipping from chart.")
    bar_entities  = [c for c in bar_entities if c in available_certs]
    line_entities = [c for c in line_entities if c in available_certs]
    if subject_bank_cert not in available_certs:
        print(f"    [WARNING] Subject bank CERT {subject_bank_cert} missing. Cannot generate chart.")
        return None, None

    df["Period_Label"] = "Q" + df["REPDTE"].dt.quarter.astype(str) + "-" + (df["REPDTE"].dt.year % 100).astype(str).str.zfill(2)
    subj     = df[df["CERT"] == subject_bank_cert][["REPDTE","Period_Label"]].drop_duplicates().sort_values("REPDTE")
    timeline = subj[["REPDTE"]].copy()
    x        = np.arange(len(subj))
    xticks   = subj["Period_Label"].tolist()
    last_dt  = timeline["REPDTE"].max()

    fig = plt.figure(figsize=figsize, constrained_layout=False)
    gs  = GridSpec(nrows=2, ncols=1, height_ratios=[10, 2], hspace=0.08, figure=fig)
    ax  = fig.add_subplot(gs[0, 0]); ax2 = ax.twinx()
    leg_ax = fig.add_subplot(gs[1, 0]); leg_ax.axis("off")
    fig.patch.set_alpha(0)
    for a in (ax, ax2): a.set_facecolor("none"); a.grid(False)
    if economist_style:
        for sp in ["top","right"]:
            ax.spines[sp].set_visible(False); ax2.spines[sp].set_visible(False)
        for s in ax.spines.values():  s.set_linewidth(1.1); s.set_color("#2B2B2B")
        for s in ax2.spines.values(): s.set_linewidth(1.1); s.set_color("#2B2B2B")
        ax.tick_params(axis="both", labelsize=tick_size, colors="#2B2B2B")
        ax2.tick_params(axis="y",   labelsize=tick_size, colors="#2B2B2B")

    def to_decimal(series: pd.Series) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce").astype(float)
        med = s.dropna().abs().median()
        return s/100.0 if (pd.notna(med) and med >= 0.01) else s

    def series_for(cert: int, metric: str) -> pd.Series:
        ed = timeline.merge(df[df["CERT"] == cert][["REPDTE", metric]], on="REPDTE", how="left")
        return to_decimal(ed[metric])

    def lighten(hex_color: str, factor: float = 0.85) -> str:
        h = hex_color.lstrip("#"); r,g,b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        r = int(r + (255-r)*factor); g = int(g + (255-g)*factor); b = int(b + (255-b)*factor)
        return f"#{r:02X}{g:02X}{b:02X}"

    left_vals  = [series_for(c, bar_metric)  for c in bar_entities]
    right_vals = [series_for(c, line_metric) for c in line_entities]
    left_all   = pd.concat(left_vals,  axis=0) if left_vals  else pd.Series([0.0])
    right_all  = pd.concat(right_vals, axis=0) if right_vals else pd.Series([0.0])
    if left_all.notna().any():
        lo, hi = float(left_all.min()), float(left_all.max())
        rng = max(hi - lo, 1e-6)
        ax.set_ylim(lo - 0.18*rng, hi + 0.18*rng)
    else:
        ax.set_ylim(-0.01, 0.01)
    if right_all.notna().any():
        rlo, rhi = float(right_all.min()), float(right_all.max())
        rrng = max(rhi - rlo, 1e-6)
        ax2.set_ylim(rlo - 0.12*rrng, rhi + 0.12*rrng)
    else:
        ax2.set_ylim(-0.01, 0.01)

    n = max(len(bar_entities), 1)
    bar_w   = 0.8 / n
    offsets = {c: (i - (n - 1) / 2) * bar_w for i, c in enumerate(bar_entities)}

    bar_handles, bar_labels, line_handles, line_labels = [], [], [], []
    suppressed_series = []
    for c in bar_entities:
        vals = series_for(c, bar_metric)
        # Suppress all-NaN / low-coverage series: do not plot or add to legend
        if vals.isna().all():
            ent_name = names.get(c, f"CERT {c}")
            suppressed_series.append(f"{ent_name} {bar_metric.replace('_', ' ')}")
            print(f"    [SUPPRESSED] {ent_name} {bar_metric} — all NaN (low coverage)")
            continue
        b = ax.bar(x + offsets[c], vals, width=bar_w, color=colors.get(c, "#7F8C8D"), alpha=0.92,
                   label=f"{names.get(c,f'CERT {c}')} {bar_metric.replace('_',' ')}", zorder=2)
        bar_handles.append(b[0]); bar_labels.append(f"{names.get(c,f'CERT {c}')} {bar_metric.replace('_',' ')}")

    qtr = timeline["REPDTE"].dt.quarter
    idx_to_label = np.where((qtr == 4) | (timeline["REPDTE"] == last_dt))[0]

    DPI = fig.dpi

    def rects_overlap(a, b):
        ax0,ay0,ax1,ay1 = a; bx0,by0,bx1,by1 = b
        return not (ax1 < bx0 or bx1 < ax0 or ay1 < by0 or by1 < ay0)

    class Placer:
        def __init__(self, ax, tag_sz):
            self.ax = ax
            self.tag_sz = tag_sz
            self.fixed_rects = []
            self.items = []

        def _rect_for(self, x_data, y_data, xpx, ypx, text, pad=6):
            xd, yd = self.ax.transData.transform((x_data, y_data))
            cx, cy = xd + xpx, yd + ypx
            w = max(40.0, 0.62*self.tag_sz*len(text)) + 2*pad
            h = self.tag_sz*1.7 + 2*pad
            return (cx - w/2, cy - h/2, cx + w/2, cy + h/2)

        def can_place(self, rect, skip_index: Optional[int] = None):
            for r in self.fixed_rects:
                if rects_overlap(rect, r): return False
            for i, it in enumerate(self.items):
                if skip_index is not None and i == skip_index:
                    continue
                if rects_overlap(rect, it["rect"]): return False
            return True

        def add_fixed(self, rect):
            self.fixed_rects.append(rect)

        def add_line_ann(self, ann, rect):
            self.items.append({"ann": ann, "rect": rect})

        def relax(self, max_iter=30, step_px=7):
            for _ in range(max_iter):
                moved = False
                for i in range(len(self.items)):
                    for j in range(i+1, len(self.items)):
                        ri, rj = self.items[i]["rect"], self.items[j]["rect"]
                        if rects_overlap(ri, rj):
                            for idx, sgn in ((i, +1), (j, -1)):
                                ann = self.items[idx]["ann"]
                                xoff, yoff = ann.get_position()
                                new = (xoff, yoff + sgn*step_px)
                                ann.set_position(new)
                                xd, yd = self.ax.transData.transform(ann.xy)
                                rect = (xd + new[0] - (ri[2]-ri[0])/2,
                                        yd + new[1] - (ri[3]-ri[1])/2,
                                        xd + new[0] + (ri[2]-ri[0])/2,
                                        yd + new[1] + (ri[3]-ri[1])/2)
                                if not self.can_place(rect, skip_index=idx):
                                    ann.set_position((new[0] + (6 if sgn>0 else -6), new[1]))
                                    xd, yd = self.ax.transData.transform(ann.xy)
                                    rect = (xd + ann.get_position()[0] - (ri[2]-ri[0])/2,
                                            yd + ann.get_position()[1] - (ri[3]-ri[1])/2,
                                            xd + ann.get_position()[0] + (ri[2]-ri[0])/2,
                                            yd + ann.get_position()[1] + (ri[3]-ri[1])/2)
                                self.items[idx]["rect"] = rect
                                moved = True
                if not moved:
                    break

        def rect_for_text(self, x_data, y_data, xpx, ypx, text):
            return self._rect_for(x_data, y_data, xpx, ypx, text)

    placer = Placer(ax2, tag_size)

    for c in line_entities:
        s = series_for(c, line_metric)
        if s.isna().all():
            print(f"    [WARNING] CERT {c} has all-NaN for {line_metric}. Skipping peer line.")
            continue
        ln, = ax2.plot(
            x, s, color=colors.get(c, "#7F8C8D"),
            linewidth=2.4, linestyle="-" if c == subject_bank_cert else "--",
            marker="o", markersize=4.4, zorder=3,
            label=f"{names.get(c,f'CERT {c}')} {line_metric.replace('_',' ')}"
        )
        line_handles.append(ln); line_labels.append(f"{names.get(c,f'CERT {c}')} {line_metric.replace('_',' ')}")

    ylo, yhi = ax.get_ylim(); yrng = yhi - ylo
    zero_in_view = (ylo < 0 < yhi)
    baseline = 0.0 if zero_in_view else ylo + 0.02*yrng
    above_gap = 0.016*yrng
    below_gap = 0.018*yrng
    tip_pad   = 0.020*yrng

    def add_fixed_bar_label(text, x_data, y_data):
        ann = ax.annotate(
            text, xy=(x_data, y_data), xytext=(0, 0), textcoords="offset pixels",
            ha="center", va="center", fontsize=tag_size, fontweight="bold",
            color="#2B2B2B", clip_on=False, zorder=9,
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="none", alpha=0.95)
        )
        xd, yd = ax.transData.transform((x_data, y_data))
        w = max(40.0, 0.62*tag_size*len(text)) + 2*6
        h = tag_size*1.7 + 2*6
        placer.add_fixed((xd - w/2, yd - h/2, xd + w/2, yd + h/2))

    for i in idx_to_label:
        for j, c in enumerate(bar_entities):
            s = series_for(c, bar_metric)
            v = s.iloc[i]
            if pd.isna(v):
                continue
            xpos = x[i] + offsets[c]
            desired = (baseline + above_gap) if (j % 2 == 0) else (baseline - below_gap)
            desired = max(ylo + 0.01*yrng, min(yhi - 0.01*yrng, desired))
            add_fixed_bar_label(f"{v:.2%}", xpos, desired)
            if (j % 2 == 0):
                if abs(v - desired) < 1e-9:
                    add_fixed_bar_label(f"{v:.2%}", xpos, (v + tip_pad))

    def place_line_tag(text, xd_data, yd_data, pref_up=True, fc="#FFFFFF"):
        candidates = [(0, 0)]
        base_offsets = [8, 16, 24, 36, 48]
        verticals = base_offsets if pref_up else [-b for b in base_offsets]
        for dy in verticals + [-b for b in base_offsets]:
            candidates += [(0, dy), (12, dy), (-12, dy), (22, dy), (-22, dy)]
        for xpx, ypx in candidates:
            rect = placer.rect_for_text(xd_data, yd_data, xpx, ypx, text)
            if placer.can_place(rect):
                ann = ax2.annotate(
                    text, xy=(xd_data, yd_data), xytext=(xpx, ypx),
                    textcoords="offset pixels", fontsize=tag_size,
                    fontweight="bold", color="#1F2937", zorder=10,
                    bbox=dict(boxstyle="round,pad=0.25", fc=fc, ec="none", alpha=0.96)
                )
                placer.add_line_ann(ann, rect)
                return
        ann = ax2.annotate(
            text, xy=(xd_data, yd_data), xytext=(0, 0),
            textcoords="offset pixels", fontsize=tag_size,
            fontweight="bold", color="#1F2937", zorder=10,
            bbox=dict(boxstyle="round,pad=0.25", fc=fc, ec="none", alpha=0.96)
        )
        xd, yd = ax2.transData.transform((xd_data, yd_data))
        w = max(40.0, 0.62*tag_size*len(text)) + 12
        h = tag_size*1.7 + 12
        placer.add_line_ann(ann, (xd - w/2, yd - h/2, xd + w/2, yd + h/2))

    # Label placement strategy to reduce clutter:
    # - Subject bank: label at every Q4 + latest (full context)
    # - Peer entities: label at latest period ONLY (avoids label overload)
    for c in line_entities:
        s = series_for(c, line_metric)
        label_indices = idx_to_label if c == subject_bank_cert else [idx_to_label[-1]] if idx_to_label else []
        for k in label_indices:
            val = s.iloc[k]
            if pd.notna(val):
                place_line_tag(f"{val:.2%}", x[k], float(val), pref_up=(val >= 0), fc=lighten(colors[c], 0.85))

    placer.relax(max_iter=40, step_px=8)

    ax.set_xticks(x); ax.set_xticklabels(xticks, fontsize=tick_size)
    ax.set_ylabel(bar_metric.replace("_"," "),  fontsize=axis_label_size, fontweight="bold")
    ax2.set_ylabel(line_metric.replace("_"," "), fontsize=axis_label_size, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.2%}"))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.2%}"))

    title = custom_title or f"{bar_metric.replace('_',' ')} (bars) vs {line_metric.replace('_',' ')} (lines)"
    fig.text(0.5, 0.97, title, ha="center", va="top",
             fontsize=title_size, fontweight="bold", color="#2B2B2B")
    if suppressed_series:
        fig.text(0.5, 0.93, f"Suppressed (low coverage): {', '.join(suppressed_series)}",
                 ha="center", va="top", fontsize=10, color="#7F8C8D", style="italic")

    handles = bar_handles + line_handles
    labels  = bar_labels  + line_labels
    leg = leg_ax.legend(handles, labels, ncol=3, loc="center", frameon=True,
                        columnspacing=2.0, handlelength=2.2, fontsize=legend_fontsize)
    leg.get_frame().set_alpha(0.96)

    # tight_layout call removed — conflicts with twinx() + GridSpec, causing
    # noisy warnings. savefig(..., bbox_inches="tight") handles layout instead.
    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig, ax


def plot_scatter_dynamic(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    subject_cert: int = 34221,
    peer_avg_cert_primary: int = 90003,
    peer_avg_cert_alt: int = 90001,
    wealth_peer_cert: Optional[int] = None,
    use_alt_peer_avg: bool = False,
    composite_certs: Optional[set] = None,
    show_peers_avg_label: bool = True,
    show_mspbna_label: bool = True,
    identify_outliers: bool = True,
    outliers_topn: int = 2,
    figsize: Tuple[float, float] = (6.0, 6.0),
    title_size: int = 18,
    axis_label_size: int = 12,
    tick_size: int = 12,
    tag_size: int = 12,
    economist_style: bool = True,
    transparent_bg: bool = True,
    square_axes: bool = True,
    save_path: Optional[str] = None
) -> Tuple[plt.Figure, plt.Axes]:
    if df.empty: raise ValueError("scatter DF is empty")
    GOLD, PEER, GUIDE = _C_MSPBNA, _C_PEER_CLOUD, _C_GUIDE
    CC = CHART_COLORS

    def to_decimals_series(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        if s.dropna().max() > 0.05:
            return s/100.0
        return s

    # Default composite exclusion set: all synthetic CERTs that must never appear as "peer" dots
    if composite_certs is None:
        composite_certs = ALL_COMPOSITE_CERTS

    peers_cert = peer_avg_cert_alt if use_alt_peer_avg else peer_avg_cert_primary
    # Wealth Peers = the "alt" peer when primary is All Peers, or vice versa
    wealth_cert = peer_avg_cert_alt if not use_alt_peer_avg else peer_avg_cert_primary
    df = df.copy()
    df[x_col] = pd.to_numeric(df[x_col], errors="coerce")
    df[y_col] = pd.to_numeric(df[y_col], errors="coerce")

    fig, ax = plt.subplots(figsize=figsize)
    if transparent_bg: fig.patch.set_alpha(0); ax.set_facecolor("none")

    if economist_style:
        for sp in ["top","right"]: ax.spines[sp].set_visible(False)
        for s in ax.spines.values(): s.set_linewidth(1.1); s.set_color("#2B2B2B")
        ax.tick_params(axis="both", labelsize=tick_size, colors="#2B2B2B")
        ax.grid(True, color="#D0D0D0", linewidth=0.8, alpha=0.35)

    # Exclude subject + ALL composite/alias CERTs from peer dots
    exclude_set = composite_certs | {subject_cert}
    mspbna = df[df["CERT"] == subject_cert]
    peer_avg = df[df["CERT"] == peers_cert]
    wealth_avg = df[df["CERT"] == wealth_cert]
    others = df[~df["CERT"].isin(exclude_set)]

    Xo, Yo = to_decimals_series(others[x_col]), to_decimals_series(others[y_col])
    ax.scatter(Xo, Yo, s=42, alpha=0.7, color=CC["peer_cloud"], edgecolor="white",
               linewidth=0.6, label="Peers")

    xi = yi = None
    if not mspbna.empty:
        xi = float(to_decimals_series(mspbna[x_col]).iloc[0])
        yi = float(to_decimals_series(mspbna[y_col]).iloc[0])
        ax.scatter(xi, yi, s=80, color=CC["subject"], edgecolor="black",
                   linewidth=0.7, label="MSPBNA", zorder=5)

    # Wealth Peers point (purple diamond)
    wx = wy = None
    if not wealth_avg.empty:
        wx = float(to_decimals_series(wealth_avg[x_col]).iloc[0])
        wy = float(to_decimals_series(wealth_avg[y_col]).iloc[0])
        ax.scatter(wx, wy, s=80, color=CC["wealth_peers"], marker="D",
                   edgecolor="black", linewidth=0.7, label="Wealth Peers", zorder=4)

    # All Peers point (blue square) + guide lines
    px = py = None
    if not peer_avg.empty:
        px = float(to_decimals_series(peer_avg[x_col]).iloc[0])
        py = float(to_decimals_series(peer_avg[y_col]).iloc[0])
        ax.scatter(px, py, s=90, color=_C_ALL_PEERS, marker="s", edgecolor="black", linewidth=0.7, label="All Peers")
        ax.axvline(px, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)
        ax.axhline(py, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)

    # Wealth Peers marker (explicit styled point, not implied in peer cloud)
    wx = wy = None
    if wealth_peer_cert is not None:
        wealth_row = df[df["CERT"] == wealth_peer_cert]
        if not wealth_row.empty:
            wx = float(to_decimals_series(wealth_row[x_col]).iloc[0])
            wy = float(to_decimals_series(wealth_row[y_col]).iloc[0])
            ax.scatter(wx, wy, s=90, color=_C_WEALTH, marker="^", edgecolor="black",
                       linewidth=0.7, zorder=4, label="Wealth Peers")

    SUFFIX_RE = re.compile(r"(\s*,?\s*THE)?(\s*\(.*?\))?(\s+(NATIONAL(\s+ASSOCIATION|(\s+ASSN\.?))|N\.?A\.?|NA|FEDERAL(\s+SAVINGS\s+BANK)?|SAVINGS\s+BANK|STATE\s+BANK|NATIONAL\s+BANK|BANK|BANCORP(?:ORATION)?|CORP(?:ORATION)?|COMPANY|CO\.?|INC|INC\.?|LTD\.?|LIMITED|FSB|F\.S\.B\.?|ASSOCIATION|ASSN\.?))+\s*$", re.IGNORECASE)
    def short_name(s): s=str(s).strip(); s=re.sub(r"\s+"," ",s); return SUFFIX_RE.sub("", s).strip(", ").strip() or s

    placed=[]
    def pick_offset(px_, py_, along_line=False):
        cands = ([(12,0), (18,0), (-42,0), (-60,0)] if along_line
                 else [(10,12),(12,-12),(-10,12),(-12,-12),(16,0),(-16,0),(0,16),(0,-16)])
        for dx,dy in cands:
            sx,sy = ax.transData.transform((px_,py_)); tx,ty = sx+dx, sy+dy
            if all(abs(tx-ox)>=58 or abs(ty-oy)>=22 for ox,oy in placed):
                placed.append((tx,ty)); return dx,dy
        placed.append(ax.transData.transform((px_,py_))); return (10,12)

    def tag(px_, py_, text, xytext, color="black", box=True):
        ax.annotate(text, xy=(px_,py_), xytext=xytext, textcoords="offset points",
                    fontsize=tag_size, fontweight="bold", color=color,
                    bbox=(dict(boxstyle="round,pad=0.25", fc="white", ec="black", lw=0.6, alpha=0.95) if box else None),
                    arrowprops=(dict(arrowstyle="->", lw=1.0, color="black") if box else None), va="center")

    if show_peers_avg_label and (px is not None):
        placer.place(px, py, "All Peers", color=CC["guide"], box=False, inline=True, priority=1)

    if show_mspbna_label and (xi is not None):
        placer.place(xi, yi, "MSPBNA", color="black", box=True, priority=2)

    if wx is not None:
        placer.place(wx, wy, "Wealth Peers", color=CC["wealth_peers"], box=True, priority=3)

    if identify_outliers and outliers_topn > 0:
        X_all = to_decimals_series(df[x_col]); Y_all = to_decimals_series(df[y_col])
        cx = px if px is not None else float(X_all.mean())
        cy = py if py is not None else float(Y_all.mean())
        d = ((X_all - cx)**2 + (Y_all - cy)**2)**0.5
        mask_excl = df["CERT"].isin(exclude_set)
        cand = d[~mask_excl].sort_values(ascending=False)

        top_idx = list(cand.index[:outliers_topn+1])
        if not mspbna.empty and int(mspbna.index[0]) in top_idx:
            top_idx = [i for i in top_idx if i != int(mspbna.index[0])][:1]
        else:
            top_idx = top_idx[:outliers_topn]

        for i in top_idx:
            ox = float(to_decimals_series(pd.Series([df.loc[i, x_col]])).iloc[0])
            oy = float(to_decimals_series(pd.Series([df.loc[i, y_col]])).iloc[0])
            cert_i = int(df.loc[i, "CERT"]) if "CERT" in df.columns else None
            name_i = df.loc[i, "NAME"] if "NAME" in df.columns else None
            label = resolve_display_label(cert_i, name=name_i, subject_cert=subject_cert) if cert_i is not None else str(i)
            placer.place(ox, oy, label, color="black", box=True, priority=10)

    _extra_x = [s for s in [xi, px, wx] if s is not None]
    _extra_y = [s for s in [yi, py, wy] if s is not None]
    all_x = pd.concat([Xo] + [pd.Series([v]) for v in _extra_x], ignore_index=True).dropna()
    all_y = pd.concat([Yo] + [pd.Series([v]) for v in _extra_y], ignore_index=True).dropna()
    def padded(s):
        lo, hi = float(s.min()), float(s.max()); rng = max(hi-lo, 1e-6); pad = 0.10*rng
        return max(0.0, lo-pad), hi+pad
    xlo,xhi = padded(all_x); ylo,yhi = padded(all_y)
    ax.set_xlim(xlo,xhi); ax.set_ylim(ylo,yhi)
    if square_axes: ax.set_aspect("equal", adjustable="box")

    ax.set_xlabel(x_col.replace("_"," "), fontsize=axis_label_size, fontweight="bold", color="#2B2B2B")
    ax.set_ylabel(y_col.replace("_"," "), fontsize=axis_label_size, fontweight="bold", color="#2B2B2B")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1%}"))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1%}"))
    ax.set_title(f"{y_col.replace('_',' ')} vs {x_col.replace('_',' ')} — 8Q Avg", fontsize=title_size,
                 fontweight="bold", color="#2B2B2B")

    leg = ax.legend(loc="lower right", frameon=True, fontsize=tick_size); leg.get_frame().set_alpha(0.96)
    plt.tight_layout()
    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig, ax


# ==================================================================================
# SEGMENT-LEVEL CHART FUNCTIONS
# ==================================================================================

def plot_portfolio_mix(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Stacked area chart showing portfolio composition over time for the subject bank."""
    series_cols = ["SBL_Composition", "Wealth_Resi_Composition",
                   "CRE_Investment_Composition", "CRE_OO_Composition"]
    available = [c for c in series_cols if c in df.columns]
    if not available:
        print("  Skipped portfolio mix: no composition columns found")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped portfolio mix: no subject bank data")
        return None

    subj = subj.sort_values("REPDTE")
    for c in available:
        subj[c] = pd.to_numeric(subj[c], errors="coerce").fillna(0)

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    colors = [_C_MSPBNA, _C_ALL_PEERS, _C_WEALTH, _C_GUIDE]
    labels = [c.replace("_Composition", "").replace("_", " ") for c in available]

    ax.stackplot(
        subj["REPDTE"],
        *[subj[c] for c in available],
        labels=labels,
        colors=colors[:len(available)],
        alpha=0.85,
    )

    ax.set_title("Portfolio Mix Shift Over Time", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.set_xlabel("Reporting Period", fontsize=13, fontweight="bold")
    ax.set_ylabel("Share (%)", fontsize=13, fontweight="bold")
    ax.legend(loc="upper left", frameon=True, fontsize=11)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_problem_asset_attribution(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Stacked bar chart showing nonaccrual rate attribution by segment over time."""
    series_cols = ["RIC_CRE_Nonaccrual_Rate", "RIC_Resi_Nonaccrual_Rate",
                   "RIC_Comm_Nonaccrual_Rate", "RIC_Constr_Nonaccrual_Rate"]
    available = [c for c in series_cols if c in df.columns]
    if not available:
        print("  Skipped problem-asset attribution: no RIC nonaccrual columns found")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped problem-asset attribution: no subject bank data")
        return None

    subj = subj.sort_values("REPDTE")
    for c in available:
        subj[c] = pd.to_numeric(subj[c], errors="coerce").fillna(0)

    subj["Period_Label"] = "Q" + subj["REPDTE"].dt.quarter.astype(str) + "-" + \
                           (subj["REPDTE"].dt.year % 100).astype(str).str.zfill(2)

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    colors = ["#E74C3C", "#3498DB", "#F39C12", "#9B59B6"]
    x = np.arange(len(subj))
    bottom = np.zeros(len(subj))

    for i, col in enumerate(available):
        label = col.replace("RIC_", "").replace("_Nonaccrual_Rate", "").replace("_", " ")
        vals = subj[col].values
        ax.bar(x, vals, bottom=bottom, label=label, color=colors[i % len(colors)], alpha=0.85, width=0.7)
        bottom += vals

    # Show every 4th label
    tick_labels = ["" if i % 4 != 0 else lbl for i, lbl in enumerate(subj["Period_Label"])]
    ax.set_xticks(x)
    ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=10)

    ax.set_title("Problem Asset Attribution by Segment", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.set_xlabel("Reporting Period", fontsize=13, fontweight="bold")
    ax.set_ylabel("Nonaccrual Rate", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2%}"))
    ax.legend(loc="upper left", frameon=True, fontsize=11)
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_reserve_risk_allocation(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Grouped bar chart comparing ACL share vs Loan share for the latest quarter."""
    # Discover available segment pairs (RIC_*_ACL_Share vs RIC_*_Loan_Share)
    acl_cols = [c for c in df.columns if c.startswith("RIC_") and c.endswith("_ACL_Share")]
    segments = []
    for acl_col in acl_cols:
        seg = acl_col.replace("RIC_", "").replace("_ACL_Share", "")
        loan_col = f"RIC_{seg}_Loan_Share"
        if loan_col in df.columns:
            segments.append((seg, acl_col, loan_col))

    if not segments:
        print("  Skipped reserve-risk allocation: no matching ACL/Loan share pairs found")
        return None

    latest_date = df["REPDTE"].max()
    subj = df[(df["CERT"] == subject_bank_cert) & (df["REPDTE"] == latest_date)]
    if subj.empty:
        print("  Skipped reserve-risk allocation: no subject bank data for latest quarter")
        return None
    subj = subj.iloc[0]

    seg_labels, acl_vals, loan_vals = [], [], []
    for seg, acl_col, loan_col in segments:
        a = pd.to_numeric(subj.get(acl_col, np.nan), errors="coerce")
        l = pd.to_numeric(subj.get(loan_col, np.nan), errors="coerce")
        if pd.notna(a) and pd.notna(l):
            seg_labels.append(seg.replace("_", " "))
            acl_vals.append(float(a))
            loan_vals.append(float(l))

    if not seg_labels:
        print("  Skipped reserve-risk allocation: all values are N/A")
        return None

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    x = np.arange(len(seg_labels))
    width = 0.35
    ax.bar(x - width / 2, acl_vals, width, label="Share of ACL", color=_C_MSPBNA, alpha=0.85)
    ax.bar(x + width / 2, loan_vals, width, label="Share of Loans", color=_C_ALL_PEERS, alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(seg_labels, rotation=30, ha="right", fontsize=11)
    ax.set_title("Reserve Allocation vs Risk Exposure", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.set_ylabel("Share (%)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.legend(loc="upper right", frameon=True, fontsize=12)
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_migration_ladder(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Comparative line chart: early-warning migration ladder for MSPBNA,
    Wealth Peers, and All Peers.

    Plots each metric as a solid line for the subject bank and dashed lines
    for Wealth Peers (Core PB, 90001) and All Peers (90003).
    """
    series_map = {
        "TTM_PD30_Rate": ("Past Due 30-90d", _C_ALL_PEERS),
        "TTM_PD90_Rate": ("Past Due 90d+", _C_MSPBNA),
        "Norm_Nonaccrual_Rate": ("Nonaccrual", _C_WEALTH),
        "TTM_NCO_Rate": ("NCO", _C_TEXT),
    }
    available = {k: v for k, v in series_map.items() if k in df.columns}
    if not available:
        print("  Skipped migration ladder: no pipeline columns found")
        return None

    # Entities: subject + composites
    wealth_cert = ACTIVE_STANDARD_COMPOSITES["core_pb"]   # 90001
    all_peers_cert = ACTIVE_STANDARD_COMPOSITES["all_peers"]  # 90003
    entity_map = {
        subject_bank_cert: ("MSPBNA", "-", 2.4),
        wealth_cert: ("Wealth Peers", "--", 1.6),
        all_peers_cert: ("All Peers", ":", 1.6),
    }

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped migration ladder: no subject bank data")
        return None

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    for cert, (ent_label, lstyle, lw) in entity_map.items():
        ent = df[df["CERT"] == cert].copy()
        if ent.empty:
            continue
        ent = ent.sort_values("REPDTE")
        for c in available:
            ent[c] = pd.to_numeric(ent[c], errors="coerce")
        for col, (metric_label, color) in available.items():
            vals = ent[col]
            if vals.isna().all():
                continue
            label = f"{ent_label} — {metric_label}" if cert == subject_bank_cert else None
            ax.plot(ent["REPDTE"], vals, color=color, linewidth=lw,
                    linestyle=lstyle, marker="o" if cert == subject_bank_cert else None,
                    markersize=4, label=label, alpha=1.0 if cert == subject_bank_cert else 0.6)

    # Add entity legend entries (one per entity for line-style identification)
    from matplotlib.lines import Line2D
    entity_handles = [
        Line2D([0], [0], color="black", linewidth=2.4, linestyle="-", label="MSPBNA"),
        Line2D([0], [0], color="black", linewidth=1.6, linestyle="--", label="Wealth Peers"),
        Line2D([0], [0], color="black", linewidth=1.6, linestyle=":", label="All Peers"),
    ]
    metric_handles = [
        Line2D([0], [0], color=color, linewidth=2.0, label=label)
        for _, (label, color) in available.items()
    ]
    ax.legend(handles=metric_handles + entity_handles, loc="upper left",
              frameon=True, fontsize=10, ncol=2)

    ax.set_title("Early-Warning Migration Ladder — MSPBNA vs Peers",
                 fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.set_xlabel("Reporting Period", fontsize=13, fontweight="bold")
    ax.set_ylabel("Rate", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2%}"))
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


# ==================================================================================
# ROADMAP CHART FUNCTIONS
# ==================================================================================

def _economist_ax(ax):
    """Apply economist-style formatting to an axes."""
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    for s in ax.spines.values():
        s.set_linewidth(1.1)
        s.set_color("#2B2B2B")
    ax.tick_params(axis="both", colors="#2B2B2B")
    ax.grid(True, alpha=0.3, color="#D0D0D0")


def plot_years_of_reserves(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Lollipop chart showing years-of-reserves by loan segment for the latest quarter."""
    reserve_cols = {k: k.replace("RIC_", "").replace("_Years_of_Reserves", "")
                    for k in df.columns if k.endswith("_Years_of_Reserves")}
    if not reserve_cols:
        print("  Skipped years-of-reserves: no RIC_*_Years_of_Reserves columns found")
        return None

    CC = CHART_COLORS
    latest_date = df["REPDTE"].max()
    subj = df[(df["CERT"] == subject_bank_cert) & (df["REPDTE"] == latest_date)]
    if subj.empty:
        print("  Skipped years-of-reserves: no subject bank data")
        return None
    subj = subj.iloc[0]

    # Get All Peers (90003) and Wealth Peers (90001) for comparison
    all_peer = df[(df["CERT"] == ACTIVE_STANDARD_COMPOSITES["all_peers"]) & (df["REPDTE"] == latest_date)]
    all_peer = all_peer.iloc[0] if not all_peer.empty else None
    wealth_peer = df[(df["CERT"] == ACTIVE_STANDARD_COMPOSITES["core_pb"]) & (df["REPDTE"] == latest_date)]
    wealth_peer = wealth_peer.iloc[0] if not wealth_peer.empty else None

    segments, subj_vals, all_peer_vals, wealth_peer_vals = [], [], [], []
    for col, seg_label in reserve_cols.items():
        sv = pd.to_numeric(subj.get(col, np.nan), errors="coerce")
        if pd.notna(sv):
            segments.append(seg_label.replace("_", " "))
            subj_vals.append(float(sv))
            apv = pd.to_numeric(all_peer.get(col, np.nan), errors="coerce") if all_peer is not None else np.nan
            all_peer_vals.append(float(apv) if pd.notna(apv) else 0.0)
            wpv = pd.to_numeric(wealth_peer.get(col, np.nan), errors="coerce") if wealth_peer is not None else np.nan
            wealth_peer_vals.append(float(wpv) if pd.notna(wpv) else 0.0)

    if not segments:
        print("  Skipped years-of-reserves: all values N/A")
        return None

    fig, ax = plt.subplots(figsize=(12, max(5, len(segments) * 0.8 + 2)))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    y = np.arange(len(segments))
    # Lollipop: horizontal stems + dots
    ax.hlines(y, 0, subj_vals, color=_C_MSPBNA, linewidth=2.5, zorder=2)
    ax.scatter(subj_vals, y, color=_C_MSPBNA, s=100, zorder=3, label="MSPBNA")
    if any(v > 0 for v in wealth_peer_vals):
        ax.scatter(wealth_peer_vals, y, color=_C_WEALTH, s=80, marker="^", zorder=3, label="Wealth Peers")
    if any(v > 0 for v in all_peer_vals):
        ax.scatter(all_peer_vals, y, color=_C_ALL_PEERS, s=80, marker="D", zorder=3, label="All Peers")

    ax.set_yticks(y)
    ax.set_yticklabels(segments, fontsize=12)
    ax.set_xlabel("Years of Reserves", fontsize=13, fontweight="bold")
    # Conditional title: if only CRE segment is available, name it specifically
    if len(segments) == 1 and "CRE" in segments[0].upper():
        title = "CRE Years of Reserves"
    else:
        title = "Years of Reserves by Segment"
    ax.set_title(title, fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.legend(loc="lower right", frameon=True, fontsize=11)
    ax.invert_yaxis()
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_growth_vs_deterioration(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Scatter plot: loan growth TTM (x) vs NCO rate (y) for peers, with MSPBNA highlighted."""
    # Try multiple growth column names
    growth_col = None
    for cand in ["MSPBNA_CRE_Growth_TTM", "CRE_Growth_TTM", "Loan_Growth_TTM", "Total_Loan_Growth_TTM"]:
        if cand in df.columns:
            growth_col = cand
            break
    nco_col = "TTM_NCO_Rate"

    if growth_col is None or nco_col not in df.columns:
        print("  Skipped growth-vs-deterioration: missing growth or NCO columns")
        return None

    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date].copy()
    latest[growth_col] = pd.to_numeric(latest[growth_col], errors="coerce")
    latest[nco_col] = pd.to_numeric(latest[nco_col], errors="coerce")
    latest = latest.dropna(subset=[growth_col, nco_col])
    if latest.empty:
        print("  Skipped growth-vs-deterioration: no valid data")
        return None

    CC = CHART_COLORS
    # Exclude composite CERTs from the scatter cloud
    composite = ALL_COMPOSITE_CERTS
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]
    wealth = latest[latest["CERT"] == ACTIVE_STANDARD_COMPOSITES["core_pb"]]
    all_peers_row = latest[latest["CERT"] == ACTIVE_STANDARD_COMPOSITES["all_peers"]]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    ax.scatter(peers[growth_col], peers[nco_col], s=50, alpha=0.7, color=_C_PEER_CLOUD,
               edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[growth_col], subj[nco_col], s=120, color=_C_MSPBNA,
                   edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")
    if not wealth.empty:
        ax.scatter(wealth[growth_col], wealth[nco_col], s=90, color=_C_WEALTH,
                   marker="^", edgecolor="black", linewidth=0.7, zorder=4, label="Wealth Peers")
    if not all_peers_row.empty:
        ax.scatter(all_peers_row[growth_col], all_peers_row[nco_col], s=90, color=_C_ALL_PEERS,
                   marker="s", edgecolor="black", linewidth=0.7, zorder=4, label="All Peers")

    # Quadrant lines at medians
    mx = latest[growth_col].median()
    my = latest[nco_col].median()
    ax.axvline(mx, linestyle="--", color=_C_GUIDE, alpha=0.7, linewidth=1)
    ax.axhline(my, linestyle="--", color=_C_GUIDE, alpha=0.7, linewidth=1)

    ax.set_xlabel(growth_col.replace("_", " "), fontsize=13, fontweight="bold")
    ax.set_ylabel("TTM NCO Rate", fontsize=13, fontweight="bold")
    ax.set_title("Growth vs Deterioration Quadrant", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.legend(loc="upper right", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_growth_vs_deterioration_bookwide(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Bookwide growth vs deterioration: total loan growth TTM (x) vs TTM NCO Rate (y).

    Uses the pre-computed Total_Loans_Growth_TTM column from the data engine
    (trailing-4Q growth on LNLS).  Falls back to NPL_to_Gross_Loans_Rate for
    y-axis if TTM_NCO_Rate is unavailable.
    """
    # X-axis: bookwide loan growth (pre-computed by MSPBNA_CR_Normalized.py)
    growth_col = None
    for cand in ["Total_Loans_Growth_TTM", "Total_Loan_Growth_TTM", "Loan_Growth_TTM"]:
        if cand in df.columns:
            growth_col = cand
            break
    if growth_col is None:
        print("  Skipped growth-vs-deterioration-bookwide: no Total_Loans_Growth_TTM column")
        return None

    # Y-axis: prefer TTM_NCO_Rate, fall back to NPL_to_Gross_Loans_Rate
    y_col = None
    for cand in ["TTM_NCO_Rate", "NPL_to_Gross_Loans_Rate"]:
        if cand in df.columns:
            y_col = cand
            break
    if y_col is None:
        print("  Skipped growth-vs-deterioration-bookwide: no deterioration column")
        return None

    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date].copy()
    latest[growth_col] = pd.to_numeric(latest[growth_col], errors="coerce")
    latest[y_col] = pd.to_numeric(latest[y_col], errors="coerce")
    latest = latest.dropna(subset=[growth_col, y_col])
    if latest.empty:
        print("  Skipped growth-vs-deterioration-bookwide: no valid data")
        return None
    composite = ALL_COMPOSITE_CERTS
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]
    wealth = latest[latest["CERT"] == ACTIVE_STANDARD_COMPOSITES["core_pb"]]
    all_peers_row = latest[latest["CERT"] == ACTIVE_STANDARD_COMPOSITES["all_peers"]]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    ax.scatter(peers[growth_col], peers[y_col], s=50, alpha=0.7, color=_C_PEER_CLOUD,
               edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[growth_col], subj[y_col], s=120, color=_C_MSPBNA,
                   edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")
    if not wealth.empty:
        ax.scatter(wealth[growth_col], wealth[y_col], s=90, color=_C_WEALTH,
                   marker="^", edgecolor="black", linewidth=0.7, zorder=4, label="Wealth Peers")
    if not all_peers_row.empty:
        ax.scatter(all_peers_row[growth_col], all_peers_row[y_col], s=90, color=_C_ALL_PEERS,
                   marker="s", edgecolor="black", linewidth=0.7, zorder=4, label="All Peers")

    # Quadrant lines at medians
    mx = latest[growth_col].median()
    my = latest[y_col].median()
    ax.axvline(mx, linestyle="--", color=_C_GUIDE, alpha=0.7, linewidth=1)
    ax.axhline(my, linestyle="--", color=_C_GUIDE, alpha=0.7, linewidth=1)

    # Format x-axis as %
    from matplotlib.ticker import FuncFormatter
    ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x*100:.1f}%"))

    y_label = "TTM NCO Rate" if y_col == "TTM_NCO_Rate" else "NPL to Gross Loans Rate"
    ax.set_xlabel("Bookwide Loan Growth (Trailing 4Q)", fontsize=13, fontweight="bold")
    ax.set_ylabel(y_label, fontsize=13, fontweight="bold")
    ax.set_title("Growth vs Deterioration — Bookwide", fontsize=18, fontweight="bold", color=_C_TEXT)
    ax.legend(loc="upper right", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_risk_adjusted_return(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Bubble scatter: Norm_Loss_Adj_Yield (x) vs Norm_Risk_Adj_Return (y), bubble size = Norm_NCO_Rate."""
    x_col = "Norm_Loss_Adj_Yield"
    y_col = "Norm_Risk_Adj_Return"
    size_col = "Norm_NCO_Rate"

    missing = [c for c in [x_col, y_col, size_col] if c not in df.columns]
    if missing:
        print(f"  Skipped risk-adjusted return: missing columns {missing}")
        return None

    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date].copy()
    for c in [x_col, y_col, size_col]:
        latest[c] = pd.to_numeric(latest[c], errors="coerce")
    latest = latest.dropna(subset=[x_col, y_col])
    if latest.empty:
        print("  Skipped risk-adjusted return: no valid data")
        return None

    CC = CHART_COLORS
    composite = ALL_COMPOSITE_CERTS
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]
    wealth = latest[latest["CERT"] == ACTIVE_NORMALIZED_COMPOSITES["core_pb"]]
    all_peers_row = latest[latest["CERT"] == ACTIVE_NORMALIZED_COMPOSITES["all_peers"]]

    # Bubble sizes: scale NCO rate to reasonable dot sizes
    def bubble_size(s):
        s = s.fillna(0).abs()
        if s.max() > 0:
            return 50 + (s / s.max()) * 400
        return pd.Series([100] * len(s), index=s.index)

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    if not peers.empty:
        ax.scatter(peers[x_col], peers[y_col], s=bubble_size(peers[size_col]),
                   alpha=0.55, color=_C_PEER_CLOUD, edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[x_col].values, subj[y_col].values,
                   s=bubble_size(subj[size_col]).values,
                   color=_C_MSPBNA, edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")
    if not wealth.empty:
        ax.scatter(wealth[x_col].values, wealth[y_col].values,
                   s=bubble_size(wealth[size_col]).values if size_col in wealth.columns else [100],
                   color=_C_WEALTH, marker="^", edgecolor="black", linewidth=0.7, zorder=4, label="Wealth Peers")
    if not all_peers_row.empty:
        ax.scatter(all_peers_row[x_col].values, all_peers_row[y_col].values,
                   s=bubble_size(all_peers_row[size_col]).values if size_col in all_peers_row.columns else [100],
                   color=_C_ALL_PEERS, marker="s", edgecolor="black", linewidth=0.7, zorder=4, label="All Peers")

    ax.set_xlabel("Norm Loss-Adjusted Yield", fontsize=13, fontweight="bold")
    ax.set_ylabel("Norm Risk-Adjusted Return", fontsize=13, fontweight="bold")
    ax.set_title("Risk-Adjusted Return Frontier\n(bubble size = Norm NCO Rate)",
                 fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.legend(loc="upper left", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_concentration_vs_capital(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Quadrant scatter: CRE Concentration Risk (x) vs C&I to Capital Risk (y)."""
    x_col = "CRE_Concentration_Capital_Risk"
    y_col = "CI_to_Capital_Risk"

    if x_col not in df.columns or y_col not in df.columns:
        print(f"  Skipped concentration-vs-capital: missing {x_col} or {y_col}")
        return None

    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date].copy()
    latest[x_col] = pd.to_numeric(latest[x_col], errors="coerce")
    latest[y_col] = pd.to_numeric(latest[y_col], errors="coerce")
    latest = latest.dropna(subset=[x_col, y_col])
    if latest.empty:
        print("  Skipped concentration-vs-capital: no valid data")
        return None

    CC = CHART_COLORS
    composite = ALL_COMPOSITE_CERTS
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]
    wealth = latest[latest["CERT"] == ACTIVE_STANDARD_COMPOSITES["core_pb"]]
    all_peers_row = latest[latest["CERT"] == ACTIVE_STANDARD_COMPOSITES["all_peers"]]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    ax.scatter(peers[x_col], peers[y_col], s=50, alpha=0.7, color=_C_PEER_CLOUD,
               edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[x_col].values, subj[y_col].values, s=120, color=_C_MSPBNA,
                   edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")
    if not wealth.empty:
        ax.scatter(wealth[x_col].values, wealth[y_col].values, s=90, color=_C_WEALTH,
                   marker="^", edgecolor="black", linewidth=0.7, zorder=4, label="Wealth Peers")
    if not all_peers_row.empty:
        ax.scatter(all_peers_row[x_col].values, all_peers_row[y_col].values, s=90, color=_C_ALL_PEERS,
                   marker="s", edgecolor="black", linewidth=0.7, zorder=4, label="All Peers")

    # Quadrant lines at medians
    mx = latest[x_col].median()
    my = latest[y_col].median()
    ax.axvline(mx, linestyle="--", color=_C_GUIDE, alpha=0.7, linewidth=1)
    ax.axhline(my, linestyle="--", color=_C_GUIDE, alpha=0.7, linewidth=1)

    # Quadrant labels
    xlims, ylims = ax.get_xlim(), ax.get_ylim()
    ax.text(xlims[1], ylims[1], "High CRE + High C&I", ha="right", va="top",
            fontsize=9, color=_C_GUIDE, style="italic")
    ax.text(xlims[0], ylims[0], "Low CRE + Low C&I", ha="left", va="bottom",
            fontsize=9, color=_C_GUIDE, style="italic")

    ax.set_xlabel("CRE Concentration / Capital Risk (%)", fontsize=13, fontweight="bold")
    ax.set_ylabel("C&I / Capital Risk (%)", fontsize=13, fontweight="bold")
    ax.set_title("Concentration vs Capital Sensitivity", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.legend(loc="upper left", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_liquidity_overlay(
    df: pd.DataFrame,
    subject_bank_cert: int,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Combo chart: Loans-to-Deposits, Liquidity Ratio, HQLA Ratio over time for the subject bank."""
    series_map = {
        "Loans_to_Deposits": ("Loans / Deposits", _C_MSPBNA, "-"),
        "Liquidity_Ratio": ("Liquidity Ratio", _C_ALL_PEERS, "--"),
        "HQLA_Ratio": ("HQLA Ratio", _C_WEALTH, "-."),
    }
    available = {k: v for k, v in series_map.items() if k in df.columns}
    if not available:
        print("  Skipped liquidity overlay: no liquidity columns found")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped liquidity overlay: no subject bank data")
        return None
    subj = subj.sort_values("REPDTE")
    for c in available:
        subj[c] = pd.to_numeric(subj[c], errors="coerce")

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    for col, (label, color, ls) in available.items():
        ax.plot(subj["REPDTE"], subj[col], label=label, color=color,
                linewidth=2.2, linestyle=ls, marker="o", markersize=4)

    ax.set_xlabel("Reporting Period", fontsize=13, fontweight="bold")
    ax.set_ylabel("Ratio (%)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.0%}"))
    ax.set_title("Liquidity / Draw-Risk Overlay", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.legend(loc="upper left", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


# =====================================================================
# Macro Series Constants — deterministic, no heuristic fallback
# =====================================================================

# Exact required FRED series IDs for macro charts
MACRO_CORR_INTERNAL_METRICS = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
    "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
    "RIC_CRE_Nonaccrual_Rate", "RIC_CRE_NCO_Rate", "RIC_CRE_ACL_Coverage",
]

MACRO_CORR_FRED_SERIES = [
    "FEDFUNDS", "T10Y2Y", "BAMLH0A0HYM2", "VIXCLS", "NFCI",
    "STLFSI4", "DRTSCILM", "DRALACBS", "DRCRELEXFACBS", "DRSFRMACBS",
    "MORTGAGE30US", "HOUST", "CSUSHPISA",
]

# Human-readable display names for FRED series
_FRED_DISPLAY = {
    "FEDFUNDS": "Fed Funds", "T10Y2Y": "10Y-2Y Spread",
    "BAMLH0A0HYM2": "HY OAS", "VIXCLS": "VIX", "NFCI": "NFCI",
    "STLFSI4": "St. Louis FSI", "DRTSCILM": "C&I Standards",
    "DRALACBS": "All Loans Delinq", "DRCRELEXFACBS": "CRE Delinq",
    "DRSFRMACBS": "Resi Delinq", "MORTGAGE30US": "30Y Mortgage",
    "HOUST": "Housing Starts", "CSUSHPISA": "Case-Shiller (SA)",
}


def _fred_to_quarterly(fred_df: pd.DataFrame, series_id: str) -> pd.Series:
    """Extract a single FRED series and resample to quarter-end frequency.

    Returns a Series indexed by quarterly period-end dates.
    """
    sel = fred_df[fred_df["SeriesID"] == series_id].copy()
    if sel.empty:
        return pd.Series(dtype=float)
    sel = sel.sort_values("DATE")
    sel["VALUE"] = pd.to_numeric(sel["VALUE"], errors="coerce")
    sel = sel.dropna(subset=["VALUE"]).set_index("DATE")
    # Resample to quarter-end, taking the last available observation per quarter
    qtr = sel["VALUE"].resample("QE").last().dropna()
    return qtr


def generate_macro_corr_heatmap(
    df: pd.DataFrame,
    subject_cert: int,
    excel_file: str,
    trailing_quarters: int = 20,
    internal_metrics: Optional[list] = None,
    fred_series: Optional[list] = None,
    save_path: Optional[str] = None,
) -> Optional[str]:
    """Generate a self-contained HTML correlation heatmap.

    Rows = internal bank metrics, Columns = FRED macro series.
    Macro series are lagged +1 quarter relative to internal metrics
    (i.e., macro Q(t-1) correlated with bank metric Q(t)).
    Pearson correlation over trailing window.  Insufficient overlap → N/A.
    """
    if internal_metrics is None:
        internal_metrics = MACRO_CORR_INTERNAL_METRICS
    if fred_series is None:
        fred_series = MACRO_CORR_FRED_SERIES

    # --- Load internal bank data ---
    if "REPDTE" not in df.columns:
        return None
    subj = df[df["CERT"] == subject_cert].copy()
    if subj.empty:
        print("  Skipped macro_corr_heatmap: no subject bank data")
        return None
    subj["REPDTE"] = pd.to_datetime(subj["REPDTE"])
    subj = subj.sort_values("REPDTE").set_index("REPDTE")
    available_internal = [m for m in internal_metrics if m in subj.columns]
    if not available_internal:
        print("  Skipped macro_corr_heatmap: no internal metrics in data")
        return None

    # --- Load FRED data ---
    try:
        fred_raw, desc = _load_fred_tables(excel_file)
    except Exception as e:
        print(f"  Skipped macro_corr_heatmap: {e}")
        return None

    # Build quarterly FRED DataFrame
    fred_q_dict = {}
    for sid in fred_series:
        qtr = _fred_to_quarterly(fred_raw, sid)
        if not qtr.empty:
            fred_q_dict[sid] = qtr
    if not fred_q_dict:
        print("  Skipped macro_corr_heatmap: no FRED series available")
        return None
    fred_q = pd.DataFrame(fred_q_dict)

    # --- Align: lag FRED by +1 quarter (shift FRED index forward by 1Q) ---
    fred_q_lagged = fred_q.copy()
    fred_q_lagged.index = fred_q_lagged.index + pd.DateOffset(months=3)

    # Restrict internal metrics to trailing window
    dates = sorted(subj.index.unique())
    cutoff_dates = dates[-trailing_quarters:] if len(dates) >= trailing_quarters else dates
    subj_trail = subj.loc[subj.index.isin(cutoff_dates), available_internal]

    # Align indices
    common_idx = subj_trail.index.intersection(fred_q_lagged.index)
    if len(common_idx) < 4:
        print(f"  Skipped macro_corr_heatmap: only {len(common_idx)} overlapping quarters (need ≥4)")
        return None

    sub_aligned = subj_trail.loc[common_idx]
    fred_aligned = fred_q_lagged.loc[common_idx]

    # --- Compute Pearson correlations ---
    from metric_semantics import get_semantic
    import scipy.stats as stats

    n_rows = len(available_internal)
    n_cols = len(fred_q_dict)
    corr_data = []
    for m in available_internal:
        sem = get_semantic(m)
        label = sem.display_name if sem else m.replace("_", " ")
        row = {"metric": label}
        m_vals = pd.to_numeric(sub_aligned[m], errors="coerce")
        for sid in fred_q_dict:
            f_vals = pd.to_numeric(fred_aligned[sid], errors="coerce") if sid in fred_aligned.columns else pd.Series(dtype=float)
            valid = m_vals.notna() & f_vals.notna()
            n_valid = valid.sum()
            if n_valid >= 4:
                r, _ = stats.pearsonr(m_vals[valid], f_vals[valid])
                row[sid] = r
            else:
                row[sid] = None  # N/A — insufficient overlap
        corr_data.append(row)

    # --- Build HTML ---
    fred_cols = list(fred_q_dict.keys())
    date_str = common_idx.max().strftime("%Y Q%q").replace("Q%q", f"Q{(common_idx.max().month - 1) // 3 + 1}") if hasattr(common_idx.max(), 'strftime') else str(common_idx.max())

    def _corr_color(v):
        """Return background color for a correlation value."""
        if v is None:
            return "#F5F5F5"
        if v > 0.7:
            return "#C62828"  # strong positive (red = risk comovement)
        if v > 0.4:
            return "#EF9A9A"  # moderate positive
        if v > 0.2:
            return "#FFCDD2"  # weak positive
        if v > -0.2:
            return "#F5F5F5"  # negligible
        if v > -0.4:
            return "#C8E6C9"  # weak negative
        if v > -0.7:
            return "#81C784"  # moderate negative
        return "#2E7D32"  # strong negative (green = counter-cyclical)

    def _corr_text_color(v):
        if v is None:
            return "#999"
        return "#FFF" if abs(v) > 0.7 else "#333"

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; }}
        .corr-container {{ max-width: 1100px; margin: 0 auto; padding: 20px; }}
        h3 {{ color: #002F6C; text-align: center; margin-bottom: 5px; }}
        p.subtitle {{ text-align: center; color: #555; font-size: 11px; margin-top: 0; }}
        table {{ border-collapse: collapse; font-size: 10px; width: 100%; }}
        th {{ background-color: #002F6C; color: white; padding: 5px 6px;
              border: 1px solid #1a3a5c; text-align: center; font-size: 9px;
              white-space: nowrap; }}
        th.metric-hdr {{ text-align: left; min-width: 160px; }}
        td {{ padding: 4px 6px; border: 1px solid #e0e0e0; text-align: center;
              font-variant-numeric: tabular-nums; font-weight: 600; }}
        td.metric-name {{ text-align: left; font-weight: 700; color: #2c3e50;
                          white-space: nowrap; background: #f8f9fa; }}
        .legend {{ margin-top: 10px; font-size: 10px; color: #555; text-align: center; }}
    </style></head><body>
    <div class="corr-container">
        <h3>Macro–Credit Correlation Heatmap (Lag +1Q)</h3>
        <p class="subtitle">Pearson ρ | Trailing {trailing_quarters}Q window ending {date_str} | Macro series lagged one quarter</p>
        <table>
        <thead><tr>
            <th class="metric-hdr">Internal Metric</th>
"""
    for sid in fred_cols:
        disp = _FRED_DISPLAY.get(sid, sid)
        html += f'            <th>{disp}</th>\n'
    html += "        </tr></thead>\n        <tbody>\n"

    for row in corr_data:
        html += f'        <tr><td class="metric-name">{row["metric"]}</td>\n'
        for sid in fred_cols:
            v = row.get(sid)
            bg = _corr_color(v)
            tc = _corr_text_color(v)
            cell = f"{v:+.2f}" if v is not None else "N/A"
            html += f'            <td style="background:{bg};color:{tc};">{cell}</td>\n'
        html += "        </tr>\n"

    html += """        </tbody></table>
        <div class="legend">
            Color scale: <span style="color:#2E7D32;">■</span> strong negative (ρ &lt; −0.7) →
            <span style="color:#81C784;">■</span> moderate negative →
            neutral →
            <span style="color:#EF9A9A;">■</span> moderate positive →
            <span style="color:#C62828;">■</span> strong positive (ρ &gt; 0.7) |
            N/A = insufficient overlap (&lt;4 quarters)
        </div>
    </div></body></html>"""

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
    return html


def plot_macro_overlay_credit_stress(
    df: pd.DataFrame,
    subject_bank_cert: int,
    excel_file: str,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Dual-axis chart: MSPBNA Norm_NCO_Rate (left) vs BAMLH0A0HYM2 & NFCI z-scores (right).

    Series selection is deterministic — BAMLH0A0HYM2 (HY OAS) and NFCI only.
    Right-axis series are z-scored over the plotted window for readability.
    Title explicitly names the series used.
    """
    nco_col = "Norm_NCO_Rate"
    if nco_col not in df.columns:
        print("  Skipped macro_overlay_credit_stress: Norm_NCO_Rate not in data")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped macro_overlay_credit_stress: no subject bank data")
        return None
    subj = subj.sort_values("REPDTE")
    subj["REPDTE"] = pd.to_datetime(subj["REPDTE"])
    subj[nco_col] = pd.to_numeric(subj[nco_col], errors="coerce")

    try:
        fred_raw, desc = _load_fred_tables(excel_file)
    except Exception as e:
        print(f"  Skipped macro_overlay_credit_stress: {e}")
        return None

    # Extract quarterly FRED series — deterministic IDs, no fallback
    hy_oas = _fred_to_quarterly(fred_raw, "BAMLH0A0HYM2")
    nfci = _fred_to_quarterly(fred_raw, "NFCI")
    if hy_oas.empty and nfci.empty:
        print("  Skipped macro_overlay_credit_stress: neither BAMLH0A0HYM2 nor NFCI available")
        return None

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    # Left axis: MSPBNA Norm NCO Rate
    ax.plot(subj["REPDTE"], subj[nco_col], color="#F7A81B", linewidth=2.5,
            marker="o", markersize=4, label="MSPBNA Norm NCO Rate", zorder=3)
    ax.set_ylabel("Norm NCO Rate", fontsize=13, fontweight="bold", color="#F7A81B")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2%}"))

    # Right axis: z-scored stress indicators
    ax2 = ax.twinx()
    right_series = []
    colors_right = {"BAMLH0A0HYM2": "#4C78A8", "NFCI": "#E15759"}
    labels_right = {"BAMLH0A0HYM2": "HY OAS (z)", "NFCI": "NFCI (z)"}
    for sid, qtr_data in [("BAMLH0A0HYM2", hy_oas), ("NFCI", nfci)]:
        if qtr_data.empty:
            continue
        # Z-score over the plotted window
        mu = qtr_data.mean()
        sigma = qtr_data.std()
        if sigma > 0:
            z = (qtr_data - mu) / sigma
        else:
            z = qtr_data * 0.0
        ax2.plot(z.index, z.values, color=colors_right[sid], linewidth=1.8,
                 linestyle="--", alpha=0.85, label=labels_right[sid])
        right_series.append(sid)

    ax2.set_ylabel("Z-Score", fontsize=13, fontweight="bold", color="#4C78A8")
    for sp in ["top"]:
        ax2.spines[sp].set_visible(False)
    ax2.axhline(0, color="#888", linewidth=0.5, linestyle=":")

    ax.set_xlabel("Date", fontsize=13, fontweight="bold")
    series_names = " + ".join(labels_right[s].replace(" (z)", "") for s in right_series)
    ax.set_title(f"Credit Stress Overlay: Norm NCO Rate vs {series_names}",
                 fontsize=16, fontweight="bold", color="#2B2B2B")

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_macro_overlay_rates_housing(
    df: pd.DataFrame,
    subject_bank_cert: int,
    excel_file: str,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Dual-axis chart: Resi credit quality (left) vs rates & housing macro (right).

    Left axis: RIC_Resi_Nonaccrual_Rate if present, else Norm_Nonaccrual_Rate.
    Right axis: FEDFUNDS, MORTGAGE30US, CSUSHPISA (converted to YoY % change).
    All series quarterly-aligned. Title names the actual selected series.
    """
    # Determine left-axis metric
    left_col = None
    for candidate in ["RIC_Resi_Nonaccrual_Rate", "Norm_Nonaccrual_Rate"]:
        if candidate in df.columns:
            left_col = candidate
            break
    if left_col is None:
        print("  Skipped macro_overlay_rates_housing: no resi/norm nonaccrual metric")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped macro_overlay_rates_housing: no subject bank data")
        return None
    subj = subj.sort_values("REPDTE")
    subj["REPDTE"] = pd.to_datetime(subj["REPDTE"])
    subj[left_col] = pd.to_numeric(subj[left_col], errors="coerce")

    try:
        fred_raw, desc = _load_fred_tables(excel_file)
    except Exception as e:
        print(f"  Skipped macro_overlay_rates_housing: {e}")
        return None

    # Extract quarterly FRED series — deterministic IDs
    fedfunds = _fred_to_quarterly(fred_raw, "FEDFUNDS")
    mortgage30 = _fred_to_quarterly(fred_raw, "MORTGAGE30US")
    csushpisa_raw = _fred_to_quarterly(fred_raw, "CSUSHPISA")

    # Convert CSUSHPISA to YoY % change
    csushpisa_yoy = csushpisa_raw.pct_change(4) * 100 if len(csushpisa_raw) > 4 else pd.Series(dtype=float)
    csushpisa_yoy = csushpisa_yoy.dropna()

    if fedfunds.empty and mortgage30.empty and csushpisa_yoy.empty:
        print("  Skipped macro_overlay_rates_housing: no FRED macro series available")
        return None

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    left_label = left_col.replace("_", " ")
    ax.plot(subj["REPDTE"], subj[left_col], color="#F7A81B", linewidth=2.5,
            marker="o", markersize=4, label=f"MSPBNA {left_label}", zorder=3)
    ax.set_ylabel(left_label, fontsize=13, fontweight="bold", color="#F7A81B")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2%}"))

    ax2 = ax.twinx()
    right_plotted = []
    macro_specs = [
        ("FEDFUNDS", fedfunds, "#4C78A8", "-", "Fed Funds Rate (%)"),
        ("MORTGAGE30US", mortgage30, "#70AD47", "--", "30Y Mortgage (%)"),
        ("CSUSHPISA", csushpisa_yoy, "#9C6FB6", "-.", "Case-Shiller YoY (%)"),
    ]
    for sid, series, color, ls, label in macro_specs:
        if series.empty:
            continue
        ax2.plot(series.index, series.values, color=color, linewidth=1.8,
                 linestyle=ls, alpha=0.85, label=label)
        right_plotted.append(label)

    ax2.set_ylabel("Rate / YoY %", fontsize=13, fontweight="bold", color="#4C78A8")
    for sp in ["top"]:
        ax2.spines[sp].set_visible(False)

    ax.set_xlabel("Date", fontsize=13, fontweight="bold")
    right_names = ", ".join(right_plotted)
    ax.set_title(f"Rates & Housing Overlay: {left_label} vs {right_names}",
                 fontsize=14, fontweight="bold", color="#2B2B2B")

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", frameon=True, fontsize=10)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


# ==================================================================================
# SCRIPT EXECUTION
# ==================================================================================

if __name__ == "__main__":
    import sys as _sys
    # CLI: python report_generator.py [full_local|corp_safe]
    _cli_mode = _sys.argv[1] if len(_sys.argv) > 1 else None
    generate_reports(render_mode=_cli_mode)
