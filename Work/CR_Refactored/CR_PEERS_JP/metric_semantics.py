#!/usr/bin/env python3
"""
Metric Semantic Layer
======================

Extends the metric registry with presentation-level semantics needed by
executive charts (heatmaps, bullet charts, sparkline tables).

This module does **not** duplicate the validation/dependency logic in
``metric_registry.py``.  It adds a thin, non-overlapping layer:

  * **polarity** — is an increase in this metric favorable or adverse?
  * **delta_format** — how should YoY changes be displayed?
  * **display_format** — how should the absolute value be displayed?
  * **thresholds** — optional advisory bands for bullet-chart ranges.
  * **group** — logical grouping for heatmap row ordering.

The canonical truth for *formula*, *dependencies*, *bounds*, and *consumers*
remains in ``metric_registry.py``.
"""

from __future__ import annotations

import dataclasses
import enum
from typing import Dict, Optional, Tuple


# =====================================================================
# Polarity Enum
# =====================================================================

class Polarity(enum.Enum):
    """Whether an increase in the metric is favorable or adverse."""
    FAVORABLE = "favorable"    # higher is better (e.g., ACL coverage)
    ADVERSE = "adverse"        # higher is worse  (e.g., NCO rate, nonaccrual)
    NEUTRAL = "neutral"        # context-dependent or size metric (e.g., ASSET)


# =====================================================================
# Display / Delta Format
# =====================================================================

class DisplayFormat(enum.Enum):
    PERCENT = "pct"            # 0.0125 → "1.25%"
    BASIS_POINTS = "bps"       # 0.0125 → "125 bps"
    DOLLARS_B = "dollars_b"    # 254706000 (FDIC $K) → "$254.7B"
    MULTIPLE = "x"             # 1.23 → "1.23x"
    RATIO = "ratio"            # plain decimal (years, counts)


# =====================================================================
# Metric Semantic Spec
# =====================================================================

@dataclasses.dataclass(frozen=True)
class MetricSemantic:
    """Presentation-layer metadata for a single metric."""
    code: str
    display_name: str
    polarity: Polarity
    display_format: DisplayFormat = DisplayFormat.PERCENT
    delta_format: DisplayFormat = DisplayFormat.BASIS_POINTS
    group: str = "Other"
    # Optional threshold bands for bullet charts: (poor, watch, target, good)
    # Each tuple is (lower_bound, upper_bound).  None = no band.
    threshold_bands: Optional[Tuple[
        Tuple[float, float],   # poor
        Tuple[float, float],   # watch
        Tuple[float, float],   # target/acceptable
    ]] = None

    def direction_label(self, delta: float) -> str:
        """Return 'favorable' / 'adverse' / 'flat' for a given delta."""
        if abs(delta) < 1e-8:
            return "flat"
        if self.polarity == Polarity.NEUTRAL:
            return "neutral"
        positive_is_good = (self.polarity == Polarity.FAVORABLE)
        if (delta > 0) == positive_is_good:
            return "favorable"
        return "adverse"

    def css_class(self, delta: float) -> str:
        """Return a CSS class name for conditional formatting."""
        label = self.direction_label(delta)
        return {
            "favorable": "good-trend",
            "adverse": "bad-trend",
            "flat": "neutral-trend",
            "neutral": "neutral-trend",
        }[label]


# =====================================================================
# Semantic Registry
# =====================================================================

METRIC_SEMANTICS: Dict[str, MetricSemantic] = {}


def _sem(code: str, display_name: str, polarity: Polarity,
         display_format: DisplayFormat = DisplayFormat.PERCENT,
         delta_format: DisplayFormat = DisplayFormat.BASIS_POINTS,
         group: str = "Other",
         threshold_bands=None) -> MetricSemantic:
    spec = MetricSemantic(
        code=code, display_name=display_name, polarity=polarity,
        display_format=display_format, delta_format=delta_format,
        group=group, threshold_bands=threshold_bands,
    )
    METRIC_SEMANTICS[code] = spec
    return spec


# ── Credit Quality (Standard) ────────────────────────────────────────

_sem("TTM_NCO_Rate", "NCO Rate (TTM)", Polarity.ADVERSE,
     group="Credit Quality",
     threshold_bands=((0.005, 0.999), (0.002, 0.005), (0.0, 0.002)))
_sem("Nonaccrual_to_Gross_Loans_Rate", "Nonaccrual Rate", Polarity.ADVERSE,
     group="Credit Quality",
     threshold_bands=((0.005, 0.999), (0.002, 0.005), (0.0, 0.002)))
_sem("NPL_to_Gross_Loans_Rate", "NPL Rate", Polarity.ADVERSE,
     group="Credit Quality")
_sem("Past_Due_Rate", "Delinquency Rate (30+)", Polarity.ADVERSE,
     group="Credit Quality")

# ── Credit Quality (Normalized) ──────────────────────────────────────

_sem("Norm_NCO_Rate", "Norm NCO Rate", Polarity.ADVERSE,
     group="Credit Quality (Norm)",
     threshold_bands=((0.005, 0.999), (0.002, 0.005), (0.0, 0.002)))
_sem("Norm_Nonaccrual_Rate", "Norm Nonaccrual Rate", Polarity.ADVERSE,
     group="Credit Quality (Norm)")
_sem("Norm_Delinquency_Rate", "Norm Delinquency Rate", Polarity.ADVERSE,
     group="Credit Quality (Norm)")

# ── Coverage & Reserves ──────────────────────────────────────────────

_sem("Allowance_to_Gross_Loans_Rate", "Headline ACL Ratio", Polarity.FAVORABLE,
     group="Coverage",
     threshold_bands=((0.0, 0.005), (0.005, 0.01), (0.01, 0.999)))
_sem("Risk_Adj_Allowance_Coverage", "Risk-Adj ACL Ratio", Polarity.FAVORABLE,
     group="Coverage")
_sem("Norm_ACL_Coverage", "Norm ACL Ratio", Polarity.FAVORABLE,
     group="Coverage")
_sem("Norm_Risk_Adj_Allowance_Coverage", "Norm Risk-Adj ACL Ratio", Polarity.FAVORABLE,
     group="Coverage")
_sem("RIC_CRE_ACL_Coverage", "CRE ACL Coverage", Polarity.FAVORABLE,
     group="Coverage")
_sem("RIC_CRE_Risk_Adj_Coverage", "CRE NPL Coverage", Polarity.FAVORABLE,
     display_format=DisplayFormat.MULTIPLE, delta_format=DisplayFormat.MULTIPLE,
     group="Coverage")
_sem("RIC_Resi_Risk_Adj_Coverage", "Resi NPL Coverage", Polarity.FAVORABLE,
     display_format=DisplayFormat.MULTIPLE, delta_format=DisplayFormat.MULTIPLE,
     group="Coverage")

# ── Composition ──────────────────────────────────────────────────────

_sem("SBL_Composition", "SBL % of Loans", Polarity.NEUTRAL, group="Composition")
_sem("Norm_SBL_Composition", "Norm SBL % of Loans", Polarity.NEUTRAL, group="Composition")
_sem("Norm_Wealth_Resi_Composition", "Norm Resi % of Loans", Polarity.NEUTRAL, group="Composition")
_sem("Norm_CRE_Investment_Composition", "Norm CRE % of Loans", Polarity.NEUTRAL, group="Composition")
_sem("RIC_CRE_Loan_Share", "CRE % of Loans", Polarity.NEUTRAL, group="Composition")
_sem("RIC_Resi_Loan_Share", "Resi % of Loans", Polarity.NEUTRAL, group="Composition")
_sem("RIC_CRE_ACL_Share", "CRE % of ACL", Polarity.NEUTRAL, group="Composition")
_sem("RIC_Resi_ACL_Share", "Resi % of ACL", Polarity.NEUTRAL, group="Composition")
_sem("Norm_CRE_ACL_Share", "Norm CRE % of ACL", Polarity.NEUTRAL, group="Composition")
_sem("Norm_Resi_ACL_Share", "Norm Resi % of ACL", Polarity.NEUTRAL, group="Composition")

# ── CRE Segment ──────────────────────────────────────────────────────

_sem("RIC_CRE_Nonaccrual_Rate", "CRE Nonaccrual Rate", Polarity.ADVERSE,
     group="CRE Segment")
_sem("RIC_CRE_NCO_Rate", "CRE NCO Rate (TTM)", Polarity.ADVERSE,
     group="CRE Segment")
_sem("RIC_CRE_Delinquency_Rate", "CRE Delinquency Rate", Polarity.ADVERSE,
     group="CRE Segment")

# ── Resi Segment ─────────────────────────────────────────────────────

_sem("RIC_Resi_Nonaccrual_Rate", "Resi Nonaccrual Rate", Polarity.ADVERSE,
     group="Resi Segment")
_sem("RIC_Resi_NCO_Rate", "Resi NCO Rate (TTM)", Polarity.ADVERSE,
     group="Resi Segment")
_sem("RIC_Resi_Delinquency_Rate", "Resi Delinquency Rate", Polarity.ADVERSE,
     group="Resi Segment")

# ── Size / Scale (descriptive) ───────────────────────────────────────

_sem("ASSET", "Total Assets", Polarity.NEUTRAL,
     display_format=DisplayFormat.DOLLARS_B, delta_format=DisplayFormat.DOLLARS_B,
     group="Size")
_sem("LNLS", "Total Loans", Polarity.NEUTRAL,
     display_format=DisplayFormat.DOLLARS_B, delta_format=DisplayFormat.DOLLARS_B,
     group="Size")
_sem("Norm_Gross_Loans", "Norm Gross Loans", Polarity.NEUTRAL,
     display_format=DisplayFormat.DOLLARS_B, delta_format=DisplayFormat.DOLLARS_B,
     group="Size")

# ── Capital Concentration ───────────────────────────────────────────

_sem("CRE_Concentration_Capital_Risk", "CRE / Tier 1 Capital", Polarity.ADVERSE,
     display_format=DisplayFormat.MULTIPLE, delta_format=DisplayFormat.MULTIPLE,
     group="Coverage")
_sem("CI_to_Capital_Risk", "C&I / Tier 1 Capital", Polarity.ADVERSE,
     display_format=DisplayFormat.MULTIPLE, delta_format=DisplayFormat.MULTIPLE,
     group="Coverage")


# =====================================================================
# Convenience: lookup with fallback
# =====================================================================

def get_semantic(code: str) -> Optional[MetricSemantic]:
    """Return MetricSemantic for *code*, or None if not registered."""
    return METRIC_SEMANTICS.get(code)


def get_polarity(code: str) -> Polarity:
    """Return polarity for *code*, defaulting to NEUTRAL."""
    sem = METRIC_SEMANTICS.get(code)
    return sem.polarity if sem else Polarity.NEUTRAL


def get_direction(code: str, delta: float) -> str:
    """Return 'favorable'/'adverse'/'flat'/'neutral' for a delta."""
    sem = METRIC_SEMANTICS.get(code)
    if sem is None:
        return "neutral"
    return sem.direction_label(delta)


def get_css_class(code: str, delta: float) -> str:
    """Return CSS class for conditional formatting of a delta."""
    sem = METRIC_SEMANTICS.get(code)
    if sem is None:
        return "neutral-trend"
    return sem.css_class(delta)


# =====================================================================
# Group ordering for heatmap rows
# =====================================================================

GROUP_ORDER = [
    "Credit Quality",
    "Credit Quality (Norm)",
    "Coverage",
    "CRE Segment",
    "Resi Segment",
    "Composition",
    "Size",
    "Other",
]

def ordered_metrics(codes: list[str]) -> list[str]:
    """Sort metric codes by group order, then by display name within group."""
    def _key(code):
        sem = METRIC_SEMANTICS.get(code)
        if sem is None:
            return (len(GROUP_ORDER), code)
        try:
            gidx = GROUP_ORDER.index(sem.group)
        except ValueError:
            gidx = len(GROUP_ORDER)
        return (gidx, sem.display_name)
    return sorted(codes, key=_key)
