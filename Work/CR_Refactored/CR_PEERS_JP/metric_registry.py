"""
Metric Registry & Validation Engine
=====================================

Central registry for all derived metrics in the MSPBNA Credit Risk pipeline.
Each metric is formally specified via a ``MetricSpec`` dataclass that captures:

- **dependencies** — upstream columns required to compute the metric
- **compute** — a lambda that recomputes the metric from raw columns
- **unit** — semantic type (fraction, dollars, count, multiple, years)
- **bounds** — optional min/max sanity checks
- **consumers** — downstream charts/tables that depend on this metric

The validation engine (``run_upstream_validation_suite``) recomputes each metric
from its declared formula, compares against the stored value, and flags mismatches.
Results are written to the ``Data_Validation_Report`` sheet in the Excel output.
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

@dataclass
class MetricSpec:
    code: str
    dependencies: List[str]
    compute: Callable[[pd.DataFrame], pd.Series]
    unit: str = "fraction"   # fraction, dollars, count, multiple, years
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allow_negative: bool = False
    consumers: List[str] = field(default_factory=list)
    severity: str = "high"   # high, medium, low


# ---------------------------------------------------------------------------
# Safe division (mirrors upstream safe_div but returns NaN instead of 0)
# ---------------------------------------------------------------------------

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    return np.where((pd.notna(d)) & (d != 0), n / d, np.nan)


# ---------------------------------------------------------------------------
# Derived Metric Specifications
# ---------------------------------------------------------------------------

DERIVED_METRIC_SPECS: Dict[str, MetricSpec] = {

    # ── Standard Credit Quality ──────────────────────────────────────────
    "Risk_Adj_Allowance_Coverage": MetricSpec(
        code="Risk_Adj_Allowance_Coverage",
        dependencies=["Total_ACL", "Gross_Loans", "SBL_Balance"],
        compute=lambda df: pd.Series(
            _safe_div(df["Total_ACL"], df["Gross_Loans"] - df["SBL_Balance"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["executive_summary", "standard_table", "ratio_components_standard"],
    ),
    "Allowance_to_Gross_Loans_Rate": MetricSpec(
        code="Allowance_to_Gross_Loans_Rate",
        dependencies=["Total_ACL", "Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Total_ACL"], df["Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=0.50,
        consumers=["executive_summary", "standard_table", "ratio_components_standard"],
    ),
    "Nonaccrual_to_Gross_Loans_Rate": MetricSpec(
        code="Nonaccrual_to_Gross_Loans_Rate",
        dependencies=["Total_Nonaccrual", "Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Total_Nonaccrual"], df["Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=0.50,
        consumers=["executive_summary", "standard_credit_chart", "standard_table"],
    ),
    "SBL_Composition": MetricSpec(
        code="SBL_Composition",
        dependencies=["SBL_Balance", "Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["SBL_Balance"], df["Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["executive_summary", "ratio_components_standard", "segment_focus"],
    ),

    # ── Normalized Credit Quality ────────────────────────────────────────
    "Norm_NCO_Rate": MetricSpec(
        code="Norm_NCO_Rate",
        dependencies=["Norm_Total_NCO", "Norm_Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Norm_Total_NCO"], df["Norm_Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=-0.10,
        max_value=0.25,
        allow_negative=True,
        consumers=["normalized_credit_chart_bar", "normalized_table", "detailed_peer_table"],
    ),
    "Norm_Nonaccrual_Rate": MetricSpec(
        code="Norm_Nonaccrual_Rate",
        dependencies=["Norm_Total_Nonaccrual", "Norm_Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Norm_Total_Nonaccrual"], df["Norm_Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=0.50,
        consumers=["normalized_credit_chart_line", "normalized_table"],
    ),
    "Norm_ACL_Coverage": MetricSpec(
        code="Norm_ACL_Coverage",
        dependencies=["Total_ACL", "Norm_Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Total_ACL"], df["Norm_Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["normalized_table", "ratio_components_normalized"],
    ),

    # ── Segment: CRE ────────────────────────────────────────────────────
    "RIC_CRE_ACL_Coverage": MetricSpec(
        code="RIC_CRE_ACL_Coverage",
        dependencies=["RIC_CRE_ACL", "RIC_CRE_Cost"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_CRE_ACL"], df["RIC_CRE_Cost"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["executive_summary", "segment_focus_cre"],
    ),
    "RIC_CRE_Risk_Adj_Coverage": MetricSpec(
        code="RIC_CRE_Risk_Adj_Coverage",
        dependencies=["RIC_CRE_ACL", "RIC_CRE_Nonaccrual"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_CRE_ACL"], df["RIC_CRE_Nonaccrual"]),
            index=df.index
        ),
        unit="multiple",
        min_value=0.0,
        consumers=["executive_summary", "segment_focus_cre", "detailed_peer_table"],
    ),
    "RIC_CRE_Nonaccrual_Rate": MetricSpec(
        code="RIC_CRE_Nonaccrual_Rate",
        dependencies=["RIC_CRE_Nonaccrual", "RIC_CRE_Cost"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_CRE_Nonaccrual"], df["RIC_CRE_Cost"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["segment_focus_cre", "detailed_peer_table"],
    ),

    # ── Segment: Resi ───────────────────────────────────────────────────
    "RIC_Resi_ACL_Coverage": MetricSpec(
        code="RIC_Resi_ACL_Coverage",
        dependencies=["RIC_Resi_ACL", "RIC_Resi_Cost"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_Resi_ACL"], df["RIC_Resi_Cost"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["segment_focus_resi", "ratio_components_standard"],
    ),
    "RIC_Resi_Nonaccrual_Rate": MetricSpec(
        code="RIC_Resi_Nonaccrual_Rate",
        dependencies=["RIC_Resi_Nonaccrual", "RIC_Resi_Cost"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_Resi_Nonaccrual"], df["RIC_Resi_Cost"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["segment_focus_resi"],
    ),

    # ── Upstream Choke-Point Placeholders ────────────────────────────────
    # These are *source* columns, not derived.  Registered so the reverse-
    # dependency graph knows who depends on them.
    "Norm_Gross_Loans": MetricSpec(
        code="Norm_Gross_Loans",
        dependencies=["Gross_Loans", "Excluded_Balance"],
        compute=lambda df: pd.Series(
            (pd.to_numeric(df["Gross_Loans"], errors="coerce")
             - pd.to_numeric(df["Excluded_Balance"], errors="coerce")).clip(lower=0),
            index=df.index
        ),
        unit="dollars",
        min_value=0.0,
        consumers=[],
        severity="high",
    ),
    "Total_ACL": MetricSpec(
        code="Total_ACL",
        dependencies=["LNATRES"],
        compute=lambda df: pd.to_numeric(df.get("LNATRES", 0), errors="coerce").fillna(0),
        unit="dollars",
        min_value=0.0,
        consumers=[],
        severity="high",
    ),
    "Gross_Loans": MetricSpec(
        code="Gross_Loans",
        dependencies=["LNLS"],
        compute=lambda df: pd.to_numeric(df.get("LNLS", 0), errors="coerce"),
        unit="dollars",
        min_value=0.0,
        consumers=[],
        severity="high",
    ),
    "SBL_Balance": MetricSpec(
        code="SBL_Balance",
        dependencies=["RCFD1545"],
        compute=lambda df: pd.to_numeric(
            df.get("RCFD1545", df.get("RCON1545", 0)), errors="coerce"
        ).fillna(0),
        unit="dollars",
        min_value=0.0,
        consumers=[],
        severity="medium",
    ),
    "Total_Nonaccrual": MetricSpec(
        code="Total_Nonaccrual",
        dependencies=[],
        compute=lambda df: pd.to_numeric(df.get("Total_Nonaccrual", 0), errors="coerce").fillna(0),
        unit="dollars",
        min_value=0.0,
        consumers=[],
        severity="medium",
    ),
}


# ---------------------------------------------------------------------------
# Report Consumer Map (metric → list of downstream charts/tables)
# ---------------------------------------------------------------------------

REPORT_CONSUMER_MAP: Dict[str, List[str]] = {}
for _code, _spec in DERIVED_METRIC_SPECS.items():
    REPORT_CONSUMER_MAP[_code] = _spec.consumers


# ═══════════════════════════════════════════════════════════════════════════
# VALIDATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def validate_metric_series(
    df: pd.DataFrame,
    spec: MetricSpec,
    atol: float = 1e-10,
    rtol: float = 1e-6,
) -> pd.DataFrame:
    """Row-level validation: recompute metric from formula, compare to stored value."""
    out = pd.DataFrame(index=df.index)
    actual = pd.to_numeric(df.get(spec.code), errors="coerce")

    # Check that all dependency columns exist before computing
    missing_deps = [d for d in spec.dependencies if d not in df.columns]
    if missing_deps:
        out["Metric_Code"] = spec.code
        out["Expected"] = np.nan
        out["Actual"] = actual
        out["Abs_Error"] = np.nan
        out["Rel_Error"] = np.nan
        out["Formula_Pass"] = np.nan
        out["Min_Bound_Pass"] = True
        out["Max_Bound_Pass"] = True
        out["Negative_Pass"] = True
        out["Dependencies"] = ", ".join(spec.dependencies)
        out["Missing_Deps"] = ", ".join(missing_deps)
        out["Consumers"] = ", ".join(spec.consumers)
        out["Severity"] = spec.severity
        out["Validation_Pass"] = False
        return out

    expected = pd.to_numeric(spec.compute(df), errors="coerce")

    out["Metric_Code"] = spec.code
    out["Expected"] = expected
    out["Actual"] = actual
    out["Abs_Error"] = (actual - expected).abs()

    denom = expected.abs().replace(0, np.nan)
    out["Rel_Error"] = out["Abs_Error"] / denom
    out["Formula_Pass"] = (
        np.isclose(actual, expected, atol=atol, rtol=rtol, equal_nan=True)
        | (actual.isna() & expected.isna())
    )

    out["Min_Bound_Pass"] = (
        actual.isna() | (actual >= spec.min_value)
        if spec.min_value is not None
        else True
    )
    out["Max_Bound_Pass"] = (
        actual.isna() | (actual <= spec.max_value)
        if spec.max_value is not None
        else True
    )
    out["Negative_Pass"] = (
        actual.isna() | (actual >= 0)
        if not spec.allow_negative
        else True
    )

    out["Dependencies"] = ", ".join(spec.dependencies)
    out["Missing_Deps"] = ""
    out["Consumers"] = ", ".join(spec.consumers)
    out["Severity"] = spec.severity
    out["Validation_Pass"] = (
        out["Formula_Pass"]
        & out["Min_Bound_Pass"]
        & out["Max_Bound_Pass"]
        & out["Negative_Pass"]
    )
    return out


def build_reverse_dependency_map(
    specs: Dict[str, MetricSpec] = DERIVED_METRIC_SPECS,
) -> Dict[str, List[str]]:
    """Return {upstream_col: [list of derived metrics that depend on it]}."""
    rev: Dict[str, List[str]] = {}
    for metric, spec in specs.items():
        for dep in spec.dependencies:
            rev.setdefault(dep, []).append(metric)
    return rev


def get_upstream_metrics_to_test(
    specs: Dict[str, MetricSpec] = DERIVED_METRIC_SPECS,
) -> set:
    """Return the set of upstream columns that at least one derived metric depends on."""
    rev = build_reverse_dependency_map(specs)
    return {metric for metric in rev if len(rev[metric]) > 0}


def run_upstream_validation_suite(
    df: pd.DataFrame,
    specs: Dict[str, MetricSpec] = DERIVED_METRIC_SPECS,
) -> pd.DataFrame:
    """Run validation for all registered metrics and return a combined report.

    Results include CERT and REPDTE for row-level tracing in the Excel audit sheet.
    """
    upstream = get_upstream_metrics_to_test(specs)
    results = []
    for code, spec in specs.items():
        if code in upstream or spec.consumers:
            res = validate_metric_series(df, spec)
            # Tag with CERT and REPDTE for row-level tracing
            res["CERT"] = df.get("CERT")
            res["REPDTE"] = df.get("REPDTE")
            results.append(res)
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()
