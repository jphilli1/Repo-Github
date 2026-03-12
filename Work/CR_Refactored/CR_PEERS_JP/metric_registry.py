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
        dependencies=["Norm_ACL_Balance", "Norm_Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Norm_ACL_Balance"], df["Norm_Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["normalized_table", "ratio_components_normalized"],
    ),

    "Norm_Delinquency_Rate": MetricSpec(
        code="Norm_Delinquency_Rate",
        dependencies=["Norm_PD30", "Norm_PD90", "Norm_Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(
                pd.to_numeric(df.get("Norm_PD30", 0), errors="coerce").fillna(0)
                + pd.to_numeric(df.get("Norm_PD90", 0), errors="coerce").fillna(0),
                df["Norm_Gross_Loans"]
            ),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["normalized_table", "ratio_components_normalized", "detailed_peer_table"],
    ),

    "Norm_Risk_Adj_Allowance_Coverage": MetricSpec(
        code="Norm_Risk_Adj_Allowance_Coverage",
        dependencies=["Norm_ACL_Balance", "Norm_Gross_Loans", "SBL_Balance"],
        compute=lambda df: pd.Series(
            _safe_div(df["Norm_ACL_Balance"], df["Norm_Gross_Loans"] - df["SBL_Balance"].fillna(0)),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["normalized_table", "ratio_components_normalized", "detailed_peer_table"],
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
    # Share metrics: segment ACL / total ACL pool (NOT coverage)
    "RIC_CRE_ACL_Share": MetricSpec(
        code="RIC_CRE_ACL_Share",
        dependencies=["RIC_CRE_ACL", "Total_ACL"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_CRE_ACL"], df["Total_ACL"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["ratio_components_standard", "segment_focus_cre", "detailed_peer_table"],
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
    "RIC_Resi_ACL_Share": MetricSpec(
        code="RIC_Resi_ACL_Share",
        dependencies=["RIC_Resi_ACL", "Total_ACL"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_Resi_ACL"], df["Total_ACL"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["ratio_components_standard", "segment_focus_resi", "detailed_peer_table"],
    ),
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

    # ── Normalized Residential ───────────────────────────────────────────
    "Norm_Wealth_Resi_Composition": MetricSpec(
        code="Norm_Wealth_Resi_Composition",
        dependencies=["Wealth_Resi_Balance", "Norm_Gross_Loans"],
        compute=lambda df: pd.Series(
            _safe_div(df["Wealth_Resi_Balance"], df["Norm_Gross_Loans"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["detailed_peer_table", "ratio_components_normalized", "segment_focus_resi"],
    ),
    "Norm_Resi_ACL_Share": MetricSpec(
        code="Norm_Resi_ACL_Share",
        dependencies=["RIC_Resi_ACL", "Norm_ACL_Balance"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_Resi_ACL"], df.get("Norm_ACL_Balance", 0)),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["ratio_components_normalized", "segment_focus_resi"],
    ),
    "Norm_Resi_ACL_Coverage": MetricSpec(
        code="Norm_Resi_ACL_Coverage",
        dependencies=["RIC_Resi_ACL", "Wealth_Resi_Balance"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_Resi_ACL"], df["Wealth_Resi_Balance"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["segment_focus_resi"],
    ),
    "Norm_CRE_ACL_Share": MetricSpec(
        code="Norm_CRE_ACL_Share",
        dependencies=["RIC_CRE_ACL", "Norm_ACL_Balance"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_CRE_ACL"], df.get("Norm_ACL_Balance", 0)),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        max_value=1.0,
        consumers=["ratio_components_normalized", "segment_focus_cre", "detailed_peer_table"],
    ),
    "Norm_CRE_ACL_Coverage": MetricSpec(
        code="Norm_CRE_ACL_Coverage",
        dependencies=["RIC_CRE_ACL", "CRE_Investment_Pure_Balance"],
        compute=lambda df: pd.Series(
            _safe_div(df["RIC_CRE_ACL"], df["CRE_Investment_Pure_Balance"]),
            index=df.index
        ),
        unit="fraction",
        min_value=0.0,
        consumers=["segment_focus_cre", "detailed_peer_table"],
    ),

    # ── Upstream Choke-Point Placeholders ────────────────────────────────
    # These are *source* columns, not derived.  Registered so the reverse-
    # dependency graph knows who depends on them.
    "Norm_Gross_Loans": MetricSpec(
        code="Norm_Gross_Loans",
        dependencies=["Gross_Loans", "Excluded_Balance"],
        compute=lambda df: pd.Series(
            np.where(
                (pd.to_numeric(df["Gross_Loans"], errors="coerce")
                 - pd.to_numeric(df["Excluded_Balance"], errors="coerce")) >= 0,
                pd.to_numeric(df["Gross_Loans"], errors="coerce")
                - pd.to_numeric(df["Excluded_Balance"], errors="coerce"),
                np.where(
                    (pd.to_numeric(df["Gross_Loans"], errors="coerce")
                     - pd.to_numeric(df["Excluded_Balance"], errors="coerce"))
                    / pd.to_numeric(df["Gross_Loans"], errors="coerce").replace(0, np.nan) >= -0.05,
                    0.0, np.nan
                )
            ),
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


# ═══════════════════════════════════════════════════════════════════════════
# SEMANTIC VALIDATION RULES (A–E)
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ValidationRule:
    """A single semantic validation rule."""
    rule_id: str
    description: str
    check: Callable[[pd.DataFrame], pd.DataFrame]
    severity: str = "high"   # high, medium, low


def _check_over_exclusion(df: pd.DataFrame) -> pd.DataFrame:
    """Rule A: Flag rows where Excluded_* > Total_* (over-exclusion)."""
    rows = []
    pairs = [
        ("Excluded_NCO_TTM", "Total_NCO_TTM", "NCO"),
        ("Excluded_Nonaccrual", "Total_Nonaccrual", "Nonaccrual"),
        ("Excluded_Balance", "Gross_Loans", "Balance"),
    ]
    for excl_col, total_col, label in pairs:
        if excl_col not in df.columns or total_col not in df.columns:
            continue
        excl = pd.to_numeric(df[excl_col], errors="coerce").fillna(0)
        total = pd.to_numeric(df[total_col], errors="coerce").fillna(0)
        mask = excl > total
        if mask.any():
            flagged = df.loc[mask, ["CERT", "REPDTE"]].copy() if "CERT" in df.columns else pd.DataFrame(index=df.index[mask])
            flagged["Rule"] = f"OverExclusion_{label}"
            flagged["Detail"] = (excl[mask].astype(str) + " > " + total[mask].astype(str))
            flagged["Severity"] = "high"
            flagged["Pct_Over"] = ((excl[mask] - total[mask]) / total[mask].replace(0, np.nan) * 100).round(2)
            rows.append(flagged)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def _check_flatline_anomaly(df: pd.DataFrame) -> pd.DataFrame:
    """Rule B: Flag metrics that are constant (zero or otherwise) across all CERTs for latest quarter."""
    rows = []
    if "REPDTE" not in df.columns:
        return pd.DataFrame()
    latest_q = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_q]
    norm_metrics = ["Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
                    "Norm_ACL_Coverage", "Norm_Exclusion_Pct"]
    for metric in norm_metrics:
        if metric not in latest.columns:
            continue
        vals = pd.to_numeric(latest[metric], errors="coerce").dropna()
        if len(vals) > 1 and vals.nunique() <= 1:
            rows.append({
                "Rule": f"Flatline_{metric}",
                "Detail": f"All {len(vals)} values = {vals.iloc[0]:.6f} in {latest_q}",
                "Severity": "medium",
                "REPDTE": latest_q,
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _check_duplicate_composites(df: pd.DataFrame) -> pd.DataFrame:
    """Rule C: Flag if standard and normalized composites produce identical values."""
    rows = []
    pairs_to_check = [(90001, 90004), (90002, 90005), (90003, 90006)]
    rate_cols = ["TTM_NCO_Rate", "NPL_to_Gross_Loans_Rate", "Norm_NCO_Rate", "Norm_Nonaccrual_Rate"]
    if "CERT" not in df.columns or "REPDTE" not in df.columns:
        return pd.DataFrame()
    latest_q = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_q]
    for std_cert, norm_cert in pairs_to_check:
        std_row = latest[latest["CERT"] == std_cert]
        norm_row = latest[latest["CERT"] == norm_cert]
        if std_row.empty or norm_row.empty:
            continue
        for col in rate_cols:
            if col not in latest.columns:
                continue
            sv = pd.to_numeric(std_row[col].iloc[0], errors="coerce")
            nv = pd.to_numeric(norm_row[col].iloc[0], errors="coerce")
            if pd.notna(sv) and pd.notna(nv) and np.isclose(sv, nv, rtol=1e-6):
                rows.append({
                    "Rule": "DuplicateComposite",
                    "Detail": f"CERT {std_cert} vs {norm_cert}: {col} = {sv:.6f} (identical)",
                    "Severity": "medium",
                    "REPDTE": latest_q,
                })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _check_output_contamination(df: pd.DataFrame) -> pd.DataFrame:
    """Rule D: Flag if composite CERTs appear where they shouldn't (e.g., 90004-90006 in standard metrics)."""
    rows = []
    if "CERT" not in df.columns:
        return pd.DataFrame()
    std_composites = {90001, 90002, 90003}
    norm_composites = {90004, 90005, 90006}
    norm_rate_cols = ["Norm_NCO_Rate", "Norm_Nonaccrual_Rate"]
    std_rate_cols = ["TTM_NCO_Rate", "NPL_to_Gross_Loans_Rate"]
    # Standard composites should have NaN norm rates
    for cert in std_composites:
        cert_data = df[df["CERT"] == cert]
        if cert_data.empty:
            continue
        for col in norm_rate_cols:
            if col in cert_data.columns:
                valid = pd.to_numeric(cert_data[col], errors="coerce").dropna()
                if not valid.empty and (valid != 0).any():
                    rows.append({
                        "Rule": "OutputContamination",
                        "Detail": f"Standard composite {cert} has non-null {col}",
                        "Severity": "high",
                    })
    # Normalized composites should have NaN standard rates
    for cert in norm_composites:
        cert_data = df[df["CERT"] == cert]
        if cert_data.empty:
            continue
        for col in std_rate_cols:
            if col in cert_data.columns:
                valid = pd.to_numeric(cert_data[col], errors="coerce").dropna()
                if not valid.empty and (valid != 0).any():
                    rows.append({
                        "Rule": "OutputContamination",
                        "Detail": f"Normalized composite {cert} has non-null {col}",
                        "Severity": "high",
                    })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _check_consumer_linkage(
    specs: Dict[str, MetricSpec] = DERIVED_METRIC_SPECS,
) -> pd.DataFrame:
    """Rule E: Flag metrics with no declared consumers (orphaned metrics)."""
    rows = []
    for code, spec in specs.items():
        if not spec.consumers:
            rows.append({
                "Rule": "OrphanedMetric",
                "Detail": f"Metric '{code}' has no declared downstream consumers",
                "Severity": "low",
            })
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _check_unsupported_mappings(df: pd.DataFrame) -> pd.DataFrame:
    """Rule F: Flag rows with unsupported mapping audit flags set to True."""
    rows = []
    audit_flags = {
        "_audit_unsupported_ndfi_pdna": "NDFI PD/NA unsupported in CR-only mode",
        "_audit_ndfi_nco_unsupported": "NDFI NCO unsupported in CR-only mode",
        "_audit_ag_pd_fallback_to_zero": "Ag PD fell back to 0 (P3AG/P9AG absent)",
        "_audit_resi_balance_fallback_used": "Resi balance used LNRERES fallback (RC-C components absent)",
    }
    if "REPDTE" not in df.columns:
        return pd.DataFrame()
    latest_q = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_q]
    for flag_col, desc in audit_flags.items():
        if flag_col not in latest.columns:
            continue
        flagged = latest[latest[flag_col] == True]
        if not flagged.empty:
            for idx in flagged.index:
                row = {"Rule": f"UnsupportedMapping_{flag_col}",
                       "Detail": f"{desc} (CERT {latest.loc[idx, 'CERT']})" if "CERT" in latest.columns else desc,
                       "Severity": "low", "REPDTE": latest_q}
                rows.append(row)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


SEMANTIC_VALIDATION_RULES = [
    ValidationRule("A_OverExclusion", "Excluded values exceed totals", _check_over_exclusion, "high"),
    ValidationRule("B_FlatlineAnomaly", "Metric flatlines across all entities", _check_flatline_anomaly, "medium"),
    ValidationRule("C_DuplicateComposite", "Standard/Normalized composites produce identical values", _check_duplicate_composites, "medium"),
    ValidationRule("D_OutputContamination", "Composite CERTs have metric values they shouldn't", _check_output_contamination, "high"),
    ValidationRule("E_ConsumerLinkage", "Metrics with no downstream consumers", lambda df: _check_consumer_linkage(), "low"),
    ValidationRule("F_UnsupportedMappings", "Audit flags for unsupported CR-only mappings", _check_unsupported_mappings, "low"),
]


def run_semantic_validation(df: pd.DataFrame) -> pd.DataFrame:
    """Run all semantic validation rules and return a combined report."""
    results = []
    for rule in SEMANTIC_VALIDATION_RULES:
        try:
            res = rule.check(df)
            if not res.empty:
                res["Rule_ID"] = rule.rule_id
                res["Rule_Description"] = rule.description
                results.append(res)
        except Exception as e:
            results.append(pd.DataFrame([{
                "Rule_ID": rule.rule_id,
                "Rule_Description": rule.description,
                "Rule": "ERROR",
                "Detail": str(e),
                "Severity": rule.severity,
            }]))
    return pd.concat(results, ignore_index=True) if results else pd.DataFrame()


def run_full_validation_suite(df: pd.DataFrame) -> tuple:
    """Run both formula and semantic validation. Returns (formula_report, semantic_report)."""
    formula_report = run_upstream_validation_suite(df)
    semantic_report = run_semantic_validation(df)
    return formula_report, semantic_report
