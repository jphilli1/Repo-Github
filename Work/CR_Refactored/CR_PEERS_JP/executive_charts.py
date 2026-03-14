#!/usr/bin/env python3
"""
Executive Chart Generators
===========================

Three high-value executive visuals built from existing workbook data:

1. **YoY Heatmap** (HTML — both modes)
   Directionally-dynamic heatmap showing latest-quarter values and
   year-over-year changes with polarity-aware color coding.

2. **KRI Bullet Chart** (matplotlib — full_local only)
   Football-field style comparison: MSPBNA value vs peer composite
   range, with threshold bands where available.

3. **Sparkline Summary Table** (HTML — both modes)
   Compact trend table with inline SVG sparklines for the trailing
   8 quarters, suitable for PowerPoint / email embedding.

All three consume the same ``FDIC_Data`` sheet already loaded by
``generate_reports()``.  No new API dependencies.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from metric_semantics import (
    METRIC_SEMANTICS, MetricSemantic, Polarity, DisplayFormat,
    get_semantic, get_polarity, get_direction, get_css_class,
    ordered_metrics, GROUP_ORDER,
)

# Lazy-import matplotlib (only needed for bullet chart)
plt = None
def _ensure_mpl():
    global plt
    if plt is None:
        import matplotlib.pyplot as _plt
        plt = _plt


# =====================================================================
# Shared format helpers (reuse report_generator conventions)
# =====================================================================

def _fmt_val(v: float, fmt: DisplayFormat) -> str:
    """Format a value according to its display format."""
    if pd.isna(v):
        return "N/A"
    if fmt == DisplayFormat.DOLLARS_B:
        # FDIC stores $ in thousands
        if abs(v) >= 1e6:
            return f"${v / 1e6:.1f}B"
        elif abs(v) >= 1e3:
            return f"${v / 1e3:.0f}M"
        else:
            return f"${v:.0f}K"
    if fmt == DisplayFormat.MULTIPLE:
        return f"{v:.2f}x"
    if fmt == DisplayFormat.BASIS_POINTS:
        return f"{v * 10000:+.0f} bps"
    # Default: percent
    if abs(v) < 1.0:
        return f"{v * 100:.2f}%"
    return f"{v:.2f}%"


def _fmt_delta(delta: float, fmt: DisplayFormat) -> str:
    """Format a delta value."""
    if pd.isna(delta):
        return "N/A"
    if fmt == DisplayFormat.DOLLARS_B:
        if abs(delta) >= 1e6:
            return f"{delta / 1e6:+.1f}B"
        elif abs(delta) >= 1e3:
            return f"{delta / 1e3:+.0f}M"
        else:
            return f"{delta:+.0f}K"
    if fmt == DisplayFormat.MULTIPLE:
        return f"{delta:+.2f}x"
    if fmt == DisplayFormat.BASIS_POINTS:
        return f"{delta * 10000:+.0f} bps"
    # Percent-point delta
    if abs(delta) < 1.0:
        return f"{delta * 100:+.2f}%"
    return f"{delta:+.2f}%"


# =====================================================================
# 1. YoY HEATMAP (HTML — both modes)
# =====================================================================

# Default metrics for the heatmap — covers the key KRIs
HEATMAP_METRICS_STANDARD = [
    "TTM_NCO_Rate", "Nonaccrual_to_Gross_Loans_Rate", "Past_Due_Rate",
    "Allowance_to_Gross_Loans_Rate", "Risk_Adj_Allowance_Coverage",
    "SBL_Composition", "RIC_CRE_Loan_Share", "RIC_Resi_Loan_Share",
    "RIC_CRE_ACL_Coverage", "RIC_CRE_Risk_Adj_Coverage",
    "RIC_CRE_Nonaccrual_Rate", "RIC_CRE_NCO_Rate",
]

HEATMAP_METRICS_NORMALIZED = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
    "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
    "Norm_SBL_Composition", "Norm_Wealth_Resi_Composition",
    "Norm_CRE_Investment_Composition",
    "Norm_CRE_ACL_Share",
]


def build_yoy_heatmap_data(
    df: pd.DataFrame,
    subject_cert: int,
    peer_cert: int,
    metrics: Optional[List[str]] = None,
    is_normalized: bool = False,
) -> Optional[pd.DataFrame]:
    """Build the data backing a YoY heatmap.

    Returns a DataFrame with columns:
        metric, display_name, group, polarity,
        subj_current, subj_prior, subj_yoy, subj_direction,
        peer_current, peer_prior, peer_yoy, peer_direction,
        vs_peer_delta, vs_peer_direction

    Returns None if insufficient data.
    """
    if "REPDTE" not in df.columns:
        return None
    df = df.copy()
    df["REPDTE"] = pd.to_datetime(df["REPDTE"])

    # Identify latest and year-ago quarter
    dates = sorted(df["REPDTE"].dropna().unique())
    if len(dates) < 2:
        return None
    latest = dates[-1]
    # Find the date closest to 1 year ago
    target_prior = latest - pd.DateOffset(years=1)
    prior = min(dates, key=lambda d: abs(pd.Timestamp(d) - target_prior))

    if metrics is None:
        metrics = HEATMAP_METRICS_NORMALIZED if is_normalized else HEATMAP_METRICS_STANDARD
    # Filter to columns that exist
    metrics = [m for m in metrics if m in df.columns]
    if not metrics:
        return None

    def _row_for_cert(cert, dt):
        mask = (df["CERT"] == cert) & (df["REPDTE"] == dt)
        sub = df.loc[mask]
        if sub.empty:
            return pd.Series(dtype=float)
        return sub.iloc[0]

    rows = []
    for code in ordered_metrics(metrics):
        sem = get_semantic(code)
        if sem is None:
            # Build a minimal fallback
            display_name = code.replace("_", " ")
            polarity = Polarity.NEUTRAL
            disp_fmt = DisplayFormat.PERCENT
            delta_fmt = DisplayFormat.BASIS_POINTS
            group = "Other"
        else:
            display_name = sem.display_name
            polarity = sem.polarity
            disp_fmt = sem.display_format
            delta_fmt = sem.delta_format
            group = sem.group

        subj_cur = _row_for_cert(subject_cert, latest).get(code, np.nan)
        subj_pri = _row_for_cert(subject_cert, prior).get(code, np.nan)
        peer_cur = _row_for_cert(peer_cert, latest).get(code, np.nan)
        peer_pri = _row_for_cert(peer_cert, prior).get(code, np.nan)

        subj_cur = pd.to_numeric(subj_cur, errors="coerce")
        subj_pri = pd.to_numeric(subj_pri, errors="coerce")
        peer_cur = pd.to_numeric(peer_cur, errors="coerce")
        peer_pri = pd.to_numeric(peer_pri, errors="coerce")

        # Composition/share metrics use percent-change YoY (balance growth)
        # instead of simple subtraction to avoid confusing share-point deltas
        # with true growth/shrinkage of the underlying balance.
        _is_composition = any(k in code for k in ("Composition", "Loan_Share", "ACL_Share"))
        if _is_composition:
            subj_yoy = ((subj_cur / subj_pri) - 1.0 if (
                pd.notna(subj_cur) and pd.notna(subj_pri)
                and abs(subj_pri) > 1e-12) else np.nan)
            peer_yoy = ((peer_cur / peer_pri) - 1.0 if (
                pd.notna(peer_cur) and pd.notna(peer_pri)
                and abs(peer_pri) > 1e-12) else np.nan)
        else:
            subj_yoy = subj_cur - subj_pri if pd.notna(subj_cur) and pd.notna(subj_pri) else np.nan
            peer_yoy = peer_cur - peer_pri if pd.notna(peer_cur) and pd.notna(peer_pri) else np.nan
        vs_peer = subj_cur - peer_cur if pd.notna(subj_cur) and pd.notna(peer_cur) else np.nan

        rows.append({
            "metric": code,
            "display_name": display_name,
            "group": group,
            "polarity": polarity.value,
            "display_format": disp_fmt.value,
            "delta_format": delta_fmt.value,
            "is_composition": _is_composition,
            "subj_current": subj_cur,
            "subj_prior": subj_pri,
            "subj_yoy": subj_yoy,
            "subj_direction": get_direction(code, subj_yoy) if pd.notna(subj_yoy) else "N/A",
            "peer_current": peer_cur,
            "peer_prior": peer_pri,
            "peer_yoy": peer_yoy,
            "peer_direction": get_direction(code, peer_yoy) if pd.notna(peer_yoy) else "N/A",
            "vs_peer_delta": vs_peer,
            "vs_peer_direction": get_direction(code, vs_peer) if pd.notna(vs_peer) else "N/A",
        })

    return pd.DataFrame(rows) if rows else None


def render_yoy_heatmap_html(
    heatmap_df: pd.DataFrame,
    title: str = "Key Risk Indicators — YoY Heatmap",
    latest_date: Optional[pd.Timestamp] = None,
    peer_label: str = "Peers",
) -> str:
    """Render a heatmap DataFrame as a self-contained HTML table."""
    date_str = latest_date.strftime("%B %d, %Y") if latest_date else ""

    _DIR_COLORS = {
        "favorable": "#388e3c",
        "adverse": "#d32f2f",
        "flat": "#757575",
        "neutral": "#757575",
        "N/A": "#999999",
    }
    _DIR_BG = {
        "favorable": "rgba(56, 142, 60, 0.10)",
        "adverse": "rgba(211, 47, 47, 0.10)",
        "flat": "transparent",
        "neutral": "transparent",
        "N/A": "transparent",
    }

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; }}
        .heatmap-container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
        h3 {{ color: #002F6C; text-align: center; margin-bottom: 5px; }}
        p.date {{ text-align: center; color: #555; font-weight: bold; margin-top: 0; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 11px; }}
        th {{ background-color: #002F6C; color: white; padding: 8px 6px; border: 1px solid #1a3a5c;
              text-align: center; font-size: 10px; }}
        td {{ padding: 6px 8px; border: 1px solid #e0e0e0; text-align: center; }}
        .metric-name {{ text-align: left !important; font-weight: 600; color: #2c3e50;
                        min-width: 160px; white-space: nowrap; }}
        .group-row td {{ background-color: #f0f4f8; font-weight: bold; color: #002F6C;
                         text-align: left !important; font-size: 11px; padding: 4px 8px; }}
        .val {{ font-variant-numeric: tabular-nums; }}
    </style></head><body>
    <div class="heatmap-container">
        <h3>{title}</h3>
        <p class="date">{date_str}</p>
        <table>
        <thead><tr>
            <th>Metric</th>
            <th>MSPBNA<br>Current</th>
            <th>MSPBNA<br>YoY</th>
            <th>Direction</th>
            <th>{peer_label}<br>Current</th>
            <th>{peer_label}<br>YoY</th>
            <th>Direction</th>
            <th>vs {peer_label}</th>
        </tr></thead>
        <tbody>
    """

    current_group = None
    for _, row in heatmap_df.iterrows():
        # Group separator
        if row["group"] != current_group:
            current_group = row["group"]
            html += f'<tr class="group-row"><td colspan="8">{current_group}</td></tr>'

        disp_fmt = DisplayFormat(row["display_format"])
        delta_fmt = DisplayFormat(row["delta_format"])
        is_comp = row.get("is_composition", False)

        subj_v = _fmt_val(row["subj_current"], disp_fmt)
        peer_v = _fmt_val(row["peer_current"], disp_fmt)
        vs_peer = _fmt_delta(row["vs_peer_delta"], delta_fmt)

        # Composition YoY is already a percent-change ratio; format as ±X.X%
        if is_comp:
            _sy = row["subj_yoy"]
            subj_yoy = f"{_sy * 100:+.1f}%" if pd.notna(_sy) else "N/A"
            _py = row["peer_yoy"]
            peer_yoy = f"{_py * 100:+.1f}%" if pd.notna(_py) else "N/A"
        else:
            subj_yoy = _fmt_delta(row["subj_yoy"], delta_fmt)
            peer_yoy = _fmt_delta(row["peer_yoy"], delta_fmt)

        s_dir = row["subj_direction"]
        p_dir = row["peer_direction"]
        vp_dir = row["vs_peer_direction"]

        def _dir_cell(direction, text):
            color = _DIR_COLORS.get(direction, "#999")
            bg = _DIR_BG.get(direction, "transparent")
            arrow = {"favorable": "&#9650;", "adverse": "&#9660;",
                     "flat": "&#9644;", "neutral": "&#9644;", "N/A": ""}.get(direction, "")
            return (f'<td class="val" style="color:{color}; background:{bg};">'
                    f'{arrow} {text}</td>')

        html += f"""<tr>
            <td class="metric-name">{row['display_name']}</td>
            <td class="val">{subj_v}</td>
            {_dir_cell(s_dir, subj_yoy)}
            <td style="color:{_DIR_COLORS.get(s_dir, '#999')}; font-weight:bold; font-size:10px;">
                {s_dir.upper()}</td>
            <td class="val">{peer_v}</td>
            {_dir_cell(p_dir, peer_yoy)}
            <td style="color:{_DIR_COLORS.get(p_dir, '#999')}; font-weight:bold; font-size:10px;">
                {p_dir.upper()}</td>
            {_dir_cell(vp_dir, vs_peer)}
        </tr>"""

    html += """</tbody></table>
        <p style="font-size:9px; color:#888; margin-top:8px;">
            &#9650; Favorable &nbsp; &#9660; Adverse &nbsp; &#9644; Flat/Neutral
            &nbsp;&nbsp;|&nbsp;&nbsp; YoY = year-over-year change
        </p>
    </div></body></html>"""
    return html


def generate_yoy_heatmap(
    df: pd.DataFrame,
    subject_cert: int,
    peer_cert: int,
    is_normalized: bool = False,
    save_path: Optional[str] = None,
    peer_label: Optional[str] = None,
) -> Optional[str]:
    """Full pipeline: build data → render HTML → optionally save.

    Parameters
    ----------
    peer_label : str, optional
        Human-readable peer group name (e.g. "Wealth Peers", "All Peers").
        Used in title and column headers. Defaults to "Peers".
    """
    hdata = build_yoy_heatmap_data(df, subject_cert, peer_cert,
                                    is_normalized=is_normalized)
    if hdata is None or hdata.empty:
        print(f"  Skipped YoY heatmap ({'norm' if is_normalized else 'std'}): insufficient data")
        return None

    latest = df["REPDTE"].max() if "REPDTE" in df.columns else None
    norm_label = "Normalized" if is_normalized else "Standard"
    _peer = peer_label or "Peers"
    title = f"Key Risk Indicators — YoY Heatmap ({norm_label} — {_peer})"

    html = render_yoy_heatmap_html(hdata, title=title, latest_date=latest,
                                    peer_label=_peer)
    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
    return html


# =====================================================================
# 2. KRI BULLET CHART (matplotlib — full_local only)
# =====================================================================

BULLET_METRICS_STANDARD = [
    "TTM_NCO_Rate", "Nonaccrual_to_Gross_Loans_Rate", "Past_Due_Rate",
    "Allowance_to_Gross_Loans_Rate", "Risk_Adj_Allowance_Coverage",
    "RIC_CRE_ACL_Coverage", "RIC_CRE_Risk_Adj_Coverage",
]

# Standard metrics split by unit family (Part 3: never mix % rates with x-multiples)
BULLET_METRICS_STANDARD_RATES = [
    "TTM_NCO_Rate", "Nonaccrual_to_Gross_Loans_Rate", "Past_Due_Rate",
    "Allowance_to_Gross_Loans_Rate", "RIC_CRE_ACL_Coverage",
]
BULLET_METRICS_STANDARD_COVERAGE = [
    "Risk_Adj_Allowance_Coverage", "RIC_CRE_Risk_Adj_Coverage",
]

BULLET_METRICS_NORMALIZED = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
    "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
    "Norm_CRE_ACL_Share",
]

# Split normalized metrics into rates vs composition for separate charts
BULLET_METRICS_NORMALIZED_RATES = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
    "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
]

BULLET_METRICS_NORMALIZED_COMPOSITION = [
    "Norm_SBL_Composition", "Norm_Wealth_Resi_Composition",
    "Norm_CRE_Investment_Composition",
    "Norm_CRE_ACL_Share",
]

# Backward-compatible alias (points to standard list)
BULLET_METRICS = BULLET_METRICS_STANDARD


def generate_kri_bullet_chart(
    df: pd.DataFrame,
    subject_cert: int,
    wealth_cert: int = 90001,
    all_peers_cert: int = 90003,
    metrics: Optional[List[str]] = None,
    is_normalized: bool = False,
    save_path: Optional[str] = None,
    title_override: Optional[str] = None,
    wealth_member_certs: Optional[List[int]] = None,
    all_peers_member_certs: Optional[List[int]] = None,
) -> Optional["plt.Figure"]:
    """Generate a horizontal football-field chart with nested peer range bands.

    For each metric, shows:
      - Outer lighter band: All Peers min–max range (light gray)
      - Inner darker band: Wealth Peers min–max range (muted purple-gray)
      - Gold diamond: MSPBNA value
      - Optional median markers and edge labels with min/max peer tickers

    Parameters
    ----------
    is_normalized : bool
        If True, uses normalized metric list and comparator CERTs.
    wealth_member_certs : list of int, optional
        Individual bank CERTs that belong to the Wealth Peers group.
        Used to compute the actual min/max range for the inner band.
        Falls back to composite CERT if not provided.
    all_peers_member_certs : list of int, optional
        Individual bank CERTs that belong to the All Peers group.
        Used to compute the actual min/max range for the outer band.
        Falls back to composite CERT if not provided.
    """
    _ensure_mpl()

    if metrics is None:
        metrics = BULLET_METRICS_NORMALIZED if is_normalized else BULLET_METRICS_STANDARD
    metrics = [m for m in metrics if m in df.columns]
    if not metrics:
        print("  Skipped KRI bullet chart: no matching metrics in data")
        return None

    latest = df["REPDTE"].max()
    latest_df = df[df["REPDTE"] == latest].copy()
    for m in metrics:
        latest_df[m] = pd.to_numeric(latest_df[m], errors="coerce")

    def _val(cert, col):
        row = latest_df[latest_df["CERT"] == cert]
        return float(row[col].iloc[0]) if not row.empty and pd.notna(row[col].iloc[0]) else np.nan

    def _range_for_group(certs, col):
        """Compute (min, max, median, min_cert, max_cert) across a set of member CERTs."""
        vals = {}
        for c in certs:
            v = _val(c, col)
            if pd.notna(v):
                vals[c] = v
        if not vals:
            return None
        min_cert = min(vals, key=vals.get)
        max_cert = max(vals, key=vals.get)
        values = list(vals.values())
        return {
            "lo": min(values), "hi": max(values),
            "median": float(np.median(values)),
            "mean": float(np.mean(values)),
            "min_cert": min_cert, "max_cert": max_cert,
            "count": len(values),
        }

    # Try to resolve display label for edge annotations
    try:
        from report_generator import resolve_display_label
        _has_resolver = True
    except ImportError:
        _has_resolver = False

    def _cert_label(cert):
        if _has_resolver:
            row = latest_df[latest_df["CERT"] == cert]
            name = row["REPNM"].iloc[0] if not row.empty and "REPNM" in row.columns else ""
            return resolve_display_label(cert, name)
        return str(cert)

    # Build chart data — nested bands
    # For each metric: outer_range (All Peers), inner_range (Wealth Peers), subject value
    _COLOR_RANGE_ALL = "#D0D0D0"      # Light gray — outer band
    _COLOR_RANGE_WEALTH = "#B8A0C8"   # Muted purple-gray — inner band
    _COLOR_MSPBNA = "#F7A81B"         # Gold
    _COLOR_REF = "#808080"            # Single-comparator reference line

    chart_rows = []
    for code in metrics:
        sem = get_semantic(code)
        label = sem.display_name if sem else code.replace("_", " ")
        sv = _val(subject_cert, code)
        if pd.isna(sv):
            continue

        # Compute ranges from individual peer members if available
        all_range = None
        wealth_range = None
        if all_peers_member_certs:
            all_range = _range_for_group(all_peers_member_certs, code)
        if wealth_member_certs:
            wealth_range = _range_for_group(wealth_member_certs, code)

        # Fallback: use composite CERT values as single-point bands
        if all_range is None:
            av = _val(all_peers_cert, code)
            if pd.notna(av):
                all_range = {"lo": av, "hi": av, "median": av, "mean": av,
                             "min_cert": all_peers_cert, "max_cert": all_peers_cert, "count": 1}
        if wealth_range is None:
            wv = _val(wealth_cert, code)
            if pd.notna(wv):
                wealth_range = {"lo": wv, "hi": wv, "median": wv, "mean": wv,
                                "min_cert": wealth_cert, "max_cert": wealth_cert, "count": 1}

        has_all = all_range is not None
        has_wealth = wealth_range is not None
        if not has_all and not has_wealth:
            # Neither comparator group available: skip metric
            continue

        chart_rows.append({
            "label": label, "code": code, "sv": sv,
            "all_range": all_range, "wealth_range": wealth_range,
        })

    if not chart_rows:
        print("  Skipped KRI bullet chart: all values N/A")
        return None

    n = len(chart_rows)
    fig, ax = plt.subplots(figsize=(10, max(3, n * 0.9 + 1.5)))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    y = np.arange(n)
    bar_height_outer = 0.38
    bar_height_inner = 0.24

    labels = []
    for i, row in enumerate(chart_rows):
        labels.append(row["label"])
        ar = row["all_range"]
        wr = row["wealth_range"]

        # Outer band: All Peers range
        if ar and ar["hi"] > ar["lo"]:
            ax.barh(y[i], ar["hi"] - ar["lo"], left=ar["lo"],
                    height=bar_height_outer * 2,
                    color=_COLOR_RANGE_ALL, alpha=0.5, edgecolor="#B0B0B0",
                    linewidth=0.5, zorder=2)
            # Median marker for All Peers
            ax.plot([ar["median"]], [y[i]], marker="|", color="#888888",
                    markersize=10, markeredgewidth=1.5, zorder=3)
        elif ar:
            # Single-point All Peers: thin reference line
            ax.plot([ar["lo"], ar["lo"]],
                    [y[i] - bar_height_outer, y[i] + bar_height_outer],
                    color=_COLOR_REF, linewidth=1.5, zorder=3)

        # Inner band: Wealth Peers range
        if wr and wr["hi"] > wr["lo"]:
            ax.barh(y[i], wr["hi"] - wr["lo"], left=wr["lo"],
                    height=bar_height_inner * 2,
                    color=_COLOR_RANGE_WEALTH, alpha=0.7, edgecolor="#9070A0",
                    linewidth=0.5, zorder=3)
            # Median marker for Wealth Peers
            ax.plot([wr["median"]], [y[i]], marker="|", color="#6B3D7B",
                    markersize=8, markeredgewidth=1.5, zorder=4)
        elif wr:
            # Single-point Wealth Peers: thin reference line
            ax.plot([wr["lo"], wr["lo"]],
                    [y[i] - bar_height_inner, y[i] + bar_height_inner],
                    color="#7B2D8E", linewidth=2.0, zorder=4)

        # Edge labels: min/max peer tickers on the outer band
        if ar and ar["count"] > 1:
            min_lbl = _cert_label(ar["min_cert"])
            max_lbl = _cert_label(ar["max_cert"])
            ax.annotate(min_lbl, xy=(ar["lo"], y[i]), xytext=(-3, -12),
                        textcoords="offset points", fontsize=7, color="#666666",
                        ha="center", va="top")
            ax.annotate(max_lbl, xy=(ar["hi"], y[i]), xytext=(3, -12),
                        textcoords="offset points", fontsize=7, color="#666666",
                        ha="center", va="top")

    # MSPBNA markers (gold diamonds)
    subj_vals = [r["sv"] for r in chart_rows]
    ax.scatter(subj_vals, y, s=130, color=_COLOR_MSPBNA, edgecolor="black",
               linewidth=0.8, zorder=6, marker="D", label="MSPBNA")

    # Wealth PB Avg markers (purple triangle-up) — mean of wealth peer members
    _COLOR_WEALTH_AVG = "#7B2D8E"   # Purple — matches Wealth Peers band
    _COLOR_ALL_AVG = "#5B9BD5"      # Blue — matches All Peers palette
    _has_wealth_avg = False
    _has_all_avg = False
    for i, row in enumerate(chart_rows):
        wr = row["wealth_range"]
        if wr and wr["count"] > 1:
            ax.scatter([wr["mean"]], [y[i]], s=80, color=_COLOR_WEALTH_AVG,
                       edgecolor="black", linewidth=0.6, zorder=5, marker="^")
            _has_wealth_avg = True

    # All Peers Avg markers (blue square) — mean of all peer members
    for i, row in enumerate(chart_rows):
        ar = row["all_range"]
        if ar and ar["count"] > 1:
            ax.scatter([ar["mean"]], [y[i]], s=70, color=_COLOR_ALL_AVG,
                       edgecolor="black", linewidth=0.6, zorder=5, marker="s")
            _has_all_avg = True

    # Value annotations next to MSPBNA diamond
    for i, row in enumerate(chart_rows):
        sem = get_semantic(row["code"])
        disp_fmt = sem.display_format if sem else DisplayFormat.PERCENT
        ax.annotate(_fmt_val(row["sv"], disp_fmt),
                    xy=(row["sv"], y[i]), xytext=(8, 0),
                    textcoords="offset points", fontsize=9, fontweight="bold",
                    color="#2B2B2B", va="center")

    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10)
    ax.invert_yaxis()
    ax.set_xlabel("Metric Value", fontsize=11, fontweight="bold")
    if title_override:
        ax.set_title(title_override,
                     fontsize=16, fontweight="bold", color="#2B2B2B", pad=15)
    else:
        norm_label = "Normalized" if is_normalized else "Standard"
        ax.set_title(f"Key Risk Indicators — MSPBNA vs Peer Range ({norm_label})",
                     fontsize=16, fontweight="bold", color="#2B2B2B", pad=15)
    for sp in ["top", "right"]:
        ax.spines[sp].set_visible(False)
    ax.grid(True, axis="x", alpha=0.3)

    # Legend — up to 5 entries depending on data availability
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker="D", color="w", markerfacecolor=_COLOR_MSPBNA,
               markeredgecolor="black", markersize=10, label="MSPBNA"),
    ]
    if _has_wealth_avg:
        legend_elements.append(
            Line2D([0], [0], marker="^", color="w", markerfacecolor=_COLOR_WEALTH_AVG,
                   markeredgecolor="black", markersize=9, label="Wealth PB Avg (Mean)"))
    if _has_all_avg:
        legend_elements.append(
            Line2D([0], [0], marker="s", color="w", markerfacecolor=_COLOR_ALL_AVG,
                   markeredgecolor="black", markersize=9, label="All Peers Avg (Mean)"))
    legend_elements.extend([
        Patch(facecolor=_COLOR_RANGE_WEALTH, edgecolor="#9070A0", alpha=0.7,
              label="Wealth Peers Range"),
        Patch(facecolor=_COLOR_RANGE_ALL, edgecolor="#B0B0B0", alpha=0.5,
              label="All Peers Range"),
    ])
    ax.legend(handles=legend_elements, loc="lower right", frameon=True, fontsize=9)

    plt.tight_layout()
    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


# =====================================================================
# 3. SPARKLINE SUMMARY TABLE (HTML — both modes)
# =====================================================================

SPARKLINE_METRICS = [
    "TTM_NCO_Rate", "Nonaccrual_to_Gross_Loans_Rate",
    "Allowance_to_Gross_Loans_Rate", "Risk_Adj_Allowance_Coverage",
    "Past_Due_Rate",
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_ACL_Coverage",
    "RIC_CRE_Nonaccrual_Rate", "RIC_CRE_ACL_Coverage",
]

# Standard-only sparkline metrics (non-Norm, non-segment-normalized)
SPARKLINE_METRICS_STANDARD = [
    "TTM_NCO_Rate", "Nonaccrual_to_Gross_Loans_Rate",
    "Allowance_to_Gross_Loans_Rate", "Risk_Adj_Allowance_Coverage",
    "Past_Due_Rate",
    "RIC_CRE_Nonaccrual_Rate", "RIC_CRE_ACL_Coverage",
]

# Normalized-only sparkline metrics
SPARKLINE_METRICS_NORMALIZED = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_ACL_Coverage",
    "Norm_Risk_Adj_Allowance_Coverage", "Norm_Delinquency_Rate",
]


def _svg_sparkline(values: List[float], width: int = 120, height: int = 24,
                   color: str = "#4C78A8") -> str:
    """Generate an inline SVG sparkline from a list of numeric values."""
    clean = [(i, v) for i, v in enumerate(values) if pd.notna(v)]
    if len(clean) < 2:
        return '<svg width="{}" height="{}"></svg>'.format(width, height)

    indices, vals = zip(*clean)
    vmin, vmax = min(vals), max(vals)
    vrange = max(vmax - vmin, 1e-9)

    padding = 2
    usable_w = width - 2 * padding
    usable_h = height - 2 * padding

    n = len(values)
    points = []
    for idx, val in clean:
        x = padding + (idx / max(n - 1, 1)) * usable_w
        y = padding + usable_h - ((val - vmin) / vrange) * usable_h
        points.append(f"{x:.1f},{y:.1f}")

    polyline = " ".join(points)
    # Endpoint dot
    last_x, last_y = points[-1].split(",")

    return (
        f'<svg width="{width}" height="{height}" xmlns="http://www.w3.org/2000/svg">'
        f'<polyline fill="none" stroke="{color}" stroke-width="1.5" points="{polyline}"/>'
        f'<circle cx="{last_x}" cy="{last_y}" r="2.5" fill="{color}"/>'
        f'</svg>'
    )


def generate_sparkline_table(
    df: pd.DataFrame,
    subject_cert: int,
    peer_cert: int = 90003,
    norm_peer_cert: int = 90006,
    metrics: Optional[List[str]] = None,
    trailing_quarters: int = 8,
    save_path: Optional[str] = None,
    peer_label: Optional[str] = None,
) -> Optional[str]:
    """Generate an HTML table with inline SVG sparklines for trend context.

    Each row shows: metric name | sparkline | current value | YoY change | vs peers.

    Parameters
    ----------
    peer_cert : int
        Comparator CERT for standard metrics (default 90003 = All Peers).
    norm_peer_cert : int
        Comparator CERT for Norm_* metrics (default 90006 = All Peers Norm).
    peer_label : str, optional
        Human-readable peer group name for title/headers. Defaults to "Peers".
    """
    if "REPDTE" not in df.columns:
        return None
    df = df.copy()
    df["REPDTE"] = pd.to_datetime(df["REPDTE"])

    if metrics is None:
        metrics = SPARKLINE_METRICS
    metrics = [m for m in metrics if m in df.columns]
    if not metrics:
        print("  Skipped sparkline table: no matching metrics")
        return None

    dates = sorted(df["REPDTE"].dropna().unique())
    if len(dates) < 2:
        return None
    latest = dates[-1]
    trail_dates = dates[-trailing_quarters:] if len(dates) >= trailing_quarters else dates

    # Year-ago
    target_prior = latest - pd.DateOffset(years=1)
    prior = min(dates, key=lambda d: abs(pd.Timestamp(d) - target_prior))

    subj = df[df["CERT"] == subject_cert].copy()
    subj = subj.sort_values("REPDTE")

    def _subj_val(dt, col):
        row = subj[subj["REPDTE"] == dt]
        if row.empty:
            return np.nan
        return pd.to_numeric(row[col].iloc[0], errors="coerce")

    def _peer_val(dt, col):
        # Use normalized peer CERT for Norm_ metrics, standard peer for others
        effective_cert = norm_peer_cert if col.startswith("Norm_") else peer_cert
        row = df[(df["CERT"] == effective_cert) & (df["REPDTE"] == dt)]
        if row.empty:
            return np.nan
        return pd.to_numeric(row[col].iloc[0], errors="coerce")

    date_str = latest.strftime("%B %d, %Y") if hasattr(latest, 'strftime') else str(latest)
    _peer_lbl = peer_label or "Peers"

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; }}
        .spark-container {{ max-width: 900px; margin: 0 auto; padding: 20px; }}
        h3 {{ color: #002F6C; text-align: center; margin-bottom: 5px; }}
        p.date {{ text-align: center; color: #555; font-weight: bold; margin-top: 0; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 11px; }}
        th {{ background-color: #002F6C; color: white; padding: 6px; border: 1px solid #1a3a5c;
              text-align: center; font-size: 10px; }}
        td {{ padding: 5px 8px; border: 1px solid #e0e0e0; text-align: center; }}
        .metric-name {{ text-align: left !important; font-weight: 600; color: #2c3e50;
                        min-width: 150px; white-space: nowrap; }}
        .spark-cell {{ padding: 2px 4px; }}
        .val {{ font-variant-numeric: tabular-nums; }}
        .good-trend {{ color: #388e3c; font-weight: bold; }}
        .bad-trend {{ color: #d32f2f; font-weight: bold; }}
        .neutral-trend {{ color: #757575; }}
    </style></head><body>
    <div class="spark-container">
        <h3>Credit Risk Dashboard — Trend Summary ({_peer_lbl})</h3>
        <p class="date">{date_str}</p>
        <table>
        <thead><tr>
            <th>Metric</th>
            <th>Trend ({trailing_quarters}Q)</th>
            <th>Current</th>
            <th>YoY</th>
            <th>vs {_peer_lbl}</th>
        </tr></thead>
        <tbody>
    """

    for code in ordered_metrics(metrics):
        sem = get_semantic(code)
        label = sem.display_name if sem else code.replace("_", " ")
        disp_fmt = sem.display_format if sem else DisplayFormat.PERCENT
        delta_fmt = sem.delta_format if sem else DisplayFormat.BASIS_POINTS

        # Trail values for sparkline
        trail_vals = [_subj_val(d, code) for d in trail_dates]

        # Sparkline color based on polarity and trend
        pol = get_polarity(code)
        first_clean = next((v for v in trail_vals if pd.notna(v)), None)
        last_clean = next((v for v in reversed(trail_vals) if pd.notna(v)), None)
        if first_clean is not None and last_clean is not None and pol != Polarity.NEUTRAL:
            delta_sign = last_clean - first_clean
            if pol == Polarity.ADVERSE:
                spark_color = "#d32f2f" if delta_sign > 0 else "#388e3c"
            else:
                spark_color = "#388e3c" if delta_sign > 0 else "#d32f2f"
        else:
            spark_color = "#4C78A8"

        sparkline = _svg_sparkline(trail_vals, color=spark_color)

        cur = _subj_val(latest, code)
        pri = _subj_val(prior, code)
        peer_v = _peer_val(latest, code)

        yoy = cur - pri if pd.notna(cur) and pd.notna(pri) else np.nan
        vs_peer = cur - peer_v if pd.notna(cur) and pd.notna(peer_v) else np.nan

        yoy_cls = get_css_class(code, yoy) if pd.notna(yoy) else "neutral-trend"
        vp_cls = get_css_class(code, vs_peer) if pd.notna(vs_peer) else "neutral-trend"

        html += f"""<tr>
            <td class="metric-name">{label}</td>
            <td class="spark-cell">{sparkline}</td>
            <td class="val">{_fmt_val(cur, disp_fmt)}</td>
            <td class="val {yoy_cls}">{_fmt_delta(yoy, delta_fmt)}</td>
            <td class="val {vp_cls}">{_fmt_delta(vs_peer, delta_fmt)}</td>
        </tr>"""

    html += """</tbody></table>
        <p style="font-size:9px; color:#888; margin-top:8px;">
            Sparklines show trailing quarters for the subject bank. Color reflects directional trend.
        </p>
    </div></body></html>"""

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
    return html


# =====================================================================
# 4. CUMULATIVE GROWTH: TARGET LOANS vs TARGET ACL (matplotlib)
# =====================================================================

_CRE_BAL_COLS = ["RIC_CRE_Cost"]
_RESI_BAL_COLS = ["Wealth_Resi_Balance", "RIC_Resi_Cost", "LNRERES"]
_CRE_ACL_COLS = ["RIC_CRE_ACL"]
_GROWTH_START_DATE = pd.Timestamp("2015-12-31")

# Entity colors — mirror CHART_PALETTE in report_generator.py
_COLOR_MSPBNA = "#F7A81B"
_COLOR_WEALTH = "#7B2D8E"
_COLOR_ALL_PEERS = "#5B9BD5"


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return the first column from *candidates* that exists in *df*."""
    return next((c for c in candidates if c in df.columns), None)


def prepare_cumulative_growth_data(
    df: pd.DataFrame,
    cert: int,
    start_date: pd.Timestamp = _GROWTH_START_DATE,
) -> Optional[pd.DataFrame]:
    """Build cumulative % growth from *start_date* for Target Loans and ACL.

    Target Loans = CRE Balance + RESI Balance
    Target ACL   = CRE ACL Balance

    Returns a DataFrame with columns:
        REPDTE, target_loans_growth, target_acl_growth,
        target_loans_cagr, target_acl_cagr
    or None if required columns are missing.
    """
    ent = df[df["CERT"] == cert].copy()
    if ent.empty:
        return None

    ent["REPDTE"] = pd.to_datetime(ent["REPDTE"])
    ent = ent.sort_values("REPDTE")

    # Resolve column names
    cre_col = _find_col(ent, _CRE_BAL_COLS)
    resi_col = _find_col(ent, _RESI_BAL_COLS)
    acl_col = _find_col(ent, _CRE_ACL_COLS)

    if cre_col is None or resi_col is None or acl_col is None:
        return None

    # Compute target balances
    ent["_target_loans"] = ent[cre_col].fillna(0) + ent[resi_col].fillna(0)
    ent["_target_acl"] = ent[acl_col].fillna(0)

    # Find anchor: closest date to start_date
    date_diffs = (ent["REPDTE"] - start_date).abs()
    anchor_idx = date_diffs.idxmin()
    anchor_loans = ent.loc[anchor_idx, "_target_loans"]
    anchor_acl = ent.loc[anchor_idx, "_target_acl"]

    # Auto-advance base period if anchor has zero values
    if anchor_loans == 0 or anchor_acl == 0:
        anchor_date = ent.loc[anchor_idx, "REPDTE"]
        candidates = ent[
            (ent["REPDTE"] > anchor_date)
            & (ent["_target_loans"] > 0)
            & (ent["_target_acl"] > 0)
        ]
        if candidates.empty:
            return None
        anchor_idx = candidates.index[0]
        anchor_loans = ent.loc[anchor_idx, "_target_loans"]
        anchor_acl = ent.loc[anchor_idx, "_target_acl"]

    # Filter to dates >= anchor
    anchor_date = ent.loc[anchor_idx, "REPDTE"]
    ent = ent[ent["REPDTE"] >= anchor_date].copy()

    # Cumulative % growth: (Current - Base) / Base
    ent["target_loans_growth"] = (ent["_target_loans"] - anchor_loans) / anchor_loans
    ent["target_acl_growth"] = (ent["_target_acl"] - anchor_acl) / anchor_acl

    # CAGR at each point: (Current / Base)^(1/years) - 1
    ent["_years"] = (ent["REPDTE"] - anchor_date).dt.days / 365.25
    ent["target_loans_cagr"] = np.where(
        ent["_years"] > 0,
        (ent["_target_loans"] / anchor_loans) ** (1.0 / ent["_years"]) - 1,
        0.0,
    )
    ent["target_acl_cagr"] = np.where(
        ent["_years"] > 0,
        (ent["_target_acl"] / anchor_acl) ** (1.0 / ent["_years"]) - 1,
        0.0,
    )

    return ent[["REPDTE", "target_loans_growth", "target_acl_growth",
                "target_loans_cagr", "target_acl_cagr"]].reset_index(drop=True)


def _nudge_cagr_labels(
    positions: List[Tuple[float, str, str]],
    min_gap: float = 0.03,
) -> List[Tuple[float, str, str]]:
    """Sort CAGR label positions by y-value and nudge to avoid overlap.

    Each entry is (y_value, label_text, color).
    Returns the list with y_values adjusted so adjacent labels are at
    least *min_gap* apart.
    """
    if not positions:
        return positions
    # Sort by y
    items = sorted(positions, key=lambda x: x[0])
    adjusted = [items[0]]
    for i in range(1, len(items)):
        y, txt, col = items[i]
        prev_y = adjusted[-1][0]
        if y - prev_y < min_gap:
            y = prev_y + min_gap
        adjusted.append((y, txt, col))
    return adjusted


def plot_cumulative_growth_loans_vs_acl(
    df: pd.DataFrame,
    subject_cert: int,
    peer_cert: int,
    peer_label: str = "Peers",
    save_path: Optional[str] = None,
) -> Optional[str]:
    """Plot cumulative % growth of Target Loans vs Target ACL.

    Produces 4 lines:
      - MSPBNA Loans (solid, gold)
      - MSPBNA ACL (dashed, gold)
      - Peer Loans (solid, peer color)
      - Peer ACL (dashed, peer color)

    CAGR annotations at the endpoint with anti-overlap.
    Returns the save_path on success, None on failure.
    """
    _ensure_mpl()
    if plt is None:
        return None

    from matplotlib.lines import Line2D

    subj_data = prepare_cumulative_growth_data(df, subject_cert)
    peer_data = prepare_cumulative_growth_data(df, peer_cert)

    if subj_data is None or peer_data is None:
        return None

    # Determine peer color
    peer_color = _COLOR_WEALTH if "Wealth" in peer_label else _COLOR_ALL_PEERS

    fig, ax = plt.subplots(figsize=(12, 6))

    # --- Plot lines ---
    ax.plot(subj_data["REPDTE"], subj_data["target_loans_growth"],
            color=_COLOR_MSPBNA, linewidth=2, linestyle="-",
            label="MSPBNA Loans")
    ax.plot(subj_data["REPDTE"], subj_data["target_acl_growth"],
            color=_COLOR_MSPBNA, linewidth=2, linestyle="--",
            label="MSPBNA ACL")
    ax.plot(peer_data["REPDTE"], peer_data["target_loans_growth"],
            color=peer_color, linewidth=2, linestyle="-",
            label=f"{peer_label} Loans")
    ax.plot(peer_data["REPDTE"], peer_data["target_acl_growth"],
            color=peer_color, linewidth=2, linestyle="--",
            label=f"{peer_label} ACL")

    # --- CAGR annotations at endpoints ---
    cagr_labels = []
    for data, color, entity in [
        (subj_data, _COLOR_MSPBNA, "MSPBNA"),
        (peer_data, peer_color, peer_label),
    ]:
        last = data.iloc[-1]
        for metric, style_label in [
            ("target_loans_cagr", "Loans"),
            ("target_acl_cagr", "ACL"),
        ]:
            cagr_val = last[metric]
            growth_col = metric.replace("_cagr", "_growth")
            y_pos = last[growth_col]
            label_txt = f"{entity} {style_label} CAGR: {cagr_val:.1%}"
            cagr_labels.append((y_pos, label_txt, color))

    # Anti-overlap nudge
    cagr_labels = _nudge_cagr_labels(cagr_labels)

    # Draw CAGR reference lines and labels
    last_date = max(subj_data["REPDTE"].iloc[-1], peer_data["REPDTE"].iloc[-1])
    for y_pos, label_txt, color in cagr_labels:
        ax.axhline(y=y_pos, color=color, linewidth=0.7, linestyle=":",
                    alpha=0.5, zorder=1)
        ax.annotate(
            label_txt,
            xy=(last_date, y_pos),
            xytext=(10, 0),
            textcoords="offset points",
            fontsize=8,
            color=color,
            va="center",
            fontweight="bold",
        )

    # --- Formatting ---
    ax.set_title(
        f"Cumulative Growth: Target Loans vs CRE ACL — MSPBNA vs {peer_label}\n"
        f"(Indexed to Q4 2015 = 0%)",
        fontsize=12, fontweight="bold", color="#2B2B2B",
    )
    ax.set_ylabel("Cumulative % Growth from Q4 2015", fontsize=10)
    ax.set_xlabel("")
    ax.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: f"{v:.0%}")
    )
    ax.axhline(y=0, color="#888888", linewidth=0.8, linestyle="-", alpha=0.4)
    ax.grid(axis="y", alpha=0.3)
    ax.tick_params(axis="x", rotation=45)

    # Legend
    handles = [
        Line2D([0], [0], color=_COLOR_MSPBNA, lw=2, ls="-", label="MSPBNA Loans"),
        Line2D([0], [0], color=_COLOR_MSPBNA, lw=2, ls="--", label="MSPBNA ACL"),
        Line2D([0], [0], color=peer_color, lw=2, ls="-", label=f"{peer_label} Loans"),
        Line2D([0], [0], color=peer_color, lw=2, ls="--", label=f"{peer_label} ACL"),
    ]
    ax.legend(handles=handles, loc="upper left", fontsize=9, framealpha=0.9)

    fig.subplots_adjust(right=0.82)
    fig.tight_layout(rect=[0, 0, 0.82, 1])

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

    return save_path
