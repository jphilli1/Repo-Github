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
    "Norm_CRE_ACL_Share", "Norm_Resi_ACL_Share",
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
            <th>Peers<br>Current</th>
            <th>Peers<br>YoY</th>
            <th>Direction</th>
            <th>vs Peers</th>
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

        subj_v = _fmt_val(row["subj_current"], disp_fmt)
        subj_yoy = _fmt_delta(row["subj_yoy"], delta_fmt)
        peer_v = _fmt_val(row["peer_current"], disp_fmt)
        peer_yoy = _fmt_delta(row["peer_yoy"], delta_fmt)
        vs_peer = _fmt_delta(row["vs_peer_delta"], delta_fmt)

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
) -> Optional[str]:
    """Full pipeline: build data → render HTML → optionally save."""
    hdata = build_yoy_heatmap_data(df, subject_cert, peer_cert,
                                    is_normalized=is_normalized)
    if hdata is None or hdata.empty:
        print(f"  Skipped YoY heatmap ({'norm' if is_normalized else 'std'}): insufficient data")
        return None

    latest = df["REPDTE"].max() if "REPDTE" in df.columns else None
    norm_label = "Normalized" if is_normalized else "Standard"
    title = f"Key Risk Indicators — YoY Heatmap ({norm_label})"

    html = render_yoy_heatmap_html(hdata, title=title, latest_date=latest)
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

BULLET_METRICS_NORMALIZED = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
    "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
    "Norm_CRE_ACL_Share", "Norm_Resi_ACL_Share",
]

# Split normalized metrics into rates vs composition for separate charts
BULLET_METRICS_NORMALIZED_RATES = [
    "Norm_NCO_Rate", "Norm_Nonaccrual_Rate", "Norm_Delinquency_Rate",
    "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
]

BULLET_METRICS_NORMALIZED_COMPOSITION = [
    "Norm_SBL_Composition", "Norm_Wealth_Resi_Composition",
    "Norm_CRE_Investment_Composition",
    "Norm_CRE_ACL_Share", "Norm_Resi_ACL_Share",
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
) -> Optional["plt.Figure"]:
    """Generate a horizontal bullet / football-field chart.

    For each metric, shows:
      - Gray band: range between Wealth Peers and All Peers
      - Gold marker: MSPBNA value
      - Optional threshold bands from metric_semantics

    Parameters
    ----------
    is_normalized : bool
        If True, uses normalized metric list and comparator CERTs
        (90004 Wealth Peers Norm, 90006 All Peers Norm).
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

    # Build chart data
    # ref_markers tracks metrics where only one comparator is available:
    #   index → x-value of the single available comparator (drawn as thin marker)
    labels, subj_vals, lo_vals, hi_vals = [], [], [], []
    ref_markers: Dict[int, float] = {}
    for code in metrics:
        sem = get_semantic(code)
        label = sem.display_name if sem else code.replace("_", " ")
        sv = _val(subject_cert, code)
        wv = _val(wealth_cert, code)
        av = _val(all_peers_cert, code)
        if pd.isna(sv):
            continue
        both_present = pd.notna(wv) and pd.notna(av)
        one_present = pd.notna(wv) or pd.notna(av)
        if both_present:
            # Normal case: gray band between the two comparators
            labels.append(label)
            subj_vals.append(sv)
            lo_vals.append(min(wv, av))
            hi_vals.append(max(wv, av))
        elif one_present:
            # Single comparator: thin reference marker (NOT collapse to subject)
            single_val = wv if pd.notna(wv) else av
            labels.append(label)
            subj_vals.append(sv)
            lo_vals.append(single_val)
            hi_vals.append(single_val)
            ref_markers[len(labels) - 1] = single_val
        else:
            # Neither comparator available: skip this metric row entirely
            continue

    if not labels:
        print("  Skipped KRI bullet chart: all values N/A")
        return None

    n = len(labels)
    fig, ax = plt.subplots(figsize=(10, max(3, n * 0.8 + 1.5)))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    y = np.arange(n)
    bar_height = 0.35

    # Peer range bands (gray) or thin reference markers (single comparator)
    for i in range(n):
        lo, hi = lo_vals[i], hi_vals[i]
        if i in ref_markers:
            # Single comparator: thin vertical reference line
            ax.plot([ref_markers[i], ref_markers[i]],
                    [y[i] - bar_height, y[i] + bar_height],
                    color="#808080", linewidth=2.0, zorder=3)
        else:
            ax.barh(y[i], hi - lo, left=lo, height=bar_height * 2,
                    color="#D0D0D0", alpha=0.6, edgecolor="#B0B0B0", linewidth=0.5)

    # MSPBNA markers (gold diamonds)
    ax.scatter(subj_vals, y, s=120, color="#F7A81B", edgecolor="black",
               linewidth=0.8, zorder=5, marker="D", label="MSPBNA")

    # Value annotations
    for i in range(n):
        sem = get_semantic(labels[i]) or get_semantic(
            [m for m in metrics if (get_semantic(m) and get_semantic(m).display_name == labels[i]) or m.replace("_", " ") == labels[i]][0]
            if any((get_semantic(m) and get_semantic(m).display_name == labels[i]) or m.replace("_", " ") == labels[i] for m in metrics) else ""
        )
        disp_fmt = sem.display_format if sem else DisplayFormat.PERCENT
        ax.annotate(_fmt_val(subj_vals[i], disp_fmt),
                    xy=(subj_vals[i], y[i]), xytext=(8, 0),
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

    # Legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker="D", color="w", markerfacecolor="#F7A81B",
               markeredgecolor="black", markersize=10, label="MSPBNA"),
        Patch(facecolor="#D0D0D0", edgecolor="#B0B0B0",
              label="Wealth Peers Norm ↔ All Peers Norm" if is_normalized
              else "Wealth Peers ↔ All Peers"),
    ]
    if ref_markers:
        legend_elements.append(
            Line2D([0], [0], color="#808080", linewidth=2.0,
                   label="Single Comparator Ref."))
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
) -> Optional[str]:
    """Generate an HTML table with inline SVG sparklines for trend context.

    Each row shows: metric name | sparkline | current value | YoY change | vs peers.

    Parameters
    ----------
    peer_cert : int
        Comparator CERT for standard metrics (default 90003 = All Peers).
    norm_peer_cert : int
        Comparator CERT for Norm_* metrics (default 90006 = All Peers Norm).
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
        <h3>Credit Risk Dashboard — Trend Summary</h3>
        <p class="date">{date_str}</p>
        <table>
        <thead><tr>
            <th>Metric</th>
            <th>Trend ({trailing_quarters}Q)</th>
            <th>Current</th>
            <th>YoY</th>
            <th>vs Peers</th>
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
