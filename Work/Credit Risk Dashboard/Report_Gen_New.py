#!/usr/bin/env python3
"""
Bank Performance Report Generator (MSPBNA V6 Edition)
=====================================================
A self-contained reporting engine that automatically locates the latest processed
Excel file and generates professional charts and HTML tables for credit risk analysis.
"""

import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from matplotlib.gridspec import GridSpec
import warnings

# Suppress warnings for clean output
warnings.filterwarnings("ignore")

# Set style for better-looking charts
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

# ==================================================================================
# CONFIGURATION AND UTILITY FUNCTIONS
# ==================================================================================

def find_latest_excel_file(directory: str = "output") -> Optional[str]:
    """Return newest Bank_Performance_Dashboard_*.xlsx."""
    try:
        base = Path(__file__).parent.resolve()
    except Exception:
        base = Path.cwd().resolve()

    out = (base / directory)
    try:
        out.mkdir(parents=True, exist_ok=True)
        search_root = out
    except OSError:
        search_root = Path.cwd()

    files = []
    # Search in output dir
    for p in search_root.glob("Bank_Performance_Dashboard_*.xlsx"):
        files.append((p.stat().st_mtime, p))

    # Fallback to recursive search if not found
    if not files:
        for p in search_root.glob("**/Bank_Performance_Dashboard_*.xlsx"):
            files.append((p.stat().st_mtime, p))

    if not files:
        return None

    return str(sorted(files, key=lambda t: t[0], reverse=True)[0][1])

def load_config() -> Dict[str, Any]:
    """Configuration for MSPBNA Private Bank Dashboard."""
    return {
        'subject_bank_cert': 34221,  # MSPBNA
        'peer_composites': {
            'Core_PB': 90001,      # Core Private Bank Peers
            'MS_Wealth': 90002,    # MS + Extended Wealth
            'All_Peers': 90003     # Full Universe
        },
        'output_dir': 'output'
    }

def clean_value_string(val_str: str) -> float:
    """Robustly cleans currency/percent strings to floats."""
    if not isinstance(val_str, str):
        return float(val_str)

    clean = val_str.replace(',', '').replace('$', '').replace('M', '').replace('%', '').replace('x', '')
    # Handle negative signs that might be weirdly placed
    if clean.startswith('+'):
        clean = clean[1:]
    return float(clean)

def get_diff_class(diff_str: str, metric_name: str) -> str:
    """
    Determines CSS class based on difference value for color coding.
    Logic:
    - Risk Metrics (NCO, NPL): Positive Diff = Red (Bad), Negative Diff = Green (Good)
    - Growth/Size Metrics: Positive Diff = Green (Good), Negative Diff = Red (Bad)
    """
    if "N/A" in str(diff_str):
        return "neutral"

    try:
        val = clean_value_string(str(diff_str))

        # Define Risk Metrics (Lower is Better)
        risk_metrics = ['Rate', 'Risk', 'NCO', 'Nonaccrual', 'Past Due']
        is_risk = any(r in metric_name for r in risk_metrics)

        if is_risk:
            if val > 0.05: return "positive"  # Red (Worse)
            if val < -0.05: return "negative" # Green (Better)
        else:
            if val > 0.05: return "negative"  # Green (Bigger/Growing)
            if val < -0.05: return "positive" # Red (Smaller/Shrinking)

        return "neutral"
    except:
        return "neutral"

# ==================================================================================
# CHART GENERATION FUNCTIONS
# ==================================================================================

def create_credit_deterioration_chart_ppt(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    start_date: str = "2022-01-01",
    bar_metric: str = "TTM_NCO_Rate",
    line_metric: str = "Nonaccrual_to_Gross_Loans_Rate", # UPDATED V6 METRIC
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

    # MSPBNA Brand Colors
    GOLD = "#002F6C"  # MSPBNA (Deep Blue)
    BLUE = "#4C78A8"  # Peer (Lighter Blue)
    PURPLE = "#9C6FB6" # Extended Peer

    default_entities = [subject_bank_cert, 90001, 90002]
    bar_entities  = bar_entities  or default_entities
    line_entities = line_entities or [subject_bank_cert, 90001]

    # Mapping for Legend Names
    names  = {
        subject_bank_cert: "MSPBNA",
        90001: "Core PB Avg",
        90002: "MS+Wealth Avg",
        90003: "All Peers Avg"
    }
    colors = {subject_bank_cert: GOLD, 90001: BLUE, 90002: PURPLE, 90003: "#7F8C8D"}

    # Filter Data
    df = proc_df_with_peers.loc[
        proc_df_with_peers["CERT"].isin(set(bar_entities + line_entities))
    ].copy()
    df = df[df["REPDTE"] >= pd.to_datetime(start_date)].sort_values(["REPDTE","CERT"])

    if df.empty:
        print(f"No data found for chart after {start_date}")
        return None, None

    df["Period_Label"] = "Q" + df["REPDTE"].dt.quarter.astype(str) + "-" + (df["REPDTE"].dt.year % 100).astype(str).str.zfill(2)
    subj = df[df["CERT"] == subject_bank_cert][["REPDTE","Period_Label"]].drop_duplicates().sort_values("REPDTE")
    timeline = subj[["REPDTE"]].copy()
    x = np.arange(len(subj))
    xticks = subj["Period_Label"].tolist()

    # Create Figure
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111)
    ax2 = ax.twinx()

    if economist_style:
        for sp in ["top","right"]:
            ax.spines[sp].set_visible(False)
            ax2.spines[sp].set_visible(False)
        ax.grid(axis='y', alpha=0.3)
        ax2.grid(False)

    # Plot Bars
    n_bars = len(bar_entities)
    width = 0.8 / n_bars

    for i, cert in enumerate(bar_entities):
        vals = []
        for dt in timeline["REPDTE"]:
            row = df[(df["CERT"] == cert) & (df["REPDTE"] == dt)]
            val = row[bar_metric].values[0] if not row.empty else 0
            vals.append(val)

        offset = (i - n_bars/2 + 0.5) * width
        ax.bar(x + offset, vals, width=width, label=f"{names.get(cert, cert)} (Bar)",
               color=colors.get(cert, 'gray'), alpha=0.8)

    # Plot Lines
    for cert in line_entities:
        vals = []
        for dt in timeline["REPDTE"]:
            row = df[(df["CERT"] == cert) & (df["REPDTE"] == dt)]
            val = row[line_metric].values[0] if not row.empty else np.nan
            vals.append(val)

        ax2.plot(x, vals, marker='o', linewidth=3, label=f"{names.get(cert, cert)} (Line)",
                 color=colors.get(cert, 'black'), linestyle='-' if cert == subject_bank_cert else '--')

    # Formatting
    ax.set_xticks(x)
    ax.set_xticklabels(xticks, rotation=0, fontsize=tick_size)

    # Format Y Axis as Percent
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.2f}%'))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.2f}%'))

    ax.set_ylabel(bar_metric.replace('_', ' '), fontsize=axis_label_size, fontweight='bold')
    ax2.set_ylabel(line_metric.replace('_', ' '), fontsize=axis_label_size, fontweight='bold')

    title = custom_title or f"{bar_metric} vs {line_metric}"
    plt.title(title, fontsize=title_size, fontweight='bold', pad=20)

    # Unified Legend
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc='upper center', bbox_to_anchor=(0.5, -0.1), ncol=4)

    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300, bbox_inches="tight")

    return fig, ax

def plot_scatter_dynamic(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    subject_cert: int = 34221,
    peer_avg_cert_primary: int = 90001,
    peer_avg_cert_alt: int = 90002,
    use_alt_peer_avg: bool = False,
    show_peers_avg_label: bool = True,
    show_idb_label: bool = True,
    figsize: Tuple[float, float] = (8.0, 8.0),
    title_size: int = 16,
    economist_style: bool = True,
    square_axes: bool = False,
    save_path: Optional[str] = None
) -> Tuple[plt.Figure, plt.Axes]:

    if df.empty: return None, None

    # Colors
    MS_BLUE = "#002F6C"
    PEER_BLUE = "#4C78A8"
    GRAY = "#7F8C8D"

    fig, ax = plt.subplots(figsize=figsize)

    # Plot Regular Peers
    peers = df[~df["CERT"].isin([subject_cert, peer_avg_cert_primary, peer_avg_cert_alt])]
    ax.scatter(peers[x_col], peers[y_col], c=PEER_BLUE, alpha=0.6, s=100, label="Peers")

    # Plot Subject Bank (MSPBNA)
    subj = df[df["CERT"] == subject_cert]
    if not subj.empty:
        ax.scatter(subj[x_col], subj[y_col], c=MS_BLUE, s=250, edgecolor='white', linewidth=2, label="MSPBNA", zorder=10)
        if show_idb_label:
            ax.text(subj[x_col].values[0], subj[y_col].values[0], "  MSPBNA",
                    fontsize=12, fontweight='bold', color=MS_BLUE, va='center')

    # Plot Peer Avg
    avg_cert = peer_avg_cert_alt if use_alt_peer_avg else peer_avg_cert_primary
    avg = df[df["CERT"] == avg_cert]
    if not avg.empty:
        ax.scatter(avg[x_col], avg[y_col], c='black', marker='X', s=150, label="Peer Avg", zorder=9)
        # Add Quadrant Lines based on Peer Avg
        ax.axvline(avg[x_col].values[0], color=GRAY, linestyle='--', alpha=0.5)
        ax.axhline(avg[y_col].values[0], color=GRAY, linestyle='--', alpha=0.5)

    # Formatting
    ax.set_xlabel(x_col.replace('_', ' '), fontsize=12, fontweight='bold')
    ax.set_ylabel(y_col.replace('_', ' '), fontsize=12, fontweight='bold')

    # Check if axes should be percent
    if 'Rate' in x_col or 'Composition' in x_col or 'Pct' in x_col:
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.1f}%'))
    if 'Rate' in y_col or 'Coverage' in y_col or 'Pct' in y_col:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:.1f}%'))

    if square_axes:
        ax.set_aspect('equal', adjustable='datalim')

    plt.title(f"{y_col.replace('_',' ')} vs {x_col.replace('_',' ')}", fontsize=title_size, fontweight='bold')
    plt.legend()
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, dpi=300)

    return fig, ax

# ==================================================================================
# HTML TABLE GENERATION
# ==================================================================================

def _fmt_money_millions(v: float) -> str:
    """Return $X,XXXM."""
    if pd.isna(v): return "N/A"
    return f"${abs(v):,.0f}M"

def _fmt_money_millions_with_sign(diff: float) -> str:
    """Return +/-$X,XXXM."""
    if pd.isna(diff): return "N/A"
    sign = "+" if diff >= 0 else "-"
    return f"{sign}{_fmt_money_millions(abs(diff))[1:]}"

def _fmt_percent_auto(v: float) -> str:
    if pd.isna(v): return "N/A"
    # Logic: if value is small (<1), assume decimal (0.05 = 5%). If large (>1), assume percent (5.0 = 5%)
    # EXCEPT for known Ratio fields (RRI) which are roughly 1.0
    val = float(v)
    # Heuristic for likely decimals
    if abs(val) <= 1.0:
        val *= 100
    return f"{val:.2f}%"

def generate_html_email_table(df: pd.DataFrame, report_date: datetime) -> str:
    formatted_date = report_date.strftime('%B %d, %Y')

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; background-color: #f5f5f5; color: #333; }}
            .email-container {{ background-color: white; padding: 25px; border-radius: 8px; max-width: 950px; margin: 0 auto; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h2 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
            p.subtitle {{ text-align: center; color: #7f8c8d; font-size: 14px; margin-top: 0; margin-bottom: 20px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }}
            th {{ background-color: #002F6C; color: white; padding: 12px; border: 1px solid #002F6C; text-align: center; }}
            td {{ padding: 10px; text-align: center; border: 1px solid #e0e0e0; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .metric-name {{ text-align: left !important; font-weight: 600; color: #2c3e50; }}
            .subject-value {{ background-color: #E6F3FF; font-weight: bold; color: #002F6C; border: 1px solid #b3d7ff; }}
            .positive {{ color: #d32f2f; font-weight: 700; }} /* Red/Risk */
            .negative {{ color: #2e7d32; font-weight: 700; }} /* Green/Good */
            .neutral {{ color: #555; }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <h2>Private Banking Risk Monitor</h2>
            <p class="subtitle">MSPBNA vs. Core Private Bank Peers | {formatted_date}</p>
            <table>
                <thead>
                    <tr>
                        <th style="width: 28%;">Metric</th>
                        <th style="width: 16%;">MSPBNA</th>
                        <th style="width: 16%;">Core PB Avg</th>
                        <th style="width: 16%;">MS+Wealth Avg</th>
                        <th style="width: 12%;">Diff vs Core</th>
                        <th style="width: 12%;">Diff vs MS+</th>
                    </tr>
                </thead>
                <tbody>
    """
    for _, row in df.iterrows():
        diff_val = row['Diff vs Core']
        cls = get_diff_class(diff_val, row['Metric'])

        html += f"""
            <tr>
                <td class="metric-name">{row['Metric']}</td>
                <td class="subject-value">{row['MSPBNA']}</td>
                <td>{row['Core PB Avg']}</td>
                <td>{row['MS+Wealth Avg']}</td>
                <td class="{cls}">{row['Diff vs Core']}</td>
                <td>{row['Diff vs MS+']}</td>
            </tr>
        """
    html += "</tbody></table>"
    html += "<p style='font-size:11px; color:#999; text-align:center;'>Generated automatically using FDIC Call Report Data. Confidential.</p>"
    html += "</div></body></html>"
    return html

def generate_credit_metrics_email_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:

    # V6 Strategic Metrics Map
    important_metrics = {
        "ASSET": "Total Assets",
        "Total_Loan_Growth_TTM": "Total Loan Growth (YoY)",
        "SBL_Composition": "SBL % of Loans",
        "Wealth_Resi_Composition": "Wealth Resi % of Loans",
        "Fund_Finance_Composition": "Fund Banking % of Loans",
        "Risk_Adj_Allowance_Coverage": "Risk-Adj ACL Coverage",
        "Nonaccrual_to_Gross_Loans_Rate": "Nonaccrual Rate",
        "RRI_Residential": "RRI: Residential",
        "RRI_Commercial": "RRI: Commercial"
    }

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    idb = latest_data[latest_data["CERT"] == subject_bank_cert]
    core_pb = latest_data[latest_data["CERT"] == 90001]
    ms_plus = latest_data[latest_data["CERT"] == 90002]

    if idb.empty:
        print("Missing Subject Bank Data")
        return None, None

    idb = idb.iloc[0]
    core_pb = core_pb.iloc[0] if not core_pb.empty else pd.Series()
    ms_plus = ms_plus.iloc[0] if not ms_plus.empty else pd.Series()

    rows = []
    for code, disp in important_metrics.items():
        if code not in idb.index: continue

        v_idb  = idb.get(code, np.nan)
        v_core = core_pb.get(code, np.nan)
        v_ms   = ms_plus.get(code, np.nan)

        d_core = (v_idb - v_core) if pd.notna(v_core) else np.nan
        d_ms   = (v_idb - v_ms) if pd.notna(v_ms) else np.nan

        # Formatting Selection
        if code in ["ASSET"]:
            fmt = _fmt_money_millions
            fmt_diff = _fmt_money_millions_with_sign
        elif "RRI" in code:
            fmt = lambda x: f"{x:.2f}x" if pd.notna(x) else "N/A"
            fmt_diff = lambda x: f"{x:+.2f}x" if pd.notna(x) else "N/A"
        else:
            fmt = _fmt_percent_auto
            fmt_diff = _fmt_percent_auto

        rows.append({
            "Metric": disp,
            "MSPBNA": fmt(v_idb),
            "Core PB Avg": fmt(v_core),
            "MS+Wealth Avg": fmt(v_ms),
            "Diff vs Core": fmt_diff(d_core),
            "Diff vs MS+": fmt_diff(d_ms),
        })

    df = pd.DataFrame(rows)
    html = generate_html_email_table(df, latest_date)
    return html, df

# ==================================================================================
# MAIN EXECUTION
# ==================================================================================

def generate_reports():
    print("=" * 80)
    print("MSPBNA DASHBOARD REPORT GENERATOR")
    print("=" * 80)

    cfg = load_config()
    subject_cert = cfg["subject_bank_cert"]

    # 1. Find Data
    excel_file = find_latest_excel_file(cfg["output_dir"])
    if not excel_file:
        print("ERROR: No Excel file found.")
        return

    output_root = Path(excel_file).parent.resolve()
    peers_dir = output_root / "Reports"
    peers_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading: {excel_file}")

    try:
        # Load Data
        with pd.ExcelFile(excel_file) as xls:
            df_fdic = pd.read_excel(xls, sheet_name="FDIC_Data")
            df_avg = pd.read_excel(xls, sheet_name="Averages_8Q_All_Metrics")

        df_fdic["REPDTE"] = pd.to_datetime(df_fdic["REPDTE"])
        stamp = datetime.now().strftime("%Y%m%d")

        # 2. Email Table
        print("\n[1/3] Generating Executive Summary HTML...")
        html, _ = generate_credit_metrics_email_table(df_fdic, subject_cert)
        if html:
            path = peers_dir / f"MSPBNA_Executive_Summary_{stamp}.html"
            with open(path, "w") as f: f.write(html)
            print(f"      Saved: {path.name}")

        # 3. Credit Chart (NPL vs Coverage)
        print("\n[2/3] Generating Credit Trends Chart...")
        create_credit_deterioration_chart_ppt(
            proc_df_with_peers=df_fdic,
            subject_bank_cert=subject_cert,
            start_date="2022-01-01",
            bar_metric="Nonaccrual_to_Gross_Loans_Rate", # Using Nonaccruals
            line_metric="Risk_Adj_Allowance_Coverage",   # Using Risk-Adj Coverage
            custom_title="Asset Quality: Nonaccruals (Bars) vs Risk-Adj Coverage (Lines)",
            save_path=str(peers_dir / f"Credit_Trends_{stamp}.png")
        )
        print("      Saved: Credit_Trends.png")

        # 4. Strategic Scatters
        print("\n[3/3] Generating Strategic Scatter Plots...")

        # Coverage Discipline
        plot_scatter_dynamic(
            df=df_avg,
            x_col="Nonaccrual_to_Gross_Loans_Rate",
            y_col="Risk_Adj_Allowance_Coverage",
            subject_cert=subject_cert,
            title_size=14,
            save_path=str(peers_dir / f"Scatter_Coverage_Discipline_{stamp}.png")
        )

        # Wealth Strategy
        plot_scatter_dynamic(
            df=df_avg,
            x_col="Wealth_Resi_Composition",
            y_col="RRI_Residential",
            subject_cert=subject_cert,
            title_size=14,
            save_path=str(peers_dir / f"Scatter_Wealth_Strategy_{stamp}.png")
        )
        print("      Saved: Strategic Scatters")

        print("\n" + "="*80)
        print(f"DONE. Reports saved to: {peers_dir}")
        print("="*80)

    except Exception as e:
        print(f"\nCRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    generate_reports()