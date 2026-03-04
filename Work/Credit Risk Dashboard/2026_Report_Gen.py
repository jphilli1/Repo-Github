#!/usr/bin/env python3
"""
Bank Performance Report Generator
=================================

A self-contained reporting engine that automatically locates the latest processed
Excel file and generates professional charts and HTML tables for credit risk analysis.

Usage:
    python report_generator.py

Output:
    - Credit deterioration chart (PNG)
    - Email-ready HTML table
    - Custom flexible HTML table
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

# Set style for better-looking charts
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

# ==================================================================================
# CONFIGURATION AND UTILITY FUNCTIONS
# ==================================================================================

from pathlib import Path

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
            # try one level up as a last resort
            for p in glob_root.glob("**/Bank_Performance_Dashboard_*.xlsx"):
                try:
                    files.append((p.stat().st_mtime, p))
                except OSError:
                    pass
        return str(sorted(files, key=lambda t: t[0], reverse=True)[0][1]) if files else None

    return newest(search_root)


def load_config() -> Dict[str, Any]:
    """
    Configuration for MSPBNA Private Bank Dashboard.

    Peer Groups:
    - 34221: MSPBNA (subject bank)
    - 32992: MSBNA (Morgan Stanley Bank NA - sibling comparison)
    - 90001: Core Private Bank Peers (GS + UBS average)
    - 90002: MS + Extended Wealth Peers
    - 90003: Full Peer Universe (all peers average)

    Normalized Peer Groups (Ex-Commercial/Ex-Consumer):
    - 90011: Core PB Normalized (GS + UBS, excluding C&I/NDFI/ADC/Consumer)
    - 90012: MS+Wealth Normalized
    - 90013: All Peers Normalized
    """
    return {
        'subject_bank_cert': 34221,  # MSPBNA
        'sibling_bank_cert': 32992,  # MSBNA (for table comparisons)
        # Peer composites created by CR_Bank_DashvMSPB.py
        'peer_composites': {
            'Core_PB': 90001,      # Core Private Bank Peers (GS + UBS)
            'MS_Wealth': 90002,    # MS + Extended Wealth
            'All_Peers': 90003,    # Full Universe
            # Normalized (Ex-Commercial/Ex-Consumer)
            'Core_PB_Norm': 90011,   # Core PB with normalized metrics
            'MS_Wealth_Norm': 90012, # MS+Wealth with normalized metrics
            'All_Peers_Norm': 90013  # All Peers with normalized metrics
        },
        # Individual peer CERTs for reference
        'core_pb_members': {
            'GS': 33124,           # Goldman Sachs Bank USA
            'UBS': 57565           # UBS Bank USA
        },
        'output_dir': 'output'
    }



def get_diff_class(diff_str: str) -> str:
    """
    Determines CSS class based on difference value for color coding.

    Args:
        diff_str (str): Difference value as a string (e.g., "+1.25%")

    Returns:
        str: CSS class name
    """
    if diff_str == "N/A":
        return "neutral"

    try:
        # Remove % sign and convert to float
        diff_value = float(diff_str.replace('%', '').replace('+', ''))

        if diff_value > 0.05:  # More than 5 basis points worse
            return "positive"  # Red (worse performance)
        elif diff_value < -0.05:  # More than 5 basis points better
            return "negative"  # Green (better performance)
        else:
            return "neutral"   # Brown (neutral)
    except:
        return "neutral"

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
    with MS brand colors for subject bank and improved labeling strategy.

    Args:
        proc_df_with_peers (pd.DataFrame): Processed data with all banks and peers
        subject_bank_cert (int): CERT number of the subject bank (MSPBNA = 34221)
        show_both_peer_groups (bool): Whether to show both peer groups

    Returns:
        Tuple[Optional[plt.Figure], Optional[plt.Axes]]: Chart figure and axes
    """
    # Define entities to plot based on options
    entities_to_plot = [subject_bank_cert]  # Always include subject bank

    if show_both_peer_groups:
        entities_to_plot.extend([32992, 90001])  # MSBNA and Core PB
        colors = {
            subject_bank_cert: '#002F6C',  # MS Deep Blue for subject bank
            32992: '#4C78A8',              # Light Blue for MSBNA
            90001: '#F7A81B'               # Gold for Core PB
        }
        entity_names = {
            subject_bank_cert: "MSPBNA",
            32992: "MSBNA",
            90001: "Core PB"
        }
    else:
        entities_to_plot.append(90001)  # Only Core PB
        colors = {
            subject_bank_cert: '#002F6C',  # MS Deep Blue for subject bank
            90001: '#F7A81B'               # Gold for Core PB
        }
        entity_names = {
            subject_bank_cert: "MSPBNA",
            90001: "Core PB"
        }

    chart_df = proc_df_with_peers[proc_df_with_peers['CERT'].isin(entities_to_plot)].copy()

    if chart_df.empty:
        print("No data found for charting")
        return None, None

    # Sort by date
    chart_df = chart_df.sort_values('REPDTE')

    # Filter for dates after Q3 2019 (2019-09-30)
    chart_df = chart_df[chart_df['REPDTE'] >= '2019-10-01']

    if chart_df.empty:
        print("No data found after Q3 2019")
        return None, None

    # Create period labels - show Q#-YY format
    chart_df['Quarter'] = chart_df['REPDTE'].dt.quarter
    chart_df['Year'] = chart_df['REPDTE'].dt.year
    chart_df['Period_Label'] = 'Q' + chart_df['Quarter'].astype(str) + '-' + (chart_df['Year'] % 100).astype(str).str.zfill(2)

    # Labeling logic: Show every 4 periods starting from latest going backward
    subject_data = chart_df[chart_df['CERT'] == subject_bank_cert].reset_index(drop=True)
    num_periods = len(subject_data)

    # Create mask for showing labels - every 4 periods from the end
    show_label_mask = [False] * num_periods
    for i in range(num_periods - 1, -1, -4):  # Start from latest, go backward by 4
        show_label_mask[i] = True

    # Also show Q4 and Q2 periods regardless
    for i, row in subject_data.iterrows():
        quarter = row['Quarter']
        if quarter in [2, 4]:  # Q2 and Q4
            show_label_mask[i] = True

    # Apply the mask to all entities
    chart_df['Show_Label'] = False  # Initialize all as False
    for cert in entities_to_plot:
        cert_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if len(cert_data) == len(show_label_mask):
            # Get the original indices for this cert
            cert_indices = chart_df[chart_df['CERT'] == cert].index
            chart_df.loc[cert_indices, 'Show_Label'] = show_label_mask

    # Create the chart
    fig, ax = plt.subplots(figsize=(16, 8))

    # Get the subject bank data for x-axis positioning
    x_positions = np.arange(len(subject_data))

    # Calculate bar width and positions based on number of entities
    num_entities = len(entities_to_plot)
    bar_width = 0.8 / num_entities
    bar_positions = {}

    for i, cert in enumerate(entities_to_plot):
        offset = (i - (num_entities - 1) / 2) * bar_width
        bar_positions[cert] = x_positions + offset

    # Plot TTM NCO Rate (bars)
    for cert in entities_to_plot:
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            label = entity_names.get(cert, f"CERT {cert}")

            # Plot TTM NCO Rate as bars
            nco_rate = entity_data['TTM_NCO_Rate'].fillna(0) / 100  # Convert to decimal

            bars = ax.bar(bar_positions[cert], nco_rate, alpha=0.7, color=colors[cert],
                         label=f'{label} TTM NCO Rate', width=bar_width)

    # Create second y-axis for NPL rates (lines)
    ax2 = ax.twinx()

    # Plot NPL-to-Gross Loans Rate (lines)
    line_styles = ['-', '--', '-.']  # Different line styles for each entity
    for i, cert in enumerate(entities_to_plot):
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            label = entity_names.get(cert, f"CERT {cert}")

            # Plot NPL rate as line
            npl_rate = entity_data['Nonaccrual_to_Gross_Loans_Rate'].fillna(0) / 100  # Convert to decimal

            line_style = line_styles[i % len(line_styles)]
            ax2.plot(x_positions, npl_rate, color=colors[cert],
                    linestyle=line_style, marker='o', linewidth=2.5,
                    label=f'{label} NPL-to-Book', markersize=5)

    # Format the chart
    ax.set_xlabel('Reporting Period', fontsize=12, fontweight='bold')
    ax.set_ylabel('TTM NCO Rate', fontsize=12, fontweight='bold', color='black')
    ax2.set_ylabel('Nonaccrual-to-Gross Loans Rate', fontsize=12, fontweight='bold', color='black')

    # Format y-axes as percentages
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.2%}'.format(y)))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.2%}'.format(y)))

    # Set x-axis labels - only show labels where Show_Label is True
    ax.set_xticks(x_positions)

    # Create labels array - empty string for periods we don't want to show
    labels = []
    for i, (_, row) in enumerate(subject_data.iterrows()):
        if show_label_mask[i]:
            labels.append(row['Period_Label'])
        else:
            labels.append('')  # Empty string for hidden labels

    ax.set_xticklabels(labels, rotation=45, ha='right', fontsize=10)

    # Add value labels on bars and points (only for labeled periods)
    for cert in entities_to_plot:
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            # NCO Rate labels (bars) - only for shown periods
            nco_rate = entity_data['TTM_NCO_Rate'].fillna(0) / 100
            entity_show_labels = entity_data['Show_Label'].values if 'Show_Label' in entity_data.columns else [False] * len(entity_data)

            for i, rate in enumerate(nco_rate):
                if i < len(entity_show_labels) and entity_show_labels[i] and not np.isnan(rate) and rate > 0:
                    ax.text(bar_positions[cert][i], rate + 0.0005, f'{rate:.2%}',
                           ha='center', va='bottom', fontsize=8, fontweight='bold')

            # NPL Rate labels (lines) - only for shown periods and only for subject bank to avoid clutter
            if cert == subject_bank_cert:
                npl_rate = entity_data['Nonaccrual_to_Gross_Loans_Rate'].fillna(0) / 100
                for i, rate in enumerate(npl_rate):
                    if i < len(entity_show_labels) and entity_show_labels[i] and not np.isnan(rate) and rate > 0:
                        ax2.text(i, rate + 0.0005, f'{rate:.2%}',
                                ha='center', va='bottom', fontsize=8, fontweight='bold',
                                bbox=dict(boxstyle='round,pad=0.2', facecolor='white', alpha=0.7))

    # Combine legends
    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc='upper left', frameon=True, fancybox=True)

    # Set title
    title = 'Credit Deterioration Analysis\nTTM NCO Rate (bars) vs NPL-to-Book (lines)'
    if show_both_peer_groups:
        title += '\n(F&V = Flagstar & Valley excluded from green series)'
    plt.title(title, fontsize=14, fontweight='bold', pad=20)

    # Adjust layout
    plt.tight_layout()

    # Set grid
    ax.grid(True, alpha=0.3)
    ax2.grid(False)

    # Improve spacing
    plt.subplots_adjust(bottom=0.15)

    return fig, ax

# ==================================================================================
# HTML TABLE GENERATION FUNCTIONS
# ==================================================================================
# ---- currency formatting helpers (values are already in millions) ----
CURRENCY_MM_CODES = {"ASSET", "LNLS"}  # Assets, Gross Loans

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
    # Heuristic: treat values < 1 as decimals; otherwise already percent-points
    if abs(f) < 1.0:
        f *= 100.0
    return f"{f:.2f}%"

def generate_html_email_table(df: pd.DataFrame, report_date: datetime) -> str:
    formatted_date = report_date.strftime('%B %d, %Y')

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; background-color: #f5f5f5; }}
            .email-container {{ background-color: white; padding: 20px; border-radius: 8px; max-width: 900px; margin: 0 auto; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 12px; }}
            th {{ background-color: #002F6C; color: white; padding: 10px; border: 1px solid #2c3e50; }} /* MS Blue */
            td {{ padding: 8px; text-align: center; border: 1px solid #bdc3c7; }}
            .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; }}
            .subject-value {{ background-color: #E6F3FF; font-weight: bold; color: #002F6C; }} /* Light Blue */
            .positive {{ color: #d32f2f; font-weight: 600; }} /* Red/Risk */
            .negative {{ color: #388e3c; font-weight: 600; }} /* Green/Good */
            .neutral {{ color: #000000; }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div style="text-align:center; color:#002F6C;">
                <h2>Private Banking Risk Monitor</h2>
                <p>MSPBNA vs. Core Private Bank Peers | {formatted_date}</p>
            </div>
            <table>
                <thead>
                    <tr>
                        <th style="width: 30%;">Metric</th>
                        <th style="width: 15%;">MSPBNA</th>
                        <th style="width: 15%;">Core PB Avg</th>
                        <th style="width: 15%;">MS+Wealth Avg</th>
                        <th style="width: 12%;">Diff vs Core</th>
                        <th style="width: 12%;">Diff vs MS+</th>
                    </tr>
                </thead>
                <tbody>
    """
    for _, row in df.iterrows():
        diff_val = row['Diff vs Core']
        cls = "neutral"

        try:
            if "N/A" not in diff_val:
                # CLEANING: Remove $, M, %, x, comma, and + to get raw number
                # Example: "-$92M" -> "-92"
                clean_val = diff_val.replace(',','').replace('$','').replace('M','').replace('%','').replace('x','').replace('+','')
                val = float(clean_val)

                # LOGIC:
                # For Risk Metrics (Rates, Ratios, NCOs), Positive Diff = Higher Risk (Red/Positive class)
                # For Growth/Asset Metrics, Positive Diff = Growth (Green/Negative class)

                is_risk_metric = any(x in row['Metric'] for x in ['Rate', 'Risk', 'NCO', 'RRI', 'Nonaccrual'])

                if is_risk_metric:
                    # Higher risk metric = Bad (Red/Positive Class)
                    cls = "positive" if val > 0 else "negative"
                else:
                    # Higher growth/assets = Good (Green/Negative Class)
                    cls = "negative" if val > 0 else "positive"

        except Exception:
            # If parsing fails (e.g. unexpected format), keep neutral to prevent crash
            cls = "neutral"

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
    html += "</tbody></table></div></body></html>"
    return html

# ==================================================================================
#  HELPER: FORMATTERS
# ==================================================================================
def _fmt_call_report_date(dt: datetime) -> str:
    """Converts 2025-09-30 -> 'Call Report Q3-25'"""
    if pd.isna(dt): return "N/A"
    q = (dt.month - 1) // 3 + 1
    yy = dt.strftime('%y')
    return f"Call Report Q{q}-{yy}"

def _fmt_money_billions(v: float) -> str:
    # Added comma for thousands separator: {:,.1f}
    return f"${v/1_000_000:,.1f}B" if pd.notna(v) else "N/A"

def _fmt_money_billions_diff(v: float) -> str:
    # Added comma for thousands separator: {:+,.1f}
    return f"{v/1_000_000:+,.1f}B" if pd.notna(v) else "N/A"

def _fmt_percent(v: float) -> str:
    if pd.isna(v): return "N/A"
    # Logic: if < 1.0 (e.g. 0.015), mult by 100. If > 1.0 (1.5), keep.
    val = v * 100.0 if abs(v) < 1.0 else v
    return f"{val:.2f}%"

def _fmt_multiple(v: float) -> str:
    if pd.isna(v): return "N/A"
    return f"{v:.2f}x"

def _fmt_multiple_diff(v: float) -> str:
    if pd.isna(v): return "N/A"
    return f"{v:+.2f}x"

# ==================================================================================
#  TABLE 1: THE "SKINNY" SUMMARY (MSPBNA, MSBNA, Core Avg, All Avg, Diffs)
# ==================================================================================
def generate_credit_metrics_email_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:

    # 1. Define Metrics - UPDATED with Normalized and Restored Metrics
    metric_map = {
        # === BALANCE SHEET ===
        "ASSET": ("Total Assets ($B)", 'B'),
        "LNLS":  ("Total Loans ($B)", 'B'),

        # === STANDARD CREDIT METRICS ===
        "Risk_Adj_Allowance_Coverage":   ("Risk-Adj ACL Ratio (%)", '%'),
        "Allowance_to_Gross_Loans_Rate": ("Headline ACL Ratio (%)", '%'),
        "Nonaccrual_to_Gross_Loans_Rate":("Nonaccrual Rate (%)", '%'),
        "TTM_NCO_Rate":                  ("NCO Rate (TTM) (%)", '%'),

        # === NORMALIZED CREDIT METRICS (Ex-Commercial/Ex-Consumer) ===
        "Norm_NCO_Rate":           ("Norm NCO Rate (%)", '%'),
        "Norm_Nonaccrual_Rate":    ("Norm Nonaccrual Rate (%)", '%'),
        "Norm_ACL_Coverage":       ("Norm ACL Ratio (%)", '%'),
        "Norm_Exclusion_Pct":      ("Excluded Loans (%)", '%'),

        # === COMPOSITION ===
        "SBL_Composition":     ("SBL % of Loans", '%'),
        "RIC_Resi_Loan_Share": ("Resi % of Loans", '%'),
        "RIC_CRE_Loan_Share":  ("CRE % of Loans", '%'),
        "RIC_CRE_ACL_Share":   ("CRE % of ACL", '%'),

        # === CRE SEGMENT ===
        "RIC_CRE_ACL_Coverage":    ("CRE ACL Ratio (%)", '%'),
        "RIC_CRE_Risk_Adj_Coverage": ("CRE NPL Coverage (x)", 'x'),
        "RIC_CRE_Nonaccrual_Rate": ("% of CRE in Nonaccrual", '%'),
        "RIC_CRE_NCO_Rate":        ("CRE NCO Rate (TTM)", '%'),

        # === NORMALIZED COMPOSITION ===
        "Norm_SBL_Composition":          ("Norm SBL % of Loans", '%'),
        "Norm_Fund_Finance_Composition": ("Norm Fund Finance %", '%'),
        "Norm_Wealth_Resi_Composition":  ("Norm Wealth Resi %", '%'),

        # === LIQUIDITY (RESTORED) ===
        "Liquidity_Ratio":   ("Liquidity Ratio (%)", '%'),
        "HQLA_Ratio":        ("HQLA Ratio (%)", '%'),
        "Loans_to_Deposits": ("Loans to Deposits (%)", '%'),

        # === PROFITABILITY (RESTORED - Raw Series) ===
        "ROA":               ("ROA (%)", '%'),
        "ROE":               ("ROE (%)", '%'),
        "NIMY":              ("Net Interest Margin (%)", '%'),
        "EEFFR":             ("Efficiency Ratio (%)", '%'),

        # === NORMALIZED PROFITABILITY ===
        "Norm_Loan_Yield":       ("Norm Loan Yield (%)", '%'),
        "Norm_Provision_Rate":   ("Norm Provision Rate (%)", '%'),
        "Norm_Loss_Adj_Yield":   ("Norm Loss-Adj Yield (%)", '%'),
    }

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # 2. Get Data Rows
    # MSPBNA (Subject), MSBNA (32992), Core PB (90001), All Peers (90003)
    try:
        idb = latest_data[latest_data["CERT"] == subject_bank_cert].iloc[0]
        msbna = latest_data[latest_data["CERT"] == 32992].iloc[0]
        core = latest_data[latest_data["CERT"] == 90001].iloc[0]
        all_peers = latest_data[latest_data["CERT"] == 90003].iloc[0]
    except IndexError:
        return None, None # Missing data

    rows = []
    for code, (disp, fmt) in metric_map.items():
        if code not in idb.index: continue

        # Values
        v_idb, v_msbna, v_core, v_all = idb.get(code), msbna.get(code), core.get(code), all_peers.get(code)

        # Diffs
        d_msbna = v_idb - v_msbna if pd.notna(v_msbna) else np.nan
        d_core = v_idb - v_core if pd.notna(v_core) else np.nan
        d_all = v_idb - v_all if pd.notna(v_all) else np.nan

        # Formatters
        if fmt == 'B': f, fd = _fmt_money_billions, _fmt_money_billions_diff
        elif fmt == 'x': f, fd = _fmt_multiple, _fmt_multiple_diff
        else: f, fd = _fmt_percent, _fmt_percent # Percent handles diffs same way

        rows.append({
            "Metric": disp,
            "MSPBNA": f(v_idb),
            "MSBNA": f(v_msbna),
            "Core PB Avg": f(v_core),
            "All Peers Avg": f(v_all),
            "Diff vs MSBNA": fd(d_msbna),
            "Diff vs Core PB": fd(d_core),
            "Diff vs All Peers": fd(d_all)
        })

    df = pd.DataFrame(rows)
    html = generate_html_email_table_dynamic(df, latest_date, table_type="summary")
    return html, df

# ==================================================================================
#  HELPER: SMART NAME CLEANER
# ==================================================================================
def _clean_bank_name(name: str, cert: int) -> str:
    """Smart mapping for bank names to avoid 'OF' errors."""
    # 1. Hardcoded Map for G-SIBs / Common Peers (Safest)
    known_map = {
        3510: "BofA",          # Bank of America
        639:  "BNY Mellon",    # Bank of NY Mellon
        7213: "Citibank",
        628:  "JPMorgan",
        3511: "Wells Fargo",
        32992: "MSBNA",
        34221: "MSPBNA",
        57565: "UBS",
        33124: "GS Bank",
        541:  "State St",
        # Standard peer composites
        90001: "Core Avg",
        90002: "MS+Wealth",
        90003: "Peer Avg",
        # Normalized peer composites
        90011: "Core Avg (Norm)",
        90012: "MS+Wealth (Norm)",
        90013: "Peer Avg (Norm)"
    }
    if cert in known_map:
        return known_map[cert]

    # 2. Algorithmic Fallback
    # Remove legal suffixes
    clean = name.replace("NATIONAL ASSOCIATION", "").replace(" N.A.", "").replace(" NA", "")
    clean = clean.strip()

    # Logic: If starts with "BANK OF", keep "Bank of X" or acronym
    if clean.startswith("BANK OF"):
        # e.g. "BANK OF THE WEST" -> "BotW" or just keep first 2 words
        words = clean.split()
        if len(words) >= 3:
            return f"{words[0]} {words[2]}" # "BANK OF WEST" -> "BANK WEST" (Rough, but better than OF)
        return clean[:10]

    # Standard: Remove "BANK" and take first word
    clean = clean.replace("BANK", "").strip()
    return clean.split()[0] if clean else name[:10]

# ==================================================================================
#  TABLE 2: THE "WIDE" DETAILED TABLE (Individual Banks + Peer Avg)
# ==================================================================================
def generate_detailed_peer_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:

    metric_map = {
        # === BALANCE SHEET ===
        "ASSET": ("Total Assets ($B)", 'B'),
        "LNLS":  ("Total Loans ($B)", 'B'),

        # === STANDARD CREDIT METRICS ===
        "Risk_Adj_Allowance_Coverage":   ("Risk-Adj ACL Ratio (%)", '%'),
        "Allowance_to_Gross_Loans_Rate": ("Headline ACL Ratio (%)", '%'),
        "Nonaccrual_to_Gross_Loans_Rate":("Nonaccrual Rate (%)", '%'),
        "TTM_NCO_Rate":                  ("NCO Rate (TTM) (%)", '%'),

        # === NORMALIZED CREDIT METRICS ===
        "Norm_NCO_Rate":           ("Norm NCO Rate (%)", '%'),
        "Norm_Nonaccrual_Rate":    ("Norm NA Rate (%)", '%'),
        "Norm_ACL_Coverage":       ("Norm ACL Ratio (%)", '%'),

        # === COMPOSITION ===
        "SBL_Composition":     ("SBL % of Loans", '%'),
        "RIC_Resi_Loan_Share": ("Resi % of Loans", '%'),
        "RIC_CRE_Loan_Share":  ("CRE % of Loans", '%'),
        "RIC_CRE_ACL_Share":   ("CRE % of ACL", '%'),

        # === CRE SEGMENT ===
        "RIC_CRE_ACL_Coverage":    ("CRE ACL Ratio (%)", '%'),
        "RIC_CRE_Risk_Adj_Coverage": ("CRE NPL Coverage (x)", 'x'),
        "RIC_CRE_Nonaccrual_Rate": ("% of CRE in Nonaccrual", '%'),
        "RIC_CRE_NCO_Rate":        ("CRE NCO Rate (TTM)", '%'),

        # === LIQUIDITY (RESTORED) ===
        "Liquidity_Ratio":   ("Liquidity Ratio (%)", '%'),
        "Loans_to_Deposits": ("Loans/Deposits (%)", '%'),

        # === PROFITABILITY ===
        "ROA":     ("ROA (%)", '%'),
        "ROE":     ("ROE (%)", '%'),
        "NIMY":    ("NIM (%)", '%'),
        "EEFFR":   ("Efficiency Ratio (%)", '%'),
    }

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date].copy()

    # --- SORTING LOGIC ---
    # 1. Fixed Slots: MSPBNA -> MSBNA -> UBS (57565) -> GS (33124)
    fixed_certs = [subject_bank_cert, 32992, 57565, 33124]

    # 2. Dynamic Slots: Everyone else (excluding Aggregates 9xxxx)
    real_banks = latest_data[
        (~latest_data["CERT"].isin(fixed_certs)) &
        (latest_data["CERT"] < 90000)
    ]

    # Sort them by Total Loans (LNLS) ascending
    sorted_others = real_banks.sort_values("LNLS", ascending=True)["CERT"].tolist()

    # 3. Final Column Order
    final_cert_order = fixed_certs + sorted_others + [90003]

    rows = []
    for code, (disp, fmt) in metric_map.items():
        row_dict = {"Metric": disp}

        if fmt == 'B': f = _fmt_money_billions
        elif fmt == 'x': f = _fmt_multiple
        else: f = _fmt_percent

        for cert in final_cert_order:
            if cert not in latest_data["CERT"].values: continue
            val = latest_data[latest_data["CERT"] == cert][code].iloc[0]

            # --- USE SMART NAME CLEANER HERE ---
            raw_name = latest_data[latest_data["CERT"]==cert]["NAME"].iloc[0]
            c_name = _clean_bank_name(raw_name, cert)

            row_dict[c_name] = f(val)

        rows.append(row_dict)

    df = pd.DataFrame(rows)
    html = generate_html_email_table_dynamic(df, latest_date, table_type="detailed")
    return html, df

# ==================================================================================
#  TABLE 3: NORMALIZED METRICS COMPARISON (Standard vs Normalized)
# ==================================================================================
def generate_normalized_comparison_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates a side-by-side comparison of Standard vs Normalized metrics.
    This is the "apples-to-apples" view that strips out C&I/NDFI/ADC/Consumer.
    """

    # Define metric pairs: Standard -> Normalized
    metric_pairs = {
        # (Standard Metric, Normalized Metric, Display Name, Format)
        ("LNLS", "Norm_Gross_Loans"): ("Gross Loans ($B)", "Norm Loans ($B)", 'B'),
        ("TTM_NCO_Rate", "Norm_NCO_Rate"): ("NCO Rate (%)", "Norm NCO Rate (%)", '%'),
        ("Nonaccrual_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate"): ("NA Rate (%)", "Norm NA Rate (%)", '%'),
        ("Allowance_to_Gross_Loans_Rate", "Norm_ACL_Coverage"): ("ACL Ratio (%)", "Norm ACL Ratio (%)", '%'),
        ("SBL_Composition", "Norm_SBL_Composition"): ("SBL % of Loans", "Norm SBL %", '%'),
        ("Fund_Finance_Composition", "Norm_Fund_Finance_Composition"): ("Fund Finance %", "Norm Fund Fin %", '%'),
    }

    # Exclusion breakdown metrics
    exclusion_metrics = {
        "Norm_Exclusion_Pct": ("Total Excluded (%)", '%'),
        "Excl_CI_Balance": ("C&I Excluded ($B)", 'B'),
        "Excl_NDFI_Balance": ("NDFI Excluded ($B)", 'B'),
        "Excl_ADC_Balance": ("ADC Excluded ($B)", 'B'),
        "Excl_CreditCard_Balance": ("Credit Cards ($B)", 'B'),
        "Excl_Auto_Balance": ("Auto Loans ($B)", 'B'),
        "Excl_Ag_Balance": ("Ag Loans ($B)", 'B'),
    }

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Get data rows
    try:
        mspbna = latest_data[latest_data["CERT"] == subject_bank_cert].iloc[0]
        msbna = latest_data[latest_data["CERT"] == 32992].iloc[0]
        core = latest_data[latest_data["CERT"] == 90001].iloc[0]
    except IndexError:
        return None, None

    rows = []

    # Add header row for paired metrics
    rows.append({
        "Metric Type": "=== STANDARD vs NORMALIZED ===",
        "MSPBNA (Std)": "", "MSPBNA (Norm)": "",
        "MSBNA (Std)": "", "MSBNA (Norm)": "",
        "Core PB (Std)": "", "Core PB (Norm)": ""
    })

    for (std_code, norm_code), (std_disp, norm_disp, fmt) in metric_pairs.items():
        if std_code not in mspbna.index or norm_code not in mspbna.index:
            continue

        if fmt == 'B': f = _fmt_money_billions
        elif fmt == 'x': f = _fmt_multiple
        else: f = _fmt_percent

        rows.append({
            "Metric Type": std_disp,
            "MSPBNA (Std)": f(mspbna.get(std_code)),
            "MSPBNA (Norm)": f(mspbna.get(norm_code)),
            "MSBNA (Std)": f(msbna.get(std_code)),
            "MSBNA (Norm)": f(msbna.get(norm_code)),
            "Core PB (Std)": f(core.get(std_code)),
            "Core PB (Norm)": f(core.get(norm_code))
        })

    # Add header row for exclusion breakdown
    rows.append({
        "Metric Type": "=== EXCLUSION BREAKDOWN ===",
        "MSPBNA (Std)": "", "MSPBNA (Norm)": "",
        "MSBNA (Std)": "", "MSBNA (Norm)": "",
        "Core PB (Std)": "", "Core PB (Norm)": ""
    })

    for code, (disp, fmt) in exclusion_metrics.items():
        if code not in mspbna.index:
            continue

        if fmt == 'B': f = _fmt_money_billions
        else: f = _fmt_percent

        rows.append({
            "Metric Type": disp,
            "MSPBNA (Std)": f(mspbna.get(code)),
            "MSPBNA (Norm)": "-",
            "MSBNA (Std)": f(msbna.get(code)),
            "MSBNA (Norm)": "-",
            "Core PB (Std)": f(core.get(code)),
            "Core PB (Norm)": "-"
        })

    df = pd.DataFrame(rows)
    html = generate_normalized_html_table(df, latest_date)
    return html, df


def generate_normalized_html_table(df: pd.DataFrame, report_date: datetime) -> str:
    """Custom HTML formatter for the normalized comparison table."""
    date_str = _fmt_call_report_date(report_date)

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{
            background-color: transparent;
            padding: 20px;
            max-width: 1400px;
            margin: 0 auto;
            text-align: center;
        }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 0 auto; font-size: 11px; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; }}

        .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 150px; }}
        .section-header {{ background-color: #E6F3FF !important; font-weight: bold; color: #002F6C; text-align: left !important; }}
        .std-value {{ background-color: #FFF9E6 !important; }}
        .norm-value {{ background-color: #E6FFE6 !important; font-weight: bold; }}

        .footnote {{
            font-size: 10px; color: #666; margin-top: 20px; border-top: 1px solid #ccc; padding-top: 10px; text-align: left;
        }}
    </style></head><body>

    <div class="email-container">
        <h3>Normalized Metrics Comparison (Ex-Commercial/Ex-Consumer)</h3>
        <p class="date-header">{date_str}</p>
        <table><thead><tr>
            <th>Metric</th>
            <th colspan="2" style="background-color:#003D7A;">MSPBNA</th>
            <th colspan="2" style="background-color:#4C78A8;">MSBNA</th>
            <th colspan="2" style="background-color:#F7A81B; color:#000;">Core PB</th>
        </tr>
        <tr>
            <th></th>
            <th>Standard</th><th>Normalized</th>
            <th>Standard</th><th>Normalized</th>
            <th>Standard</th><th>Normalized</th>
        </tr></thead><tbody>
    """

    for _, row in df.iterrows():
        metric = row["Metric Type"]

        # Check if this is a section header
        if "===" in str(metric):
            html += f'<tr><td colspan="7" class="section-header">{metric}</td></tr>'
            continue

        html += f"""<tr>
            <td class="metric-name">{metric}</td>
            <td class="std-value">{row['MSPBNA (Std)']}</td>
            <td class="norm-value">{row['MSPBNA (Norm)']}</td>
            <td class="std-value">{row['MSBNA (Std)']}</td>
            <td class="norm-value">{row['MSBNA (Norm)']}</td>
            <td class="std-value">{row['Core PB (Std)']}</td>
            <td class="norm-value">{row['Core PB (Norm)']}</td>
        </tr>"""

    html += """</tbody></table>

    <div class="footnote">
        <p><b>Normalization Methodology:</b></p>
        <ul>
            <li><b>Excluded Segments:</b> Domestic C&I, NDFI (Fund Finance), ADC (Construction), Credit Cards, Auto Loans, Ag Loans</li>
            <li><b>Purpose:</b> Creates "apples-to-apples" comparison by removing Mass Market Consumer and Commercial Banking segments that MSPBNA does not participate in</li>
            <li><b>Norm Gross Loans:</b> Total Loans minus all Excluded Balances</li>
            <li><b>Norm Rates:</b> Numerator exclusions (NCOs, Nonaccruals) divided by Norm Gross Loans</li>
        </ul>
    </div>

    </div></body></html>
    """
    return html

# ==================================================================================
#  SHARED HTML ENGINE (Styling & Footnotes)
# ==================================================================================
def generate_html_email_table_dynamic(df: pd.DataFrame, report_date: datetime, table_type: str) -> str:
    cols = df.columns.tolist()
    date_str = _fmt_call_report_date(report_date)

    title = "Executive Credit Summary" if table_type == "summary" else "Detailed Peer Analysis"

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{
            background-color: transparent;
            padding: 20px;
            max-width: 1200px;
            margin: 0 auto;
            text-align: center;
        }}
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

        .footnote {{
            font-size: 10px; color: #666; margin-top: 20px; border-top: 1px solid #ccc; padding-top: 10px; text-align: left;
        }}
    </style></head><body>

    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{date_str}</p>
        <table><thead><tr>"""

    for c in cols:
        html += f"<th>{c}</th>"
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

            if table_type == "summary" and "Diff" in c and "N/A" not in str(val):
                try:
                    num = float(str(val).replace('+','').replace('%','').replace('x','').replace('$','').replace('B','').replace(',',''))
                    is_risk = any(k in metric for k in ['Nonaccrual', 'NCO', 'Delinq'])
                    is_safe = any(k in metric for k in ['Coverage', 'Ratio', 'Equity'])
                    if is_risk: cls = 'class="bad-trend"' if num > 0 else 'class="good-trend"'
                    elif is_safe: cls = 'class="good-trend"' if num > 0 else 'class="bad-trend"'
                except: pass

            html += f"<td {cls}>{val}</td>"
        html += "</tr>"

    # --- UPDATED FOOTNOTE DESCRIPTION ---
    html += """</tbody></table>

    <div class="footnote">
        <p><b>Peer Definitions:</b></p>
        <p><b>1. Core PB:</b> Goldman Sachs Bank USA, UBS Bank USA.</p>
        <p><b>2. All Peers:</b> US G-SIB Credit Intermediaries + UBS.</p>
        <br>
        <p><b>Methodology:</b></p>
        <p><b>Risk-Adj ACL Ratio:</b> Total ACL / (Gross Loans - SBL). Removes low-risk SBL to show coverage on core credit.</p>
        <p><b>CRE NPL Coverage:</b> CRE-Specific ACL / CRE Nonaccrual Loans. ($ Reserved per $ of Bad Loans).</p>
    </div></div></body></html>"""

    return html


def generate_flexible_html_table(
    proc_df_with_peers: pd.DataFrame,
    metrics_to_display: Dict[str, str],
    title: str,
    subject_bank_cert: int = 34221,
    col_names: Optional[Dict[str, str]] = None
) -> str:
    """
    Generates a flexible HTML table with user-defined metrics and styling.

    Args:
        proc_df_with_peers (pd.DataFrame): Processed data with all banks and peers
        metrics_to_display (Dict[str, str]): Mapping of metric codes to display names
        title (str): Table title
        subject_bank_cert (int): CERT number of the subject bank (MSPBNA = 34221)
        col_names (Optional[Dict[str, str]]): Custom column names

    Returns:
        str: Complete HTML string
    """
    if proc_df_with_peers.empty or not metrics_to_display:
        return "<p>No data or metrics provided for table generation.</p>"

    # Get latest quarter data
    latest_date = proc_df_with_peers['REPDTE'].max()
    latest_data = proc_df_with_peers[proc_df_with_peers['REPDTE'] == latest_date]

    # Get all banks and peer groups to be included in the table
    # MSPBNA, MSBNA, Core PB (GS+UBS avg)
    certs_to_include = [subject_bank_cert, 32992, 90001]

    # Define default column names
    if col_names is None:
        col_names = {
            'Latest': 'Latest Value',
            '8Q_Avg': '8-Qtr Average'
        }

    # Generate HTML string with professional styling
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
            .table-header {{ background-color: #002F6C; color: #ffffff; font-weight: bold; }}
            .sub-header {{ background-color: #002F6C; color: #ffffff; font-weight: bold; text-align: center; }}
            th, td {{ padding: 12px 15px; text-align: left; border: 1px solid #e2e8f0; }}
            th {{ font-size: 14px; text-transform: uppercase; letter-spacing: 0.05em; }}
            .metric-name {{ font-weight: normal; color: #2d3748; }}
            .value-cell {{ text-align: center; }}
            tr:nth-child(even) {{ background-color: #f7fafc; }}
            tr:hover {{ background-color: #edf2f7; }}
            .idb-row {{ background-color: #E6F3FF; font-weight: 600; }}
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

    # Dynamically generate the metric headers
    for display_name in metrics_to_display.values():
        html += f'<th class="value-cell">{display_name}</th>'

    html += """
                    </tr>
                </thead>
                <tbody>
    """

    # Generate rows for each entity
    for cert in certs_to_include:
        latest_row = latest_data[latest_data['CERT'] == cert]
        if latest_row.empty:
            continue

        latest_row = latest_row.iloc[0]
        name = latest_row.get('NAME', 'N/A')
        hq = latest_row.get('HQ_STATE', 'N/A')

        # Special styling for IDB
        row_class = "idb-row" if cert == subject_bank_cert else ""

        html += f"""
                    <tr class="{row_class}">
                        <td class="metric-name">{name}</td>
                        <td>{hq}</td>
        """

        # Add metric values
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
# MAIN REPORT GENERATION FUNCTION
# ==================================================================================

# ---------- helpers (drop in once) ----------
from pathlib import Path
def _ensure_dir(p: Path) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def to_decimal(series: pd.Series, metric_name: str = "") -> pd.Series:
    """
    Smart conversion:
    - If metric implies magnitude (Years, Coverage > 100%), leave as is.
    - If values look like percent points (median > 1.0), divide by 100.
    - Otherwise (median <= 1.0), assume already decimal.
    """
    s = pd.to_numeric(series, errors="coerce").astype(float)

    # 1. Whitelist: Metrics that are integers or ratios > 1.0 (Do NOT scale down)
    # Coverage is typically 1.5 - 5.0 (150%-500%), Years is 5-20.
    keywords_no_scale = ["Coverage", "Years", "Ratio", "Index", "Multiple"]
    if any(k in metric_name for k in keywords_no_scale):
        return s

    # 2. Standard Heuristic for Rates
    med = s.dropna().abs().median()
    if pd.isna(med):
        return s

    # If median > 1.0, assume it's in percent points (e.g. 5.50 for 5.5%), so scale.
    # If median <= 1.0, assume it's already decimal (e.g. 0.055), keep as is.
    return s / 100.0 if med > 1.0 else s

def _fmt_pct(x: float) -> str:
    return f"{x:.2%}"

def _place_titles(fig: plt.Figure, title: str, subtitle: str | None, title_size: int, subtitle_size: int):
    # Title at ~0.97, subtitle at ~0.935 of figure height; won’t collide with plot area.
    fig.text(0.5, 0.97, title, ha="center", va="top", fontsize=title_size, fontweight="bold", color="#2B2B2B")
    if subtitle:
        fig.text(0.5, 0.935, subtitle, ha="center", va="top", fontsize=subtitle_size, color="#6E6E6E")

def _load_fred_tables(xlsx_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Flexible sheet names; normalizes columns
    with pd.ExcelFile(xlsx_path) as xls:
        # data
        for cand in ["FRED_Data", "FRED_data", "FRED", "fred"]:
            if cand in xls.sheet_names:
                fred = pd.read_excel(xls, sheet_name=cand)
                break
        else:
            raise FileNotFoundError("FRED data sheet not found (expected one of: FRED_Data/FRED_data/FRED).")
        # descriptions
        for cand in ["FRED_Descriptions", "FRED_Dictionary", "FRED Meta"]:
            if cand in xls.sheet_names:
                desc = pd.read_excel(xls, sheet_name=cand)
                break
        else:
            raise FileNotFoundError("FRED_Descriptions sheet not found.")
    # normalize
    fred.columns = [c.strip() for c in fred.columns]
    desc.columns = [c.strip() for c in desc.columns]
    # map required columns
    # Expect: Series ID, DATE, VALUE (case tolerant)
    id_col   = next((c for c in fred.columns if c.lower().startswith("series")), None)
    date_col = next((c for c in fred.columns if "date" in c.lower()), None)
    val_col  = next((c for c in fred.columns if "value" in c.lower()), None)
    if not all([id_col, date_col, val_col]):
        raise ValueError("FRED Data columns not recognized. Need Series*, *date*, *value* columns.")
    fred = fred.rename(columns={id_col:"SeriesID", date_col:"DATE", val_col:"VALUE"})
    fred["DATE"] = pd.to_datetime(fred["DATE"], errors="coerce")
    # descriptions expect: Series ID, Short Name
    sid_col = next((c for c in desc.columns if c.lower().startswith("series")), None)
    short_col = next((c for c in desc.columns if "short" in c.lower()), None)
    if not all([sid_col, short_col]):
        raise ValueError("FRED_Descriptions needs 'Series ID' and 'Short Name' columns.")
    desc = desc.rename(columns={sid_col:"SeriesID", short_col:"ShortName"})
    return fred, desc

def build_fred_macro_table(xlsx_path: str, short_names: list[str]) -> tuple[str, pd.DataFrame]:
    fred, desc = _load_fred_tables(xlsx_path)
    # filter series list by ShortName
    sel = desc[desc["ShortName"].isin(short_names)].copy()
    if sel.empty:
        raise ValueError("None of the requested Short Names were found in FRED_Descriptions.")
    # Join to get SeriesIDs we need
    fred_sel = fred.merge(sel[["SeriesID","ShortName"]], on="SeriesID", how="inner")
    if fred_sel.empty:
        raise ValueError("No FRED data for the selected Short Names.")
    # latest and last prior-year print
    today = fred_sel["DATE"].max()
    curr_year = today.year
    rows = []
    for sname, g in fred_sel.groupby("ShortName"):
        g = g.sort_values("DATE")
        # latest non-null, non-zero in current year
        g_curr = g[g["DATE"].dt.year == curr_year].dropna(subset=["VALUE"])
        g_curr = g_curr[g_curr["VALUE"] != 0]
        if g_curr.empty:
            continue  # skip series with no usable current-year value
        latest_row = g_curr.iloc[-1]
        # last print from prior year (use last available date in prior year)
        g_prev = g[g["DATE"].dt.year == (curr_year-1)].dropna(subset=["VALUE"])
        prev_row = g_prev.iloc[-1] if not g_prev.empty else None
        # format values; detect % vs levels: if abs(median)>=0.5 and <=1000 assume percent pts; else raw
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
    # HTML (compact, email friendly)
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

# ---------- MAIN: drop-in replacement ----------
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
    # Macro table: pass Short Names; we’ll map to Series ID via FRED_Descriptions
    fred_short_names: Optional[List[str]] = None,
) -> None:
    """
    End-to-end runner:
    - finds the latest processed Excel file in output/,
    - writes all outputs into <that file’s folder>/Peers/,
    - generates the email table, bar+line credit chart, two square scatters,
      and the FRED macro table keyed by FRED_Descriptions.
    """
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

    print("=" * 80)
    print("BANK PERFORMANCE REPORT GENERATOR")
    print("=" * 80)

    cfg = load_config()
    subject_bank_cert = cfg["subject_bank_cert"]

    # 1) Find latest processed Excel and set output roots
    excel_file = find_latest_excel_file(cfg["output_dir"])
    if not excel_file:
        print("ERROR: No Excel files found in output/. Run the pipeline first.")
        return

    output_root = Path(excel_file).parent.resolve()
    peers_dir = output_root / "Peers"   # write all figures here
    peers_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found latest file: {excel_file}")
    print(f"File created: {datetime.fromtimestamp(os.path.getmtime(excel_file)).strftime('%Y-%m-%d')}")

    stamp = datetime.now().strftime("%Y%m%d")   # date only
    base = Path(excel_file).stem

    try:
        # ------------------------------------------------------------------
        # LOAD SHEETS
        # ------------------------------------------------------------------
        print("\nLoading data from Excel sheets...")
        with pd.ExcelFile(excel_file) as xls:
            proc_df_with_peers = pd.read_excel(xls, sheet_name="FDIC_Data")
            proc_df_with_peers["REPDTE"] = pd.to_datetime(proc_df_with_peers["REPDTE"])

            # 8Q averages sheet: pick the one that starts with Averages_8Q*
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

        # ------------------------------------------------------------------
        # EMAIL TABLE
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING EMAIL HTML TABLE")
        print("-" * 60)
        html_table, table_df = generate_credit_metrics_email_table(proc_df_with_peers, subject_bank_cert)
        if html_table:
            email_path = peers_dir / f"{base}_email_table_{stamp}.html"
            with open(email_path, "w", encoding="utf-8") as f:
                f.write(html_table)
            print(f"✓ Email table saved: {email_path}")
            if table_df is not None:
                print(f"✓ Table contains {len(table_df)} credit metrics")
                print("\nSample metrics included:")
                for i, m in enumerate(table_df["Metric"].head(5)):
                    print(f"  {i + 1}. {m}")

        # ------------------------------------------------------------------
        # CREDIT DETERIORATION (bars + lines)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING HTML TABLES (Executive & Detailed)")
        print("-" * 60)

        # 1. Generate Executive Summary (The "Skinny" Table)
        html_summary, df_summary = generate_credit_metrics_email_table(proc_df_with_peers, subject_bank_cert)
        if html_summary:
            summary_path = peers_dir / f"{base}_executive_summary_{stamp}.html"
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(html_summary)
            print(f"✓ Executive Summary Table saved: {summary_path}")

        # 2. Generate Detailed Peer Table (The "Wide" Table)
        html_detailed, df_detailed = generate_detailed_peer_table(proc_df_with_peers, subject_bank_cert)
        if html_detailed:
            detailed_path = peers_dir / f"{base}_detailed_peer_analysis_{stamp}.html"
            with open(detailed_path, "w", encoding="utf-8") as f:
                f.write(html_detailed)
            print(f"✓ Detailed Peer Analysis Table saved: {detailed_path}")

        # 3. Generate Normalized Comparison Table (Standard vs Normalized)
        html_norm, df_norm = generate_normalized_comparison_table(proc_df_with_peers, subject_bank_cert)
        if html_norm:
            norm_path = peers_dir / f"{base}_normalized_comparison_{stamp}.html"
            with open(norm_path, "w", encoding="utf-8") as f:
                f.write(html_norm)
            print(f"✓ Normalized Comparison Table saved: {norm_path}")

        # Validation Output
        if df_summary is not None:
            print(f"✓ Generation complete. {len(df_summary)} metrics processed.")
            print("\nMetrics included:")
            for i, m in enumerate(df_summary["Metric"].head(5)):
                print(f"  {i + 1}. {m}")

        # ------------------------------------------------------------------
        # SCATTERS: PRIVATE BANK STRATEGY (Risk-Adj Coverage & RRI)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING PRIVATE BANK SCATTERS")
        print("-" * 60)
        peers_dir = Path(excel_file).parent / "Peers"
        peers_dir.mkdir(parents=True, exist_ok=True)

        s1_path = peers_dir / f"{base}_NPL_to_RiskAdjACL_{stamp}.png"

        plot_scatter_dynamic(
            df=rolling8q_df,
            x_col="Nonaccrual_to_Gross_Loans_Rate",
            y_col="Risk_Adj_Allowance_Coverage",
            subject_cert=subject_bank_cert,     # MSPBNA
            sibling_cert=32992,                  # MSBNA
            peer_avg_cert_primary=90003,         # All Peers
            peer_avg_cert_alt=90001,             # Core PB
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
            show_sibling_label=True,
            identify_outliers=True,
            outliers_topn=2,
            figsize=(9, 9),
            title_size=16,
            economist_style=True,
            square_axes=False,
            save_path=str(s1_path)
        )

        print(f"✓ Scatter saved: Coverage vs Nonaccrual @ {s1_path}")
        s2_path = peers_dir / f"{base}_CRE_RiskAdjCov_vs_NA_{stamp}.png"

        plot_scatter_dynamic(
            df=rolling8q_df,
            x_col="RIC_CRE_Nonaccrual_Rate",
            y_col="RIC_CRE_Risk_Adj_Coverage",
            subject_cert=subject_bank_cert,     # MSPBNA
            sibling_cert=32992,                  # MSBNA
            peer_avg_cert_primary=90003,
            peer_avg_cert_alt=90001,
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
            show_sibling_label=True,
            identify_outliers=True,
            outliers_topn=2,
            figsize=(9, 9),
            title_size=16,
            economist_style=True,
            square_axes=False,
            exclude_certs=[14, 639],
            save_path=str(s2_path)
        )

        print(f"✓ Scatter saved: CRE Risk-Adj Coverage vs NA @ {s2_path}")
        s2_path = peers_dir / f"{base}_CRE_NCO_RATE_ACL_{stamp}.png"

        plot_scatter_dynamic(
            df=rolling8q_df,
            x_col="RIC_CRE_NCO_Rate",
            y_col="RIC_CRE_ACL_Share",
            subject_cert=subject_bank_cert,     # MSPBNA
            sibling_cert=32992,                  # MSBNA
            peer_avg_cert_primary=90003,
            peer_avg_cert_alt=90001,
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
            show_sibling_label=True,
            identify_outliers=True,
            outliers_topn=2,
            figsize=(9, 9),
            title_size=16,
            economist_style=True,
            square_axes=False,
            exclude_certs=[14, 639],
            save_path=str(s2_path)
        )

        print(f"✓ Scatter saved: _CRE_NCO_RATE_ACL_@ {s2_path}")
        rolling8q_df['RIC_CRE_ACL_Share'] = (
            rolling8q_df['RIC_CRE_ACL'] / rolling8q_df['Total_ACL']
        )
        s3_path = peers_dir / f"{base}_CRE_ACL_Share_vs_NA_{stamp}.png"

        plot_scatter_dynamic(
            df=rolling8q_df,
            x_col="RIC_CRE_Nonaccrual_Rate",
            y_col="RIC_CRE_ACL_Share",
            subject_cert=subject_bank_cert,
            sibling_cert=32992,
            peer_avg_cert_primary=90003,
            peer_avg_cert_alt=90001,
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
            show_sibling_label=True,
            identify_outliers=True,
            outliers_topn=2,
            figsize=(9, 9),
            title_size=16,
            economist_style=True,
            square_axes=False,
            save_path=str(s3_path)
        )

        print(f"✓ Scatter saved: CRE ACL Share vs NA @ {s3_path}")
        s3_path = peers_dir / f"{base}SCATTER_CRE_ACL_Share_vs_NCO_{stamp}.png"

        plot_scatter_dynamic(
            df=rolling8q_df,
            x_col="RIC_CRE_Nonaccrual_Rate",
            y_col="RIC_CRE_NCO_Rate",
            subject_cert=subject_bank_cert,
            sibling_cert=32992,
            peer_avg_cert_primary=90003,
            peer_avg_cert_alt=90001,
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
            show_sibling_label=True,
            identify_outliers=True,
            outliers_topn=2,
            figsize=(9, 9),
            title_size=16,
            economist_style=True,
            square_axes=False,
            save_path=str(s3_path)
        )

        print(f"✓ Scatter saved: CRE ACL Share vs NA @ {s3_path}")


        # ------------------------------------------------------------------
        # FDIC Time Series
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING Call Report time-series Bar/Lines")
        print("-" * 60)

        # --- CHART 1: CRE Credit Quality vs Reserve Levels ---
        cre_qual_path = peers_dir / f"{base}_CRE_Credit_Qual_Chart_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "COREPB"],
            line_entities=["MSPBNA", "COREPB"],
            bar_metric="RIC_CRE_ACL_Coverage",
            line_metric="RIC_CRE_Nonaccrual_Rate",
            title="CRE Credit Quality vs Reserve Levels",
            subtitle="ACL Coverage (Bars) vs Nonaccrual (Lines)",
            bar_axis_label="CRE ACL Coverage (%)",
            line_axis_label="CRE Nonaccrual Rate (%)",
            save_path=str(cre_qual_path)
        )
        print(f"✓ Chart saved: CRE Credit Quality @ {cre_qual_path}")

        # --- CHART 2: CRE Reserve Allocation vs NCOs ---
        cre_nco_path = peers_dir / f"{base}_CRE_ACL_NCO_timeseries_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "COREPB"],
            line_entities=["MSPBNA", "COREPB"],
            bar_metric="RIC_CRE_ACL_Coverage",
            line_metric="RIC_CRE_NCO_Rate",
            title="CRE Reserve Allocation Analysis",
            subtitle="CRE ACL Coverage (Bars) vs Actual Loss Rate (Lines)",
            bar_axis_label="CRE ACL Coverage (%)",
            line_axis_label="CRE Net Charge-off Rate (%)",
            save_path=str(cre_nco_path)
        )
        print(f"✓ Chart saved: CRE Allocation Analysis @ {cre_nco_path}")

        # --- CHART 3: CRE Reserve Adequacy (Migration vs Years) ---
        cre_mig_path = peers_dir / f"{base}_CRE_ACL_VS_Comp_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "COREPB"],
            line_entities=["MSPBNA", "COREPB"],
            bar_metric="RIC_CRE_Loan_Share",
            line_metric="RIC_CRE_ACL_Coverage",
            title="CRE Portfolio Profile: Exposure & Reserve Levels",
            subtitle="Concentration as % of Loans (Bars) vs. ACL Coverage (Lines)",
            bar_axis_label="CRE Lns to Total Book %",
            line_axis_label="CRE ACL Coverage",
            save_path=str(cre_mig_path)
        )
        print(f"✓ Chart saved: CRE Reserve Adequacy @ {cre_mig_path}")

        # ------------------------------------------------------------------
        # NORMALIZED METRICS CHARTS (Ex-Commercial/Ex-Consumer)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING NORMALIZED METRIC CHARTS (Ex-Commercial/Ex-Consumer)")
        print("-" * 60)

        # --- CHART N1: Normalized NCO Rate vs Nonaccrual Rate ---
        norm_credit_path = peers_dir / f"{base}_NORM_Credit_Quality_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "COREPB"],
            line_entities=["MSPBNA", "COREPB"],
            bar_metric="Norm_NCO_Rate",
            line_metric="Norm_Nonaccrual_Rate",
            title="Normalized Credit Quality (Ex-Commercial/Ex-Consumer)",
            subtitle="Norm NCO Rate (Bars) vs Norm Nonaccrual Rate (Lines)",
            bar_axis_label="Norm NCO Rate (%)",
            line_axis_label="Norm Nonaccrual Rate (%)",
            save_path=str(norm_credit_path)
        )
        print(f"✓ Chart saved: Normalized Credit Quality @ {norm_credit_path}")

        # --- CHART N2: Normalized ACL Coverage vs Standard ACL ---
        norm_acl_path = peers_dir / f"{base}_NORM_ACL_Comparison_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "COREPB"],
            line_entities=["MSPBNA", "COREPB"],
            bar_metric="Allowance_to_Gross_Loans_Rate",
            line_metric="Norm_ACL_Coverage",
            title="ACL Coverage: Standard vs Normalized",
            subtitle="Headline ACL Ratio (Bars) vs Normalized ACL Ratio (Lines)",
            bar_axis_label="Headline ACL Ratio (%)",
            line_axis_label="Norm ACL Ratio (%)",
            save_path=str(norm_acl_path)
        )
        print(f"✓ Chart saved: ACL Coverage Comparison @ {norm_acl_path}")

        # --- CHART N3: Normalized Loan Yield vs Loss-Adjusted Yield ---
        norm_yield_path = peers_dir / f"{base}_NORM_Yield_Analysis_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "COREPB"],
            line_entities=["MSPBNA", "COREPB"],
            bar_metric="Norm_Loan_Yield",
            line_metric="Norm_Loss_Adj_Yield",
            title="Normalized Yield Analysis (Private Bank Portfolio)",
            subtitle="Loan Yield (Bars) vs Loss-Adjusted Yield (Lines)",
            bar_axis_label="Norm Loan Yield (%)",
            line_axis_label="Norm Loss-Adj Yield (%)",
            save_path=str(norm_yield_path)
        )
        print(f"✓ Chart saved: Normalized Yield Analysis @ {norm_yield_path}")

        # --- CHART N4: Portfolio Exclusion Breakdown ---
        norm_exclusion_path = peers_dir / f"{base}_NORM_Exclusion_Pct_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "MSBNA", "COREPB"],
            line_entities=["MSPBNA", "MSBNA", "COREPB"],
            bar_metric="Norm_Exclusion_Pct",
            line_metric="SBL_Composition",
            title="Portfolio Normalization: Exclusion % vs SBL %",
            subtitle="Excluded Loans (Bars) vs SBL as % of Loans (Lines)",
            bar_axis_label="Excluded Loans (%)",
            line_axis_label="SBL % of Loans",
            save_path=str(norm_exclusion_path)
        )
        print(f"✓ Chart saved: Exclusion Analysis @ {norm_exclusion_path}")

        # ------------------------------------------------------------------
        # LIQUIDITY & PROFITABILITY CHARTS (RESTORED)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING LIQUIDITY & PROFITABILITY CHARTS")
        print("-" * 60)

        # --- CHART L1: Liquidity Ratio vs Loans/Deposits ---
        liq_path = peers_dir / f"{base}_Liquidity_Analysis_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "MSBNA", "COREPB"],
            line_entities=["MSPBNA", "MSBNA", "COREPB"],
            bar_metric="Liquidity_Ratio",
            line_metric="Loans_to_Deposits",
            title="Liquidity Profile: Liquid Assets vs Funding Usage",
            subtitle="Liquidity Ratio (Bars) vs Loans-to-Deposits (Lines)",
            bar_axis_label="Liquidity Ratio (%)",
            line_axis_label="Loans to Deposits (%)",
            save_path=str(liq_path)
        )
        print(f"✓ Chart saved: Liquidity Analysis @ {liq_path}")

        # --- CHART P1: ROA vs Efficiency Ratio ---
        prof_path = peers_dir / f"{base}_Profitability_Analysis_{stamp}.png"

        create_dual_axis_chart(
            proc_df_with_peers,
            bar_entities=["MSPBNA", "MSBNA", "COREPB"],
            line_entities=["MSPBNA", "MSBNA", "COREPB"],
            bar_metric="ROA",
            line_metric="EEFFR",
            title="Profitability Analysis: Returns vs Efficiency",
            subtitle="ROA (Bars) vs Efficiency Ratio (Lines)",
            bar_axis_label="ROA (%)",
            line_axis_label="Efficiency Ratio (%)",
            save_path=str(prof_path)
        )
        print(f"✓ Chart saved: Profitability Analysis @ {prof_path}")

        # ------------------------------------------------------------------
        # FRED MACRO TABLE (Short Name -> Series ID via FRED_Descriptions)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING FRED MACRO TABLE")
        print("-" * 60)
        try:
            fred_html, fred_df = build_fred_macro_table(excel_file, list(fred_short_names))
            fred_path = peers_dir / f"{base}_fred_table_{stamp}.html"
            with open(fred_path, "w", encoding="utf-8") as f:
                f.write(fred_html)
            print(f"✓ FRED macro table saved: {fred_path}")
        except Exception as e:
            print(f"⚠️  Skipped FRED table: {e}")

        # ------------------------------------------------------------------
        # SUMMARY
        # ------------------------------------------------------------------
        print("\n" + "=" * 80)
        print("REPORT GENERATION COMPLETE")
        print("=" * 80)
        print(f"Source file: {excel_file}")
        print(f"Output directory: {peers_dir}")
        print(f"Subject bank CERT: {subject_bank_cert}")

    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
    finally:
        plt.close("all")




def create_credit_deterioration_chart_ppt(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    start_date: str = "2023-01-01",
    bar_metric: str = "TTM_NCO_Rate",
    line_metric: str = "Nonaccrual_to_Gross_Loans_Rate",
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
    """
    Generates a dual-axis chart (Bars vs Lines) with advanced label collision detection.
    Restored full parameter support (tick_size, tag_size, etc.) per original requirements.
    """
    if proc_df_with_peers.empty:
        return None, None

    # ---------- palette / entities
    # MS Brand Colors + Standard Peers
    MS_BLUE = "#002F6C"    # MSPBNA (Deep MS Blue)
    MS_LIGHT = "#4C78A8"   # MSBNA / Affiliates (Lighter Blue)
    CORE_GOLD = "#F7A81B"  # Core PB Peers (Gold/Amber)
    PEER_GRAY = "#7F8C8D"  # All Peers (Gray)

    # Default entities: MSPBNA, MSBNA, Core PB, All Peers
    default_entities = [subject_bank_cert, 32992, 90001, 90003]  # MSPBNA, MSBNA, Core PB, All Peers
    bar_entities  = bar_entities  or default_entities
    line_entities = line_entities or list(bar_entities)

    # Dynamic naming and coloring
    names  = {
        34221: "MSPBNA",
        32992: "MSBNA",
        90001: "Core PB",
        90002: "MS+Wealth",
        90003: "All Peers"
    }
    colors = {
        34221: MS_BLUE,
        32992: MS_LIGHT,
        90001: CORE_GOLD,
        90002: "#2ca02c",
        90003: PEER_GRAY
    }

    # ---------- filter & timeline
    df = proc_df_with_peers.loc[
        proc_df_with_peers["CERT"].isin(set(bar_entities + line_entities))
    ].copy()
    df = df[df["REPDTE"] >= pd.to_datetime(start_date)].sort_values(["REPDTE","CERT"])
    if df.empty:
        return None, None

    df["Period_Label"] = "Q" + df["REPDTE"].dt.quarter.astype(str) + "-" + (df["REPDTE"].dt.year % 100).astype(str).str.zfill(2)
    subj     = df[df["CERT"] == subject_bank_cert][["REPDTE","Period_Label"]].drop_duplicates().sort_values("REPDTE")
    timeline = subj[["REPDTE"]].copy()
    x        = np.arange(len(subj))
    xticks   = subj["Period_Label"].tolist()
    last_dt  = timeline["REPDTE"].max()

    # ---------- figure / axes
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

    # =========================================================================
    # HELPER FUNCTIONS
    # =========================================================================
    def to_decimal(series: pd.Series, metric_name: str = "") -> pd.Series:
        """
        Smart conversion:
        - If metric implies magnitude (Years, Coverage > 100%), leave as is.
        - If values look like percent points (median > 1.0), divide by 100.
        - Otherwise (median <= 1.0), assume already decimal.
        """
        s = pd.to_numeric(series, errors="coerce").astype(float)

        # 1. Whitelist: Metrics that are integers or ratios > 1.0 (Do NOT scale down)
        keywords_no_scale = ["Coverage", "Years", "Ratio", "Index", "Multiple", "Buffer"]
        if any(k in metric_name for k in keywords_no_scale):
            return s

        # 2. Standard Heuristic for Rates
        med = s.dropna().abs().median()
        if pd.isna(med):
            return s

        # If median > 1.0, assume it's in percent points (e.g. 5.50 for 5.5%), so scale.
        # If median <= 1.0, assume it's already decimal (e.g. 0.055), keep as is.
        return s / 100.0 if med > 1.0 else s

    def series_for(cert: int, metric: str) -> pd.Series:
        """Get time-aligned series for a given cert and metric."""
        ed = timeline.merge(df[df["CERT"] == cert][["REPDTE", metric]], on="REPDTE", how="left")
        # PASS THE METRIC NAME HERE to trigger the logic above
        return to_decimal(ed[metric], metric_name=metric)

    def lighten(hex_color: str, factor: float = 0.85) -> str:
        if not hex_color.startswith("#"): return hex_color
        h = hex_color.lstrip("#"); r,g,b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        r = int(r + (255-r)*factor); g = int(g + (255-g)*factor); b = int(b + (255-b)*factor)
        return f"#{r:02X}{g:02X}{b:02X}"

    # ---------- axis limits
    left_vals  = [series_for(c, bar_metric)  for c in bar_entities]
    right_vals = [series_for(c, line_metric) for c in line_entities]
    left_all   = pd.concat(left_vals,  axis=0) if left_vals  else pd.Series([0.0])
    right_all  = pd.concat(right_vals, axis=0) if right_vals else pd.Series([0.0])

    if left_all.notna().any():
        lo, hi = float(left_all.min()), float(left_all.max())
        rng = max(hi - lo, 1e-6)
        ax.set_ylim(lo - 0.18*rng, hi + 0.18*rng)

    if right_all.notna().any():
        rlo, rhi = float(right_all.min()), float(right_all.max())
        rrng = max(rhi - rlo, 1e-6)
        ax2.set_ylim(rlo - 0.12*rrng, rhi + 0.12*rrng)

    # ---------- bars
    n = max(len(bar_entities), 1)
    bar_w   = 0.8 / n
    offsets = {c: (i - (n - 1) / 2) * bar_w for i, c in enumerate(bar_entities)}

    bar_handles, bar_labels, line_handles, line_labels = [], [], [], []
    for c in bar_entities:
        vals = series_for(c, bar_metric)
        col = colors.get(c, "#7F8C8D")
        b = ax.bar(x + offsets[c], vals, width=bar_w, color=col, alpha=0.92,
                   label=f"{names.get(c,f'CERT {c}')} {bar_metric.replace('_',' ')}", zorder=2)
        bar_handles.append(b[0])
        bar_labels.append(f"{names.get(c,f'CERT {c}')} (Bar)")

    # ---------- labels & collision logic
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

        def can_place(self, rect, skip_index: Optional[int] = None):
            for r in self.fixed_rects:
                if rects_overlap(rect, r): return False
            for i, it in enumerate(self.items):
                if skip_index is not None and i == skip_index: continue
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
                                # Recalculate rect
                                xd, yd = self.ax.transData.transform(ann.xy)
                                rect = (xd + new[0] - (ri[2]-ri[0])/2, yd + new[1] - (ri[3]-ri[1])/2,
                                        xd + new[0] + (ri[2]-ri[0])/2, yd + new[1] + (ri[3]-ri[1])/2)
                                self.items[idx]["rect"] = rect
                                moved = True
                if not moved: break

        def rect_for_text(self, x_data, y_data, xpx, ypx, text):
            xd, yd = self.ax.transData.transform((x_data, y_data))
            cx, cy = xd + xpx, yd + ypx
            w = max(40.0, 0.62*self.tag_sz*len(text)) + 12
            h = self.tag_sz*1.7 + 12
            return (cx - w/2, cy - h/2, cx + w/2, cy + h/2)

    placer = Placer(ax2, tag_size)

    # Place Lines
    for c in line_entities:
        s = series_for(c, line_metric)
        col = colors.get(c, "#7F8C8D")
        ln, = ax2.plot(x, s, color=col, linewidth=2.4,
                       linestyle="-" if c == subject_bank_cert else "--",
                       marker="o", markersize=4.4, zorder=3)
        line_handles.append(ln)
        line_labels.append(f"{names.get(c,f'CERT {c}')} (Line)")

    # Place Bar Labels (Fixed)
    ylo, yhi = ax.get_ylim()
    for i in idx_to_label:
        for j, c in enumerate(bar_entities):
            s = series_for(c, bar_metric)
            v = s.iloc[i]
            if pd.isna(v): continue

            xpos = x[i] + offsets[c]
            # Simple alternating logic
            ypos = v + (yhi-ylo)*0.02

            ann = ax.annotate(f"{v:.2%}", xy=(xpos, ypos), xytext=(0, 0), textcoords="offset pixels",
                             ha="center", va="bottom", fontsize=tag_size, fontweight="bold",
                             color="#2B2B2B", bbox=dict(boxstyle="round,pad=0.1", fc="white", alpha=0.7))

            # Register rect
            xd, yd = ax.transData.transform((xpos, ypos))
            w, h = 40, 20 # approx
            placer.add_fixed((xd-w/2, yd-h/2, xd+w/2, yd+h/2))

    # Place Line Labels (Dynamic)
    for c in line_entities:
        s = series_for(c, line_metric)
        col = colors.get(c, "#7F8C8D")
        for k in idx_to_label:
            val = s.iloc[k]
            if pd.notna(val):
                txt = f"{val:.2%}"
                # Try placement
                found = False
                for yoff in [15, -15, 25, -25]:
                    rect = placer.rect_for_text(x[k], float(val), 0, yoff, txt)
                    if placer.can_place(rect):
                        ann = ax2.annotate(txt, xy=(x[k], float(val)), xytext=(0, yoff),
                                         textcoords="offset pixels", fontsize=tag_size,
                                         fontweight="bold", color=col,
                                         bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=col))
                        placer.add_line_ann(ann, rect)
                        found = True
                        break
                if not found:
                    # Fallback
                    ax2.annotate(txt, xy=(x[k], float(val)), xytext=(0, 15),
                               textcoords="offset pixels", fontsize=tag_size, fontweight="bold", color=col)

    placer.relax()

    # Final Polish
    ax.set_xticks(x); ax.set_xticklabels(xticks, fontsize=tick_size, rotation=0)
    ax.set_ylabel(bar_metric.replace("_"," "), fontsize=axis_label_size, fontweight="bold")
    ax2.set_ylabel(line_metric.replace("_"," "), fontsize=axis_label_size, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.1%}"))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.1%}"))

    title = custom_title or f"{bar_metric} vs {line_metric}"
    fig.text(0.5, 0.97, title, ha="center", va="top", fontsize=title_size, fontweight="bold", color="#2B2B2B")

    leg = leg_ax.legend(bar_handles + line_handles, bar_labels + line_labels,
                        ncol=len(bar_entities)+len(line_entities), loc="center",
                        frameon=True, fontsize=legend_fontsize)

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig, ax


# ==================================================================================
# FLEXIBLE DUAL-AXIS CHART FUNCTION
# ==================================================================================

def create_dual_axis_chart(
    proc_df_with_peers: pd.DataFrame,
    # Entity selection - use names like "MSPBNA", "MSBNA", "COREPB", "ALLPEERS"
    bar_entities: Optional[List[str]] = None,
    line_entities: Optional[List[str]] = None,
    # Metric selection
    bar_metric: str = "TTM_NCO_Rate",
    line_metric: str = "Nonaccrual_to_Gross_Loans_Rate",
    # Title/Subtitle (optional)
    title: Optional[str] = None,
    subtitle: Optional[str] = None,
    # Axis labels (optional - defaults to metric name)
    bar_axis_label: Optional[str] = None,
    line_axis_label: Optional[str] = None,
    # Time range
    start_date: str = "2022-12-31",
    # Styling
    figsize: Tuple[float, float] = (14, 7),
    title_size: int = 20,
    subtitle_size: int = 13,
    axis_label_size: int = 14,
    tick_size: int = 12,
    tag_size: int = 10,
    legend_fontsize: int = 12,
    economist_style: bool = True,
    transparent_bg: bool = True,
    # Custom colors (optional - dict mapping entity name to hex color)
    custom_colors: Optional[Dict[str, str]] = None,
    # Output
    save_path: Optional[str] = None,
) -> Tuple[Optional[plt.Figure], Optional[plt.Axes]]:
    """
    Flexible dual-axis chart (Bars + Lines) with entity and metric selection.

    This function provides all features of create_credit_deterioration_chart_ppt
    but with more intuitive entity selection by name and flexible metric choices.

    Args:
        proc_df_with_peers: DataFrame with FDIC data including CERT, REPDTE, and metrics

        bar_entities: List of entity names for bars. Options:
            - "MSPBNA" (34221) - Morgan Stanley Private Bank NA
            - "MSBNA" (32992) - Morgan Stanley Bank NA
            - "COREPB" (90001) - Core Private Bank Peers (GS + UBS avg)
            - "MSWEALTH" (90002) - MS + Extended Wealth Peers
            - "ALLPEERS" (90003) - Full Peer Universe
            - "GS" (33124) - Goldman Sachs Bank USA
            - "UBS" (57565) - UBS Bank USA
            - "JPM" (628) - JPMorgan Chase Bank NA
            - "BAC" (3510) - Bank of America NA
            - "WFC" (3511) - Wells Fargo Bank NA
            - "CITI" (7213) - Citibank NA
            - "CNB" (17281) - City National Bank
            Default: ["MSPBNA", "MSBNA", "COREPB", "ALLPEERS"]

        line_entities: List of entity names for lines. Same options as bar_entities.
            Default: Same as bar_entities

        bar_metric: Column name for bar values (e.g., "TTM_NCO_Rate", "RIC_CRE_NCO_Rate")
        line_metric: Column name for line values (e.g., "Nonaccrual_to_Gross_Loans_Rate")

        title: Chart title (optional). If None, auto-generates from metrics.
        subtitle: Chart subtitle (optional). Displayed below title in smaller font.

        bar_axis_label: Left Y-axis label. Default: bar_metric with underscores replaced
        line_axis_label: Right Y-axis label. Default: line_metric with underscores replaced

        start_date: Filter data to this date and later (YYYY-MM-DD format)

        figsize: Figure size as (width, height) tuple
        title_size, subtitle_size, axis_label_size, tick_size, tag_size, legend_fontsize: Font sizes
        economist_style: If True, applies clean Economist-style formatting
        transparent_bg: If True, figure background is transparent

        custom_colors: Optional dict mapping entity names to hex colors.
            Example: {"MSPBNA": "#FF0000", "COREPB": "#00FF00"}

        save_path: If provided, saves chart to this path

    Returns:
        Tuple of (figure, axes) or (None, None) if no data

    Example:
        >>> fig, ax = create_dual_axis_chart(
        ...     df,
        ...     bar_entities=["MSPBNA", "COREPB"],
        ...     line_entities=["MSPBNA", "COREPB"],
        ...     bar_metric="RIC_CRE_NCO_Rate",
        ...     line_metric="RIC_CRE_Nonaccrual_Rate",
        ...     title="CRE Portfolio Health",
        ...     subtitle="NCO Rate (Bars) vs Nonaccrual Rate (Lines)"
        ... )
    """
    if proc_df_with_peers.empty:
        return None, None

    # =========================================================================
    # ENTITY MAPPING: Name -> CERT
    # =========================================================================
    ENTITY_MAP = {
        # Subject and sibling
        "MSPBNA": 34221,
        "MSBNA": 32992,
        # Peer composites (Standard)
        "COREPB": 90001,
        "CORE_PB": 90001,
        "CORE": 90001,
        "MSWEALTH": 90002,
        "MS_WEALTH": 90002,
        "ALLPEERS": 90003,
        "ALL_PEERS": 90003,
        "ALL": 90003,
        # Peer composites (Normalized - Ex-Commercial/Ex-Consumer)
        "COREPB_NORM": 90011,
        "CORE_PB_NORM": 90011,
        "CORE_NORM": 90011,
        "MSWEALTH_NORM": 90012,
        "MS_WEALTH_NORM": 90012,
        "ALLPEERS_NORM": 90013,
        "ALL_PEERS_NORM": 90013,
        "ALL_NORM": 90013,
        # Individual banks
        "GS": 33124,
        "GOLDMAN": 33124,
        "UBS": 57565,
        "JPM": 628,
        "JPMORGAN": 628,
        "BAC": 3510,
        "BOFA": 3510,
        "WFC": 3511,
        "WELLS": 3511,
        "CITI": 7213,
        "CITIBANK": 7213,
        "CNB": 17281,
        "CITYNATIONAL": 17281,
    }

    # Display names for legend
    DISPLAY_NAMES = {
        34221: "MSPBNA",
        32992: "MSBNA",
        90001: "Core PB",
        90002: "MS+Wealth",
        90003: "All Peers",
        # Normalized peer groups
        90011: "Core PB (Norm)",
        90012: "MS+Wealth (Norm)",
        90013: "All Peers (Norm)",
        33124: "Goldman Sachs",
        57565: "UBS",
        628: "JPMorgan",
        3510: "BofA",
        3511: "Wells Fargo",
        7213: "Citi",
        17281: "City National",
    }

    # Default color palette (MS brand + complementary)
    DEFAULT_COLORS = {
        34221: "#002F6C",  # MSPBNA - Deep MS Blue
        32992: "#4C78A8",  # MSBNA - Light Blue
        90001: "#F7A81B",  # Core PB - Gold
        90002: "#2ca02c",  # MS+Wealth - Green
        90003: "#7F8C8D",  # All Peers - Gray
        33124: "#1f77b4",  # GS - Blue
        57565: "#d62728",  # UBS - Red
        628: "#9467bd",    # JPM - Purple
        3510: "#8c564b",   # BofA - Brown
        3511: "#e377c2",   # WFC - Pink
        7213: "#17becf",   # Citi - Cyan
        17281: "#bcbd22",  # CNB - Olive
    }

    # =========================================================================
    # RESOLVE ENTITIES
    # =========================================================================
    def resolve_entity(name_or_cert):
        """Convert entity name to CERT number."""
        if isinstance(name_or_cert, int):
            return name_or_cert
        name_upper = str(name_or_cert).upper().replace(" ", "_")
        if name_upper in ENTITY_MAP:
            return ENTITY_MAP[name_upper]
        # Try as integer
        try:
            return int(name_or_cert)
        except ValueError:
            raise ValueError(f"Unknown entity: {name_or_cert}. Valid options: {list(ENTITY_MAP.keys())}")

    # Default entities
    if bar_entities is None:
        bar_entities = ["MSPBNA", "MSBNA", "COREPB", "ALLPEERS"]
    if line_entities is None:
        line_entities = list(bar_entities)

    # Convert to CERTs
    bar_certs = [resolve_entity(e) for e in bar_entities]
    line_certs = [resolve_entity(e) for e in line_entities]
    all_certs = list(set(bar_certs + line_certs))

    # Subject bank (first in bar_entities)
    subject_cert = bar_certs[0]

    # Build color mapping
    colors = dict(DEFAULT_COLORS)
    if custom_colors:
        for name, color in custom_colors.items():
            cert = resolve_entity(name)
            colors[cert] = color

    # =========================================================================
    # FILTER DATA
    # =========================================================================
    df = proc_df_with_peers[proc_df_with_peers["CERT"].isin(all_certs)].copy()
    df = df[df["REPDTE"] >= pd.to_datetime(start_date)].sort_values(["REPDTE", "CERT"])

    if df.empty:
        print(f"No data found for entities {all_certs} after {start_date}")
        return None, None

    # Create period labels
    df["Period_Label"] = "Q" + df["REPDTE"].dt.quarter.astype(str) + "-" + \
                         (df["REPDTE"].dt.year % 100).astype(str).str.zfill(2)

    # Build timeline from subject bank
    subj = df[df["CERT"] == subject_cert][["REPDTE", "Period_Label"]].drop_duplicates().sort_values("REPDTE")
    if subj.empty:
        print(f"No data for subject entity (CERT {subject_cert})")
        return None, None

    timeline = subj[["REPDTE"]].copy()
    x = np.arange(len(subj))
    xticks = subj["Period_Label"].tolist()
    last_dt = timeline["REPDTE"].max()

    # =========================================================================
    # HELPER FUNCTIONS
    # =========================================================================
    def to_decimal(series: pd.Series, metric_name: str = "") -> pd.Series:
        """
        Smart conversion:
        - If metric implies magnitude (Years, Coverage > 100%), leave as is.
        - If values look like percent points (median > 1.0), divide by 100.
        - Otherwise (median <= 1.0), assume already decimal.
        """
        s = pd.to_numeric(series, errors="coerce").astype(float)

        # 1. Whitelist: Metrics that are integers or ratios > 1.0 (Do NOT scale down)
        keywords_no_scale = ["Coverage", "Years", "Ratio", "Index", "Multiple", "Buffer"]
        if any(k in metric_name for k in keywords_no_scale):
            return s

        # 2. Standard Heuristic for Rates
        med = s.dropna().abs().median()
        if pd.isna(med):
            return s

        # If median > 1.0, assume it's in percent points (e.g. 5.50 for 5.5%), so scale.
        # If median <= 1.0, assume it's already decimal (e.g. 0.055), keep as is.
        return s / 100.0 if med > 1.0 else s

    def series_for(cert: int, metric: str) -> pd.Series:
        """Get time-aligned series for a given cert and metric."""
        ed = timeline.merge(df[df["CERT"] == cert][["REPDTE", metric]], on="REPDTE", how="left")
        # PASS THE METRIC NAME HERE to trigger the logic above
        return to_decimal(ed[metric], metric_name=metric)

    def lighten(hex_color: str, factor: float = 0.85) -> str:
        """Lighten a hex color."""
        if not hex_color.startswith("#"):
            return hex_color
        h = hex_color.lstrip("#")
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        r = int(r + (255 - r) * factor)
        g = int(g + (255 - g) * factor)
        b = int(b + (255 - b) * factor)
        return f"#{r:02X}{g:02X}{b:02X}"

    # =========================================================================
    # CREATE FIGURE
    # =========================================================================
    fig = plt.figure(figsize=figsize, constrained_layout=False)
    gs = GridSpec(nrows=2, ncols=1, height_ratios=[10, 2], hspace=0.08, figure=fig)
    ax = fig.add_subplot(gs[0, 0])
    ax2 = ax.twinx()
    leg_ax = fig.add_subplot(gs[1, 0])
    leg_ax.axis("off")

    if transparent_bg:
        fig.patch.set_alpha(0)
    for a in (ax, ax2):
        a.set_facecolor("none")
        a.grid(False)

    if economist_style:
        for sp in ["top", "right"]:
            ax.spines[sp].set_visible(False)
            ax2.spines[sp].set_visible(False)
        for s in ax.spines.values():
            s.set_linewidth(1.1)
            s.set_color("#2B2B2B")
        for s in ax2.spines.values():
            s.set_linewidth(1.1)
            s.set_color("#2B2B2B")
        ax.tick_params(axis="both", labelsize=tick_size, colors="#2B2B2B")
        ax2.tick_params(axis="y", labelsize=tick_size, colors="#2B2B2B")

    # =========================================================================
    # SET AXIS LIMITS
    # =========================================================================
    left_vals = [series_for(c, bar_metric) for c in bar_certs]
    right_vals = [series_for(c, line_metric) for c in line_certs]
    left_all = pd.concat(left_vals, axis=0) if left_vals else pd.Series([0.0])
    right_all = pd.concat(right_vals, axis=0) if right_vals else pd.Series([0.0])

    if left_all.notna().any():
        lo, hi = float(left_all.min()), float(left_all.max())
        rng = max(hi - lo, 1e-6)
        ax.set_ylim(lo - 0.18 * rng, hi + 0.18 * rng)

    if right_all.notna().any():
        rlo, rhi = float(right_all.min()), float(right_all.max())
        rrng = max(rhi - rlo, 1e-6)
        ax2.set_ylim(rlo - 0.12 * rrng, rhi + 0.12 * rrng)

    # =========================================================================
    # PLOT BARS
    # =========================================================================
    n = max(len(bar_certs), 1)
    bar_w = 0.8 / n
    offsets = {c: (i - (n - 1) / 2) * bar_w for i, c in enumerate(bar_certs)}

    bar_handles, bar_labels, line_handles, line_labels = [], [], [], []

    for c in bar_certs:
        vals = series_for(c, bar_metric)
        col = colors.get(c, "#7F8C8D")
        b = ax.bar(x + offsets[c], vals, width=bar_w, color=col, alpha=0.92, zorder=2)
        bar_handles.append(b[0])
        bar_labels.append(f"{DISPLAY_NAMES.get(c, f'CERT {c}')} (Bar)")

    # =========================================================================
    # COLLISION DETECTION FOR LABELS
    # =========================================================================
    qtr = timeline["REPDTE"].dt.quarter
    idx_to_label = np.where((qtr == 4) | (timeline["REPDTE"] == last_dt))[0]

    def rects_overlap(a, b):
        ax0, ay0, ax1, ay1 = a
        bx0, by0, bx1, by1 = b
        return not (ax1 < bx0 or bx1 < ax0 or ay1 < by0 or by1 < ay0)

    class Placer:
        def __init__(self, ax, tag_sz):
            self.ax = ax
            self.tag_sz = tag_sz
            self.fixed_rects = []
            self.items = []

        def can_place(self, rect, skip_index=None):
            for r in self.fixed_rects:
                if rects_overlap(rect, r):
                    return False
            for i, it in enumerate(self.items):
                if skip_index is not None and i == skip_index:
                    continue
                if rects_overlap(rect, it["rect"]):
                    return False
            return True

        def add_fixed(self, rect):
            self.fixed_rects.append(rect)

        def add_line_ann(self, ann, rect):
            self.items.append({"ann": ann, "rect": rect})

        def relax(self, max_iter=30, step_px=7):
            for _ in range(max_iter):
                moved = False
                for i in range(len(self.items)):
                    for j in range(i + 1, len(self.items)):
                        ri, rj = self.items[i]["rect"], self.items[j]["rect"]
                        if rects_overlap(ri, rj):
                            for idx, sgn in ((i, +1), (j, -1)):
                                ann = self.items[idx]["ann"]
                                xoff, yoff = ann.get_position()
                                new = (xoff, yoff + sgn * step_px)
                                ann.set_position(new)
                                xd, yd = self.ax.transData.transform(ann.xy)
                                rect = (xd + new[0] - (ri[2] - ri[0]) / 2,
                                        yd + new[1] - (ri[3] - ri[1]) / 2,
                                        xd + new[0] + (ri[2] - ri[0]) / 2,
                                        yd + new[1] + (ri[3] - ri[1]) / 2)
                                self.items[idx]["rect"] = rect
                                moved = True
                if not moved:
                    break

        def rect_for_text(self, x_data, y_data, xpx, ypx, text):
            xd, yd = self.ax.transData.transform((x_data, y_data))
            cx, cy = xd + xpx, yd + ypx
            w = max(40.0, 0.62 * self.tag_sz * len(text)) + 12
            h = self.tag_sz * 1.7 + 12
            return (cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2)

    placer = Placer(ax2, tag_size)

    # =========================================================================
    # PLOT LINES
    # =========================================================================
    for c in line_certs:
        s = series_for(c, line_metric)
        col = colors.get(c, "#7F8C8D")
        ln, = ax2.plot(x, s, color=col, linewidth=2.4,
                       linestyle="-" if c == subject_cert else "--",
                       marker="o", markersize=4.4, zorder=3)
        line_handles.append(ln)
        line_labels.append(f"{DISPLAY_NAMES.get(c, f'CERT {c}')} (Line)")

    # =========================================================================
    # PLACE BAR LABELS
    # =========================================================================
    ylo, yhi = ax.get_ylim()
    for i in idx_to_label:
        for j, c in enumerate(bar_certs):
            s = series_for(c, bar_metric)
            v = s.iloc[i]
            if pd.isna(v):
                continue

            xpos = x[i] + offsets[c]
            ypos = v + (yhi - ylo) * 0.02

            ann = ax.annotate(f"{v:.2%}", xy=(xpos, ypos), xytext=(0, 0),
                              textcoords="offset pixels", ha="center", va="bottom",
                              fontsize=tag_size, fontweight="bold", color="#2B2B2B",
                              bbox=dict(boxstyle="round,pad=0.1", fc="white", alpha=0.7))

            xd, yd = ax.transData.transform((xpos, ypos))
            w, h = 40, 20
            placer.add_fixed((xd - w / 2, yd - h / 2, xd + w / 2, yd + h / 2))

    # =========================================================================
    # PLACE LINE LABELS
    # =========================================================================
    for c in line_certs:
        s = series_for(c, line_metric)
        col = colors.get(c, "#7F8C8D")
        for k in idx_to_label:
            val = s.iloc[k]
            if pd.notna(val):
                txt = f"{val:.2%}"
                found = False
                for yoff in [15, -15, 25, -25]:
                    rect = placer.rect_for_text(x[k], float(val), 0, yoff, txt)
                    if placer.can_place(rect):
                        ann = ax2.annotate(txt, xy=(x[k], float(val)), xytext=(0, yoff),
                                           textcoords="offset pixels", fontsize=tag_size,
                                           fontweight="bold", color=col,
                                           bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=col))
                        placer.add_line_ann(ann, rect)
                        found = True
                        break
                if not found:
                    ax2.annotate(txt, xy=(x[k], float(val)), xytext=(0, 15),
                                 textcoords="offset pixels", fontsize=tag_size,
                                 fontweight="bold", color=col)

    placer.relax()

    # =========================================================================
    # FINAL POLISH
    # =========================================================================
    ax.set_xticks(x)
    ax.set_xticklabels(xticks, fontsize=tick_size, rotation=0)

    # Axis labels
    left_label = bar_axis_label or bar_metric.replace("_", " ")
    right_label = line_axis_label or line_metric.replace("_", " ")
    ax.set_ylabel(left_label, fontsize=axis_label_size, fontweight="bold")
    ax2.set_ylabel(right_label, fontsize=axis_label_size, fontweight="bold")

    # Dynamic Formatting
    def get_formatter(metric_name):
        if "Years" in metric_name:
            return plt.FuncFormatter(lambda y, _: f"{y:.1f} Yrs")
        elif "Coverage" in metric_name or "Ratio" in metric_name:
            # If values are small (like 0.015), format as %. If > 1.0, format as 'x' or raw
            return plt.FuncFormatter(lambda y, _: f"{y:.2f}x" if abs(y) > 0.5 else f"{y:.1%}")
        else:
            return plt.FuncFormatter(lambda y, _: f"{y:.2%}")

    ax.yaxis.set_major_formatter(get_formatter(bar_metric))
    ax2.yaxis.set_major_formatter(get_formatter(line_metric))

    # Title and subtitle
    chart_title = title or f"{bar_metric.replace('_', ' ')} vs {line_metric.replace('_', ' ')}"
    fig.text(0.5, 0.97, chart_title, ha="center", va="top",
             fontsize=title_size, fontweight="bold", color="#2B2B2B")

    if subtitle:
        fig.text(0.5, 0.90, subtitle, ha="center", va="top",
                 fontsize=subtitle_size, color="#6E6E6E")

    # Legend
    leg = leg_ax.legend(bar_handles + line_handles, bar_labels + line_labels,
                        ncol=len(bar_certs) + len(line_certs), loc="center",
                        frameon=True, fontsize=legend_fontsize)

    # Save if requested
    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)

    return fig, ax


def create_credit_deterioration_chart_ppt_old(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    start_date: str = "2023-01-01",
    bar_metric: str = "TTM_NCO_Rate",
    line_metric: str = "Nonaccrual_to_Gross_Loans_Rate",
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

    # ---------- palette / entities
    MS_BLUE, MS_LIGHT, CORE_GOLD = "#002F6C", "#4C78A8", "#F7A81B"
    default_entities = [subject_bank_cert, 32992, 90001]  # MSPBNA, MSBNA, Core PB
    bar_entities  = bar_entities  or default_entities
    line_entities = line_entities or list(bar_entities)
    names  = {34221:"MSPBNA", 32992:"MSBNA", 90001:"Core PB", 90003:"All Peers"}
    colors = {34221:MS_BLUE, 32992:MS_LIGHT, 90001:CORE_GOLD, 90003:"#7F8C8D"}

    # ---------- filter & timeline
    df = proc_df_with_peers.loc[
        proc_df_with_peers["CERT"].isin(set(bar_entities + line_entities))
    ].copy()
    df = df[df["REPDTE"] >= pd.to_datetime(start_date)].sort_values(["REPDTE","CERT"])
    if df.empty:
        return None, None

    df["Period_Label"] = "Q" + df["REPDTE"].dt.quarter.astype(str) + "-" + (df["REPDTE"].dt.year % 100).astype(str).str.zfill(2)
    subj     = df[df["CERT"] == subject_bank_cert][["REPDTE","Period_Label"]].drop_duplicates().sort_values("REPDTE")
    timeline = subj[["REPDTE"]].copy()
    x        = np.arange(len(subj))
    xticks   = subj["Period_Label"].tolist()
    last_dt  = timeline["REPDTE"].max()

    # ---------- figure / axes
    fig = plt.figure(figsize=figsize, constrained_layout=False)
    from matplotlib.gridspec import GridSpec
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

    # =========================================================================
    # HELPER FUNCTIONS
    # =========================================================================
    def to_decimal(series: pd.Series, metric_name: str = "") -> pd.Series:
        """
        Smart conversion:
        - If metric implies magnitude (Years, Coverage > 100%), leave as is.
        - If values look like percent points (median > 1.0), divide by 100.
        - Otherwise (median <= 1.0), assume already decimal.
        """
        s = pd.to_numeric(series, errors="coerce").astype(float)

        # 1. Whitelist: Metrics that are integers or ratios > 1.0 (Do NOT scale down)
        keywords_no_scale = ["Coverage", "Years", "Ratio", "Index", "Multiple", "Buffer"]
        if any(k in metric_name for k in keywords_no_scale):
            return s

        # 2. Standard Heuristic for Rates
        med = s.dropna().abs().median()
        if pd.isna(med):
            return s

        # If median > 1.0, assume it's in percent points (e.g. 5.50 for 5.5%), so scale.
        # If median <= 1.0, assume it's already decimal (e.g. 0.055), keep as is.
        return s / 100.0 if med > 1.0 else s

    def series_for(cert: int, metric: str) -> pd.Series:
        """Get time-aligned series for a given cert and metric."""
        ed = timeline.merge(df[df["CERT"] == cert][["REPDTE", metric]], on="REPDTE", how="left")
        # PASS THE METRIC NAME HERE to trigger the logic above
        return to_decimal(ed[metric], metric_name=metric)
    def lighten(hex_color: str, factor: float = 0.85) -> str:
        h = hex_color.lstrip("#"); r,g,b = int(h[0:2],16), int(h[2:4],16), int(h[4:6],16)
        r = int(r + (255-r)*factor); g = int(g + (255-g)*factor); b = int(b + (255-b)*factor)
        return f"#{r:02X}{g:02X}{b:02X}"

    # ---------- axis limits (support negatives)
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

    # ---------- bars
    n = max(len(bar_entities), 1)
    bar_w   = 0.8 / n
    offsets = {c: (i - (n - 1) / 2) * bar_w for i, c in enumerate(bar_entities)}

    bar_handles, bar_labels, line_handles, line_labels = [], [], [], []
    for c in bar_entities:
        vals = series_for(c, bar_metric)
        b = ax.bar(x + offsets[c], vals, width=bar_w, color=colors[c], alpha=0.92,
                   label=f"{names.get(c,f'CERT {c}')} {bar_metric.replace('_',' ')}", zorder=2)
        bar_handles.append(b[0]); bar_labels.append(f"{names.get(c,f'CERT {c}')} {bar_metric.replace('_',' ')}")

    # label indices: all year-ends + latest
    qtr = timeline["REPDTE"].dt.quarter
    idx_to_label = np.where((qtr == 4) | (timeline["REPDTE"] == last_dt))[0]

    # ---------- tiny collision engine (operate in pixels)
    DPI = fig.dpi

    def rects_overlap(a, b):
        ax0,ay0,ax1,ay1 = a; bx0,by0,bx1,by1 = b
        return not (ax1 < bx0 or bx1 < ax0 or ay1 < by0 or by1 < ay0)

    class Placer:
        def __init__(self, ax, tag_sz):
            self.ax = ax
            self.tag_sz = tag_sz
            self.fixed_rects = []      # bar labels occupy here (obstacles)
            self.items = []            # line labels we can move: dict(ann, rect)

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
            # Push overlapping line labels apart vertically (and a bit horizontally if needed)
            for _ in range(max_iter):
                moved = False
                for i in range(len(self.items)):
                    for j in range(i+1, len(self.items)):
                        ri, rj = self.items[i]["rect"], self.items[j]["rect"]
                        if rects_overlap(ri, rj):
                            # push i up, j down
                            for idx, sgn in ((i, +1), (j, -1)):
                                ann = self.items[idx]["ann"]
                                xoff, yoff = ann.get_position()  # pixels (we use 'offset pixels')
                                new = (xoff, yoff + sgn*step_px)
                                ann.set_position(new)
                                # recompute rect
                                xd, yd = self.ax.transData.transform(ann.xy)
                                rect = (xd + new[0] - (ri[2]-ri[0])/2,
                                        yd + new[1] - (ri[3]-ri[1])/2,
                                        xd + new[0] + (ri[2]-ri[0])/2,
                                        yd + new[1] + (ri[3]-ri[1])/2)
                                # if now collides a fixed rect, try small x jiggle
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

    # ---------- place LINE series + initial tags (aware of fixed bar rects later)
    for c in line_entities:
        s = series_for(c, line_metric)
        ln, = ax2.plot(
            x, s, color=colors.get(c, "#7F8C8D"),
            linewidth=2.4, linestyle="-" if c == subject_bank_cert else "--",
            marker="o", markersize=4.4, zorder=3,
            label=f"{names.get(c,f'CERT {c}')} {line_metric.replace('_',' ')}"
        )
        line_handles.append(ln); line_labels.append(f"{names.get(c,f'CERT {c}')} {line_metric.replace('_',' ')}")

    # ---------- BAR labels first (alternating), added as FIXED obstacles
    ylo, yhi = ax.get_ylim(); yrng = yhi - ylo
    zero_in_view = (ylo < 0 < yhi)
    baseline = 0.0 if zero_in_view else ylo + 0.02*yrng
    above_gap = 0.016*yrng
    below_gap = 0.018*yrng
    tip_pad   = 0.020*yrng

    def add_fixed_bar_label(text, x_data, y_data):
        # create the actual annotation and register its rect as fixed
        ann = ax.annotate(
            text, xy=(x_data, y_data), xytext=(0, 0), textcoords="offset pixels",
            ha="center", va="center", fontsize=tag_size, fontweight="bold",
            color="#2B2B2B", clip_on=False, zorder=9,
            bbox=dict(boxstyle="round,pad=0.25", fc="white", ec="none", alpha=0.95)
        )
        # rect in pixels
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

            # Try desired spot. If it would overlap later with line labels we’ll still accept
            # because line labels will move, but if a *line point* is right on it, go to tip.
            add_fixed_bar_label(f"{v:.2%}", xpos, desired)

            # If the “above” choice landed inside the bar body and could be hidden by the bar,
            # also try a tip placement (outside the bar) but only when label is above baseline
            if (j % 2 == 0):  # the ones that might conflict with line tags
                if abs(v - desired) < 1e-9:  # exactly on baseline (tiny bars)
                    add_fixed_bar_label(f"{v:.2%}", xpos, (v + tip_pad))
                # If we later find global overlaps, line labels will move away.

    # ---------- now add LINE labels with global awareness, then relax
    def place_line_tag(text, xd_data, yd_data, pref_up=True, fc="#FFFFFF"):
        # candidate offsets (pixels) – try a small fan above, then below
        candidates = [(0, 0)]
        base = [8, 16, 24, 36, 48]
        verticals = base if pref_up else [-b for b in base]
        for dy in verticals + [-b for b in base]:
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
        # last resort
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

    for c in line_entities:
        s = series_for(c, line_metric)
        for k in idx_to_label:
            val = s.iloc[k]
            if pd.notna(val):
                place_line_tag(f"{val:.2%}", x[k], float(val), pref_up=(val >= 0), fc=lighten(colors[c], 0.85))

    # globally relax line labels (move them apart, respecting bar labels)
    placer.relax(max_iter=40, step_px=8)

    # ---------- axes / titles / legend
    ax.set_xticks(x); ax.set_xticklabels(xticks, fontsize=tick_size)
    ax.set_ylabel(bar_metric.replace("_"," "),  fontsize=axis_label_size, fontweight="bold")
    ax2.set_ylabel(line_metric.replace("_"," "), fontsize=axis_label_size, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.2%}"))
    ax2.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.2%}"))

    title = custom_title or f"{bar_metric.replace('_',' ')} (bars) vs {line_metric.replace('_',' ')} (lines)"
    fig.text(0.5, 0.97, title, ha="center", va="top",
             fontsize=title_size, fontweight="bold", color="#2B2B2B")

    handles = bar_handles + line_handles
    labels  = bar_labels  + line_labels
    leg = leg_ax.legend(handles, labels, ncol=3, loc="center", frameon=True,
                        columnspacing=2.0, handlelength=2.2, fontsize=legend_fontsize)
    leg.get_frame().set_alpha(0.96)

    fig.tight_layout()
    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig, ax





def plot_scatter_dynamic(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    subject_cert: int = 34221,        # MSPBNA
    sibling_cert: int = 32992,        # MSBNA  <-- ADD
    peer_avg_cert_primary: int = 90003,
    peer_avg_cert_alt: int = 90001,
    use_alt_peer_avg: bool = False,
    show_peers_avg_label: bool = True,
    show_idb_label: bool = True,
    show_sibling_label: bool = True,  # <-- ADD
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
    exclude_certs: Optional[List[int]] = None,
    save_path: Optional[str] = None
) -> Tuple[plt.Figure, plt.Axes]:
    if df.empty: raise ValueError("scatter DF is empty")

    def to_decimals_series(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        # HEURISTIC UPDATE: Only divide if values are clearly Percent Points (> 1.0)
        # Prevents 0.05 (5%) from becoming 0.0005 (0.05%)
        # Also ignores 'Years' or 'Coverage' based on magnitude if needed,
        # but scatters usually handle raw data better.
        if s.dropna().abs().median() > 1.0:
            return s / 100.0
        return s

    peers_cert = peer_avg_cert_alt if use_alt_peer_avg else peer_avg_cert_primary
    df = df.copy()

    # --- ADD THESE TWO LINES ---
    if exclude_certs:
        df = df[~df["CERT"].isin(exclude_certs)]
    # ---------------------------

    df[x_col] = pd.to_numeric(df[x_col], errors="coerce")
    df[y_col] = pd.to_numeric(df[y_col], errors="coerce")

    fig, ax = plt.subplots(figsize=figsize)
    if transparent_bg: fig.patch.set_alpha(0); ax.set_facecolor("none")

    if economist_style:
        for sp in ["top","right"]: ax.spines[sp].set_visible(False)
        for s in ax.spines.values(): s.set_linewidth(1.1); s.set_color("#2B2B2B")
        ax.tick_params(axis="both", labelsize=tick_size, colors="#2B2B2B")
        ax.grid(True, color="#D0D0D0", linewidth=0.8, alpha=0.35)

    idb = df[df["CERT"] == subject_cert]
    peer_avg = df[df["CERT"] == peers_cert]
    sib = df[df["CERT"] == sibling_cert]
    # Exclude subject and the specific peer composites from the background dots
    others = df[~df["CERT"].isin([subject_cert, sibling_cert,  peer_avg_cert_primary, peer_avg_cert_alt, 90001, 90002, 90003])]

    Xo, Yo = to_decimals_series(others[x_col]), to_decimals_series(others[y_col])
    PEER_COLOR = "#4C78A8"  # Light Blue for individual peers
    ax.scatter(Xo, Yo, s=42, alpha=0.9, color=PEER_COLOR, edgecolor="white", linewidth=0.6, label="Peers")

    xi = yi = None
    MSPBNA_BLUE = "#002F6C"  # Deep MS Blue for subject
    GUIDE = "#7F8C8D"  # Gray for reference lines

    if not idb.empty:
        xi = float(to_decimals_series(idb[x_col]).iloc[0]); yi = float(to_decimals_series(idb[y_col]).iloc[0])
        ax.scatter(xi, yi, s=100, color=MSPBNA_BLUE, edgecolor="white", linewidth=1.5, label="MSPBNA", zorder=5)

    px = py = None
    sx = sy = None
    MS_LIGHT = "#4C78A8"  # you already use this for peers / MSBNA elsewhere
    if not sib.empty:
        sx = float(to_decimals_series(sib[x_col]).iloc[0])
        sy = float(to_decimals_series(sib[y_col]).iloc[0])
        ax.scatter(sx, sy, s=90, color=MS_LIGHT, edgecolor="white", linewidth=1.2,
                   label="MSBNA", zorder=5)

    if not peer_avg.empty:
        px = float(to_decimals_series(peer_avg[x_col]).iloc[0])
        py = float(to_decimals_series(peer_avg[y_col]).iloc[0])
        ax.scatter(px, py, s=90, color=PEER_COLOR, marker="s", edgecolor="black", linewidth=0.7, label="_nolegend_")
        ax.axvline(px, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)
        ax.axhline(py, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)

    import re
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
        dx, _ = pick_offset(px, py, along_line=True)
        tag(px, py, "Peers' Average", (dx, 0), color=GUIDE, box=False)

    if show_idb_label and (xi is not None):
        # UPDATED LABEL: IDBNY -> MSPBNA
        dx, dy = pick_offset(xi, yi); tag(xi, yi, "MSPBNA", (dx, dy), color="black", box=True)
    if show_sibling_label and (sx is not None):
        dx, dy = pick_offset(sx, sy); tag(sx, sy, "MSBNA", (dx, dy), color="black", box=True)


    if identify_outliers and outliers_topn > 0:
        X_all = to_decimals_series(df[x_col]); Y_all = to_decimals_series(df[y_col])
        cx = px if px is not None else float(X_all.mean())
        cy = py if py is not None else float(Y_all.mean())
        d = ((X_all - cx)**2 + (Y_all - cy)**2)**0.5
        mask_excl = df["CERT"].isin([
            peer_avg_cert_primary, peer_avg_cert_alt, 90001, 90002, 90003,
            subject_cert, sibling_cert
        ])
        cand = d[~mask_excl].sort_values(ascending=False)
        top_idx = list(cand.index[:outliers_topn])


        for i in top_idx:
            ox = float(to_decimals_series(pd.Series([df.loc[i, x_col]])).iloc[0])
            oy = float(to_decimals_series(pd.Series([df.loc[i, y_col]])).iloc[0])
            label = short_name(df.loc[i, "NAME"]) if "NAME" in df.columns else str(df.loc[i, "CERT"])
            dx, dy = pick_offset(ox, oy); tag(ox, oy, label, (dx, dy), color="black", box=True)

    all_x = pd.concat([
        Xo,
        pd.Series([xi]) if xi is not None else pd.Series(dtype=float),
        pd.Series([sx]) if sx is not None else pd.Series(dtype=float),   # <-- ADD
        pd.Series([px]) if px is not None else pd.Series(dtype=float)
    ], ignore_index=True).dropna()

    all_y = pd.concat([
        Yo,
        pd.Series([yi]) if yi is not None else pd.Series(dtype=float),
        pd.Series([sy]) if sy is not None else pd.Series(dtype=float),   # <-- ADD
        pd.Series([py]) if py is not None else pd.Series(dtype=float)
    ], ignore_index=True).dropna()

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
# SCRIPT EXECUTION
# ==================================================================================

if __name__ == "__main__":
    generate_reports()