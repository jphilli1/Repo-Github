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
from typing import Dict, List, Tuple, Optional, Any, Union
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
    """
    return {
        'subject_bank_cert': 34221,  # MSPBNA
        'sibling_bank_cert': 32992,  # MSBNA (for table comparisons)
        # Peer composites created by CR_Bank_DashvMSPB.py
        'peer_composites': {
            'Core_PB': 90001,      # Core Private Bank Peers (GS + UBS)
            'MS_Wealth': 90002,    # MS + Extended Wealth
            'All_Peers': 90003     # Full Universe
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
def _clean_display_name(name: str) -> str:
    """
    Cleans metric/column names for display by removing internal prefixes.
    - Removes 'RIC_' prefix (Schedule RI-C internal naming)
    - Removes 'Norm_' prefix (handled separately with * notation)
    """
    if name is None:
        return name
    # Remove RIC_ prefix
    cleaned = name.replace("RIC_", "").replace("ric_", "")
    # Clean up any double underscores or leading underscores
    cleaned = cleaned.replace("__", "_").lstrip("_")
    return cleaned
# ==================================================================================
# METRIC COLOR CODING CONFIGURATION
# ==================================================================================
# This configuration controls how metrics are color-coded in HTML tables.
# Edit these lists to change color behavior WITHOUT modifying any functions.
#
# Rules:
#   - HIGHER_IS_BETTER: Green when MSPBNA > Peer Avg, Red when MSPBNA < Peer Avg
#   - LOWER_IS_BETTER:  Green when MSPBNA < Peer Avg, Red when MSPBNA > Peer Avg
#   - NEUTRAL_METRICS:  No color coding (gray/neutral) regardless of value
#   - Unlisted metrics default to NEUTRAL
#
# Match is by substring - e.g., "NCO Rate" matches "NCO Rate (TTM) (%)"
# ==================================================================================

METRIC_COLOR_CONFIG = {
    # -------------------------------------------------------------------------
    # HIGHER IS BETTER (Green = above peers, Red = below peers)
    # Use sparingly - only true performance outcomes
    # -------------------------------------------------------------------------
    "HIGHER_IS_BETTER": [
        "ROA",           # Return on Assets
        "ROE",           # Return on Equity
    ],

    # -------------------------------------------------------------------------
    # LOWER IS BETTER (Green = below peers, Red = above peers)
    # Risk incidence or inefficiency metrics
    # -------------------------------------------------------------------------
    "LOWER_IS_BETTER": [
        "Nonaccrual",        # Nonaccrual Rate, % in Nonaccrual, etc.
        "NCO Rate",          # NCO Rate (TTM), CRE NCO Rate, Norm NCO Rate
        "Delinquency",       # Any delinquency metric
        "Efficiency Ratio",  # Operating efficiency (lower = better)
    ],

    # -------------------------------------------------------------------------
    # NEUTRAL METRICS (No color coding - contextual/strategic)
    # These are explicitly neutral regardless of peer comparison
    # -------------------------------------------------------------------------
    "NEUTRAL_METRICS": [
        # Allowance / Reserving (explicitly neutral per guidance)
        "ACL Ratio",
        "ACL Coverage",
        "Coverage",
        "Risk-Adj ACL",
        "Headline ACL",
        "CRE ACL",
        "NPL Coverage",

        # Balance Sheet Size (always neutral)
        "Total Assets",
        "Total Loans",
        "Gross Loans",
        "Norm Loans",

        # Portfolio Composition (always neutral - reflects strategy, not quality)
        "% of Loans",
        "% of ACL",
        "Composition",
        "Loan Share",
        "Fund Finance",

        # Liquidity & Funding (neutral - optimal is institution-specific)
        "Liquidity Ratio",
        "Loans to Deposits",
        "Loans/Deposits",
        "Deposit",
        "Wholesale Funding",

        # Capital Structure (neutral - benchmarked against internal targets)
        "Tier 1",
        "CET1",
        "Leverage Ratio",
        "RWA",
        "Capital Ratio",

        # Other contextual metrics
        "NIM",
        "Net Interest Margin",
        "Loan Yield",
        "Provision Rate",
    ],
}


def get_metric_color_direction(metric_name: str) -> str:
    """
    Determines the color coding direction for a metric.

    Args:
        metric_name: Display name of the metric (e.g., "NCO Rate (TTM) (%)")

    Returns:
        "higher" - Higher values are better (green when above peers)
        "lower"  - Lower values are better (green when below peers)
        "neutral" - No color coding

    Usage:
        direction = get_metric_color_direction("ROA (%)")  # Returns "higher"
        direction = get_metric_color_direction("NCO Rate (TTM) (%)")  # Returns "lower"
        direction = get_metric_color_direction("SBL % of Loans")  # Returns "neutral"
    """
    # Check NEUTRAL first (most specific, should take precedence)
    for pattern in METRIC_COLOR_CONFIG["NEUTRAL_METRICS"]:
        if pattern.lower() in metric_name.lower():
            return "neutral"

    # Check HIGHER_IS_BETTER
    for pattern in METRIC_COLOR_CONFIG["HIGHER_IS_BETTER"]:
        if pattern.lower() in metric_name.lower():
            return "higher"

    # Check LOWER_IS_BETTER
    for pattern in METRIC_COLOR_CONFIG["LOWER_IS_BETTER"]:
        if pattern.lower() in metric_name.lower():
            return "lower"

    # Default: NEUTRAL (unlisted metrics get no color)
    return "neutral"


def get_color_class(
    metric_name: str,
    subject_value: float,
    peer_value: float,
    threshold_bps: float = 5.0
) -> str:
    """
    Returns the CSS class for coloring a metric value.

    Args:
        metric_name: Display name of the metric
        subject_value: MSPBNA's value (raw, not formatted)
        peer_value: Peer average value (raw, not formatted)
        threshold_bps: Threshold in basis points for neutral zone (default 5 bps)

    Returns:
        CSS class name: "mspbna-good", "mspbna-bad", or "mspbna-neutral"

    Usage:
        cls = get_color_class("ROA (%)", 1.25, 1.10)  # Returns "mspbna-good"
        cls = get_color_class("NCO Rate (%)", 0.50, 0.30)  # Returns "mspbna-bad"
    """
    # Handle missing data
    if pd.isna(subject_value) or pd.isna(peer_value):
        return "mspbna-neutral"

    direction = get_metric_color_direction(metric_name)

    # Neutral metrics get no color
    if direction == "neutral":
        return "mspbna-neutral"

    diff = subject_value - peer_value

    # Within threshold = neutral
    if abs(diff) < threshold_bps / 100:  # Convert bps to decimal if needed
        # Handle case where values are already in percentage points
        if abs(subject_value) > 1 or abs(peer_value) > 1:
            if abs(diff) < threshold_bps:
                return "mspbna-neutral"
        else:
            return "mspbna-neutral"

    # Apply directional logic
    if direction == "higher":
        # Higher is better: positive diff = good
        return "mspbna-good" if diff > 0 else "mspbna-bad"
    else:  # direction == "lower"
        # Lower is better: negative diff = good
        return "mspbna-good" if diff < 0 else "mspbna-bad"
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
def calculate_weighted_peer_average(
    bank_data: Dict[int, pd.Series],
    metric_col: str,
    weight_col: str = "Gross_Loans",
    peer_type: str = "all_ex_mspbna",
    subject_cert: int = 34221,
    bank_certs_in_table: Optional[List[int]] = None,
) -> float:
    """
    Calculate weighted peer average dynamically from bank data.

    Args:
        bank_data: Dict mapping CERT -> Series of metric values
        metric_col: Column name to average
        weight_col: Column to use as weight (Gross_Loans or Norm_Gross_Loans)
        peer_type:
            - "core_pb": Weighted average of GS (33124) and UBS (57565)
            - "all_ex_mspbna": Weighted average of all banks in table except MSPBNA
        subject_cert: CERT to exclude (default: MSPBNA 34221)
        bank_certs_in_table: List of bank CERTs included in table (for all_ex_mspbna)

    Returns:
        Weighted peer average: Σ(value_i × weight_i) / Σ(weight_i)
    """
    CORE_PB_CERTS = [33124, 57565]  # GS, UBS

    if peer_type == "core_pb":
        certs_to_avg = CORE_PB_CERTS
    else:  # all_ex_mspbna
        if bank_certs_in_table:
            # Use only banks that are in the table, excluding subject and synthetic CERTs
            certs_to_avg = [c for c in bank_certs_in_table if c != subject_cert and c < 90000]
        else:
            # Fallback: use all real banks in data
            certs_to_avg = [c for c in bank_data.keys() if c != subject_cert and c < 90000]

    weighted_sum = 0.0
    total_weight = 0.0

    for cert in certs_to_avg:
        if cert not in bank_data or bank_data[cert] is None:
            continue

        val = bank_data[cert].get(metric_col)
        weight = bank_data[cert].get(weight_col)

        if pd.notna(val) and pd.notna(weight) and weight > 0:
            weighted_sum += val * weight
            total_weight += weight

    if total_weight == 0:
        return np.nan

    return weighted_sum / total_weight


def calculate_peer_total(
    bank_data: Dict[int, pd.Series],
    metric_col: str,
    peer_type: str = "all_ex_mspbna",
    subject_cert: int = 34221,
    bank_certs_in_table: Optional[List[int]] = None,
) -> float:
    """
    Calculate peer total (sum) for balance sheet items like Total Loans.
    Used for displaying aggregate size, not for ratio calculations.
    """
    CORE_PB_CERTS = [33124, 57565]  # GS, UBS

    if peer_type == "core_pb":
        certs_to_avg = CORE_PB_CERTS
    else:
        if bank_certs_in_table:
            certs_to_avg = [c for c in bank_certs_in_table if c != subject_cert and c < 90000]
        else:
            certs_to_avg = [c for c in bank_data.keys() if c != subject_cert and c < 90000]

    total = 0.0
    count = 0

    for cert in certs_to_avg:
        if cert not in bank_data or bank_data[cert] is None:
            continue
        val = bank_data[cert].get(metric_col)
        if pd.notna(val):
            total += val
            count += 1

    # Return simple average for balance sheet items (or could return total)
    return total / count if count > 0 else np.nan


# Peer group configuration
PEER_GROUP_CONFIG = {
    "core_pb": {
        "name": "Core PB Avg",
        "short_name": "Core PB",
        "certs": [33124, 57565],  # GS, UBS
        "description": "Core Private Bank Peers (Goldman Sachs + UBS)",
    },
    "all_ex_mspbna": {
        "name": "Peer Avg",
        "short_name": "Peer Avg",
        "certs": None,  # Calculated dynamically
        "description": "All Peers excluding MSPBNA (Loan-Weighted)",
    },
}
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
def _fmt_percent_diff(v: float) -> str:
    """Format percentage difference with same logic as _fmt_percent."""
    if pd.isna(v): return "N/A"
    # Same logic as _fmt_percent: multiply by 100 if in decimal form
    val = v * 100.0 if abs(v) < 1.0 else v
    return f"{val:+.2f}%"
def _clean_metric_name(col_name: str) -> str:
    """
    Cleans internal column names for user-facing display.

    Transformations:
    - Removes 'RIC_' prefix (Schedule RI-C internal naming)
    - Converts 'Cost' to 'Bal.' for RIC series (these are amortized cost = balance)
    - Converts underscores to spaces
    """
    if col_name is None:
        return col_name

    cleaned = col_name

    # Track if this was an RIC_ series (for Cost -> Bal. replacement)
    is_ric_series = cleaned.startswith("RIC_") or cleaned.startswith("ric_")

    # Remove internal prefixes
    cleaned = cleaned.replace("RIC_", "").replace("ric_", "")
    cleaned = cleaned.replace("Norm_", "").replace("norm_", "")

    # For RIC series: Cost means amortized cost (i.e., balance)
    if is_ric_series:
        cleaned = cleaned.replace("_Cost_", "_Bal._")
        if cleaned.endswith("_Cost"):
            cleaned = cleaned[:-5] + "_Bal."
        cleaned = cleaned.replace("Cost", "Bal.")

    # Convert underscores to spaces for readability
    cleaned = cleaned.replace("_", " ")

    # Clean up extra spaces
    cleaned = " ".join(cleaned.split())

    return cleaned.strip()
# ==================================================================================
#  TABLE 1: THE "SKINNY" SUMMARY (MSPBNA, MSBNA, Core Avg, All Avg, Diffs)
# ==================================================================================

def generate_credit_metrics_email_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    normalized: bool = False,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",  # "core_pb" or "all_ex_mspbna"
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates Executive Credit Summary HTML table with loan-weighted peer averages.

    Args:
        proc_df_with_peers: Full FDIC data panel
        subject_bank_cert: CERT for subject bank (MSPBNA default)
        normalized: If True, uses normalized metrics
        bank_certs: Banks to include. Default: [MSPBNA, MSBNA, UBS, GS]
        peer_type: "core_pb" (GS+UBS weighted) or "all_ex_mspbna" (all peers weighted)

    Returns:
        Tuple of (HTML string, DataFrame)
    """

    if bank_certs is None:
        bank_certs = [34221, 32992, 57565, 33124]  # MSPBNA, MSBNA, UBS, GS

    # Metric definitions: (display_name, format, std_col, norm_col, is_composition)
    metric_defs = [
        # === BALANCE SHEET ===
        # === BALANCE SHEET ===
        ("Total Loans ($B)", 'B', "Gross_Loans", "Norm_Gross_Loans", False),

        # === CREDIT METRICS (ACL/Coverage) ===
        ("Risk-Adj ACL Ratio (%)", '%', "Risk_Adj_Allowance_Coverage", "Norm_Risk_Adj_Allowance_Coverage", False),
        ("Headline ACL Ratio (%)", '%', "Allowance_to_Gross_Loans_Rate", "Norm_ACL_Coverage", False),

        # === CREDIT METRICS (Asset Quality) ===
        ("Nonaccrual Rate (%)", '%', "Nonaccrual_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate", False),
        ("NCO Rate (TTM) (%)", '%', "TTM_NCO_Rate", "Norm_NCO_Rate", False),
        ("Delinquency Rate (%)", '%', "Total_Delinquency_Rate", "Norm_Delinquency_Rate", False),

        # === COMPOSITION (No color coding) ===
        ("SBL % of Loans", '%', "SBL_Composition", "Norm_SBL_Composition", True),
        ("Resi % of Loans", '%', "RIC_Resi_Loan_Share", "Norm_Wealth_Resi_Composition", True),
        ("CRE % of Loans", '%', "RIC_CRE_Loan_Share", "Norm_CRE_Investment_Composition", True),
        ("Fund Finance %", '%', "Fund_Finance_Composition", "Norm_Fund_Finance_Composition", True),

        # === CRE SEGMENT ===
        ("CRE % of ACL", '%', "RIC_CRE_ACL_Share", "Norm_CRE_ACL_Share", True),
        ("CRE ACL Ratio (%)", '%', "RIC_CRE_ACL_Coverage", "Norm_CRE_ACL_Coverage", False),
        ("CRE NPL Coverage (x)", 'x', "RIC_CRE_Risk_Adj_Coverage", None, False),
        ("% of CRE in Nonaccrual", '%', "RIC_CRE_Nonaccrual_Rate", None, False),
        ("CRE NCO Rate (TTM)", '%', "RIC_CRE_NCO_Rate", None, False),
        ("CRE Delinquency Rate (%)", '%', "RIC_CRE_Delinquency_Rate", None, False),

        # === RESI SEGMENT ===
        ("Resi % of ACL", '%', "RIC_Resi_ACL_Share", "Norm_Resi_ACL_Share", True),
        ("Resi ACL Ratio (%)", '%', "RIC_Resi_ACL_Coverage", "Norm_RESI_ACL_Coverage", False),
        ("Resi NPL Coverage (x)", 'x', "RIC_Resi_Risk_Adj_Coverage", None, False),
        ("% of Resi in Nonaccrual", '%', "RIC_Resi_Nonaccrual_Rate", None, False),
        ("Resi NCO Rate (TTM)", '%', "RIC_Resi_NCO_Rate", None, False),
        ("Resi Delinquency Rate (%)", '%', "RIC_Resi_Delinquency_Rate", None, False),
    ]
    # --- FILTER: Remove Fund Finance from Normalized View ---
    if normalized:
        metric_defs = [m for m in metric_defs if "Fund Finance" not in m[0]]

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Get data for each bank
    bank_data = {}
    for cert in bank_certs:
        try:
            bank_data[cert] = latest_data[latest_data["CERT"] == cert].iloc[0]
        except IndexError:
            bank_data[cert] = None

    if bank_data.get(subject_bank_cert) is None:
        return None, None

    bank_names = {
        34221: "MSPBNA", 32992: "MSBNA", 57565: "UBS", 33124: "GS",
    }

    # Determine peer average column name and weight column
    peer_config = PEER_GROUP_CONFIG.get(peer_type, PEER_GROUP_CONFIG["core_pb"])
    peer_name = peer_config["short_name"]
    weight_col = "Norm_Gross_Loans" if normalized else "Gross_Loans"

    # For segment-specific metrics, use segment balance as weight
    SEGMENT_WEIGHT_MAP = {
        # CRE: Switch to robust RC-C balance if RI-C is unreliable
        "RIC_CRE_": "CRE_Investment_Pure_Balance",

        # RESI: Switch to robust RC-C balance (The Fix)
        "RIC_Resi_": "Wealth_Resi_Balance",

        # C&I: Consider switching to 'Corp_CI_Balance' if available, else keep Cost
        "RIC_Comm_": "RIC_Comm_Cost",

        # Constr: Consider switching to 'ADC_Balance' if available
        "RIC_Constr_": "RIC_Constr_Cost",
    }

    def get_weight_col_for_metric(metric_col: str) -> str:
        """Determine appropriate weight column for a given metric."""
        for prefix, segment_weight in SEGMENT_WEIGHT_MAP.items():
            if metric_col.startswith(prefix):
                return segment_weight
        return weight_col  # Default to total loans

    rows = []
    for disp, fmt, std_col, norm_col, is_composition in metric_defs:
        # Select column based on normalized flag
        if normalized and norm_col:
            if norm_col in bank_data[subject_bank_cert].index:
                col = norm_col
            else:
                col = std_col
        else:
            col = std_col

        if col is None:
            continue

        if col not in bank_data[subject_bank_cert].index:
            continue

        # Formatters
        if fmt == 'B':
            f = _fmt_money_billions
            fd = _fmt_money_billions_diff
        elif fmt == 'x':
            f = _fmt_multiple
            fd = _fmt_multiple_diff
        else:
            f = _fmt_percent
            fd = _fmt_percent_diff

        row_dict = {"Metric": disp + ("*" if normalized and norm_col else "")}

        # Add bank columns in order
        for cert in bank_certs:
            name = bank_names.get(cert, f"Bank {cert}")
            if bank_data.get(cert) is not None:
                val = bank_data[cert].get(col)
                row_dict[name] = f(val)
            else:
                row_dict[name] = "N/A"

        # Calculate weighted peer average
        metric_weight_col = get_weight_col_for_metric(col)

        if fmt == 'B':
            # For balance sheet items, use simple average not weighted
            peer_val_raw = calculate_peer_total(
                bank_data, col, peer_type=peer_type,
                subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )
        else:
            # For ratios, use loan-weighted average
            peer_val_raw = calculate_weighted_peer_average(
                bank_data, col, weight_col=metric_weight_col,
                peer_type=peer_type, subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )

        row_dict[peer_name] = f(peer_val_raw)

        # Get subject value for diff calculation
        subj_val_raw = bank_data[subject_bank_cert].get(col)

        # Calculate diff
        diff = subj_val_raw - peer_val_raw if pd.notna(subj_val_raw) and pd.notna(peer_val_raw) else np.nan
        row_dict[f"Diff vs {peer_name}"] = fd(diff)

        # Store raw values for color coding
        row_dict["_mspbna_raw"] = subj_val_raw
        row_dict["_peer_avg_raw"] = peer_val_raw

        rows.append(row_dict)

    df = pd.DataFrame(rows)

    # Generate HTML
    html = generate_html_email_table_dynamic(
        df, latest_date, table_type="summary", comparison_col=peer_name
    )

    # Add methodology footnote
    weight_desc = "Normalized Loans" if normalized else "Gross Loans"
    methodology_note = f"""
        <p><b>Peer Average Calculation:</b> {peer_config['description']} - Weighted by {weight_desc}.</p>
        <p><b>Segment Metrics:</b> Weighted by respective segment balances (CRE metrics by CRE Balance, Resi metrics by Resi Balance).</p>
    """

    if normalized:
        footnote_insert = f"""
        <p style="font-style:italic;"><b>* Normalized metrics</b> exclude: C&I, NDFI (Fund Finance), ADC, Credit Cards, Auto, Ag loans.</p>
        {methodology_note}
        """
    else:
        footnote_insert = methodology_note

    html = html.replace("</div></div></body></html>", footnote_insert + "</div></div></body></html>")

    return html, df

def generate_executive_summary_tables(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",  # Add this parameter
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Tuple[str, pd.DataFrame]]:
    """
    Generates both standard and normalized executive summary tables.
    """
    results = {}

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    date_str = as_of_date.strftime("%Y%m%d")

    # Standard
    html_std, df_std = generate_credit_metrics_email_table(
        proc_df_with_peers, subject_bank_cert, normalized=False,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['standard'] = (html_std, df_std)

    # Normalized
    html_norm, df_norm = generate_credit_metrics_email_table(
        proc_df_with_peers, subject_bank_cert, normalized=True,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['normalized'] = (html_norm, df_norm)

    if output_dir:
        if html_std:
            with open(output_dir / f"executive_summary_standard_{date_str}.html", "w") as f:
                f.write(html_std)
        if html_norm:
            with open(output_dir / f"executive_summary_normalized_{date_str}.html", "w") as f:
                f.write(html_norm)

    return results

def generate_excluded_segments_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    bank_certs: Optional[List[int]] = None,
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates a table showing what loan segments were excluded from the normalized
    peer comparison. Shows both Core PB and All Peers averages.

    Excluded Segments:
    - C&I (Commercial & Industrial)
    - NDFI (Nondepository Financial Institutions / Fund Finance)
    - ADC (Acquisition, Development & Construction)
    - Credit Cards
    - Auto Loans
    - Agricultural Loans
    - Owner-Occupied CRE
    """

    if bank_certs is None:
        bank_certs = [34221, 32992, 57565, 33124]  # MSPBNA, MSBNA, UBS, GS

    # Metric definitions: (display_name, format, column_name, is_pct_of_total)
    metric_defs = [
        # === TOTALS ===
        ("Total Gross Loans", 'B', "Gross_Loans", False),
        ("Total Excluded Balance", 'B', "Excluded_Balance", False),
        ("Excluded % of Total", '%', "Norm_Exclusion_Pct", False),
        ("Normalized Gross Loans", 'B', "Norm_Gross_Loans", False),

        # === EXCLUDED SEGMENT BALANCES ===
        ("C&I Balance", 'B', "Excl_CI_Balance", False),
        ("C&I % of Loans", '%', "Excl_CI_Balance", True),
        ("Fund Finance (NDFI)", 'B', "Excl_NDFI_Balance", False),
        ("Fund Finance % of Loans", '%', "Excl_NDFI_Balance", True),
        ("ADC / Construction", 'B', "Excl_ADC_Balance", False),
        ("ADC % of Loans", '%', "Excl_ADC_Balance", True),
        ("Credit Cards", 'B', "Excl_CreditCard_Balance", False),
        ("Credit Cards % of Loans", '%', "Excl_CreditCard_Balance", True),
        ("Auto Loans", 'B', "Excl_Auto_Balance", False),
        ("Auto % of Loans", '%', "Excl_Auto_Balance", True),
        ("Agricultural Loans", 'B', "Excl_Ag_Balance", False),
        ("Ag % of Loans", '%', "Excl_Ag_Balance", True),
        ("Owner-Occupied CRE", 'B', "Excl_OO_CRE_Balance", False),
        ("OO CRE % of Loans", '%', "Excl_OO_CRE_Balance", True),

        # === EXCLUDED CREDIT METRICS ===
        ("Excluded NCO (TTM)", 'M', "Excluded_NCO_TTM", False),
        ("Excluded Nonaccrual", 'M', "Excluded_Nonaccrual", False),
    ]

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == as_of_date]

    # Get data for each bank
    bank_data = {}
    for cert in bank_certs:
        try:
            bank_data[cert] = latest_data[latest_data["CERT"] == cert].iloc[0]
        except IndexError:
            bank_data[cert] = None

    if bank_data.get(subject_bank_cert) is None:
        return None, None

    bank_names = {
        34221: "MSPBNA", 32992: "MSBNA", 57565: "UBS", 33124: "GS",
        7213: "Citibank", 3511: "Wells Fargo", 3510: "BofA", 628: "JPMorgan",
    }

    # Define peer groups
    CORE_PB_CERTS = [33124, 57565]  # GS, UBS
    ALL_PEER_CERTS = [c for c in bank_certs if c != subject_bank_cert and c < 90000]

    def fmt_billions(v: float) -> str:
        if pd.isna(v): return "N/A"
        return f"${v/1_000_000:,.1f}B"

    def fmt_millions(v: float) -> str:
        if pd.isna(v): return "N/A"
        if abs(v) < 1000:
            return f"${v:,.0f}K"
        return f"${v/1_000:,.0f}M"

    def fmt_pct(v: float) -> str:
        if pd.isna(v): return "N/A"
        # Handle both decimal (0.05) and percent (5.0) formats
        val = v * 100.0 if abs(v) < 1.0 else v
        return f"{val:.2f}%"

    def calc_peer_avg(col: str, is_pct_of_total: bool, peer_certs: List[int]) -> float:
        """Calculate peer average for a given column and peer group."""
        if is_pct_of_total:
            # For % metrics, calculate weighted average (segment / total loans)
            total_segment = 0.0
            total_loans = 0.0
            for cert in peer_certs:
                if bank_data.get(cert) is not None:
                    seg = bank_data[cert].get(col, 0)
                    loans = bank_data[cert].get("Gross_Loans", 0)
                    if pd.notna(seg) and pd.notna(loans) and loans > 0:
                        total_segment += seg
                        total_loans += loans
            return total_segment / total_loans if total_loans > 0 else np.nan
        else:
            # Simple average for balances
            vals = []
            for cert in peer_certs:
                if bank_data.get(cert) is not None:
                    v = bank_data[cert].get(col)
                    if pd.notna(v):
                        vals.append(v)
            return np.mean(vals) if vals else np.nan

    rows = []
    for disp, fmt, col, is_pct_of_total in metric_defs:
        if col not in bank_data[subject_bank_cert].index:
            continue

        # Select formatter
        if fmt == 'B':
            f = fmt_billions
        elif fmt == 'M':
            f = fmt_millions
        else:
            f = fmt_pct

        row_dict = {"Metric": disp}

        # Add bank columns
        for cert in bank_certs:
            name = bank_names.get(cert, f"Bank {cert}")
            if bank_data.get(cert) is not None:
                if is_pct_of_total:
                    # Calculate as % of Gross_Loans
                    segment_bal = bank_data[cert].get(col, 0)
                    gross_loans = bank_data[cert].get("Gross_Loans", 1)
                    val = segment_bal / gross_loans if gross_loans > 0 else 0
                else:
                    val = bank_data[cert].get(col)
                row_dict[name] = f(val)
            else:
                row_dict[name] = "N/A"

        # Calculate Core PB average (GS + UBS)
        core_pb_val = calc_peer_avg(col, is_pct_of_total, CORE_PB_CERTS)
        row_dict["Core PB"] = f(core_pb_val)

        # Calculate All Peers average
        all_peers_val = calc_peer_avg(col, is_pct_of_total, ALL_PEER_CERTS)
        row_dict["All Peers"] = f(all_peers_val)

        rows.append(row_dict)

    df = pd.DataFrame(rows)

    # Generate HTML
    html = _generate_excluded_segments_html(df, as_of_date)

    # Save if output_dir provided
    if output_dir:
        date_str = as_of_date.strftime("%Y%m%d")
        with open(output_dir / f"excluded_segments_{date_str}.html", "w") as file:
            file.write(html)

    return html, df


def _generate_excluded_segments_html(df: pd.DataFrame, report_date: datetime) -> str:
    """Generate styled HTML for excluded segments table."""

    date_str = _fmt_call_report_date(report_date)

    # Build table rows
    rows_html = ""
    section_rows = {
        "Total Gross Loans": True,
        "C&I Balance": True,
        "Excluded NCO (TTM)": True,
    }

    for _, row in df.iterrows():
        metric = row["Metric"]

        # Add section separator for key rows
        section_class = "section-start" if metric in section_rows else ""

        # Determine row styling based on metric type
        if "% of" in metric and "Excluded" not in metric:
            row_class = "pct-row"
        elif any(x in metric for x in ["Balance", "Loans", "NCO", "Nonaccrual"]) and "%" not in metric:
            row_class = "balance-row"
        else:
            row_class = ""

        cells = f'<td class="metric-name {section_class}">{metric}</td>'
        for col in df.columns[1:]:  # Skip 'Metric' column
            val = row[col]
            if col == "Core PB":
                cells += f'<td class="core-pb-col">{val}</td>'
            elif col == "All Peers":
                cells += f'<td class="all-peers-col">{val}</td>'
            elif "MSPBNA" in col:
                cells += f'<td class="subject-col">{val}</td>'
            else:
                cells += f'<td>{val}</td>'

        rows_html += f'<tr class="{row_class}">{cells}</tr>\n'

    # Build header
    header_cells = '<th>Segment</th>'
    for col in df.columns[1:]:
        if col == "Core PB":
            header_cells += f'<th class="core-pb-header">{col}</th>'
        elif col == "All Peers":
            header_cells += f'<th class="all-peers-header">{col}</th>'
        else:
            header_cells += f'<th>{col}</th>'

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{
            background-color: transparent;
            padding: 20px;
            max-width: 1400px;
            margin: 0 auto;
        }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 10px; }}
        p.subtitle {{ text-align: center; font-size: 11px; color: #666; margin-bottom: 20px; }}

        table {{ width: 100%; border-collapse: collapse; font-size: 11px; }}
        th {{ background-color: #002F6C; color: white; padding: 10px 8px; border: 1px solid #2c3e50; text-align: center; }}
        td {{ padding: 8px; text-align: center; border: 1px solid #e0e0e0; }}

        .metric-name {{ text-align: left !important; font-weight: 500; color: #2c3e50; min-width: 180px; }}
        .subject-col {{ background-color: rgba(0, 47, 108, 0.08); font-weight: 600; }}

        /* Peer group columns */
        .core-pb-header {{ background-color: #1a5276 !important; }}
        .all-peers-header {{ background-color: #7b7d7d !important; }}
        .core-pb-col {{ background-color: #FFF9E6; font-weight: 600; }}
        .all-peers-col {{ background-color: #F0F0F0; font-weight: 600; }}

        .section-start td {{ border-top: 2px solid #002F6C !important; }}
        .pct-row td {{ font-style: italic; color: #555; font-size: 10px; }}
        .balance-row td {{ font-weight: 500; }}

        .footnote {{
            font-size: 10px; color: #666; margin-top: 20px;
            border-top: 1px solid #ccc; padding-top: 10px; text-align: left;
        }}
        .footnote p {{ margin: 5px 0; }}
        .footnote ul {{ margin-top: 5px; padding-left: 20px; }}
        .footnote li {{ margin-bottom: 3px; }}
    </style></head><body>

    <div class="email-container">
        <h3>Excluded Segments Analysis</h3>
        <p class="date-header">{date_str}</p>
        <p class="subtitle">Loan segments excluded from the Normalized peer comparison view</p>

        <table>
            <thead>
                <tr>{header_cells}</tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>

        <div class="footnote">
            <p><b>Why Normalize?</b> Standard loan metrics include segments that vary dramatically across banks
            (consumer auto, credit cards, C&I lending, construction). Excluding these creates an "apples-to-apples"
            comparison focused on Private Bank/Wealth Management-style lending.</p>

            <p><b>Peer Groups:</b></p>
            <ul>
                <li><b>Core PB:</b> Goldman Sachs + UBS (pure private bank peers)</li>
                <li><b>All Peers:</b> All comparison banks excluding MSPBNA</li>
            </ul>

            <p><b>Excluded Segments:</b></p>
            <ul>
                <li><b>C&I:</b> Commercial & Industrial loans (varies by business model)</li>
                <li><b>Fund Finance (NDFI):</b> Capital call lines to PE/VC funds (specialty product)</li>
                <li><b>ADC:</b> Construction/Development loans (high-risk, varies by region)</li>
                <li><b>Credit Cards:</b> Unsecured consumer revolving (not PB-focused)</li>
                <li><b>Auto:</b> Consumer auto loans (mass market product)</li>
                <li><b>Agricultural:</b> Farm/Ag loans (regional specialty)</li>
                <li><b>Owner-Occupied CRE:</b> Business purpose real estate (C&I-adjacent)</li>
            </ul>

            <p><b>Normalized View Includes:</b> SBL (Securities-Based Lending), Residential Mortgages,
            Investment CRE, and Other Consumer loans typical of Private Bank portfolios.</p>
        </div>
    </div></body></html>
    """

    return html

def generate_detailed_peer_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    normalized: bool = False,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "all_ex_mspbna",
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates Detailed Peer Analysis HTML table with loan-weighted peer averages.
    Shows all banks side-by-side with calculated peer average.
    """

    if bank_certs is None:
        bank_certs = [34221, 32992, 57565, 33124, 7213, 3511, 3510, 628]

    metric_defs = [
        # === BALANCE SHEET ===
        ("Total Loans ($B)", 'B', "Gross_Loans", "Norm_Gross_Loans", False),

        # === CREDIT METRICS (ACL/Coverage) ===
        ("Risk-Adj ACL Ratio (%)", '%', "Risk_Adj_Allowance_Coverage", "Norm_Risk_Adj_Allowance_Coverage", False),
        ("Headline ACL Ratio (%)", '%', "Allowance_to_Gross_Loans_Rate", "Norm_ACL_Coverage", False),

        # === CREDIT METRICS (Asset Quality) ===
        ("Nonaccrual Rate (%)", '%', "Nonaccrual_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate", False),
        ("NCO Rate (TTM) (%)", '%', "TTM_NCO_Rate", "Norm_NCO_Rate", False),  # FIXED
        ("Delinquency Rate (%)", '%', "Total_Delinquency_Rate", "Norm_Delinquency_Rate", False),

        # === COMPOSITION (No color coding) ===
        ("SBL % of Loans", '%', "SBL_Composition", "Norm_SBL_Composition", True),
        ("Resi % of Loans", '%', "RIC_Resi_Loan_Share", "Norm_Wealth_Resi_Composition", True),
        ("CRE % of Loans", '%', "RIC_CRE_Loan_Share", "Norm_CRE_Investment_Composition", True),
        ("Fund Finance %", '%', "Fund_Finance_Composition", "Norm_Fund_Finance_Composition", True),

        # === CRE SEGMENT ===
        ("CRE % of ACL", '%', "RIC_CRE_ACL_Share", "Norm_CRE_ACL_Share", True),
        ("CRE ACL Ratio (%)", '%', "RIC_CRE_ACL_Coverage", "Norm_CRE_ACL_Coverage", False),
        ("CRE NPL Coverage (x)", 'x', "RIC_CRE_Risk_Adj_Coverage", None, False),
        ("% of CRE in Nonaccrual", '%', "RIC_CRE_Nonaccrual_Rate", None, False),
        ("CRE NCO Rate (TTM)", '%', "RIC_CRE_NCO_Rate", None, False),
        ("CRE Delinquency Rate (%)", '%', "RIC_CRE_Delinquency_Rate", None, False),

        # === RESI SEGMENT ===
        ("Resi % of ACL", '%', "RIC_Resi_ACL_Share", "Norm_Resi_ACL_Share", True),
        ("Resi ACL Ratio (%)", '%', "RIC_Resi_ACL_Coverage", "Norm_RESI_ACL_Coverage", False),
        ("Resi NPL Coverage (x)", 'x', "RIC_Resi_Risk_Adj_Coverage", None, False),
        ("% of Resi in Nonaccrual", '%', "RIC_Resi_Nonaccrual_Rate", None, False),
        ("Resi NCO Rate (TTM)", '%', "RIC_Resi_NCO_Rate", None, False),
        ("Resi Delinquency Rate (%)", '%', "RIC_Resi_Delinquency_Rate", None, False),
    ]
    # --- FILTER: Remove Fund Finance from Normalized View ---
    if normalized:
        metric_defs = [m for m in metric_defs if "Fund Finance" not in m[0]]

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    bank_data = {}
    for cert in bank_certs:
        try:
            bank_data[cert] = latest_data[latest_data["CERT"] == cert].iloc[0]
        except IndexError:
            bank_data[cert] = None

    if bank_data.get(subject_bank_cert) is None:
        return None, None

    bank_names = {
        34221: "MSPBNA", 32992: "MSBNA", 57565: "UBS", 33124: "GS Bank",
        7213: "Citibank", 3511: "Wells Fargo", 3510: "BofA", 628: "JPMorgan",
    }

    peer_config = PEER_GROUP_CONFIG.get(peer_type, PEER_GROUP_CONFIG["all_ex_mspbna"])
    peer_name = peer_config["short_name"]
    weight_col = "Norm_Gross_Loans" if normalized else "Gross_Loans"

    SEGMENT_WEIGHT_MAP = {
        "RIC_CRE_": "RIC_CRE_Cost",
        "RIC_Resi_": "RIC_Resi_Cost",
    }

    def get_weight_col_for_metric(metric_col: str) -> str:
        for prefix, segment_weight in SEGMENT_WEIGHT_MAP.items():
            if metric_col.startswith(prefix):
                return segment_weight
        return weight_col

    rows = []
    for disp, fmt, std_col, norm_col, is_composition in metric_defs:
        if normalized and norm_col:
            col = norm_col if norm_col in bank_data[subject_bank_cert].index else std_col
        else:
            col = std_col

        if col is None or col not in bank_data[subject_bank_cert].index:
            continue

        if fmt == 'B':
            f = _fmt_money_billions
        elif fmt == 'x':
            f = _fmt_multiple
        else:
            f = _fmt_percent

        row_dict = {"Metric": disp + ("*" if normalized and norm_col else "")}

        # Add all bank columns
        for cert in bank_certs:
            name = bank_names.get(cert, f"Bank {cert}")
            if bank_data.get(cert) is not None:
                row_dict[name] = f(bank_data[cert].get(col))
            else:
                row_dict[name] = "N/A"

        # Calculate weighted peer average
        metric_weight_col = get_weight_col_for_metric(col)

        if fmt == 'B':
            peer_val_raw = calculate_peer_total(
                bank_data, col, peer_type=peer_type,
                subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )
        else:
            peer_val_raw = calculate_weighted_peer_average(
                bank_data, col, weight_col=metric_weight_col,
                peer_type=peer_type, subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )

        row_dict[peer_name] = f(peer_val_raw)

        # Store raw values for coloring
        row_dict["_mspbna_raw"] = bank_data[subject_bank_cert].get(col)
        row_dict["_peer_avg_raw"] = peer_val_raw

        rows.append(row_dict)

    df = pd.DataFrame(rows)
    html = _generate_detailed_html_table(df, latest_date, normalized, peer_name)

    return html, df


def generate_detailed_peer_tables(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "all_ex_mspbna",  # Add this parameter
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Tuple[str, pd.DataFrame]]:
    """
    Generates both standard and normalized detailed peer tables.
    """
    results = {}

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    date_str = as_of_date.strftime("%Y%m%d")

    # Standard
    html_std, df_std = generate_detailed_peer_table(
        proc_df_with_peers, subject_bank_cert, normalized=False,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['standard'] = (html_std, df_std)

    # Normalized
    html_norm, df_norm = generate_detailed_peer_table(
        proc_df_with_peers, subject_bank_cert, normalized=True,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['normalized'] = (html_norm, df_norm)

    if output_dir:
        if html_std:
            with open(output_dir / f"detailed_peer_analysis_standard_{date_str}.html", "w") as f:
                f.write(html_std)
        if html_norm:
            with open(output_dir / f"detailed_peer_analysis_normalized_{date_str}.html", "w") as f:
                f.write(html_norm)

    return results

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
        90001: "Core Avg",
        90003: "Peer Avg",
        32992: "MSBNA"
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


def _generate_detailed_html_table(
    df: pd.DataFrame,
    report_date: datetime,
    normalized: bool = False,
    composition_metrics: Optional[set] = None,
) -> str:
    """
    Generates HTML for detailed peer table with MSPBNA-only color coding.
    """
    if composition_metrics is None:
        composition_metrics = set()

    date_str = _fmt_call_report_date(report_date)
    title = "Detailed Peer Analysis" + (" (Normalized)" if normalized else "")

    # Get column list (exclude hidden columns)
    visible_cols = [c for c in df.columns if not c.startswith("_")]

    # MS Brand colors
    MS_DEEP_BLUE_BG = "rgba(0, 47, 108, 0.08)"
    MS_LIGHT_BLUE_BG = "rgba(76, 120, 168, 0.08)"

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{
            background-color: transparent;
            padding: 20px;
            max-width: 1600px;
            margin: 0 auto;
            text-align: center;
        }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; margin-bottom: 20px; }}
        table {{ width: 100%; border-collapse: collapse; margin: 0 auto; font-size: 10px; background-color: transparent; }}
        th {{ background-color: #002F6C; color: white; padding: 6px; border: 1px solid #2c3e50; font-size: 9px; }}
        td {{ padding: 5px; text-align: center; border: 1px solid #e0e0e0; background-color: transparent; }}

        .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 140px; background-color: transparent; }}
        .mspbna-value {{ background-color: {MS_DEEP_BLUE_BG} !important; font-weight: bold; color: #002F6C; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .msbna-value {{ background-color: {MS_LIGHT_BLUE_BG} !important; font-weight: 600; color: #444; }}
        .peer-avg-value {{ background-color: #FFF9E6 !important; font-weight: bold; }}

        .mspbna-good {{ background-color: rgba(56, 142, 60, 0.15) !important; color: #2E7D32; font-weight: bold; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .mspbna-bad {{ background-color: rgba(211, 47, 47, 0.15) !important; color: #C62828; font-weight: bold; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .mspbna-neutral {{ background-color: {MS_DEEP_BLUE_BG} !important; font-weight: bold; color: #002F6C; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}

        .footnote {{
            font-size: 10px; color: #666; margin-top: 20px; border-top: 1px solid #ccc; padding-top: 10px; text-align: left;
        }}
    </style></head><body>

    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{date_str}</p>
        <table><thead><tr>"""

    for c in visible_cols:
        html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"

    for _, row in df.iterrows():
        metric = row['Metric']
        is_composition = row.get("_is_composition", False)
        mspbna_raw = row.get("_mspbna_raw", np.nan)
        peer_avg_raw = row.get("_peer_avg_raw", np.nan)

        # Determine if higher is better for this metric
        # Use centralized color config
        direction = get_metric_color_direction(metric)

        html += "<tr>"
        for c in visible_cols:
            val = row[c]
            cls = ""

            if c == "Metric":
                val = f"<b>{val}</b>"
                cls = 'class="metric-name"'
            elif c == "MSPBNA":
                # Use centralized color coding config
                mspbna_raw = row.get("_mspbna_raw", np.nan)
                peer_avg_raw = row.get("_peer_avg_raw", np.nan)
                cls = f'class="{get_color_class(metric, mspbna_raw, peer_avg_raw)}"'
            elif c == "MSBNA":
                cls = 'class="msbna-value"'
            elif "Avg" in c or "Peer" in c:
                cls = 'class="peer-avg-value"'

            html += f"<td {cls}>{val}</td>"
        html += "</tr>"

    # Footnotes
    norm_footnote = ""
    if normalized:
        norm_footnote = """
        <p style="font-style:italic;"><b>* Normalized metrics</b> exclude: C&I, NDFI (Fund Finance), ADC, Credit Cards, Auto, Ag loans.</p>
        """

    html += f"""</tbody></table>

    <div class="footnote">
        {norm_footnote}
        <p><b>Color Coding (MSPBNA only):</b> <span style="color:#2E7D32;font-weight:bold;">Green</span> = Favorable vs Peer Avg | <span style="color:#C62828;font-weight:bold;">Red</span> = Unfavorable vs Peer Avg | Neutral = Within 5bps or Composition metric</p>
        <p><b>Peer Avg:</b> All Peers (US G-SIB Credit Intermediaries + UBS)</p>
    </div></div></body></html>"""

    return html

# ==================================================================================
#  SHARED HTML ENGINE (Styling & Footnotes)
# ==================================================================================

def generate_html_email_table_dynamic(
    df: pd.DataFrame,
    report_date: datetime,
    table_type: str = "summary",
    comparison_col: str = "Core PB",
    custom_title: Optional[str] = None,  # ADD THIS
) -> str:
    """
    Generates styled HTML table with proper color coding.

    Args:
        df: DataFrame with Metric column and bank/peer columns
        report_date: Date for header
        table_type: "summary" or "detailed"
            (e.g., ["Total Assets ($B)", "ROA (%)", "Coverage"]).
            Metrics not in this list are treated as "lower is better" (risk metrics).
        comparison_col: Column name to use as baseline for Diff coloring
    """

    cols = df.columns.tolist()
    date_str = _fmt_call_report_date(report_date)
    title = "Executive Credit Summary" if table_type == "summary" else "Detailed Peer Analysis"
    # At the top of the function where title is set:
    if custom_title:
        title = custom_title
    elif table_type == "summary":
        title = "Executive Credit Summary"
    else:
        title = "Credit Metrics Analysis"

    # MS Brand colors for column backgrounds
    MS_DEEP_BLUE_BG = "rgba(0, 47, 108, 0.08)"      # MSPBNA light fill
    MS_LIGHT_BLUE_BG = "rgba(76, 120, 168, 0.08)"   # MSBNA greyish fill

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
        table {{ width: 100%; border-collapse: collapse; margin: 0 auto; font-size: 11px; background-color: transparent; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; background-color: transparent; }}

        .metric-name {{ text-align: left !important; font-weight: bold; color: #2c3e50; min-width: 180px; background-color: transparent; }}
        .msbna-value {{ background-color: {MS_LIGHT_BLUE_BG} !important; font-weight: 600; color: #444; }}
        .peer-value {{ background-color: transparent; }}

        .bad-trend {{ color: #d32f2f; font-weight: bold; }}
        .good-trend {{ color: #388e3c; font-weight: bold; }}
        .neutral-trend {{ color: #795548; font-weight: normal; }}

        .mspbna-good {{ background-color: rgba(56, 142, 60, 0.15) !important; color: #2E7D32; font-weight: bold; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .mspbna-bad {{ background-color: rgba(211, 47, 47, 0.15) !important; color: #C62828; font-weight: bold; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}
        .mspbna-neutral {{ background-color: rgba(0, 47, 108, 0.08) !important; font-weight: bold; color: #002F6C; border-left: 2px solid #002F6C; border-right: 2px solid #002F6C; }}

        .footnote {{
            font-size: 10px; color: #666; margin-top: 20px; border-top: 1px solid #ccc; padding-top: 10px; text-align: left;
        }}
    </style></head><body>
    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{date_str}</p>
        <table><thead><tr>"""

    for c in cols:
        if not c.startswith("_"):  # Skip hidden columns
            html += f"<th>{c}</th>"
    html += "</tr></thead><tbody>"

    for _, row in df.iterrows():
        metric = row['Metric']

        # Get raw values for color comparison
        mspbna_raw = row.get("_mspbna_raw", np.nan)
        peer_avg_raw = row.get("_peer_avg_raw", np.nan)

        # Determine color class using centralized config
        direction = get_metric_color_direction(metric)

        # Calculate color class for MSPBNA
        if direction == "neutral" or pd.isna(mspbna_raw) or pd.isna(peer_avg_raw):
            mspbna_color_class = "mspbna-neutral"
        else:
            diff = mspbna_raw - peer_avg_raw
            # Check threshold (5 bps = 0.0005 in decimal, or 0.05 in percentage points)
            threshold = 0.0005 if abs(mspbna_raw) < 1 else 0.05
            if abs(diff) < threshold:
                mspbna_color_class = "mspbna-neutral"
            elif direction == "higher":
                mspbna_color_class = "mspbna-good" if diff > 0 else "mspbna-bad"
            else:  # direction == "lower"
                mspbna_color_class = "mspbna-good" if diff < 0 else "mspbna-bad"

        # Map MSPBNA color to Diff color
        diff_color_map = {
            "mspbna-good": "good-trend",
            "mspbna-bad": "bad-trend",
            "mspbna-neutral": "neutral-trend",
        }
        diff_color_class = diff_color_map.get(mspbna_color_class, "neutral-trend")

        html += "<tr>"
        for c in cols:
            # Skip hidden columns
            if c.startswith("_"):
                continue

            val = row[c]
            cls = ""

            if c == "Metric":
                val = f"<b>{val}</b>"
                cls = 'class="metric-name"'
            elif c == "MSPBNA":
                cls = f'class="{mspbna_color_class}"'
            elif c == "MSBNA":
                cls = 'class="msbna-value"'
            elif "Diff" in c:
                cls = f'class="{diff_color_class}"'

            html += f"<td {cls}>{val}</td>"
        html += "</tr>"

    html += """</tbody></table>

    <div class="footnote">
        <p><b>Peer Definitions:</b></p>
        <p><b>1. Core PB:</b> Goldman Sachs Bank USA, UBS Bank USA.</p>
        <p><b>2. All Peers:</b> US G-SIB Credit Intermediaries + UBS.</p>
        <br>
        <p><b>Color Coding:</b> <span style="color:#388e3c;font-weight:bold;">Green</span> = Favorable vs Peers | <span style="color:#d32f2f;font-weight:bold;">Red</span> = Unfavorable vs Peers | <span style="color:#795548;">Brown</span> = Neutral (within 5bps)</p>
        <br>
        <p><b>Methodology:</b></p>
        <p><b>Risk-Adj ACL Ratio:</b> Total ACL / (Gross Loans - SBL). Removes low-risk SBL to show coverage on core credit.</p>
        <p><b>CRE NPL Coverage:</b> CRE-Specific ACL / CRE Nonaccrual Loans. ($ Reserved per $ of Bad Loans).</p>
    </div></div></body></html>"""

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
    Smart conversion of rate/ratio data to decimal form for plotting.

    The challenge: Data sources vary in format:
    - Some provide 0.0035 for 0.35% (already decimal)
    - Some provide 0.35 for 0.35% (percent points, needs /100)
    - Some provide 35 for 35% (percent points, needs /100)

    Strategy:
    1. Coverage/Multiple metrics: Never scale (values like 1.5x, 2.0x)
    2. Rate metrics (NCO, Nonaccrual, Delinquency, ACL): Use metric-aware thresholds
    3. Composition metrics: Use standard heuristic
    """
    s = pd.to_numeric(series, errors="coerce").astype(float)

    med = s.dropna().abs().median()
    if pd.isna(med) or med == 0:
        return s

    # =========================================================================
    # 1. NEVER SCALE: Coverage ratios, multiples, years
    # These are typically 0.5x - 5.0x or 1-20 years
    # =========================================================================
    no_scale_keywords = ["Coverage", "Years", "Ratio", "Index", "Multiple", "Buffer", "Risk_Adj_Allowance"]
    if any(kw in metric_name for kw in no_scale_keywords):
        return s

    # =========================================================================
    # 2. RATE METRICS: NCO, Nonaccrual, Delinquency, ACL rates
    # These are typically 0.01% - 5% in reality
    # =========================================================================
    rate_keywords = ["NCO", "Nonaccrual", "Delinquency", "ACL", "Allowance", "_Rate"]
    is_rate_metric = any(kw in metric_name for kw in rate_keywords)

    if is_rate_metric:
        # Rate metrics are typically 0% - 5% (0.0 - 0.05 in decimal)
        # If median > 0.10, data is likely in percent format (e.g., 0.35 means 0.35%)
        # If median <= 0.10, data is likely already decimal (e.g., 0.0035 means 0.35%)
        if med > 0.10:
            return s / 100.0
        else:
            return s

    # =========================================================================
    # 3. COMPOSITION METRICS: % of loans, % of ACL
    # These range from 1% - 70%
    # =========================================================================
    composition_keywords = ["Composition", "Share", "Loan_Share", "_Pct", "% of"]
    is_composition = any(kw in metric_name for kw in composition_keywords)

    if is_composition:
        # Composition metrics are typically 1% - 70% (0.01 - 0.70 in decimal)
        # If median > 1.0, it's definitely percent points
        if med > 1.0:
            return s / 100.0
        else:
            return s
def generate_cre_segment_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    normalized: bool = False,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates CRE Segment Analysis HTML table with loan-weighted peer averages.
    Shows headline metrics for context plus detailed CRE segment breakdown.
    """

    if bank_certs is None:
        bank_certs = [34221, 32992, 57565, 33124]  # MSPBNA, MSBNA, UBS, GS

    # Metric definitions: (display_name, format, std_col, norm_col, is_composition)
    metric_defs = [
        # === CONTEXT ===
        ("Total Loans ($B)", 'B', "Gross_Loans", "Norm_Gross_Loans", False),
        # === PORTFOLIO COMPOSITION ===
        ("SBL % of Loans", '%', "SBL_Composition", "Norm_SBL_Composition", True),
        ("Resi % of Loans", '%', "RIC_Resi_Loan_Share", "Norm_Wealth_Resi_Composition", True),
        ("CRE % of Loans", '%', "RIC_CRE_Loan_Share", "Norm_CRE_Investment_Composition", True),
        ("Fund Finance %", '%', "Fund_Finance_Composition", "Norm_Fund_Finance_Composition", True),

        # === HEADLINE CREDIT METRICS (for context) ===
        ("Risk-Adj ACL Ratio (%)", '%', "Risk_Adj_Allowance_Coverage", "Norm_Risk_Adj_Allowance_Coverage", False),
        ("Nonaccrual Rate (%)", '%', "Nonaccrual_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate", False),
        ("NCO Rate (TTM) (%)", '%', "TTM_NCO_Rate", "Norm_NCO_Rate", False),
        ("Delinquency Rate (%)", '%', "Total_Delinquency_Rate", "Norm_Delinquency_Rate", False),

        # === CRE SEGMENT DEEP DIVE ===
        ("CRE Balance ($B)", 'B', "CRE_Investment_Pure_Balance", None, False),
        ("CRE % of ACL", '%', "RIC_CRE_ACL_Share", "Norm_CRE_ACL_Share", True),
        ("CRE ACL Ratio (%)", '%', "RIC_CRE_ACL_Coverage", "Norm_CRE_ACL_Coverage", False),
        ("CRE NPL Coverage (x)", 'x', "RIC_CRE_Risk_Adj_Coverage", None, False),
        ("% of CRE in Nonaccrual", '%', "RIC_CRE_Nonaccrual_Rate", None, False),
        ("CRE NCO Rate (TTM)", '%', "RIC_CRE_NCO_Rate", None, False),
        ("CRE Delinquency Rate (%)", '%', "RIC_CRE_Delinquency_Rate", None, False),
    ]
    # --- FILTER: Remove Fund Finance from Normalized View ---
    if normalized:
        metric_defs = [m for m in metric_defs if "Fund Finance" not in m[0]]

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Get data for each bank
    bank_data = {}
    for cert in bank_certs:
        try:
            bank_data[cert] = latest_data[latest_data["CERT"] == cert].iloc[0]
        except IndexError:
            bank_data[cert] = None

    if bank_data.get(subject_bank_cert) is None:
        return None, None

    bank_names = {
        34221: "MSPBNA", 32992: "MSBNA", 57565: "UBS", 33124: "GS",
    }

    # Determine peer average column name and weight column
    peer_config = PEER_GROUP_CONFIG.get(peer_type, PEER_GROUP_CONFIG["core_pb"])
    peer_name = peer_config["short_name"]
    weight_col = "Norm_Gross_Loans" if normalized else "Gross_Loans"

    # For segment-specific metrics, use segment balance as weight
    SEGMENT_WEIGHT_MAP = {
        "RIC_CRE_": "RIC_CRE_Cost",
        "RIC_Resi_": "RIC_Resi_Cost",
    }

    def get_weight_col_for_metric(metric_col: str) -> str:
        """Determine appropriate weight column for a given metric."""
        for prefix, segment_weight in SEGMENT_WEIGHT_MAP.items():
            if metric_col.startswith(prefix):
                return segment_weight
        return weight_col

    rows = []
    for disp, fmt, std_col, norm_col, is_composition in metric_defs:
        # Select column based on normalized flag
        if normalized and norm_col:
            if norm_col in bank_data[subject_bank_cert].index:
                col = norm_col
            else:
                col = std_col
        else:
            col = std_col

        if col is None:
            continue

        if col not in bank_data[subject_bank_cert].index:
            continue

        # Formatters
        if fmt == 'B':
            f = _fmt_money_billions
            fd = _fmt_money_billions_diff
        elif fmt == 'x':
            f = _fmt_multiple
            fd = _fmt_multiple_diff
        else:
            f = _fmt_percent
            fd = _fmt_percent_diff

        row_dict = {"Metric": disp + ("*" if normalized and norm_col else "")}

        # Add bank columns in order
        for cert in bank_certs:
            name = bank_names.get(cert, f"Bank {cert}")
            if bank_data.get(cert) is not None:
                val = bank_data[cert].get(col)
                row_dict[name] = f(val)
            else:
                row_dict[name] = "N/A"

        # Calculate weighted peer average
        metric_weight_col = get_weight_col_for_metric(col)

        if fmt == 'B':
            peer_val_raw = calculate_peer_total(
                bank_data, col, peer_type=peer_type,
                subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )
        else:
            peer_val_raw = calculate_weighted_peer_average(
                bank_data, col, weight_col=metric_weight_col,
                peer_type=peer_type, subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )

        row_dict[peer_name] = f(peer_val_raw)

        # Get subject value for diff calculation
        subj_val_raw = bank_data[subject_bank_cert].get(col)

        # Calculate diff
        diff = subj_val_raw - peer_val_raw if pd.notna(subj_val_raw) and pd.notna(peer_val_raw) else np.nan
        row_dict[f"Diff vs {peer_name}"] = fd(diff)

        # Store raw values for color coding
        row_dict["_mspbna_raw"] = subj_val_raw
        row_dict["_peer_avg_raw"] = peer_val_raw

        rows.append(row_dict)

    df = pd.DataFrame(rows)

    # Generate HTML
    title = "CRE Segment Analysis (Normalized)" if normalized else "CRE Segment Analysis"
    html = generate_html_email_table_dynamic(
        df, latest_date, table_type="summary", comparison_col=peer_name, custom_title=title
    )

    return html, df


def generate_cre_segment_tables(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Tuple[str, pd.DataFrame]]:
    """
    Generates both standard and normalized CRE segment tables.
    """
    results = {}

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    date_str = as_of_date.strftime("%Y%m%d")

    # Standard
    html_std, df_std = generate_cre_segment_table(
        proc_df_with_peers, subject_bank_cert, normalized=False,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['standard'] = (html_std, df_std)

    # Normalized
    html_norm, df_norm = generate_cre_segment_table(
        proc_df_with_peers, subject_bank_cert, normalized=True,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['normalized'] = (html_norm, df_norm)

    if output_dir:
        if html_std:
            with open(output_dir / f"cre_segment_standard_{date_str}.html", "w") as f:
                f.write(html_std)
        if html_norm:
            with open(output_dir / f"cre_segment_normalized_{date_str}.html", "w") as f:
                f.write(html_norm)

    return results
def generate_resi_segment_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    normalized: bool = False,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates RESI Segment Analysis HTML table with loan-weighted peer averages.
    Shows headline metrics for context plus detailed RESI segment breakdown.
    """

    if bank_certs is None:
        bank_certs = [34221, 32992, 57565, 33124]  # MSPBNA, MSBNA, UBS, GS

    # Metric definitions: (display_name, format, std_col, norm_col, is_composition)
    metric_defs = [
        # === CONTEXT ===
        ("Total Loans ($B)", 'B', "Gross_Loans", "Norm_Gross_Loans", False),
        # === PORTFOLIO COMPOSITION ===
        ("SBL % of Loans", '%', "SBL_Composition", "Norm_SBL_Composition", True),
        ("Resi % of Loans", '%', "RIC_Resi_Loan_Share", "Norm_Wealth_Resi_Composition", True),
        ("CRE % of Loans", '%', "RIC_CRE_Loan_Share", "Norm_CRE_Investment_Composition", True),
        ("Fund Finance %", '%', "Fund_Finance_Composition", "Norm_Fund_Finance_Composition", True),

        # === HEADLINE CREDIT METRICS (for context) ===
        ("Risk-Adj ACL Ratio (%)", '%', "Risk_Adj_Allowance_Coverage", "Norm_Risk_Adj_Allowance_Coverage", False),
        ("Nonaccrual Rate (%)", '%', "Nonaccrual_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate", False),
        ("NCO Rate (TTM) (%)", '%', "TTM_NCO_Rate", "Norm_NCO_Rate", False),
        ("Delinquency Rate (%)", '%', "Total_Delinquency_Rate", "Norm_Delinquency_Rate", False),

        # Use 'Wealth_Resi_Balance' for the normalized view (Standard view can fallback to it too)
        # === RESI SEGMENT DEEP DIVE ===
        # 1. Balance: Use Robust Wealth Resi Balance
        ("Resi Balance ($B)", 'B', "Wealth_Resi_Balance", "Wealth_Resi_Balance", False),

        # 2. ACL & Coverage: Keep RIC keys (usually populated via Call Report allocation)
        ("Resi % of ACL", '%', "RIC_Resi_ACL_Share", "Norm_Resi_ACL_Share", True),
        ("Resi ACL Ratio (%)", '%', "RIC_Resi_ACL_Coverage", "Norm_RESI_ACL_Coverage", False),
        ("Resi NPL Coverage (x)", 'x', "RIC_Resi_Risk_Adj_Coverage", None, False),

        # 3. RATES: Switch to WEALTH RESI metrics (Fixes the 0.00% issue)
        # Replaces "RIC_Resi_Nonaccrual_Rate" -> "Wealth_Resi_NA_Rate"
        ("% of Resi in Nonaccrual", '%', "Wealth_Resi_NA_Rate", "Wealth_Resi_NA_Rate", False),

        # Replaces "RIC_Resi_NCO_Rate" -> "Wealth_Resi_TTM_NCO_Rate"
        ("Resi NCO Rate (TTM)", '%', "Wealth_Resi_TTM_NCO_Rate", "Wealth_Resi_TTM_NCO_Rate", False),

        # Replaces "RIC_Resi_Delinquency_Rate" -> "Wealth_Resi_Delinquency_Rate"
        ("Resi Delinquency Rate (%)", '%', "Wealth_Resi_Delinquency_Rate", "Wealth_Resi_Delinquency_Rate", False),
    ]
    # --- FILTER: Remove Fund Finance from Normalized View ---
    if normalized:
        metric_defs = [m for m in metric_defs if "Fund Finance" not in m[0]]

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Get data for each bank
    bank_data = {}
    for cert in bank_certs:
        try:
            bank_data[cert] = latest_data[latest_data["CERT"] == cert].iloc[0]
        except IndexError:
            bank_data[cert] = None

    if bank_data.get(subject_bank_cert) is None:
        return None, None

    bank_names = {
        34221: "MSPBNA", 32992: "MSBNA", 57565: "UBS", 33124: "GS",
    }

    # Determine peer average column name and weight column
    peer_config = PEER_GROUP_CONFIG.get(peer_type, PEER_GROUP_CONFIG["core_pb"])
    peer_name = peer_config["short_name"]
    weight_col = "Norm_Gross_Loans" if normalized else "Gross_Loans"

    # For segment-specific metrics, use segment balance as weight
    SEGMENT_WEIGHT_MAP = {
        "RIC_CRE_": "RIC_CRE_Cost",
        "RIC_Resi_": "RIC_Resi_Cost",
    }

    def get_weight_col_for_metric(metric_col: str) -> str:
        """Determine appropriate weight column for a given metric."""
        for prefix, segment_weight in SEGMENT_WEIGHT_MAP.items():
            if metric_col.startswith(prefix):
                return segment_weight
        return weight_col

    rows = []
    for disp, fmt, std_col, norm_col, is_composition in metric_defs:
        # Select column based on normalized flag
        if normalized and norm_col:
            if norm_col in bank_data[subject_bank_cert].index:
                col = norm_col
            else:
                col = std_col
        else:
            col = std_col

        if col is None:
            continue

        if col not in bank_data[subject_bank_cert].index:
            continue

        # Formatters
        if fmt == 'B':
            f = _fmt_money_billions
            fd = _fmt_money_billions_diff
        elif fmt == 'x':
            f = _fmt_multiple
            fd = _fmt_multiple_diff
        else:
            f = _fmt_percent
            fd = _fmt_percent_diff

        row_dict = {"Metric": disp + ("*" if normalized and norm_col else "")}

        # Add bank columns in order
        for cert in bank_certs:
            name = bank_names.get(cert, f"Bank {cert}")
            if bank_data.get(cert) is not None:
                val = bank_data[cert].get(col)
                row_dict[name] = f(val)
            else:
                row_dict[name] = "N/A"

        # Calculate weighted peer average
        metric_weight_col = get_weight_col_for_metric(col)

        if fmt == 'B':
            peer_val_raw = calculate_peer_total(
                bank_data, col, peer_type=peer_type,
                subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )
        else:
            peer_val_raw = calculate_weighted_peer_average(
                bank_data, col, weight_col=metric_weight_col,
                peer_type=peer_type, subject_cert=subject_bank_cert,
                bank_certs_in_table=bank_certs
            )

        row_dict[peer_name] = f(peer_val_raw)

        # Get subject value for diff calculation
        subj_val_raw = bank_data[subject_bank_cert].get(col)

        # Calculate diff
        diff = subj_val_raw - peer_val_raw if pd.notna(subj_val_raw) and pd.notna(peer_val_raw) else np.nan
        row_dict[f"Diff vs {peer_name}"] = fd(diff)

        # Store raw values for color coding
        row_dict["_mspbna_raw"] = subj_val_raw
        row_dict["_peer_avg_raw"] = peer_val_raw

        rows.append(row_dict)

    df = pd.DataFrame(rows)

    # Generate HTML
    title = "Residential Mortgage Segment Analysis (Normalized)" if normalized else "Residential Mortgage Segment Analysis"
    html = generate_html_email_table_dynamic(
        df, latest_date, table_type="summary", comparison_col=peer_name, custom_title=title
    )

    return html, df


def generate_resi_segment_tables(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Tuple[str, pd.DataFrame]]:
    """
    Generates both standard and normalized RESI segment tables.
    """
    results = {}

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    date_str = as_of_date.strftime("%Y%m%d")

    # Standard
    html_std, df_std = generate_resi_segment_table(
        proc_df_with_peers, subject_bank_cert, normalized=False,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['standard'] = (html_std, df_std)

    # Normalized
    html_norm, df_norm = generate_resi_segment_table(
        proc_df_with_peers, subject_bank_cert, normalized=True,
        bank_certs=bank_certs, peer_type=peer_type
    )
    results['normalized'] = (html_norm, df_norm)

    if output_dir:
        if html_std:
            with open(output_dir / f"resi_segment_standard_{date_str}.html", "w") as f:
                f.write(html_std)
        if html_norm:
            with open(output_dir / f"resi_segment_normalized_{date_str}.html", "w") as f:
                f.write(html_norm)

    return results
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
def generate_normalized_comparison_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates a side-by-side comparison of Standard vs Normalized metrics.
    This is the "apples-to-apples" view that strips out C&I/NDFI/ADC/Consumer.
    """

    # Define metric pairs: (Standard Metric, Normalized Metric) -> (Std Display, Norm Display, Format)
    metric_pairs = {
        ("LNLS", "Norm_Gross_Loans"): ("Gross Loans ($B)", "Loans ($B)*", 'B'),
        ("TTM_NCO_Rate", "Norm_NCO_Rate"): ("TTM NCO Rate (%)", "TTM NCO Rate (%)*", '%'),
        ("Nonaccrual_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate"): ("NA Rate (%)", "NA Rate (%)*", '%'),
        ("Allowance_to_Gross_Loans_Rate", "Norm_ACL_Coverage"): ("ACL Ratio (%)", "ACL Ratio (%)*", '%'),
        ("Risk_Adj_Allowance_Coverage", "Norm_Risk_Adj_Allowance_Coverage"): ("Risk-Adj ACL (%)", "Risk-Adj ACL (%)*", '%'),
        ("RIC_CRE_Loan_Share", "Norm_CRE_Investment_Composition"): ("CRE % of Loans", "CRE % of Loans*", '%'),
        ("RIC_CRE_ACL_Coverage", "Norm_CRE_ACL_Coverage"): ("CRE ACL Ratio (%)", "CRE ACL Ratio (%)*", '%'),
        ("SBL_Composition", "Norm_SBL_Composition"): ("SBL % of Loans", "SBL % of Loans*", '%'),
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
    html = _generate_normalized_comparison_html(df, latest_date)
    return html, df


def _generate_normalized_comparison_html(df: pd.DataFrame, report_date: datetime) -> str:
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
def generate_ratio_components_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    normalized: bool = False,
    bank_certs: Optional[List[int]] = None,
    peer_avg_cert: int = 90001,
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Generates a table showing the numerator and denominator components
    used in key credit ratio calculations.

    Args:
        proc_df_with_peers: Full FDIC data panel
        subject_bank_cert: CERT for subject bank
        normalized: If True, show normalized components; if False, show standard
        bank_certs: Banks to include. Default: [MSPBNA, MSBNA, UBS, GS]
        peer_avg_cert: Peer composite CERT

    Returns:
        Tuple of (HTML string, DataFrame)
    """

    if bank_certs is None:
        bank_certs = [34221, 32992, 57565, 33124]

    # Component definitions: (display_name, numerator_col, denominator_col, ratio_col, format)
    # Standard components
    # Component definitions: (display_name, numerator_col, denominator_col, ratio_col, format)
    # Standard components
    # Component definitions: (display_name, numerator_col, denominator_col, ratio_col, format)
    # Standard components
    standard_components = [
        # === HEADLINE RATIOS ===
        ("NCO Rate (TTM)", "Total_NCO_TTM", "Gross_Loans", "TTM_NCO_Rate", "rate"),
        ("Nonaccrual Rate", "Total_Nonaccrual", "Gross_Loans", "Nonaccrual_to_Gross_Loans_Rate", "rate"),
        ("Headline ACL Ratio", "Total_ACL", "Gross_Loans", "Allowance_to_Gross_Loans_Rate", "rate"),
        ("Risk-Adj ACL Ratio", "Total_ACL", "Gross_Loans - SBL_Balance", "Risk_Adj_Allowance_Coverage", "rate"),
        ("Delinquency Rate (30+)", "TopHouse_PD30 + TopHouse_PD90", "Gross_Loans", "Total_Delinquency_Rate", "rate"),

        # === COMPOSITION ===
        ("SBL % of Loans", "SBL_Balance", "Gross_Loans", "SBL_Composition", "rate"),
        ("Resi % of Loans", "RIC_Resi_Cost", "Gross_Loans", "RIC_Resi_Loan_Share", "rate"),
        ("CRE % of Loans", "CRE_Investment_Pure_Balance", "Gross_Loans", "RIC_CRE_Loan_Share", "rate"),
        ("Fund Finance %", "Fund_Finance_Balance", "Gross_Loans", "Fund_Finance_Composition", "rate"),

        # === CRE SEGMENT ===
        ("CRE % of ACL", "RIC_CRE_ACL", "Total_ACL", "RIC_CRE_ACL_Share", "rate"),
        ("CRE ACL Coverage", "RIC_CRE_ACL", "CRE_Investment_Pure_Balance", "RIC_CRE_ACL_Coverage", "rate"),
        ("CRE NPL Coverage", "RIC_CRE_ACL", "RIC_CRE_Nonaccrual", "RIC_CRE_Risk_Adj_Coverage", "rate"),
        ("CRE Nonaccrual Rate", "RIC_CRE_Nonaccrual", "CRE_Investment_Pure_Balance", "RIC_CRE_Nonaccrual_Rate", "rate"),
        ("CRE NCO Rate", "RIC_CRE_NCO_TTM", "CRE_Investment_Pure_Balance", "RIC_CRE_NCO_Rate", "rate"),
        ("CRE Delinquency Rate", "RIC_CRE_PD30 + RIC_CRE_PD90", "CRE_Investment_Pure_Balance", "RIC_CRE_Delinquency_Rate", "rate"),

        # === RESI SEGMENT ===
        ("Resi % of ACL", "RIC_Resi_ACL", "Total_ACL", "RIC_Resi_ACL_Share", "rate"),
        ("Resi ACL Coverage", "RIC_Resi_ACL", "RIC_Resi_Cost", "RIC_Resi_ACL_Coverage", "rate"),
        ("Resi NPL Coverage", "RIC_Resi_ACL", "RIC_Resi_Nonaccrual", "RIC_Resi_Risk_Adj_Coverage", "rate"),
        ("Resi Nonaccrual Rate", "RIC_Resi_Nonaccrual", "RIC_Resi_Cost", "RIC_Resi_Nonaccrual_Rate", "rate"),
        ("Resi NCO Rate", "RIC_Resi_NCO_TTM", "RIC_Resi_Cost", "RIC_Resi_NCO_Rate", "rate"),
        ("Resi Delinquency Rate", "RIC_Resi_PD30 + RIC_Resi_PD90", "RIC_Resi_Cost", "RIC_Resi_Delinquency_Rate", "rate"),
    ]

    # Normalized components
    normalized_components = [
        # === NORMALIZED HEADLINE RATIOS ===
        ("Norm NCO Rate", "Norm_Total_NCO", "Norm_Gross_Loans", "Norm_NCO_Rate", "rate"),
        ("Norm Nonaccrual Rate", "Norm_Total_Nonaccrual", "Norm_Gross_Loans", "Norm_Nonaccrual_Rate", "rate"),
        ("Norm ACL Ratio", "Norm_ACL_Balance", "Norm_Gross_Loans", "Norm_ACL_Coverage", "rate"),
        ("Norm Risk-Adj ACL", "Norm_ACL_Balance", "Norm_Gross_Loans - SBL_Balance", "Norm_Risk_Adj_Allowance_Coverage", "rate"),
        ("Norm Delinquency Rate", "Norm_PD30 + Norm_PD90", "Norm_Gross_Loans", "Norm_Delinquency_Rate", "rate"),

        # === NORMALIZED COMPOSITION ===
        ("Norm SBL %", "SBL_Balance", "Norm_Gross_Loans", "Norm_SBL_Composition", "rate"),
        ("Norm Resi %", "RIC_Resi_Cost", "Norm_Gross_Loans", "Norm_Wealth_Resi_Composition", "rate"),
        ("Norm CRE % (Ex-ADC)", "CRE_Investment_Pure_Balance", "Norm_Gross_Loans", "Norm_CRE_Investment_Composition", "rate"),
        # === NORMALIZED CRE ===
        ("Norm CRE ACL Coverage", "RIC_CRE_ACL", "Norm_ACL_Balance", "Norm_CRE_ACL_Coverage", "rate"),

        # === NORMALIZED RESI ===
        ("Norm Resi ACL Coverage", "RIC_Resi_ACL", "Norm_ACL_Balance", "Norm_RESI_ACL_Coverage", "rate"),

        # === EXCLUSION BREAKDOWN ===
        ("Excluded % of Total", "Excluded_Balance", "Gross_Loans", "Norm_Exclusion_Pct", "rate"),
    ]

    components = normalized_components if normalized else standard_components

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    # Get data for banks
    all_certs = bank_certs + [peer_avg_cert]
    bank_data = {}
    for cert in all_certs:
        try:
            bank_data[cert] = latest_data[latest_data["CERT"] == cert].iloc[0]
        except IndexError:
            bank_data[cert] = None

    if bank_data.get(subject_bank_cert) is None:
        return None, None

    bank_names = {
        34221: "MSPBNA", 32992: "MSBNA", 57565: "UBS", 33124: "GS",
        90001: "Core PB Avg", 90003: "All Peers Avg",
    }

    def get_value(data_row, col_expr):
        """Safely get value, handling expressions like 'A - B' or 'A + B'."""
        if data_row is None:
            return np.nan

        col_expr = col_expr.strip()

        # Handle subtraction
        if " - " in col_expr:
            parts = col_expr.split(" - ")
            val1 = data_row.get(parts[0].strip(), np.nan)
            val2 = data_row.get(parts[1].strip(), np.nan)
            if pd.notna(val1) and pd.notna(val2):
                return val1 - val2
            return np.nan

        # Handle addition
        if " + " in col_expr:
            parts = col_expr.split(" + ")
            val1 = data_row.get(parts[0].strip(), np.nan)
            val2 = data_row.get(parts[1].strip(), np.nan)
            if pd.notna(val1) and pd.notna(val2):
                return val1 + val2
            return np.nan

        # Simple column
        return data_row.get(col_expr, np.nan)

    def fmt_dollars(v):
        """Format as $XXX.XM"""
        if pd.isna(v):
            return "N/A"
        return f"${v/1_000:,.1f}M" if abs(v) >= 1_000 else f"${v:,.0f}K"

    def fmt_pct(v):
        """Format as X.XX%"""
        if pd.isna(v):
            return "N/A"
        val = v * 100 if abs(v) < 1 else v
        return f"{val:.2f}%"

    rows = []

    for disp, num_col, denom_col, ratio_col, fmt_type in components:
        # MSPBNA values
        mspbna = bank_data.get(subject_bank_cert)
        num_val = get_value(mspbna, num_col)
        denom_val = get_value(mspbna, denom_col)
        ratio_val = mspbna.get(ratio_col, np.nan) if mspbna is not None else np.nan

        # Peer avg values
        peer = bank_data.get(peer_avg_cert)
        peer_num = get_value(peer, num_col)
        peer_denom = get_value(peer, denom_col)
        peer_ratio = peer.get(ratio_col, np.nan) if peer is not None else np.nan

        rows.append({
            "Ratio": disp,
            "Numerator": _clean_metric_name(num_col),
            "MSPBNA Num ($)": fmt_dollars(num_val),
            "Denominator": _clean_metric_name(denom_col),
            "MSPBNA Denom ($)": fmt_dollars(denom_val),
            "MSPBNA Ratio": fmt_pct(ratio_val),
            "Peer Avg Ratio": fmt_pct(peer_ratio),
        })

    df = pd.DataFrame(rows)
    html = _generate_components_html(df, latest_date, normalized)
    return html, df
def generate_ratio_components_tables(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    bank_certs: Optional[List[int]] = None,
    peer_type: str = "core_pb",
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Tuple[str, pd.DataFrame]]:
    """
    Generates both standard and normalized ratio components tables.
    """
    results = {}

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    date_str = as_of_date.strftime("%Y%m%d")

    # Standard
    html_std, df_std = generate_ratio_components_table(
        proc_df_with_peers, subject_bank_cert, normalized=False,
        bank_certs=bank_certs, peer_avg_cert=90001
    )
    results['standard'] = (html_std, df_std)

    # Normalized
    html_norm, df_norm = generate_ratio_components_table(
        proc_df_with_peers, subject_bank_cert, normalized=True,
        bank_certs=bank_certs, peer_avg_cert=90001
    )
    results['normalized'] = (html_norm, df_norm)

    if output_dir:
        if html_std:
            with open(output_dir / f"ratio_components_standard_{date_str}.html", "w") as f:
                f.write(html_std)
        if html_norm:
            with open(output_dir / f"ratio_components_normalized_{date_str}.html", "w") as f:
                f.write(html_norm)

    return results

def _generate_components_html(df: pd.DataFrame, report_date: datetime, normalized: bool) -> str:
    """Generate HTML for components table."""
    date_str = _fmt_call_report_date(report_date)
    title = "Ratio Components Analysis" + (" (Normalized)" if normalized else " (Standard)")

    html = f"""
    <html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .email-container {{
            background-color: transparent;
            padding: 20px;
            max-width: 1600px;
            margin: 0 auto;
        }}
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

        .section-header {{ background-color: #E6F3FF !important; font-weight: bold; text-align: left !important; }}

        .footnote {{
            font-size: 10px; color: #666; margin-top: 20px; border-top: 1px solid #ccc; padding-top: 10px; text-align: left;
        }}
    </style></head><body>

    <div class="email-container">
        <h3>{title}</h3>
        <p class="date-header">{date_str}</p>
        <p style="text-align:center; font-size:11px; color:#666;">Shows the numerator and denominator components used to calculate each ratio</p>

        <table>
            <thead>
                <tr>
                    <th>Ratio</th>
                    <th>Numerator</th>
                    <th>MSPBNA Num ($)</th>
                    <th>Denominator</th>
                    <th>MSPBNA Denom ($)</th>
                    <th>MSPBNA Ratio</th>
                    <th>Peer Avg Ratio</th>
                </tr>
            </thead>
            <tbody>
    """

    for _, row in df.iterrows():
        html += f"""
            <tr>
                <td class="ratio-name">{row['Ratio']}</td>
                <td class="formula-col">{row['Numerator']}</td>
                <td class="value-col">{row['MSPBNA Num ($)']}</td>
                <td class="formula-col">{row['Denominator']}</td>
                <td class="value-col">{row['MSPBNA Denom ($)']}</td>
                <td class="ratio-col">{row['MSPBNA Ratio']}</td>
                <td class="peer-col">{row['Peer Avg Ratio']}</td>
            </tr>
        """

    footnote = """
        <p><b>Standard Metrics:</b> Use total gross loans as denominator.</p>
        <p><b>Risk-Adj ACL:</b> Excludes SBL from denominator (SBL is fully collateralized, requires minimal reserves).</p>
    """ if not normalized else """
        <p><b>Normalized Metrics:</b> Exclude C&I, NDFI (Fund Finance), ADC (Construction), Credit Cards, Auto, Ag loans.</p>
        <p><b>Norm_Gross_Loans:</b> Gross Loans minus Excluded Balance.</p>
        <p><b>Norm_ACL_Balance:</b> Total ACL minus reserves for ADC, Credit Cards, and Other Consumer.</p>
        <p><b>CRE Investment Pure:</b> CRE balance excluding ADC (Construction) loans.</p>
    """

    html += f"""
            </tbody>
        </table>

        <div class="footnote">
            <p><b>Calculation Methodology:</b></p>
            {footnote}
        </div>
    </div></body></html>
    """

    return html


# ---------- MAIN: drop-in replacement ----------
def generate_reports(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221,
    output_dir: Optional[Path] = None,
    as_of_date: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Master function to generate all reports.
    """
    if output_dir is None:
        output_dir = Path("output")

    peers_dir = output_dir / "peers"
    peers_dir.mkdir(parents=True, exist_ok=True)

    if as_of_date is None:
        as_of_date = proc_df_with_peers["REPDTE"].max()

    date_str = as_of_date.strftime("%Y%m%d")
    base = "MSPBNA"

    results = {}

    # =========================================================================
    # 1. Executive Summary Tables (Core PB weighted average)
    # =========================================================================
    print("\n--- Generating Executive Summary Tables ---")
    exec_tables = generate_executive_summary_tables(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        bank_certs=[34221, 32992, 57565, 33124],  # MSPBNA, MSBNA, UBS, GS
        peer_type="core_pb",  # Weighted avg of GS + UBS
        output_dir=peers_dir,
        as_of_date=as_of_date,
    )
    html_exec_std, df_exec_std = exec_tables['standard']
    html_exec_norm, df_exec_norm = exec_tables['normalized']

    if html_exec_std:
        print(f"✓ Executive Summary (Standard) saved: executive_summary_standard_{date_str}.html")
    if html_exec_norm:
        print(f"✓ Executive Summary (Normalized) saved: executive_summary_normalized_{date_str}.html")

    results['exec_summary'] = exec_tables
    # =========================================================================
    # X. Excluded Segments Table
    # =========================================================================
    print("\n--- Generating Excluded Segments Table ---")
    excl_html, excl_df = generate_excluded_segments_table(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        bank_certs=[34221, 32992, 57565, 33124, 7213, 3511, 3510, 628],  # Include all peers
        output_dir=peers_dir,
        as_of_date=as_of_date,
    )

    if excl_html:
        print(f"✓ Excluded Segments saved: excluded_segments_{date_str}.html")

    results['excluded_segments'] = (excl_html, excl_df)

    # =========================================================================
    # 2. Detailed Peer Analysis Tables (All Peers weighted average)
    # =========================================================================
    print("\n--- Generating Detailed Peer Analysis Tables ---")
    detailed_tables = generate_detailed_peer_tables(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        bank_certs=[34221, 32992, 57565, 33124, 7213, 3511, 3510, 628],
        peer_type="all_ex_mspbna",  # Weighted avg of all peers
        output_dir=peers_dir,
        as_of_date=as_of_date,
    )
    html_det_std, df_det_std = detailed_tables['standard']
    html_det_norm, df_det_norm = detailed_tables['normalized']

    if html_det_std:
        print(f"✓ Detailed Peer Analysis (Standard) saved: detailed_peer_analysis_standard_{date_str}.html")
    if html_det_norm:
        print(f"✓ Detailed Peer Analysis (Normalized) saved: detailed_peer_analysis_normalized_{date_str}.html")

    results['detailed_peer'] = detailed_tables
    # === NEW CODE START: Add the Comparison Table ===
    print("\n--- Generating Normalized Comparison Table ---")
    html_comp, df_comp = generate_normalized_comparison_table(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert
    )

    if html_comp:
        comp_path = peers_dir / f"comparison_std_vs_norm_{date_str}.html"
        with open(comp_path, "w") as f:
            f.write(html_comp)
        print(f"✓ Comparison Table (Std vs Norm) saved: comparison_std_vs_norm_{date_str}.html")

    # =========================================================================
    # 3. Ratio Components Tables
    # =========================================================================
    print("\n--- Generating Ratio Components Tables ---")
    comp_tables = generate_ratio_components_tables(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        bank_certs=[34221, 32992, 57565, 33124],
        peer_type="core_pb",
        output_dir=peers_dir,
        as_of_date=as_of_date,
    )
    html_comp_std, df_comp_std = comp_tables['standard']
    html_comp_norm, df_comp_norm = comp_tables['normalized']

    if html_comp_std:
        print(f"✓ Ratio Components (Standard) saved: ratio_components_standard_{date_str}.html")
    if html_comp_norm:
        print(f"✓ Ratio Components (Normalized) saved: ratio_components_normalized_{date_str}.html")

    results['ratio_components'] = comp_tables
    # =========================================================================
    # 4. CRE Segment Tables
    # =========================================================================
    print("\n--- Generating CRE Segment Tables ---")
    cre_tables = generate_cre_segment_tables(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        bank_certs=[34221, 32992, 57565, 33124],
        peer_type="core_pb",
        output_dir=peers_dir,
        as_of_date=as_of_date,
    )
    html_cre_std, df_cre_std = cre_tables['standard']
    html_cre_norm, df_cre_norm = cre_tables['normalized']

    if html_cre_std:
        print(f"✓ CRE Segment (Standard) saved: cre_segment_standard_{date_str}.html")
    if html_cre_norm:
        print(f"✓ CRE Segment (Normalized) saved: cre_segment_normalized_{date_str}.html")

    results['cre_segment'] = cre_tables

    # =========================================================================
    # 5. RESI Segment Tables
    # =========================================================================
    print("\n--- Generating RESI Segment Tables ---")
    resi_tables = generate_resi_segment_tables(
        proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        bank_certs=[34221, 32992, 57565, 33124],
        peer_type="core_pb",
        output_dir=peers_dir,
        as_of_date=as_of_date,
    )
    html_resi_std, df_resi_std = resi_tables['standard']
    html_resi_norm, df_resi_norm = resi_tables['normalized']

    if html_resi_std:
        print(f"✓ RESI Segment (Standard) saved: resi_segment_standard_{date_str}.html")
    if html_resi_norm:
        print(f"✓ RESI Segment (Normalized) saved: resi_segment_normalized_{date_str}.html")

    results['resi_segment'] = resi_tables

    # =========================================================================
    # 4. Scatter Plots (use weighted peer averages)
    # =========================================================================
    print("\n--- Generating Scatter Plots ---")

    # Create 8Q rolling average for scatter plots
    rolling8q_df = proc_df_with_peers.copy()  # Or apply rolling logic

    # Scatter 1: NCO vs Nonaccrual (Both modes)
    s1_path = peers_dir / f"NCO_vs_Nonaccrual_{date_str}.png"
    scatter_1 = plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="TTM_NCO_Rate",
        y_col="Nonaccrual_to_Gross_Loans_Rate",
        norm_x_col="Norm_NCO_Rate",
        norm_y_col="Norm_Nonaccrual_Rate",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        show_peers_avg_label=True,
        show_idb_label=True,
        show_sibling_label=True,
        identify_outliers=True,
        outliers_topn=2,
        figsize=(9, 9),
        title_size=16,
        economist_style=True,
        square_axes=False,
        mode="both",
        save_path=str(s1_path)
    )
    print(f"✓ Scatter: NCO vs Nonaccrual saved")

    # Scatter 2: CRE Risk-Adj Coverage vs NA
    s2_path = peers_dir / f"CRE_RiskAdjCov_vs_NA_{date_str}.png"
    plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="RIC_CRE_Nonaccrual_Rate",
        y_col="RIC_CRE_Risk_Adj_Coverage",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
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
        mode="standard",
        save_path=str(s2_path)
    )
    print(f"✓ Scatter: CRE Coverage vs NA saved")

    # Scatter 3: NCO vs ACL (Both modes)
    s3_path = peers_dir / f"NCO_vs_ACL_{date_str}.png"
    plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="TTM_NCO_Rate",
        y_col="Allowance_to_Gross_Loans_Rate",
        norm_x_col="Norm_NCO_Rate",
        norm_y_col="Norm_ACL_Coverage",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        show_peers_avg_label=False,
        show_idb_label=True,
        show_sibling_label=True,
        identify_outliers=True,
        outliers_topn=2,
        figsize=(9, 9),
        title_size=16,
        economist_style=True,
        square_axes=False,
        mode="both",
        save_path=str(s3_path)
    )
    print(f"✓ Scatter: NCO vs ACL saved")
    # =========================================================================
    # 8. SEGMENT-SPECIFIC SCATTER PLOTS (CRE & RESI)
    # =========================================================================
    print("\n--- Generating Segment Risk Scatter Plots ---")

    # --- A. CRE RISK POSITIONING ---
    # X: How bad is the portfolio? (Nonaccrual Rate)
    # Y: How well reserved are we? (ACL Coverage)
    cre_risk_path = peers_dir / f"Scatter_CRE_Risk_Pos_{date_str}.png"
    plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="RIC_CRE_Nonaccrual_Rate",
        y_col="RIC_CRE_ACL_Coverage",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        show_peers_avg_label=False,
        show_idb_label=True,
        show_sibling_label=True,
        identify_outliers=True,
        outliers_topn=2,
        figsize=(9, 9),
        title_size=16,
        economist_style=True,
        square_axes=False,
        mode="both",
        save_path=str(cre_risk_path)
    )

    # --- B. RESI RISK POSITIONING ---
    # X: Resi Nonaccrual Rate
    # Y: Resi ACL Coverage
    resi_risk_path = peers_dir / f"Scatter_Resi_Risk_Pos_{date_str}.png"
    plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="RIC_Resi_Nonaccrual_Rate",
        y_col="RIC_Resi_ACL_Coverage",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        exclude_certs=[32992, 329922],
        show_peers_avg_label=False,
        show_idb_label=True,
        show_sibling_label=False,
        identify_outliers=True,
        outliers_topn=2,
        figsize=(9, 9),
        title_size=16,
        economist_style=True,
        square_axes=False,
        mode="both",
        save_path=str(resi_risk_path)
    )

    # --- C. CRE PERFORMANCE (Losses vs NPLs) ---
    # X: Nonaccrual Rate (Current Stress)
    # Y: NCO Rate (Realized Losses)
    # *Shows if NPLs are actually turning into losses*
    cre_perf_path = peers_dir / f"Scatter_CRE_Performance_{date_str}.png"
    plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="RIC_CRE_Nonaccrual_Rate",
        y_col="RIC_CRE_NCO_Rate",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        exclude_certs=[32992, 329922],
        show_peers_avg_label=False,
        show_idb_label=True,
        show_sibling_label=False,
        identify_outliers=True,
        outliers_topn=2,
        figsize=(9, 9),
        title_size=16,
        economist_style=True,
        square_axes=False,
        mode="both",
        save_path=str(cre_perf_path)
    )

    # --- D. RESI PERFORMANCE (Losses vs NPLs) ---
    resi_perf_path = peers_dir / f"Scatter_Resi_Performance_{date_str}.png"
    plot_scatter_dynamic(
        df=rolling8q_df,
        x_col="RIC_Resi_Nonaccrual_Rate",
        y_col="RIC_Resi_NCO_Rate",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        show_peers_avg_label=False,
        show_idb_label=True,
        show_sibling_label=True,
        identify_outliers=True,
        outliers_topn=2,
        figsize=(9, 9),
        title_size=16,
        economist_style=True,
        square_axes=False,
        mode="both",
        save_path=str(resi_perf_path)
    )

    print("✓ Segment scatter plots generated.")

    # =========================================================================
    # 5. Dual-Axis Time Series Charts
    # =========================================================================
    print("\n--- Generating Dual-Axis Charts ---")

    # Chart 1: CRE Credit Quality vs Reserve Levels
    cre_qual_path = peers_dir / f"{base}_CRE_Credit_Qual_Chart_{date_str}.png"
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
        mode="both",
        save_path=str(cre_qual_path)
    )
    print(f"✓ Chart: CRE Credit Quality saved")

    # Chart 2: CRE Reserve Allocation vs NCOs
    cre_nco_path = peers_dir / f"{base}_CRE_ACL_NCO_timeseries_{date_str}.png"
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
        mode="both",
        save_path=str(cre_nco_path)
    )
    print(f"✓ Chart: CRE Allocation saved")

    # Chart 3: CRE Exposure & Reserves
    cre_exp_path = peers_dir / f"{base}_CRE_ACL_VS_Comp_{date_str}.png"
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
        mode="both",
        save_path=str(cre_exp_path)
    )
    print(f"✓ Chart: CRE Exposure saved")

    # Chart 4: CRE Problem Credits
    cre_prob_path = peers_dir / f"{base}_CRE_Problem_Credits_{date_str}.png"
    create_dual_axis_chart(
        proc_df_with_peers,
        bar_entities=["MSPBNA", "COREPB"],
        line_entities=["MSPBNA", "COREPB"],
        bar_metric="RIC_CRE_Nonaccrual_Rate",
        line_metric="RIC_CRE_Delinquency_Rate",
        title="CRE Portfolio Profile: Problem Credits",
        subtitle="Nonaccrual (Bars) vs. Delinquency Rate (Lines)",
        bar_axis_label="CRE Nonaccrual Rate (%)",
        line_axis_label="CRE Delinquency (%)",
        mode="both",
        save_path=str(cre_prob_path)
    )
    print(f"✓ Chart: CRE Problem Credits saved")

    # Chart 5: RESI Credit Quality
    resi_qual_path = peers_dir / f"{base}_RESI_Credit_Qual_Chart_{date_str}.png"
    create_dual_axis_chart(
        proc_df_with_peers,
        bar_entities=["MSPBNA", "COREPB"],
        line_entities=["MSPBNA", "COREPB"],
        bar_metric="RIC_Resi_ACL_Coverage",
        line_metric="RIC_Resi_Nonaccrual_Rate",
        title="Residential Mortgage: Reserve Adequacy",
        subtitle="ACL Coverage % (Bars) vs Nonaccrual Rate % (Lines)",
        bar_axis_label="Resi ACL Coverage (%)",
        line_axis_label="Resi Nonaccrual Rate (%)",
        mode="both",
        save_path=str(resi_qual_path)
    )
    print(f"✓ Chart: RESI Credit Quality saved")

    # Chart 6: RESI Problem Loan Flow
    resi_flow_path = peers_dir / f"{base}_RESI_Delinq_vs_NCO_{date_str}.png"
    create_dual_axis_chart(
        proc_df_with_peers,
        bar_entities=["MSPBNA", "COREPB"],
        line_entities=["MSPBNA", "COREPB"],
        bar_metric="RIC_Resi_ACL_Coverage",
        line_metric="RIC_Resi_Delinquency_Rate",
        title="Residential Mortgage: Problem Loan Flow",
        subtitle="ACL Coverage % (Bars) vs Delinquency Rate (Lines)",
        bar_axis_label="Resi ACL Coverage (%)",
        line_axis_label="Resi Delinquency Rate (%)",
        mode="both",
        save_path=str(resi_flow_path)
    )
    print(f"✓ Chart: RESI Problem Loan Flow saved")
    # =========================================================================
    # 9. STRATEGIC DUAL-AXIS CHARTS
    # =========================================================================
    print("\n--- Generating Strategic Dual-Axis Charts ---")

    # --- CHART A: CRE Distress Pipeline ---
    # Bars: Delinquency (Early Warning)
    # Line: Nonaccrual (Realized Problem)
    cre_pipeline_path = peers_dir / f"{base}_CRE_Distress_Pipeline_{date_str}.png"

    create_dual_axis_chart(
        proc_df_with_peers,
        bar_entities=["MSPBNA", "COREPB"],
        line_entities=["MSPBNA", "COREPB"],
        bar_metric="RIC_CRE_Delinquency_Rate",
        line_metric="RIC_CRE_Nonaccrual_Rate",
        title="CRE Stress: The Pipeline",
        subtitle="Delinquency Rate (Bars) vs Nonaccrual Rate (Lines)",
        bar_axis_label="CRE Delinquency (%)",
        line_axis_label="CRE Nonaccrual (%)",
        save_path=str(cre_pipeline_path)
    )
    print(f"✓ Chart saved: CRE Distress Pipeline @ {cre_pipeline_path}")

    # --- CHART B: Risk-Adjusted Profitability ---
    # Bars: Net Interest Margin (Income)
    # Line: Net Charge-off Rate (Expense/Loss)
    profit_risk_path = peers_dir / f"{base}_Profit_vs_Risk_{date_str}.png"

    create_dual_axis_chart(
        proc_df_with_peers,
        bar_entities=["MSPBNA", "COREPB"],
        line_entities=["MSPBNA", "COREPB"],
        bar_metric="NIMY",            # Net Interest Margin
        line_metric="TTM_NCO_Rate",   # Net Charge-offs
        title="Profitability vs. Credit Risk",
        subtitle="Net Interest Margin (Bars) vs NCO Rate (Lines)",
        bar_axis_label="Net Interest Margin (%)",
        line_axis_label="NCO Rate (%)",
        save_path=str(profit_risk_path)
    )
    print(f"✓ Chart saved: Profit vs Risk @ {profit_risk_path}")

    print("\n" + "="*60)
    print("REPORT GENERATION COMPLETE")
    print(f"Output directory: {output_dir}")
    print("="*60)
    # =========================================================================
    # 7. NEW VISUALIZATIONS (Dynamic Weighted Averages)
    # =========================================================================
    print("\n--- Generating New Visualizations (Standard & Normalized) ---")

    # A. Peer Percentile Bands
    # Automatically calculates "Weighted Peer Avg" from the raw peer data
    pctl_path = peers_dir / f"Percentile_NCO_{date_str}.png"
    plot_peer_percentile_bands(
        df=proc_df_with_peers,
        metric="TTM_NCO_Rate",
        subject_cert=subject_bank_cert,
        sibling_cert=32992,
        mode="both",  # Creates Standard & Normalized
        save_path=str(pctl_path)
    )

    # B. Loan Mix Stacked Chart
    # Aggregates "COREPB" dynamically from GS+UBS data
    mix_path = peers_dir / f"Loan_Mix_{date_str}.png"
    plot_loan_mix_stacked_time(
        df=proc_df_with_peers,
        entities=["MSPBNA", "MSBNA", "COREPB"],
        mode="percent",
        chart_mode="both",
        save_path=str(mix_path)
    )

    print("✓ New visualizations generated.")

    return results



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
        Smart conversion of rate/ratio data to decimal form for plotting.

        The challenge: Data sources vary in format:
        - Some provide 0.0035 for 0.35% (already decimal)
        - Some provide 0.35 for 0.35% (percent points, needs /100)
        - Some provide 35 for 35% (percent points, needs /100)

        Strategy:
        1. Coverage/Multiple metrics: Never scale (values like 1.5x, 2.0x)
        2. Rate metrics (NCO, Nonaccrual, Delinquency, ACL): Use metric-aware thresholds
        3. Composition metrics: Use standard heuristic
        """
        s = pd.to_numeric(series, errors="coerce").astype(float)

        med = s.dropna().abs().median()
        if pd.isna(med) or med == 0:
            return s

        # =========================================================================
        # 1. NEVER SCALE: Coverage ratios, multiples, years
        # These are typically 0.5x - 5.0x or 1-20 years
        # =========================================================================
        no_scale_keywords = ["Coverage", "Years", "Ratio", "Index", "Multiple", "Buffer", "Risk_Adj_Allowance"]
        if any(kw in metric_name for kw in no_scale_keywords):
            return s

        # =========================================================================
        # 2. RATE METRICS: NCO, Nonaccrual, Delinquency, ACL rates
        # These are typically 0.01% - 5% in reality
        # =========================================================================
        rate_keywords = ["NCO", "Nonaccrual", "Delinquency", "ACL", "Allowance", "_Rate"]
        is_rate_metric = any(kw in metric_name for kw in rate_keywords)

        if is_rate_metric:
            # Rate metrics are typically 0% - 5% (0.0 - 0.05 in decimal)
            # If median > 0.10, data is likely in percent format (e.g., 0.35 means 0.35%)
            # If median <= 0.10, data is likely already decimal (e.g., 0.0035 means 0.35%)
            if med > 0.10:
                return s / 100.0
            else:
                return s

        # =========================================================================
        # 3. COMPOSITION METRICS: % of loans, % of ACL
        # These range from 1% - 70%
        # =========================================================================
        composition_keywords = ["Composition", "Share", "Loan_Share", "_Pct", "% of"]
        is_composition = any(kw in metric_name for kw in composition_keywords)

        if is_composition:
            # Composition metrics are typically 1% - 70% (0.01 - 0.70 in decimal)
            # If median > 1.0, it's definitely percent points
            if med > 1.0:
                return s / 100.0
            else:
                return s

        # =========================================================================
        # 3. COMPOSITION METRICS: % of loans, % of ACL
        # These range from 1% - 70%
        # =========================================================================
        composition_keywords = ["Composition", "Share", "Loan_Share", "_Pct", "% of"]
        is_composition = any(kw in metric_name for kw in composition_keywords)

        if is_composition:
            # Composition metrics are typically 1% - 70% (0.01 - 0.70 in decimal)
            # If median > 1.0, it's definitely percent points
            if med > 1.0:
                return s / 100.0
            else:
                return s

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
    df: pd.DataFrame,                  # <--- FIXED (Matches variable used in body)
    bar_entities: Optional[List[str]] = None,
    line_entities: Optional[List[str]] = None,
    # Metric selection
    bar_metric: str = "TTM_NCO_Rate",
    line_metric: str = "Nonaccrual_to_Gross_Loans_Rate",
    # Normalized metric columns (auto-detected if None)
    norm_bar_metric: Optional[str] = None,
    norm_line_metric: Optional[str] = None,
    # Mode: "standard", "normalized", or "both"
    mode: str = "standard",
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
) -> Union[Tuple[Optional[plt.Figure], Optional[plt.Axes]], Dict[str, Tuple[plt.Figure, plt.Axes]]]:
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
        # Peer composites
        "COREPB": 90001,
        "CORE_PB": 90001,
        "CORE": 90001,
        "MSWEALTH": 90002,
        "MS_WEALTH": 90002,
        "ALLPEERS": 90003,
        "ALL_PEERS": 90003,
        "ALL": 90003,
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
        90001: "#8B8B8B",  # Core PB - Neutral Gray
        90002: "#A0A0A0",  # MS+Wealth - Light Gray
        90003: "#6B6B6B",  # All Peers - Dark Gray
        33124: "#1f77b4",  # GS - Blue
        57565: "#d62728",  # UBS - Red
        628: "#9467bd",    # JPM - Purple
        3510: "#8c564b",   # BofA - Brown
        3511: "#e377c2",   # WFC - Pink
        7213: "#17becf",   # Citi - Cyan
        17281: "#bcbd22",  # CNB - Olive
    }
    # =========================================================================
    # NORMALIZED METRIC MAPPING
    # =========================================================================
    NORM_METRIC_MAP = {
        # --- 1. Top-Level Credit Quality ---
        "TTM_NCO_Rate": "Norm_NCO_Rate",
        "Nonaccrual_to_Gross_Loans_Rate": "Norm_Nonaccrual_Rate",
        "Total_Delinquency_Rate": "Norm_Delinquency_Rate",
        "Top_House_Delinquency_Rate": "Norm_Delinquency_Rate",
        "Allowance_to_Gross_Loans_Rate": "Norm_ACL_Coverage",
        "Risk_Adj_Allowance_Coverage": "Norm_Risk_Adj_Allowance_Coverage",

        # --- 2. Balances (Level) ---
        "Gross_Loans": "Norm_Gross_Loans",
        "Total_ACL": "Norm_ACL_Balance",
        "Total_Nonaccrual": "Norm_Total_Nonaccrual",
        "NTLNLS": "Norm_Total_NCO",
        "Total_NCO_TTM": "Norm_Total_NCO",
        "TopHouse_PD30": "Norm_PD30",
        "TopHouse_PD90": "Norm_PD90",

        # --- 2a. Normalization Transparency ---
        "Exclusion_Pct": "Norm_Exclusion_Pct",

        # --- 3. Portfolio Composition ---
        "SBL_Composition": "Norm_SBL_Composition",
        "Fund_Finance_Composition": "Norm_Fund_Finance_Composition",
        "Wealth_Resi_Composition": "Norm_Wealth_Resi_Composition",
        "RIC_Resi_Loan_Share": "Norm_Wealth_Resi_Composition",  # Alias
        "CRE_Investment_Composition": "Norm_CRE_Investment_Composition",
        "RIC_CRE_Loan_Share": "Norm_CRE_Investment_Composition",  # Alias
        "CRE_OO_Composition": "Norm_CRE_OO_Composition",
        "ADC_Composition": "Norm_ADC_Composition",

        # --- 4. Segment-Specific ACL Coverage ---
        "RIC_CRE_ACL_Coverage": "Norm_CRE_ACL_Coverage",
        "RIC_Resi_ACL_Coverage": "Norm_RESI_ACL_Coverage",
        "RIC_Comm_ACL_Coverage": "Norm_Comm_ACL_Coverage",

        # --- 5. Profitability & Returns ---
        "NIMY": "Norm_Loan_Yield",
        "Loan_Yield_Proxy": "Norm_Loan_Yield",
        "Provision_to_Loans_Rate": "Norm_Provision_Rate",
        "Loss_Adj_Yield": "Norm_Loss_Adj_Yield",
        "Risk_Adj_Return": "Norm_Risk_Adj_Return",

        # --- 6. Risk Share Metrics (PD Contributions) ---
        # ADC
        "ADC_Share_of_Total_PD": "Norm_ADC_Share_of_Total_PD",
        "ADC_Share_of_Total_PD30": "Norm_ADC_Share_of_Total_PD30",
        "ADC_Share_of_Total_PD90": "Norm_ADC_Share_of_Total_PD90",
        # C&I
        "CI_Share_of_Total_PD": "Norm_CI_Share_of_Total_PD",
        "CI_Share_of_Total_PD30": "Norm_CI_Share_of_Total_PD30",
        "CI_Share_of_Total_PD90": "Norm_CI_Share_of_Total_PD90",
        # CRE
        "CRE_Share_of_Total_PD": "Norm_CRE_Share_of_Total_PD",
        "CRE_Share_of_Total_PD30": "Norm_CRE_Share_of_Total_PD30",
        "CRE_Share_of_Total_PD90": "Norm_CRE_Share_of_Total_PD90",
        # Resi
        "Resi_Share_of_Total_PD": "Norm_Resi_Share_of_Total_PD",
        "Resi_Share_of_Total_PD30": "Norm_Resi_Share_of_Total_PD30",
        "Resi_Share_of_Total_PD90": "Norm_Resi_Share_of_Total_PD90",
    }

    def get_norm_metric(metric: str) -> str:
        """Get normalized metric column name."""
        return NORM_METRIC_MAP.get(metric, f"Norm_{metric}")

    # =========================================================================
    # INNER FUNCTION FOR SINGLE CHART CREATION
    # =========================================================================
    def _create_single_chart(
        bar_m: str,
        line_m: str,
        is_normalized: bool = False,
        base_save_path: Optional[str] = None,
    ) -> Tuple[Optional[plt.Figure], Optional[plt.Axes]]:
        """Creates a single dual-axis chart."""
        # =========================================================================
        # WEIGHTED PEER AVERAGE CALCULATION
        # =========================================================================
        def calc_weighted_peer_series(
            metric: str,
            weight_metric: str = "Gross_Loans",
            synthetic_cert: int = 90001,
        ) -> pd.Series:
            """
            Calculate loan-weighted peer average time series.
            Returns a Series aligned to the timeline.

            Args:
                metric: Column name for the metric
                weight_metric: Column name for weighting
                synthetic_cert: The synthetic CERT being calculated for
            """
            # Get the real bank CERTs for this synthetic CERT
            if synthetic_cert in peer_certs_for_synthetic:
                peer_certs_for_avg = peer_certs_for_synthetic[synthetic_cert]
            else:
                # Fallback: use all real CERTs except subject
                peer_certs_for_avg = [c for c in expanded_certs if c < 90000 and c != subject_cert]

            if not peer_certs_for_avg:
                return pd.Series([np.nan] * len(timeline), index=timeline.index)

            result = []
            for _, row in timeline.iterrows():
                dt = row["REPDTE"]
                period_data = df[df["REPDTE"] == dt]

                weighted_sum = 0.0
                total_weight = 0.0

                for cert in peer_certs_for_avg:
                    cert_row = period_data[period_data["CERT"] == cert]
                    if cert_row.empty:
                        continue

                    val = cert_row[metric].iloc[0] if metric in cert_row.columns else np.nan
                    weight = cert_row[weight_metric].iloc[0] if weight_metric in cert_row.columns else np.nan

                    if pd.notna(val) and pd.notna(weight) and weight > 0:
                        weighted_sum += val * weight
                        total_weight += weight

                avg = weighted_sum / total_weight if total_weight > 0 else np.nan
                result.append(avg)

            return to_decimal(pd.Series(result), metric_name=metric)

        def get_weight_col(metric: str) -> str:
            """Determine appropriate weight column for a metric."""
            if metric.startswith("RIC_CRE_"):
                # Use robust RC-C derived balance (fallback for RIC_CRE_Cost)
                return "CRE_Investment_Pure_Balance"
            elif metric.startswith("RIC_Resi_"):
                # FIXED: Use robust RC-C derived balance (prevents 0 denominator)
                return "Wealth_Resi_Balance"
            elif metric.startswith("Norm_"):
                return "Norm_Gross_Loans"
            else:
                return "Gross_Loans"

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
            try:
                return int(name_or_cert)
            except ValueError:
                raise ValueError(f"Unknown entity: {name_or_cert}. Valid options: {list(ENTITY_MAP.keys())}")

        # Default entities
        bar_ents = bar_entities if bar_entities is not None else ["MSPBNA", "MSBNA", "COREPB", "ALLPEERS"]
        line_ents = line_entities if line_entities is not None else list(bar_ents)

        # Convert to CERTs
        bar_certs = [resolve_entity(e) for e in bar_ents]
        line_certs = [resolve_entity(e) for e in line_ents]
        all_certs = list(set(bar_certs + line_certs))

        # Subject bank (first in bar_entities)
        subject_cert = bar_certs[0]

        # =========================================================================
        # MAP SYNTHETIC CERTS TO ACTUAL BANK CERTS
        # =========================================================================
        SYNTHETIC_TO_REAL = {
            90001: [33124, 57565],           # Core PB = GS + UBS
            90002: [33124, 57565, 7213],     # MS+Wealth
            90003: [33124, 57565, 7213, 3511, 3510, 628],  # All Peers
        }

        # Expand synthetic CERTs to include underlying banks for data loading
        expanded_certs = set(all_certs)
        peer_certs_for_synthetic = {}  # Map synthetic CERT -> list of real CERTs

        for cert in all_certs:
            if cert in SYNTHETIC_TO_REAL:
                real_certs = SYNTHETIC_TO_REAL[cert]
                expanded_certs.update(real_certs)
                peer_certs_for_synthetic[cert] = real_certs

        expanded_certs = list(expanded_certs)

        # Build color mapping
        colors = dict(DEFAULT_COLORS)
        if custom_colors:
            for name, color in custom_colors.items():
                cert = resolve_entity(name)
                colors[cert] = color

        # =========================================================================
        # FILTER DATA - Include underlying peer banks
        # =========================================================================
        df = proc_df_with_peers[proc_df_with_peers["CERT"].isin(expanded_certs)].copy()
        df = df[df["REPDTE"] >= pd.to_datetime(start_date)].sort_values(["REPDTE", "CERT"])

        if df.empty:
            print(f"No data found for entities {all_certs} after {start_date}")
            return None, None

        # Check if metrics exist
        if bar_m not in df.columns:
            print(f"⚠️ Metric '{bar_m}' not found in data. Skipping.")
            return None, None
        if line_m not in df.columns:
            print(f"⚠️ Metric '{line_m}' not found in data. Skipping.")
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
            Smart conversion of rate/ratio data to decimal form for plotting.

            The challenge: Data sources vary in format:
            - Some provide 0.0035 for 0.35% (already decimal)
            - Some provide 0.35 for 0.35% (percent points, needs /100)
            - Some provide 35 for 35% (percent points, needs /100)

            Strategy:
            1. Coverage/Multiple metrics: Never scale (values like 1.5x, 2.0x)
            2. Rate metrics (NCO, Nonaccrual, Delinquency, ACL): Use metric-aware thresholds
            3. Composition metrics: Use standard heuristic
            """
            s = pd.to_numeric(series, errors="coerce").astype(float)

            med = s.dropna().abs().median()
            if pd.isna(med) or med == 0:
                return s

            # =========================================================================
            # 1. NEVER SCALE: Coverage ratios, multiples, years
            # These are typically 0.5x - 5.0x or 1-20 years
            # =========================================================================
            no_scale_keywords = ["Coverage", "Years", "Ratio", "Index", "Multiple", "Buffer", "Risk_Adj_Allowance"]
            if any(kw in metric_name for kw in no_scale_keywords):
                return s

            # =========================================================================
            # 2. RATE METRICS: NCO, Nonaccrual, Delinquency, ACL rates
            # These are typically 0.01% - 5% in reality
            # =========================================================================
            rate_keywords = ["NCO", "Nonaccrual", "Delinquency", "ACL", "Allowance", "_Rate"]
            is_rate_metric = any(kw in metric_name for kw in rate_keywords)

            if is_rate_metric:
                # Rate metrics are typically 0% - 5% (0.0 - 0.05 in decimal)
                # If median > 0.10, data is likely in percent format (e.g., 0.35 means 0.35%)
                # If median <= 0.10, data is likely already decimal (e.g., 0.0035 means 0.35%)
                if med > 0.10:
                    return s / 100.0
                else:
                    return s

            # =========================================================================
            # 3. COMPOSITION METRICS: % of loans, % of ACL
            # These range from 1% - 70%
            # =========================================================================
            composition_keywords = ["Composition", "Share", "Loan_Share", "_Pct", "% of"]
            is_composition = any(kw in metric_name for kw in composition_keywords)

            if is_composition:
                # Composition metrics are typically 1% - 70% (0.01 - 0.70 in decimal)
                # If median > 1.0, it's definitely percent points
                if med > 1.0:
                    return s / 100.0
                else:
                    return s

            # =========================================================================
            # 4. DEFAULT: Standard heuristic
            # =========================================================================
            # If median > 1.0, assume percent points and divide
            return s / 100.0 if med > 1.0 else s

        def series_for(cert: int, metric: str) -> pd.Series:
            """Get time-aligned series for a given cert and metric."""
            # For synthetic peer CERTs, calculate weighted average
            if cert >= 90000:
                weight_col = get_weight_col(metric)
                return calc_weighted_peer_series(metric, weight_col, synthetic_cert=cert)

            # For real banks, get directly from data
            ed = timeline.merge(df[df["CERT"] == cert][["REPDTE", metric]], on="REPDTE", how="left")
            return to_decimal(ed[metric], metric_name=metric)

            # For real banks, get directly from data
            ed = timeline.merge(df[df["CERT"] == cert][["REPDTE", metric]], on="REPDTE", how="left")
            return to_decimal(ed[metric], metric_name=metric)

        def lighten(hex_color: str, factor: float = 0.85) -> str:
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
        left_vals = [series_for(c, bar_m) for c in bar_certs]
        right_vals = [series_for(c, line_m) for c in line_certs]
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
            vals = series_for(c, bar_m)
            col = colors.get(c, "#7F8C8D")
            b = ax.bar(x + offsets[c], vals, width=bar_w, color=col, alpha=0.92, zorder=2)
            bar_handles.append(b[0])
            bar_labels.append(f"{DISPLAY_NAMES.get(c, f'CERT {c}')} (Bar)")

        # =========================================================================
        # PLOT LINES
        # =========================================================================
        for c in line_certs:
            vals = series_for(c, line_m)
            col = colors.get(c, "#7F8C8D")
            line, = ax2.plot(x, vals, marker="o", linewidth=2.5, markersize=6,
                            color=col, alpha=0.95, zorder=3)
            line_handles.append(line)
            line_labels.append(f"{DISPLAY_NAMES.get(c, f'CERT {c}')} (Line)")

        # =========================================================================
        # DATA LABELS WITH COLLISION DETECTION
        # =========================================================================
        # Determine which periods to label: Q4 (year-end), Q2 (June-end), latest
        qtr = timeline["REPDTE"].dt.quarter
        last_dt = timeline["REPDTE"].max()
        idx_to_label = np.where(
            (qtr == 4) | (qtr == 2) | (timeline["REPDTE"] == last_dt)
        )[0]
        # Remove duplicates (in case latest is also Q2 or Q4)
        idx_to_label = np.unique(idx_to_label)

        # Collision detection helpers
        def rects_overlap(a, b):
            ax0, ay0, ax1, ay1 = a
            bx0, by0, bx1, by1 = b
            return not (ax1 < bx0 or bx1 < ax0 or ay1 < by0 or by1 < ay0)

        class Placer:
            def __init__(self, ax_ref, tag_sz):
                self.ax = ax_ref
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
                                    rect = (
                                        xd + new[0] - (ri[2] - ri[0]) / 2,
                                        yd + new[1] - (ri[3] - ri[1]) / 2,
                                        xd + new[0] + (ri[2] - ri[0]) / 2,
                                        yd + new[1] + (ri[3] - ri[1]) / 2,
                                    )
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

        # Place Bar Labels (Fixed positions - above bars)
        # Alternate y-offset by entity to reduce overlap
        ylo, yhi = ax.get_ylim()
        y_range = yhi - ylo

        for i in idx_to_label:
            for j, c in enumerate(bar_certs):
                s = series_for(c, bar_m)
                v = s.iloc[i] if i < len(s) else np.nan
                if pd.isna(v):
                    continue

                col = colors.get(c, "#7F8C8D")
                xpos = x[i] + offsets[c]

                # Stagger y-offset based on entity index to avoid overlap
                base_offset = 0.025 + (j * 0.035)  # Stagger by entity
                ypos = v + y_range * base_offset

                ann = ax.annotate(
                    f"{v:.2%}",
                    xy=(xpos, v),
                    xytext=(0, 8 + j * 12),  # Stagger pixel offset too
                    textcoords="offset pixels",
                    ha="center",
                    va="bottom",
                    fontsize=tag_size,
                    fontweight="bold",
                    color=col,  # Font color matches series color
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec=col, lw=0.8, alpha=0.9),
                )

                # Register rect for collision detection
                xd, yd = ax.transData.transform((xpos, ypos))
                w, h = 50, 22
                placer.add_fixed((xd - w / 2, yd - h / 2, xd + w / 2, yd + h / 2))

        # Place Line Labels (Dynamic with collision avoidance)
        # Place Line Labels (Dynamic with collision avoidance)
        # Process entities in order, staggering default offsets
        for entity_idx, c in enumerate(line_certs):
            s = series_for(c, line_m)
            col = colors.get(c, "#7F8C8D")

            # Base offset direction alternates by entity
            base_dir = 1 if entity_idx % 2 == 0 else -1

            for k in idx_to_label:
                val = s.iloc[k] if k < len(s) else np.nan
                if pd.notna(val):
                    txt = f"{val:.2%}"
                    found = False

                    # Try offsets - start with direction based on entity index
                    offsets_to_try = [
                        base_dir * 18,
                        base_dir * -18,
                        base_dir * 30,
                        base_dir * -30,
                        base_dir * 42,
                        base_dir * -42,
                    ]

                    for yoff in offsets_to_try:
                        rect = placer.rect_for_text(x[k], float(val), 0, yoff, txt)
                        if placer.can_place(rect):
                            ann = ax2.annotate(
                                txt,
                                xy=(x[k], float(val)),
                                xytext=(0, yoff),
                                textcoords="offset pixels",
                                fontsize=tag_size,
                                fontweight="bold",
                                color=col,  # Font color matches series
                                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=col, lw=1.0, alpha=0.95),
                            )
                            placer.add_line_ann(ann, rect)
                            found = True
                            break

                    if not found:
                        # Fallback with staggered offset
                        fallback_yoff = 18 * base_dir + (entity_idx * 12)
                        ax2.annotate(
                            txt,
                            xy=(x[k], float(val)),
                            xytext=(0, fallback_yoff),
                            textcoords="offset pixels",
                            fontsize=tag_size,
                            fontweight="bold",
                            color=col,
                            bbox=dict(boxstyle="round,pad=0.2", fc="white", ec=col, lw=1.0, alpha=0.9),
                        )

        # Relax overlapping labels
        placer.relax()

        # =========================================================================
        # FINAL POLISH
        # =========================================================================

        # =========================================================================
        # FINAL POLISH
        # =========================================================================
        ax.set_xticks(x)
        ax.set_xticklabels(xticks, fontsize=tick_size, rotation=0)

        # Axis labels - use _clean_metric_name for display
        left_label = bar_axis_label or _clean_metric_name(bar_m)
        right_label = line_axis_label or _clean_metric_name(line_m)
        ax.set_ylabel(left_label, fontsize=axis_label_size, fontweight="bold")
        ax2.set_ylabel(right_label, fontsize=axis_label_size, fontweight="bold")

        # Dynamic Formatting
        def get_formatter(metric_name):
            if "Years" in metric_name:
                return plt.FuncFormatter(lambda y, _: f"{y:.1f} Yrs")
            elif "Coverage" in metric_name or "Ratio" in metric_name:
                return plt.FuncFormatter(lambda y, _: f"{y:.2f}x" if abs(y) > 0.5 else f"{y:.1%}")
            else:
                return plt.FuncFormatter(lambda y, _: f"{y:.2%}")

        ax.yaxis.set_major_formatter(get_formatter(bar_m))
        ax2.yaxis.set_major_formatter(get_formatter(line_m))

        # Title and subtitle with normalized indicator
        title_suffix = " (Normalized)" if is_normalized else ""
        chart_title = title or f"{_clean_metric_name(bar_m)} vs {_clean_metric_name(line_m)}"
        chart_title += title_suffix
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
        if base_save_path:
            if is_normalized:
                actual_path = base_save_path.replace(".png", "_normalized_CHA.png")
            else:
                actual_path = base_save_path.replace(".png", "_standard_CHA.png") if mode == "both" else base_save_path
            Path(os.path.dirname(actual_path)).mkdir(parents=True, exist_ok=True)
            fig.savefig(actual_path, dpi=300, bbox_inches="tight", transparent=True)
            print(f"✓ Dual-axis chart saved: {actual_path}")

        return fig, ax

    # =========================================================================
    # 3. MODE HANDLING (Standard, Normalized, Both)
    # =========================================================================

    # Resolve Normalized Metric Names
    n_bar = norm_bar_metric or get_norm_metric(bar_metric)
    n_line = norm_line_metric or get_norm_metric(line_metric)

    if mode == "standard":
        return _create_single_chart(bar_metric, line_metric, False, save_path)

    elif mode == "normalized":
        # Hybrid Logic: Use norm columns if available, else fallback to standard
        final_bar = n_bar if n_bar in df.columns else bar_metric
        final_line = n_line if n_line in df.columns else line_metric

        # Only skip if BOTH normalized metrics are missing (prevents exact duplicate of standard)
        if final_bar == bar_metric and final_line == line_metric:
            print(f"ℹ️ No normalized metrics found for {bar_metric}/{line_metric}. Skipping normalized chart.")
            return None, None

        return _create_single_chart(final_bar, final_line, True, save_path)

    elif mode == "both":
        results = {}

        # 1. Standard (Always create)
        std_path = save_path.replace(".png", "_standard_DUAL.png") if save_path else None

        # Call recursively with explicit kwargs to avoid TypeError
        results["standard"] = create_dual_axis_chart(
            df=df,
            bar_entities=bar_entities,
            line_entities=line_entities,
            bar_metric=bar_metric,
            line_metric=line_metric,
            mode="standard", # Stop recursion
            title=title,
            subtitle=subtitle,
            bar_axis_label=bar_axis_label,
            line_axis_label=line_axis_label,
            start_date=start_date,
            save_path=std_path,
            # Pass styling explicitly
            figsize=figsize, title_size=title_size, subtitle_size=subtitle_size,
            axis_label_size=axis_label_size, tick_size=tick_size, tag_size=tag_size,
            legend_fontsize=legend_fontsize, economist_style=economist_style,
            transparent_bg=transparent_bg, custom_colors=custom_colors
        )

        # 2. Normalized (Hybrid Logic)
        final_bar = n_bar if n_bar in df.columns else bar_metric
        final_line = n_line if n_line in df.columns else line_metric

        if final_bar == bar_metric and final_line == line_metric:
            print(f"ℹ️ Normalized metrics missing for Dual Axis ({n_bar}, {n_line}). Skipping chart.")
        else:
            if final_bar == bar_metric: print(f"  Note: Using standard {bar_metric} for Bars (Norm missing)")
            if final_line == line_metric: print(f"  Note: Using standard {line_metric} for Lines (Norm missing)")

            norm_path = save_path.replace(".png", "_normalized_DUAL.png") if save_path else None

            # Direct call to internal helper avoids recursion issues for the hybrid case
            fig_norm, ax_norm = _create_single_chart(final_bar, final_line, True, norm_path)
            results["normalized"] = (fig_norm, ax_norm)

        return results

    return None, None


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


def calc_dynamic_weighted_series(
    df: pd.DataFrame,
    metric: str,
    peer_certs: List[int],
    weight_col: str = "Gross_Loans"
) -> pd.Series:
    """
    Calculates a weighted average time series for a specific group of banks.
    Returns a Series indexed by REPDTE.
    """
    # Filter to peer group
    peer_df = df[df["CERT"].isin(peer_certs)].copy()

    # Calculate weighted avg per date
    results = []
    dates = sorted(peer_df["REPDTE"].unique())

    for dt in dates:
        dt_data = peer_df[peer_df["REPDTE"] == dt]
        # Drop rows where either metric or weight is missing/zero
        valid = dt_data.dropna(subset=[metric, weight_col])
        valid = valid[valid[weight_col] > 0]

        if valid.empty:
            results.append({"REPDTE": dt, "val": np.nan})
        else:
            # Weighted Avg = Sum(Val * W) / Sum(W)
            w_avg = np.average(valid[metric], weights=valid[weight_col])
            results.append({"REPDTE": dt, "val": w_avg})

    return pd.DataFrame(results).set_index("REPDTE")["val"]


# ==================================================================================
# NEW VISUALIZATION FUNCTIONS (Loan Mix, Segment Health, Percentile Bands, Rank)
# ==================================================================================
def plot_loan_mix_stacked_time(
    df: pd.DataFrame,
    entities: List[str] = ["MSPBNA", "MSBNA", "COREPB"],
    buckets: Optional[Dict[str, List[str]]] = None,
    mode: str = "percent",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    figsize: Tuple[float, float] = (14, 7),
    save_path: Optional[str] = None,
    chart_mode: str = "standard",
) -> Union[plt.Figure, Dict[str, plt.Figure]]:

    # --- BUCKET DEFINITIONS ---
    STD_BUCKETS = {
        "Resi Mtg": ["Wealth_Resi_Balance"],
        "CRE": ["CRE_Investment_Pure_Balance"],
        "SBL": ["SBL_Balance"],
        "C&I / Fund Fin": ["Commercial_Balance"],
        "Consumer": ["Consumer_Balance", "CreditCard_Balance"],
        "Other": ["Other_Loans_Balance"]
    }

    # Normalized: Excludes C&I, Consumer, ADC
    NORM_BUCKETS = {
        "Resi Mtg": ["Wealth_Resi_Balance"],
        "CRE (Ex-ADC)": ["CRE_Investment_Pure_Balance"],
        "SBL": ["SBL_Balance"],
    }

    # --- RECURSIVE CALL FOR 'BOTH' MODE ---
    if chart_mode == "both":
        results = {}
        results["standard"] = plot_loan_mix_stacked_time(
            df, entities, buckets=STD_BUCKETS, mode=mode,
            start_date=start_date, end_date=end_date, figsize=figsize,
            save_path=save_path.replace(".png", "_standard_MIX.png") if save_path else None,
            chart_mode="standard"
        )
        results["normalized"] = plot_loan_mix_stacked_time(
            df, entities, buckets=NORM_BUCKETS, mode=mode,
            start_date=start_date, end_date=end_date, figsize=figsize,
            save_path=save_path.replace(".png", "_normalized_MIX.png") if save_path else None,
            chart_mode="normalized"
        )
        return results

    # --- MAIN LOGIC ---
    current_buckets = buckets or (NORM_BUCKETS if chart_mode == "normalized" else STD_BUCKETS)

    # Define Composite Members (Fallback if composite row missing)
    COMPOSITES = {
        "COREPB": [33124, 57565], # GS + UBS
        "ALLPEERS": [c for c in df["CERT"].unique() if c < 90000 and c not in [34221, 32992]]
    }

    # Map input names to CERTs or Composite Keys
    entity_map = {
        "MSPBNA": 34221, "MSBNA": 32992, "COREPB": "COREPB",
        "ALLPEERS": "ALLPEERS", "UBS": 57565, "GS": 33124
    }

    # Prepare Data
    plot_df = df.copy()
    if start_date: plot_df = plot_df[plot_df["REPDTE"] >= pd.to_datetime(start_date)]
    if end_date: plot_df = plot_df[plot_df["REPDTE"] <= pd.to_datetime(end_date)]
    plot_df = plot_df.sort_values("REPDTE")

    fig, axes = plt.subplots(1, len(entities), figsize=figsize, sharey=True)
    if len(entities) == 1: axes = [axes]

    colors = plt.cm.Set3(np.linspace(0, 1, len(current_buckets)))

    for ax, entity_key in zip(axes, entities):
        # 1. Resolve Data for Entity
        cert_or_key = entity_map.get(entity_key, entity_key)

        # Check if it's a composite that needs aggregation
        if cert_or_key in COMPOSITES:
            member_certs = COMPOSITES[cert_or_key]
            # Sum all members
            e_df = plot_df[plot_df["CERT"].isin(member_certs)].groupby("REPDTE").sum(numeric_only=True)
        else:
            # Single bank
            e_df = plot_df[plot_df["CERT"] == cert_or_key].set_index("REPDTE")

        # 2. Build Stack
        stack_data = pd.DataFrame(index=e_df.index)
        for b_name, cols in current_buckets.items():
            valid_cols = [c for c in cols if c in e_df.columns]
            stack_data[b_name] = e_df[valid_cols].sum(axis=1) if valid_cols else 0.0

        # 3. Normalize to % if needed
        if mode == "percent":
            totals = stack_data.sum(axis=1)
            stack_data = stack_data.div(totals.replace(0, np.nan), axis=0).fillna(0) * 100

        # 4. Plot
        stack_data.plot.area(ax=ax, stacked=True, color=colors, alpha=0.85, legend=False)

        ax.set_title(entity_key, fontsize=12, fontweight='bold')
        ax.set_xlabel("")
        ax.grid(False)

        if mode == "percent":
            ax.set_ylim(0, 100)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.0f}%"))
        else:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"${y/1000:,.0f}B"))

    axes[0].set_ylabel("Portfolio Mix (%)" if mode == "percent" else "Balance ($)")

    # Legend
    handles = [plt.Rectangle((0,0),1,1, fc=c, alpha=0.85) for c in colors]
    title_suffix = " (Normalized)" if chart_mode == "normalized" else ""
    plt.figlegend(handles, list(current_buckets.keys()), loc='upper center',
                 ncol=len(current_buckets), bbox_to_anchor=(0.5, 1.05), frameon=False)

    plt.suptitle(f"Loan Portfolio Composition{title_suffix}", fontsize=14, fontweight='bold', y=1.08)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=300, bbox_inches='tight', transparent=True)
        print(f"✓ Loan Mix chart saved: {save_path}")

    return fig

def plot_peer_percentile_bands(
    df: pd.DataFrame,
    metric: str,
    subject_cert: int = 34221,
    peer_filter: Optional[List[int]] = None, # If None, uses all except subject/sibling
    percentiles: Tuple[int, ...] = (10, 25, 50, 75, 90),
    start_date: Optional[str] = None,
    sibling_cert: Optional[int] = 32992,
    figsize: Tuple[float, float] = (12, 6),
    save_path: Optional[str] = None,
    mode: str = "standard",  # "standard", "normalized", "both"
    norm_metric: Optional[str] = None,
) -> Union[plt.Figure, Dict[str, plt.Figure]]:

    # --- RECURSIVE CALL FOR 'BOTH' MODE ---
    if mode == "both":
        results = {}
        # Standard
        results["standard"] = plot_peer_percentile_bands(
            df, metric, subject_cert, peer_filter, percentiles, start_date,
            sibling_cert, figsize,
            save_path=save_path.replace(".png", "_standard_PCTL.png") if save_path else None,
            mode="standard"
        )
        # Normalized (Auto-detect norm name if not provided)
        n_metric = norm_metric or f"Norm_{metric}" if not metric.startswith("Norm_") else metric
        if n_metric in df.columns:
            results["normalized"] = plot_peer_percentile_bands(
                df, n_metric, subject_cert, peer_filter, percentiles, start_date,
                sibling_cert, figsize,
                save_path=save_path.replace(".png", "_normalized_PCTL.png") if save_path else None,
                mode="normalized"
            )
        return results

    # --- SETUP ---
    plot_df = df.copy()
    if start_date:
        plot_df = plot_df[plot_df["REPDTE"] >= pd.to_datetime(start_date)]

    # 1. Define Peer Group (Real Banks Only)
    # Exclude 90xxx composites, subject, and sibling from the "Band" calculation
    all_certs = set(plot_df["CERT"].unique())
    exclude = {subject_cert, sibling_cert}

    if peer_filter:
        real_peers = [c for c in peer_filter if c < 90000 and c not in exclude]
    else:
        real_peers = [c for c in all_certs if c < 90000 and c not in exclude]

    peer_df = plot_df[plot_df["CERT"].isin(real_peers)]

    # 2. Calculate Percentiles (The Grey Bands)
    pctl_data = peer_df.groupby("REPDTE")[metric].quantile(
        [p/100 for p in percentiles]
    ).unstack()
    pctl_data.columns = [f"p{int(p*100)}" for p in pctl_data.columns]

    # 3. Calculate Weighted Peer Average (The Black Dashed Line)
    # Use Norm_Gross_Loans for normalized mode, Gross_Loans for standard
    w_col = "Norm_Gross_Loans" if mode == "normalized" else "Gross_Loans"
    w_avg_series = calc_dynamic_weighted_series(plot_df, metric, real_peers, weight_col=w_col)

    # 4. Get Subject & Sibling Data
    subj_data = plot_df[plot_df["CERT"] == subject_cert].set_index("REPDTE")[metric]
    sib_data = plot_df[plot_df["CERT"] == sibling_cert].set_index("REPDTE")[metric] if sibling_cert else None

    # --- PLOTTING ---
    fig, ax = plt.subplots(figsize=figsize)

    # Bands
    if "p10" in pctl_data.columns and "p90" in pctl_data.columns:
        ax.fill_between(pctl_data.index, pctl_data["p10"], pctl_data["p90"],
                        alpha=0.10, color="#7F8C8D", label="10th-90th Pctl")
    if "p25" in pctl_data.columns and "p75" in pctl_data.columns:
        ax.fill_between(pctl_data.index, pctl_data["p25"], pctl_data["p75"],
                        alpha=0.20, color="#7F8C8D", label="25th-75th Pctl")

    # Lines
    ax.plot(w_avg_series.index, w_avg_series.values, color="black",
            linestyle="--", linewidth=1.5, label="Peer Weighted Avg", alpha=0.8)

    ax.plot(subj_data.index, subj_data.values, color="#002F6C",
            linewidth=3.0, marker='o', markersize=6, label="MSPBNA", zorder=10)

    if sib_data is not None:
        ax.plot(sib_data.index, sib_data.values, color="#4C78A8",
                linewidth=2, linestyle="--", marker='s', markersize=4, label="MSBNA", zorder=9)

    # Formatting
    title_suffix = " (Normalized)" if mode == "normalized" else ""
    ax.set_title(f"{metric.replace('_', ' ')} — Peer Context{title_suffix}", fontsize=14, fontweight='bold')

    # Dynamic Y-Axis Formatting (Percent vs Scalar)
    y_max = max(pctl_data.max().max(), subj_data.max())
    if y_max < 1.0: # Assume rate
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.2%}"))
    else:
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f"{y:.1f}"))

    ax.legend(loc="upper left", frameon=True)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=300, bbox_inches='tight', transparent=True)
        print(f"✓ Percentile chart saved: {save_path}")

    return fig


def plot_peer_rank_bar(
    df_latest: pd.DataFrame,
    metric: str,
    subject_cert: int = 34221,
    highlight_certs: Tuple[int, ...] = (34221, 32992),
    label_topn: int = 8,
    ascending: bool = False,
    figsize: Tuple[float, float] = (10, 8),
    save_path: Optional[str] = None,
) -> plt.Figure:
    """
    Horizontal bar chart showing peer rankings for a metric.

    Args:
        df_latest: DataFrame filtered to latest quarter only
        metric: Column name to rank
        subject_cert: CERT of subject bank (highlighted)
        highlight_certs: CERTs to highlight with distinct colors
        label_topn: How many bars to label
        ascending: If True, lower values ranked higher
        figsize: Figure size
        save_path: If provided, saves PNG

    Returns:
        matplotlib Figure
    """
    # Exclude composites
    plot_df = df_latest[df_latest["CERT"] < 90000].copy()
    plot_df = plot_df.dropna(subset=[metric])
    plot_df = plot_df.sort_values(metric, ascending=ascending)

    fig, ax = plt.subplots(figsize=figsize)

    # Colors
    colors = []
    for cert in plot_df["CERT"]:
        if cert == 34221:
            colors.append("#002F6C")  # MSPBNA
        elif cert == 32992:
            colors.append("#4C78A8")  # MSBNA
        elif cert in highlight_certs:
            colors.append("#F7A81B")  # Gold for other highlights
        else:
            colors.append("#BDC3C7")  # Gray for others

    # Get names
    names = plot_df.apply(lambda r: _clean_bank_name(r.get("NAME", ""), r["CERT"]), axis=1)

    y_pos = np.arange(len(plot_df))
    bars = ax.barh(y_pos, plot_df[metric], color=colors, edgecolor='white', linewidth=0.5)

    ax.set_yticks(y_pos)
    ax.set_yticklabels(names, fontsize=9)
    ax.invert_yaxis()

    # Add peer average line
    peer_avg = plot_df[~plot_df["CERT"].isin([34221, 32992])][metric].mean()
    ax.axvline(peer_avg, color="#7F8C8D", linestyle="--", linewidth=2, label=f"Peer Avg: {peer_avg:.2%}")

    ax.set_xlabel(metric.replace("_", " "))
    ax.set_title(f"{_clean_metric_name(metric)} — Peer Ranking", fontsize=14, fontweight='bold')
    ax.legend(loc="lower right")
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2%}" if abs(x) < 1 else f"{x:.1f}%"))

    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=300, bbox_inches='tight', transparent=True)

    return fig

import re
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, Dict, List, Optional, Union
from pathlib import Path

def plot_scatter_dynamic(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    subject_cert: int = 34221,
    sibling_cert: int = 32992,
    peer_avg_cert_primary: int = 90003,
    peer_avg_cert_alt: int = 90001,
    use_alt_peer_avg: bool = False,
    show_peers_avg_label: bool = True,
    show_idb_label: bool = True,
    show_sibling_label: bool = True,
    identify_outliers: bool = True,
    outliers_topn: int = 2,
    figsize: Tuple[float, float] = (6.0, 6.0),
    title_size: int = 18,
    axis_label_size: int = 12,
    exclude_certs: Optional[List[int]] = None,
    tick_size: int = 12,
    tag_size: int = 12,
    economist_style: bool = True,
    transparent_bg: bool = True,
    square_axes: bool = True,
    save_path: Optional[str] = None,
    mode: str = "standard",  # "standard", "normalized", or "both"
    norm_x_col: Optional[str] = None,
    norm_y_col: Optional[str] = None,
) -> Union[Tuple[plt.Figure, plt.Axes], Dict[str, Tuple[plt.Figure, plt.Axes]]]:
    """
    Creates scatter plot comparing banks.
    Includes Smart Legend, Axis Padding, Advanced Formatting, and Name Cleaning.
    """

    # --- AUTO-DETECT NORMALIZED COLUMNS ---
    def get_norm_col(col: str) -> str:
        norm_map = {
            # --- 1. Top-Level Credit Quality ---
            "TTM_NCO_Rate": "Norm_NCO_Rate",
            "Nonaccrual_to_Gross_Loans_Rate": "Norm_Nonaccrual_Rate",
            "Total_Delinquency_Rate": "Norm_Delinquency_Rate",
            "Top_House_Delinquency_Rate": "Norm_Delinquency_Rate",
            "Allowance_to_Gross_Loans_Rate": "Norm_ACL_Coverage",
            "Risk_Adj_Allowance_Coverage": "Norm_Risk_Adj_Allowance_Coverage",

            # --- 2. Balances (Level) ---
            "Gross_Loans": "Norm_Gross_Loans",
            "Total_ACL": "Norm_ACL_Balance",
            "Total_Nonaccrual": "Norm_Total_Nonaccrual",
            "NTLNLS": "Norm_Total_NCO",
            "Total_NCO_TTM": "Norm_Total_NCO",
            "TopHouse_PD30": "Norm_PD30",
            "TopHouse_PD90": "Norm_PD90",

            # --- 2a. Normalization Transparency ---
            "Exclusion_Pct": "Norm_Exclusion_Pct",

            # --- 3. Portfolio Composition ---
            "SBL_Composition": "Norm_SBL_Composition",
            "Fund_Finance_Composition": "Norm_Fund_Finance_Composition",
            "Wealth_Resi_Composition": "Norm_Wealth_Resi_Composition",
            "RIC_Resi_Loan_Share": "Norm_Wealth_Resi_Composition",  # Alias
            "CRE_Investment_Composition": "Norm_CRE_Investment_Composition",
            "RIC_CRE_Loan_Share": "Norm_CRE_Investment_Composition",  # Alias
            "CRE_OO_Composition": "Norm_CRE_OO_Composition",
            "ADC_Composition": "Norm_ADC_Composition",

            # --- 4. Segment-Specific ACL Coverage ---
            "RIC_CRE_ACL_Coverage": "Norm_CRE_ACL_Coverage",
            "RIC_Resi_ACL_Coverage": "Norm_RESI_ACL_Coverage",
            "RIC_Comm_ACL_Coverage": "Norm_Comm_ACL_Coverage",

            # --- 5. Profitability & Returns ---
            "NIMY": "Norm_Loan_Yield",
            "Loan_Yield_Proxy": "Norm_Loan_Yield",
            "Provision_to_Loans_Rate": "Norm_Provision_Rate",
            "Loss_Adj_Yield": "Norm_Loss_Adj_Yield",
            "Risk_Adj_Return": "Norm_Risk_Adj_Return",

            # --- 6. Risk Share Metrics (PD Contributions) ---
            # ADC
            "ADC_Share_of_Total_PD": "Norm_ADC_Share_of_Total_PD",
            "ADC_Share_of_Total_PD30": "Norm_ADC_Share_of_Total_PD30",
            "ADC_Share_of_Total_PD90": "Norm_ADC_Share_of_Total_PD90",
            # C&I
            "CI_Share_of_Total_PD": "Norm_CI_Share_of_Total_PD",
            "CI_Share_of_Total_PD30": "Norm_CI_Share_of_Total_PD30",
            "CI_Share_of_Total_PD90": "Norm_CI_Share_of_Total_PD90",
            # CRE
            "CRE_Share_of_Total_PD": "Norm_CRE_Share_of_Total_PD",
            "CRE_Share_of_Total_PD30": "Norm_CRE_Share_of_Total_PD30",
            "CRE_Share_of_Total_PD90": "Norm_CRE_Share_of_Total_PD90",
            # Resi
            "Resi_Share_of_Total_PD": "Norm_Resi_Share_of_Total_PD",
            "Resi_Share_of_Total_PD30": "Norm_Resi_Share_of_Total_PD30",
            "Resi_Share_of_Total_PD90": "Norm_Resi_Share_of_Total_PD90",
        }
        return norm_map.get(col, f"Norm_{col}")

    if norm_x_col is None: norm_x_col = get_norm_col(x_col)
    if norm_y_col is None: norm_y_col = get_norm_col(y_col)

    # --- INTERNAL PLOTTING FUNCTION ---
    def _create_single_scatter(
        plot_df: pd.DataFrame,
        x_metric: str,
        y_metric: str,
        is_normalized: bool,
        base_save_path: Optional[str],
    ) -> Tuple[plt.Figure, plt.Axes]:

        if plot_df.empty:
            raise ValueError("Scatter DF is empty.")
        # --- FIX START: Filter for Latest Date ---
        if "REPDTE" in plot_df.columns:
            latest_dt = plot_df["REPDTE"].max()
            plot_df = plot_df[plot_df["REPDTE"] == latest_dt].copy()
            print(f"DEBUG: Filtered scatter to latest date: {latest_dt}")
        else:
            plot_df = plot_df.copy()

        def to_decimals_series(s: pd.Series, metric_name: str = "") -> pd.Series:
            """Smart decimal conversion with metric awareness."""
            s = pd.to_numeric(s, errors="coerce")
            med = s.dropna().abs().median()

            if pd.isna(med) or med == 0:
                return s

            # Coverage/Multiple metrics - never scale
            no_scale = ["Coverage", "Risk_Adj", "Multiple", "Ratio", "Years", "Buffer"]
            if any(kw in metric_name for kw in no_scale):
                return s

            # Rate metrics (NCO, Nonaccrual, etc.) - use lower threshold
            rate_keywords = ["NCO", "Nonaccrual", "Delinquency", "ACL", "_Rate"]
            if any(kw in metric_name for kw in rate_keywords):
                # If median > 0.10, data is in percent format
                if med > 0.10:
                    return s / 100.0
                return s

            # Default heuristic
            if med > 1.0:
                return s / 100.0
            return s
        # 2. APPLY EXCLUSIONS (The requested feature)
        # Exclude specific requested banks immediately
        if exclude_certs:
            plot_df = plot_df[~plot_df["CERT"].isin(exclude_certs)]

        # Ensure numeric
        plot_df[x_metric] = pd.to_numeric(plot_df[x_metric], errors="coerce")
        plot_df[y_metric] = pd.to_numeric(plot_df[y_metric], errors="coerce")

        peers_cert = peer_avg_cert_alt if use_alt_peer_avg else peer_avg_cert_primary
        plot_df = plot_df.copy()

        if exclude_certs:
            plot_df = plot_df[~plot_df["CERT"].isin(exclude_certs)]

        plot_df[x_metric] = pd.to_numeric(plot_df[x_metric], errors="coerce")
        plot_df[y_metric] = pd.to_numeric(plot_df[y_metric], errors="coerce")

        fig, ax = plt.subplots(figsize=figsize)
        if transparent_bg:
            fig.patch.set_alpha(0)
            ax.set_facecolor("none")

        if economist_style:
            for sp in ["top", "right"]: ax.spines[sp].set_visible(False)
            for s in ax.spines.values(): s.set_linewidth(1.1); s.set_color("#2B2B2B")
            ax.tick_params(axis="both", labelsize=tick_size, colors="#2B2B2B")
            ax.grid(True, color="#D0D0D0", linewidth=0.8, alpha=0.35)

        # Define Groups
        idb = plot_df[plot_df["CERT"] == subject_cert]
        peer_avg = plot_df[plot_df["CERT"] == peers_cert]
        sib = plot_df[plot_df["CERT"] == sibling_cert]
        # Exclude Subject, Sibling, and all composites from "dot cloud"
        others = plot_df[~plot_df["CERT"].isin([
            subject_cert, sibling_cert,
            peer_avg_cert_primary, peer_avg_cert_alt,
            90001, 90002, 90003, 90011, 90012, 90013
        ])]

        # Plot Peers
        Xo, Yo = to_decimals_series(others[x_metric]), to_decimals_series(others[y_metric])
        PEER_COLOR = "#4C78A8"
        ax.scatter(Xo, Yo, s=42, alpha=0.9, color=PEER_COLOR, edgecolor="white",
                   linewidth=0.6, label="Peers")

        # Plot Subject (MSPBNA)
        xi, yi = None, None
        MSPBNA_BLUE = "#002F6C"
        GUIDE = "#7F8C8D"
        if not idb.empty:
            xi = float(to_decimals_series(idb[x_metric]).iloc[0])
            yi = float(to_decimals_series(idb[y_metric]).iloc[0])
            ax.scatter(xi, yi, s=100, color=MSPBNA_BLUE, edgecolor="white",
                       linewidth=1.5, label="MSPBNA", zorder=5)

        # Plot Sibling (MSBNA)
        sx, sy = None, None
        MS_LIGHT = "#4C78A8"
        if not sib.empty:
            sx = float(to_decimals_series(sib[x_metric]).iloc[0])
            sy = float(to_decimals_series(sib[y_metric]).iloc[0])
            ax.scatter(sx, sy, s=90, color=MS_LIGHT, edgecolor="white", linewidth=1.2,
                       label="MSBNA", zorder=5)

        # Plot Peer Average (Square)
        px, py = None, None
        if not peer_avg.empty:
            px = float(to_decimals_series(peer_avg[x_metric]).iloc[0])
            py = float(to_decimals_series(peer_avg[y_metric]).iloc[0])
            ax.scatter(px, py, s=90, color=PEER_COLOR, marker="s", edgecolor="black",
                       linewidth=0.7, label="_nolegend_")
            ax.axvline(px, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)
            ax.axhline(py, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)

        # --- 4. SUPERIOR NAME CLEANING ---
        SUFFIX_RE = re.compile(
            r"(\s*,?\s*THE)?(\s*\(.*?\))?(\s+(NATIONAL(\s+ASSOCIATION|(\s+ASSN\.?))|"
            r"N\.?A\.?|NA|FEDERAL(\s+SAVINGS\s+BANK)?|SAVINGS\s+BANK|STATE\s+BANK|"
            r"NATIONAL\s+BANK|BANK|BANCORP(?:ORATION)?|CORP(?:ORATION)?|COMPANY|CO\.?|"
            r"INC|INC\.?|LTD\.?|LIMITED|FSB|F\.S\.B\.?|ASSOCIATION|ASSN\.?))+\s*$",
            re.IGNORECASE
        )
        def short_name(s):
            s = str(s).strip()
            s = re.sub(r"\s+", " ", s)
            return SUFFIX_RE.sub("", s).strip(", ").strip() or s

        # Label Placement Helper
        placed = []
        def pick_offset(px_, py_, along_line=False):
            cands = ([(12, 0), (18, 0), (-42, 0), (-60, 0)] if along_line
                     else [(10, 12), (12, -12), (-10, 12), (-12, -12), (16, 0), (-16, 0)])
            for dx, dy in cands:
                sx_, sy_ = ax.transData.transform((px_, py_))
                tx, ty = sx_ + dx, sy_ + dy
                # Simple collision check against previously placed labels
                if all(abs(tx - ox) >= 58 or abs(ty - oy) >= 22 for ox, oy in placed):
                    placed.append((tx, ty))
                    return dx, dy
            placed.append(ax.transData.transform((px_, py_)))
            return (10, 12)

        def tag(px_, py_, text, xytext, color="black", box=True):
            ax.annotate(text, xy=(px_, py_), xytext=xytext, textcoords="offset points",
                        fontsize=tag_size, fontweight="bold", color=color,
                        bbox=(dict(boxstyle="round,pad=0.25", fc="white", ec="black",
                                   lw=0.6, alpha=0.95) if box else None),
                        arrowprops=(dict(arrowstyle="->", lw=1.0, color="black") if box else None),
                        va="center")

        if show_peers_avg_label and (px is not None):
            dx, _ = pick_offset(px, py, along_line=True)
            tag(px, py, "Peers' Average", (dx, 0), color=GUIDE, box=False)

        if show_idb_label and (xi is not None):
            dx, dy = pick_offset(xi, yi)
            tag(xi, yi, "MSPBNA", (dx, dy), color="black", box=True)

        if show_sibling_label and (sx is not None):
            dx, dy = pick_offset(sx, sy)
            tag(sx, sy, "MSBNA", (dx, dy), color="black", box=True)

        if identify_outliers and outliers_topn > 0:
            cx, cy = (px if px else 0), (py if py else 0)
            X_all, Y_all = to_decimals_series(plot_df[x_metric]), to_decimals_series(plot_df[y_metric])
            d = ((X_all - cx)**2 + (Y_all - cy)**2)**0.5

            # Exclude composites and subjects
            mask_excl = plot_df["CERT"].isin([
                peer_avg_cert_primary, peer_avg_cert_alt, 90001, 90002, 90003,
                90011, 90012, 90013, subject_cert, sibling_cert
            ])
            cand = d[~mask_excl].sort_values(ascending=False)

            for i in list(cand.index[:outliers_topn]):
                ox = float(to_decimals_series(pd.Series([plot_df.loc[i, x_metric]]), metric_name=x_metric).iloc[0])
                oy = float(to_decimals_series(pd.Series([plot_df.loc[i, y_metric]]), metric_name=y_metric).iloc[0])
                label = short_name(plot_df.loc[i, "NAME"]) if "NAME" in plot_df.columns else str(plot_df.loc[i, "CERT"])
                dx, dy = pick_offset(ox, oy)
                tag(ox, oy, label, (dx, dy), color="black", box=True)

        # --- 2. BREATHING ROOM PADDING ---
        all_x = pd.concat([Xo, pd.Series([xi, sx, px])]).dropna().astype(float)
        all_y = pd.concat([Yo, pd.Series([yi, sy, py])]).dropna().astype(float)

        def padded(s):
            if s.empty: return 0.0, 1.0
            lo, hi = s.min(), s.max()
            rng = max(hi - lo, 1e-6)
            pad = 0.10 * rng # 10% buffer
            return max(0.0, lo - pad), hi + pad

        xlo, xhi = padded(all_x)
        ylo, yhi = padded(all_y)
        ax.set_xlim(xlo, xhi)
        ax.set_ylim(ylo, yhi)
        if square_axes: ax.set_aspect("equal", adjustable="box")

        # --- 3. ADVANCED FORMATTING ---
        def _clean(s): return s.replace("Norm_", "").replace("RIC_", "").replace("_", " ")
        x_label = _clean(x_metric)
        y_label = _clean(y_metric)
        title_suffix = " (Normalized)" if is_normalized else ""

        ax.set_xlabel(x_label, fontsize=axis_label_size, fontweight="bold", color="#2B2B2B")
        ax.set_ylabel(y_label, fontsize=axis_label_size, fontweight="bold", color="#2B2B2B")

        def get_axis_formatter(metric_name: str):
            # Broader check for "Ratio", "Multiple", "Buffer"
            coverage_keywords = ["Coverage", "Risk_Adj", "NPL_Coverage", "Multiple", "Ratio", "Buffer"]
            if any(kw in metric_name for kw in coverage_keywords):
                return plt.FuncFormatter(lambda v, _: f"{v:.2f}x")
            return plt.FuncFormatter(lambda v, _: f"{v:.1%}")

        ax.xaxis.set_major_formatter(get_axis_formatter(x_metric))
        ax.yaxis.set_major_formatter(get_axis_formatter(y_metric))
        ax.set_title(f"{y_label} vs {x_label}{title_suffix}",
                     fontsize=title_size, fontweight="bold", color="#2B2B2B")

        # --- 1. SMART LEGEND PLACEMENT ---
        def get_best_legend_loc(ax, all_x, all_y):
            if len(all_x) == 0: return "lower right"
            x_min, x_max = float(all_x.min()), float(all_x.max())
            y_min, y_max = float(all_y.min()), float(all_y.max())

            # Define 20% corners
            x_lo = x_min + 0.2 * (x_max - x_min)
            x_hi = x_max - 0.2 * (x_max - x_min)
            y_lo = y_min + 0.2 * (y_max - y_min)
            y_hi = y_max - 0.2 * (y_max - y_min)

            # Count points
            lower_right = sum((all_x > x_hi) & (all_y < y_lo))
            upper_right = sum((all_x > x_hi) & (all_y > y_hi))
            upper_left  = sum((all_x < x_lo) & (all_y > y_hi))
            lower_left  = sum((all_x < x_lo) & (all_y < y_lo))

            corners = {"lower right": lower_right, "upper right": upper_right,
                       "upper left": upper_left, "lower left": lower_left}

            # Return corner with MIN points
            return min(corners, key=corners.get)

        legend_loc = get_best_legend_loc(ax, all_x, all_y)
        leg = ax.legend(loc=legend_loc, frameon=True, fontsize=tick_size)
        leg.get_frame().set_alpha(0.96)

        plt.tight_layout()

        if base_save_path:
            p = base_save_path
            if mode == "both":
                p = p.replace(".png", "_normalized_SCAT.png" if is_normalized else "_standard_SCAT.png")
            Path(os.path.dirname(p)).mkdir(parents=True, exist_ok=True)
            fig.savefig(p, dpi=300, bbox_inches='tight', transparent=True)
            print(f"✓ Scatter saved: {p}")

        return fig, ax

    # --- MAIN MODE LOGIC ---
    if mode == "standard":
        return _create_single_scatter(df, x_col, y_col, False, save_path)
    elif mode == "normalized":
        if norm_x_col not in df.columns or norm_y_col not in df.columns:
            print(f"⚠️ Norm cols missing ({norm_x_col}, {norm_y_col}). Using standard.")
            return _create_single_scatter(df, x_col, y_col, False, save_path)
        return _create_single_scatter(df, norm_x_col, norm_y_col, True, save_path)
    # ... inside plot_scatter_dynamic ...

    elif mode == "both":
        results = {}

        # 1. Standard Chart (Always runs)
        results["standard"] = _create_single_scatter(
            df, x_col, y_col, is_normalized=False, base_save_path=save_path
        )

        # 2. Normalized Chart (With Hybrid Fallback)
        # Check if intended normalized columns exist
        final_norm_x = norm_x_col if norm_x_col in df.columns else x_col
        final_norm_y = norm_y_col if norm_y_col in df.columns else y_col

        # Only skip if we effectively fell back to standard for BOTH (avoid duplicate chart)
        if final_norm_x == x_col and final_norm_y == y_col:
             print(f"ℹ️ No normalized data found for either {x_col} or {y_col}. Skipping normalized chart.")
        else:
            # If we are falling back, print a helpful note
            if final_norm_x == x_col: print(f"  Note: Using standard {x_col} for X-axis (Normalized version missing)")
            if final_norm_y == y_col: print(f"  Note: Using standard {y_col} for Y-axis (Normalized version missing)")

            results["normalized"] = _create_single_scatter(
                df, final_norm_x, final_norm_y, is_normalized=True, base_save_path=save_path
            )

        return results




# ==================================================================================
# SCRIPT EXECUTION
# ==================================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("BANK PERFORMANCE REPORT GENERATOR")
    print("=" * 80)

    # Load configuration
    cfg = load_config()
    subject_bank_cert = cfg["subject_bank_cert"]

    # 1) Find latest processed Excel and set output roots
    excel_file = find_latest_excel_file(cfg["output_dir"])
    if not excel_file:
        print("ERROR: No Excel files found in output/. Run the pipeline first.")
        exit(1)

    output_root = Path(excel_file).parent.resolve()
    peers_dir = output_root / "Peers"
    peers_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found latest file: {excel_file}")
    print(f"File created: {datetime.fromtimestamp(os.path.getmtime(excel_file)).strftime('%Y-%m-%d %H:%M:%S')}")

    # 2) Load required sheets from Excel
    print("\nLoading data sheets...")


    proc_df_with_peers = pd.read_excel(excel_file, sheet_name="FDIC_Data")
    proc_df_with_peers["REPDTE"] = pd.to_datetime(proc_df_with_peers["REPDTE"])
    print(f"  ✓ Processed_Data: {len(proc_df_with_peers):,} rows")

    # Load rolling 8Q data if available (for scatter plots)
    try:
        with pd.ExcelFile(excel_file) as xls:

            # 8Q averages sheet: pick the one that starts with Averages_8Q*
            roll_sheet = next((s for s in xls.sheet_names if s.lower().startswith("averages_8q")), None)
            if not roll_sheet:
                raise FileNotFoundError("8Q average sheet not found (expects sheet name starting with 'Averages_8Q').")
            rolling8q_df = pd.read_excel(xls, sheet_name=roll_sheet)
            if "CERT" in rolling8q_df.columns:
                rolling8q_df["CERT"] = pd.to_numeric(rolling8q_df["CERT"], errors="coerce").astype("Int64")
                rolling8q_df["REPDTE"] = pd.to_datetime(rolling8q_df["REPDTE"])
                print(f"  ✓ Rolling_8Q: {len(rolling8q_df):,} rows")
    except Exception:
        rolling8q_df = proc_df_with_peers.copy()
        print("  ⚠ Rolling_8Q not found, using Processed_Data")

    # Get latest date
    as_of_date = proc_df_with_peers["REPDTE"].max()
    print(f"\nData as of: {as_of_date.strftime('%Y-%m-%d')}")

    # 3) Generate all reports
    print("\n" + "=" * 80)
    print("GENERATING REPORTS")
    print("=" * 80)

    generate_reports(
        proc_df_with_peers=proc_df_with_peers,
        subject_bank_cert=subject_bank_cert,
        output_dir=output_root,
        as_of_date=as_of_date,
    )

    print("\n" + "=" * 80)
    print("REPORT GENERATION COMPLETE")
    print(f"Output directory: {peers_dir}")
    print("=" * 80)
    print("Loading 'df' into variable explorer...")
    globals()['proc_df_with_peers'] = proc_df_with_peers
    globals()['rolling8q_df'] = rolling8q_df
