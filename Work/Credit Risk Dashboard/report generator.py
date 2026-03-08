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
    """Load configuration from environment variables with sensible defaults."""
    # Best-effort .env loading
    try:
        from dotenv import load_dotenv
        _env = Path(__file__).parent.resolve() / ".env"
        if _env.exists():
            load_dotenv(_env)
    except ImportError:
        pass

    _mspbna = os.getenv("MSPBNA_CERT")
    _msbna = os.getenv("MSBNA_CERT")
    if not _mspbna or not _msbna:
        raise ValueError(
            "MSPBNA_CERT and MSBNA_CERT environment variables are required.\n"
            "  export MSPBNA_CERT=34221\n"
            "  export MSBNA_CERT=32992"
        )
    mspbna_cert = int(_mspbna)
    ms_combined_cert = int(os.getenv("MS_COMBINED_CERT", "88888"))

    return {
        'subject_bank_cert': mspbna_cert,
        'mspbna_cert': mspbna_cert,
        'msbna_cert': int(_msbna),
        'ms_combined_cert': ms_combined_cert,
        'output_dir': os.getenv("OUTPUT_DIR", "output"),
        'report_view': os.getenv("REPORT_VIEW", "ALL_BANKS"),
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
    subject_bank_cert: int = 19977,
    show_both_peer_groups: bool = True
) -> Tuple[Optional[plt.Figure], Optional[plt.Axes]]:
    """
    Creates a chart comparing TTM NCO Rate and NPL-to-Gross Loans Rate
    with golden yellow for subject bank and improved labeling strategy.

    Args:
        proc_df_with_peers (pd.DataFrame): Processed data with all banks and peers
        subject_bank_cert (int): CERT number of the subject bank
        show_both_peer_groups (bool): Whether to show both peer groups

    Returns:
        Tuple[Optional[plt.Figure], Optional[plt.Axes]]: Chart figure and axes
    """
    # Define entities to plot based on options
    entities_to_plot = [subject_bank_cert]  # Always include subject bank

    if show_both_peer_groups:
        entities_to_plot.extend([99999, 99998])  # Both peer groups
        colors = {
            subject_bank_cert: '#F7A81B',  # Golden yellow for subject bank
            99999: '#5B9BD5',              # Blue for all peers
            99998: '#70AD47'               # Green for selective peers
        }
        entity_names = {
            subject_bank_cert: "MSPBNA",
            99999: "All Peers",
            99998: "Peers (Ex. F&V)"
        }
    else:
        entities_to_plot.append(99998)  # Only selective peer group
        colors = {
            subject_bank_cert: '#F7A81B',  # Golden yellow for subject bank
            99998: '#5B9BD5'               # Blue for selective peers
        }
        entity_names = {
            subject_bank_cert: "MSPBNA",
            99998: "Peers (Ex. F&V)"
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
            npl_rate = entity_data['NPL_to_Gross_Loans_Rate'].fillna(0) / 100  # Convert to decimal

            line_style = line_styles[i % len(line_styles)]
            ax2.plot(x_positions, npl_rate, color=colors[cert],
                    linestyle=line_style, marker='o', linewidth=2.5,
                    label=f'{label} NPL-to-Book', markersize=5)

    # Format the chart
    ax.set_xlabel('Reporting Period', fontsize=12, fontweight='bold')
    ax.set_ylabel('TTM NCO Rate', fontsize=12, fontweight='bold', color='black')
    ax2.set_ylabel('NPL-to-Gross Loans Rate', fontsize=12, fontweight='bold', color='black')

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
                npl_rate = entity_data['NPL_to_Gross_Loans_Rate'].fillna(0) / 100
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
    """
    Generates a properly formatted HTML table for email.

    Args:
        df (pd.DataFrame): DataFrame with metrics comparison data
        report_date (datetime): The reporting date

    Returns:
        str: Complete HTML string for email
    """
    # Format the report date
    formatted_date = report_date.strftime('%B %d, %Y') if hasattr(report_date, 'strftime') else str(report_date)

    html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            .email-container {{
                background-color: white;
                padding: 30px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                max-width: 900px;
                margin: 0 auto;
            }}
            .header {{
                text-align: center;
                margin-bottom: 30px;
                color: #2c3e50;
            }}
            .header h2 {{
                margin: 0;
                font-size: 24px;
                font-weight: 600;
            }}
            .header p {{
                margin: 5px 0 0 0;
                color: #7f8c8d;
                font-size: 14px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                font-size: 12px;
            }}
            th {{
                background-color: #34495e;
                color: white;
                padding: 12px 8px;
                text-align: center;
                font-weight: 600;
                border: 1px solid #2c3e50;
            }}
            td {{
                padding: 10px 8px;
                text-align: center;
                border: 1px solid #bdc3c7;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #e3f2fd;
            }}
            .metric-name {{
                text-align: left !important;
                font-weight: 500;
                color: #2c3e50;
            }}
            .mspbna-value {{
                background-color: #fff3cd;
                font-weight: 600;
                color: #856404;
            }}
            .positive {{
                color: #d32f2f;
                font-weight: 600;
            }}
            .negative {{
                color: #388e3c;
                font-weight: 600;
            }}
            .neutral {{
                color: #5d4037;
            }}
            .footer {{
                margin-top: 30px;
                padding-top: 20px;
                border-top: 2px solid #ecf0f1;
                font-size: 12px;
                color: #7f8c8d;
                text-align: center;
            }}
            .legend {{
                margin: 20px 0;
                padding: 15px;
                background-color: #f8f9fa;
                border-radius: 5px;
                font-size: 11px;
                color: #495057;
            }}
            .legend strong {{
                color: #2c3e50;
            }}
        </style>
    </head>
    <body>
        <div class="email-container">
            <div class="header">
                <h2>Credit Risk Metrics Comparison</h2>
                <p>MSPBNA vs Peer Groups | Report Date: {formatted_date}</p>
            </div>

            <table>
                <thead>
                    <tr>
                        <th style="width: 30%;">Credit Metric</th>
                        <th style="width: 12%;">MSPBNA</th>
                        <th style="width: 12%;">All Peers</th>
                        <th style="width: 15%;">Core PB</th>
                        <th style="width: 15%;">Diff vs<br>All Peers</th>
                        <th style="width: 16%;">Diff vs<br>Core PB</th>
                    </tr>
                </thead>
                <tbody>
    """

    # Add table rows
    for _, row in df.iterrows():
        # Determine styling for difference columns
        diff_all_class = get_diff_class(row['Diff vs All'])
        diff_core_class = get_diff_class(row['Diff vs Core PB'])

        html += f"""
                    <tr>
                        <td class="metric-name">{row['Metric']}</td>
                        <td class="mspbna-value">{row['MSPBNA']}</td>
                        <td>{row['All Peers']}</td>
                        <td>{row['Core PB']}</td>
                        <td class="{diff_all_class}">{row['Diff vs All']}</td>
                        <td class="{diff_core_class}">{row['Diff vs Core PB']}</td>
                    </tr>
        """

    html += """
                </tbody>
            </table>

            <div class="legend">
                <strong>Legend:</strong><br>
                • <span style="color: #d32f2f; font-weight: 600;">Red values</span>: MSPBNA performs worse than peer group (higher is worse for most credit metrics)<br>
                • <span style="color: #388e3c; font-weight: 600;">Green values</span>: MSPBNA performs better than peer group (lower is better for most credit metrics)<br>
                • <strong>Core PB</strong>: Core Private Bank peer composite<br>
                • All values are as of the latest available quarter
            </div>

            <div class="footer">
                <p>This report is generated automatically from FDIC call report data.<br>
                For questions or additional analysis, please contact the Credit Risk team.</p>
            </div>
        </div>
    </body>
    </html>
    """

    return html

def generate_credit_metrics_email_table(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 34221
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Build the email HTML table; ASSET & LNLS are shown as $ (millions), others as %.
    """
    important_metrics = {
        "ASSET": "Assets",
        "LNLS": "Gross Loans",
        "CI_to_Capital_Risk": "C&I to Capital Risk (%)",
        "CRE_Concentration_Capital_Risk": "CRE Concentration Risk (%)",
        "IDB_CRE_Growth_TTM": "CRE Growth TTM (%)",
        "IDB_CRE_Growth_36M": "CRE Growth 36M (%)",
        "TTM_NCO_Rate": "TTM NCO Rate (%)",
        "NPL_to_Gross_Loans_Rate": "NPL to Gross Loans (%)",
        "Allowance_to_Gross_Loans_Rate": "Allowance to Gross Loans (%)",
        "TTM_Past_Due_Rate": "TTM Past Due Rate (%)",
        "TTM_PD30_Rate": "TTM Past Due (30-90 Days) Rate (%)",
        "TTM_PD90_Rate": "TTM Past Due (>90 Days) Rate (%)",
    }

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    subj = latest_data[latest_data["CERT"] == subject_bank_cert]
    peers = latest_data[latest_data["CERT"] == 90003]       # All Peers composite
    peers_ex = latest_data[latest_data["CERT"] == 90001]    # Core PB composite

    if subj.empty or peers.empty or peers_ex.empty:
        print("Missing data for one or more entities")
        return None, None

    subj = subj.iloc[0]
    peers = peers.iloc[0]
    peers_ex = peers_ex.iloc[0]

    rows = []
    for code, disp in important_metrics.items():
        if code not in subj.index:
            continue

        v_subj = subj.get(code, np.nan)
        v_all  = peers.get(code, np.nan)
        v_sel  = peers_ex.get(code, np.nan)

        # diffs
        d_all = (v_subj - v_all) if not pd.isna(v_all) else np.nan
        d_sel = (v_subj - v_sel) if not pd.isna(v_sel) else np.nan

        if code in CURRENCY_MM_CODES:
            # values are already measured in millions
            subj_s = _fmt_money_millions(v_subj)
            all_s  = _fmt_money_millions(v_all)
            sel_s  = _fmt_money_millions(v_sel)
            diff_all_s = _fmt_money_millions_with_sign(d_all)
            diff_sel_s = _fmt_money_millions_with_sign(d_sel)
        else:
            subj_s = _fmt_percent_auto(v_subj)
            all_s  = _fmt_percent_auto(v_all)
            sel_s  = _fmt_percent_auto(v_sel)
            diff_all_s = _fmt_percent_auto(d_all)
            diff_sel_s = _fmt_percent_auto(d_sel)

        rows.append({
            "Metric": disp,
            "MSPBNA": subj_s,
            "All Peers": all_s,
            "Core PB": sel_s,
            "Diff vs All": diff_all_s,
            "Diff vs Core PB": diff_sel_s,
        })

    df = pd.DataFrame(rows)
    html = generate_html_email_table(df, latest_date)
    return html, df


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
        subject_bank_cert (int): CERT number of the subject bank
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
    certs_to_include = [subject_bank_cert, 90001, 90002, 90003]

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

        # Special styling for MSPBNA
        row_class = "mspbna-row" if cert == subject_bank_cert else ""

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

def _to_decimal(series: pd.Series) -> pd.Series:
    """Robustly convert ambiguous % series to decimal for plotting.
    Rule: if median >= 0.01 (i.e., values look like percent points: 0.09, 0.8, 35.2), divide by 100.
          if median < 0.01, assume already decimal (0.0033) and return as-is.
    """
    s = pd.to_numeric(series, errors="coerce")
    med = s.dropna().abs().median()
    if pd.isna(med):
        return s
    return s/100.0 if med >= 0.01 else s

def _fmt_pct(x: float) -> str:
    return f"{x:.2%}"

def _place_titles(fig: plt.Figure, title: str, subtitle: str | None, title_size: int, subtitle_size: int):
    # Title at ~0.97, subtitle at ~0.935 of figure height; won't collide with plot area.
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

# ==================================================================================
# B4: ALIAS HELPER — inject alias rows for plot-time compatibility
# ==================================================================================

def build_plot_df_with_alias(
    df: pd.DataFrame,
    alias_map: Dict[int, int],
) -> pd.DataFrame:
    """Duplicate rows for *source_cert* and rewrite CERT to *alias_cert*.

    Parameters
    ----------
    df : DataFrame with a ``CERT`` column.
    alias_map : ``{alias_cert: source_cert, ...}``
        e.g. ``{99999: 90003, 99998: 90001}``

    Returns
    -------
    DataFrame — original data *plus* aliased rows.  No metric recomputation.
    """
    parts = [df]
    for alias_cert, source_cert in alias_map.items():
        src = df[df["CERT"] == source_cert].copy()
        if src.empty:
            continue
        src["CERT"] = alias_cert
        # Update NAME so legends are readable
        src["NAME"] = src["NAME"].astype(str).str.replace(
            r"^AVG:\s*", "AVG: ", regex=True
        )
        parts.append(src)
    return pd.concat(parts, ignore_index=True)


# ==================================================================================
# B7: 3-GROUP CREDIT HTML TABLES (browser-rich CSS)
# ==================================================================================

_CREDIT_TABLE_METRICS_STANDARD = {
    "ASSET": ("Assets", "$M"),
    "LNLS": ("Gross Loans", "$M"),
    "TTM_NCO_Rate": ("TTM NCO Rate", "%"),
    "NPL_to_Gross_Loans_Rate": ("NPL to Gross Loans", "%"),
    "Allowance_to_Gross_Loans_Rate": ("Allowance to Gross Loans", "%"),
    "TTM_Past_Due_Rate": ("TTM Past Due Rate", "%"),
    "Risk_Adj_Allowance_Coverage": ("Risk-Adj Allowance Coverage", "%"),
    "Top_House_Delinquency_Rate": ("Total Delinquency Rate", "%"),
    "Tier_1_Leverage_Ratio": ("Tier 1 Leverage Ratio", "%"),
    "Total_Assets": ("Total Assets", "$M"),
    "Net_Charge_Off_Rate": ("Net Charge-Off Rate", "%"),
}

_CREDIT_TABLE_METRICS_NORMALIZED = {
    "Norm_NCO_Rate": ("Normalized NCO Rate", "%"),
    "Norm_Nonaccrual_Rate": ("Normalized Nonaccrual Rate", "%"),
    "Norm_Delinquency_Rate": ("Normalized Delinquency Rate", "%"),
    "Norm_ACL_Coverage": ("Normalized ACL Coverage", "%"),
    "Norm_Gross_Loans": ("Normalized Gross Loans", "$M"),
    "Norm_Total_NCO": ("Normalized Total NCOs", "$M"),
    "Norm_Total_Nonaccrual": ("Normalized Total Nonaccruals", "$M"),
    "Norm_SBL_Composition": ("SBL % of Norm Loans", "%"),
    "Norm_Fund_Finance_Composition": ("Fund Finance % of Norm Loans", "%"),
    "Norm_Exclusion_Pct": ("Exclusion % of Gross Loans", "%"),
    "Norm_Loan_Yield": ("Normalized Loan Yield", "%"),
    "Norm_Loss_Adj_Yield": ("Normalized Loss-Adj Yield", "%"),
}

CURRENCY_LIKE = {"ASSET", "LNLS", "Norm_Gross_Loans", "Norm_Total_NCO",
                 "Norm_Total_Nonaccrual", "Total_Assets", "Excluded_Balance"}


def _fmt_val(val, code: str) -> str:
    """Format a metric value for HTML display."""
    if pd.isna(val):
        return "N/A"
    if code in CURRENCY_LIKE:
        return f"${val / 1000:,.0f}M" if abs(val) >= 1 else "N/A"
    # percentage: if value looks like fraction (< 1), display as %
    v = float(val)
    if abs(v) < 1:
        return f"{v:.2%}"
    return f"{v:.2f}%"


def generate_3group_credit_table(
    proc_df: pd.DataFrame,
    subject_cert: int,
    peer_certs: List[int],
    peer_labels: List[str],
    metrics: Dict[str, tuple],
    title: str,
    report_date,
) -> str:
    """Build browser-rich HTML credit table with 3 peer groupings."""
    formatted_date = (
        report_date.strftime("%B %d, %Y")
        if hasattr(report_date, "strftime")
        else str(report_date)
    )

    latest = proc_df[proc_df["REPDTE"] == proc_df["REPDTE"].max()]
    subj = latest[latest["CERT"] == subject_cert]
    if subj.empty:
        return f"<p>No data for subject CERT {subject_cert}</p>"
    subj = subj.iloc[0]

    peer_rows = []
    for pc in peer_certs:
        pr = latest[latest["CERT"] == pc]
        peer_rows.append(pr.iloc[0] if not pr.empty else None)

    # --- Build HTML ---
    hdr_cols = "".join(f'<th class="peer-col">{lbl}</th>' for lbl in peer_labels)
    diff_cols_hdr = "".join(f'<th class="diff-col">vs {lbl}</th>' for lbl in peer_labels)

    rows_html = ""
    for code, (display, _unit) in metrics.items():
        sv = subj.get(code, np.nan)
        sv_s = _fmt_val(sv, code)
        cells = ""
        for pr in peer_rows:
            pv = pr.get(code, np.nan) if pr is not None else np.nan
            cells += f'<td class="value-cell">{_fmt_val(pv, code)}</td>'
            # diff
            if pd.notna(sv) and pd.notna(pv) and code not in CURRENCY_LIKE:
                d = float(sv) - float(pv)
                cls = "positive" if d > 0.0005 else ("negative" if d < -0.0005 else "neutral")
                if abs(d) < 1:
                    ds = f"{d:+.2%}"
                else:
                    ds = f"{d:+.2f}%"
            else:
                cls, ds = "neutral", "—"
            cells += f'<td class="{cls}">{ds}</td>'

        rows_html += f"""<tr>
            <td class="metric-name">{display}</td>
            <td class="subject-value">{sv_s}</td>
            {cells}
        </tr>"""

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>
body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; background: #f0f2f5; }}
.container {{ max-width: 1100px; margin: 30px auto; background: #fff; border-radius: 12px;
    box-shadow: 0 4px 20px rgba(0,0,0,.08); overflow: hidden; }}
.header {{ background: linear-gradient(135deg, #1a237e 0%, #283593 100%); color: #fff;
    padding: 24px 32px; }}
.header h2 {{ margin: 0; font-size: 22px; }} .header p {{ margin: 6px 0 0; opacity: .8; font-size: 13px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th {{ background: #37474f; color: #fff; padding: 12px 10px; text-align: center; font-weight: 600; }}
th:first-child {{ text-align: left; }}
td {{ padding: 10px; text-align: center; border-bottom: 1px solid #e0e0e0; }}
.metric-name {{ text-align: left; font-weight: 500; color: #263238; }}
.subject-value {{ background: #fff8e1; font-weight: 700; color: #6d4c00; }}
.peer-col {{ background: #455a64; }} .diff-col {{ background: #546e7a; font-size: 11px; }}
.positive {{ color: #c62828; font-weight: 600; }}
.negative {{ color: #2e7d32; font-weight: 600; }}
.neutral  {{ color: #5d4037; }}
tr:nth-child(even) {{ background: #fafafa; }}
tr:hover {{ background: #e8eaf6; }}
.footer {{ padding: 16px 32px; font-size: 11px; color: #78909c; border-top: 2px solid #e0e0e0; text-align: center; }}
</style></head><body>
<div class="container">
<div class="header"><h2>{title}</h2><p>MSPBNA vs Peer Groups | {formatted_date}</p></div>
<table><thead><tr>
  <th>Credit Metric</th><th class="subject-value">MSPBNA</th>
  {hdr_cols}{diff_cols_hdr}
</tr></thead><tbody>
{rows_html}
</tbody></table>
<div class="footer">Auto-generated from FDIC Call Report data. Contact Credit Risk for questions.</div>
</div></body></html>"""


# ==================================================================================
# B9: FAIL-SAFE COMPOSITE SYNTHESIS (latest REPDTE only)
# ==================================================================================

# Peer group membership for fail-safe synthesis (mirrors upstream PEER_GROUPS)
_FAILSAFE_GROUPS = {
    90001: {"name": "Core Private Bank Peers", "display_order": 1},
    90002: {"name": "MSPBNA + Wealth", "display_order": 2},
    90003: {"name": "Full Peer Universe", "display_order": 3},
    90004: {"name": "Core Private Bank (Normalized)", "display_order": 4},
    90005: {"name": "MSPBNA + Wealth (Normalized)", "display_order": 5},
    90006: {"name": "Full Peer Universe (Normalized)", "display_order": 6},
}


def _failsafe_composites(
    df: pd.DataFrame,
    required_certs: List[int],
    cfg: Dict[str, Any],
) -> pd.DataFrame:
    """If any composite CERTs are missing at latest REPDTE, synthesise them."""
    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date]
    present = set(latest["CERT"].unique())
    missing = [c for c in required_certs if c not in present]
    if not missing:
        return df

    print(f"WARNING: Missing composite CERTs at {latest_date}: {missing}")

    mspbna = cfg["mspbna_cert"]
    msbna = cfg["msbna_cert"]
    _member_map = {
        90001: [mspbna, 33124, 57565],
        90002: [mspbna, 33124, 57565],
        90003: [msbna, 33124, 57565, 628, 3511, 7213, 3510],
        90004: [mspbna, 33124, 57565],
        90005: [mspbna, 33124, 57565],
        90006: [msbna, 33124, 57565, 628, 3511, 7213, 3510],
    }

    numeric_cols = df.select_dtypes(include=np.number).columns.drop("CERT", errors="ignore")
    synth = []
    for cert in missing:
        members = _member_map.get(cert, [])
        subset = latest[latest["CERT"].isin(members)]
        if subset.empty:
            print(f"  Cannot synthesize {cert}: no member data")
            continue
        row = subset[numeric_cols].mean().to_frame().T
        row["CERT"] = cert
        row["REPDTE"] = latest_date
        row["NAME"] = f"AVG: {_FAILSAFE_GROUPS.get(cert, {}).get('name', f'Group {cert}')}"
        row["HQ_STATE"] = "AVG"
        synth.append(row)
        print(f"  Synthesized {cert} from {len(subset)} members (latest REPDTE only)")

    if synth:
        df = pd.concat([df] + synth, ignore_index=True)
    return df


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
    # Macro table: pass Short Names; we'll map to Series ID via FRED_Descriptions
    fred_short_names: Optional[List[str]] = None,
) -> None:
    """
    End-to-end runner:
    - finds the latest processed Excel file in output/,
    - outputs: Peers/charts/, Peers/scatter/, Peers/tables/
    - generates normalized credit chart, dual scatters, 3-group HTML tables
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
    report_view = cfg.get("report_view", "ALL_BANKS")
    ms_combined_cert = cfg.get("ms_combined_cert", 88888)

    # 1) Find latest processed Excel and set output roots
    excel_file = find_latest_excel_file(cfg["output_dir"])
    if not excel_file:
        print("ERROR: No Excel files found in output/. Run the pipeline first.")
        return

    output_root = Path(excel_file).parent.resolve()
    # B1: Output directory routing
    peers_root = output_root / "Peers"
    charts_dir = peers_root / "charts"
    scatter_dir = peers_root / "scatter"
    tables_dir = peers_root / "tables"
    for d in (charts_dir, scatter_dir, tables_dir):
        d.mkdir(parents=True, exist_ok=True)

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

        # B9: Fail-safe composite synthesis
        proc_df_with_peers = _failsafe_composites(
            proc_df_with_peers, [90001, 90002, 90003, 90004, 90005, 90006], cfg
        )

        # ------------------------------------------------------------------
        # B7: 3-GROUP HTML TABLES (standard + normalized)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING 3-GROUP CREDIT TABLES (HTML)")
        print("-" * 60)

        report_date = proc_df_with_peers["REPDTE"].max()

        std_table_html = generate_3group_credit_table(
            proc_df=proc_df_with_peers,
            subject_cert=subject_bank_cert,
            peer_certs=[90001, 90002, 90003],
            peer_labels=["Core PB", "MSPBNA+Wealth", "All Peers"],
            metrics=_CREDIT_TABLE_METRICS_STANDARD,
            title="Credit Risk Metrics — Standard View",
            report_date=report_date,
        )
        std_path = tables_dir / f"{base}_credit_table_standard_{stamp}.html"
        with open(std_path, "w", encoding="utf-8") as f:
            f.write(std_table_html)
        print(f"✓ Standard credit table saved: {std_path}")

        norm_table_html = generate_3group_credit_table(
            proc_df=proc_df_with_peers,
            subject_cert=subject_bank_cert,
            peer_certs=[90004, 90005, 90006],
            peer_labels=["Core PB Norm", "MSPBNA+Wealth Norm", "All Peers Norm"],
            metrics=_CREDIT_TABLE_METRICS_NORMALIZED,
            title="Credit Risk Metrics — Normalized (Ex-Commercial/Ex-Consumer)",
            report_date=report_date,
        )
        norm_path = tables_dir / f"{base}_credit_table_normalized_{stamp}.html"
        with open(norm_path, "w", encoding="utf-8") as f:
            f.write(norm_table_html)
        print(f"✓ Normalized credit table saved: {norm_path}")

        # ------------------------------------------------------------------
        # B5: NORMALIZED-ONLY CREDIT CHART
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING NORMALIZED CREDIT DETERIORATION CHART")
        print("-" * 60)

        # Determine normalized line metric
        norm_line = None
        for cand in ["Norm_Nonaccrual_Rate", "Norm_NPL_to_Gross_Loans_Rate"]:
            if cand in proc_df_with_peers.columns:
                norm_line = cand
                break
        if norm_line is None:
            avail = [c for c in proc_df_with_peers.columns if c.startswith("Norm_")]
            raise ValueError(f"No Norm_Nonaccrual_Rate found. Available Norm_* columns: {avail}")

        if "Norm_NCO_Rate" not in proc_df_with_peers.columns:
            raise ValueError("Norm_NCO_Rate column missing from FDIC_Data — cannot create normalized chart.")

        # Alias: 99999 = All Peers Norm (90006), 99998 = Core PB Norm (90004)
        norm_df_plot = build_plot_df_with_alias(
            proc_df_with_peers, {99999: 90006, 99998: 90004}
        )

        fig, ax = create_credit_deterioration_chart_ppt(
            proc_df_with_peers=norm_df_plot,
            subject_bank_cert=subject_bank_cert,
            start_date=start_date,
            bar_metric="Norm_NCO_Rate",
            line_metric=norm_line,
            bar_entities=[subject_bank_cert, 99999, 99998],
            line_entities=[subject_bank_cert, 99999, 99998],
            figsize=credit_figsize,
            title_size=title_size,
            axis_label_size=axis_label_size,
            tick_size=tick_size,
            tag_size=tag_size,
            legend_fontsize=legend_fontsize,
            economist_style=True,
            custom_title=credit_title or "Normalized NCO Rate (bars) vs Nonaccrual Rate (lines)",
            save_path=str(charts_dir / f"{base}_credit_chart_normalized_{stamp}.png"),
        )
        print(f"✓ Normalized chart saved: {charts_dir / f'{base}_credit_chart_normalized_{stamp}.png'}")

        # ------------------------------------------------------------------
        # B6: DUAL SCATTERS — standard + normalized
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING SCATTER PLOTS (8Q AVG)")
        print("-" * 60)

        # B7: Conditionally exclude MS_COMBINED_CERT from all-bank outputs
        if report_view == "MSPBNA_WEALTH_NORM":
            proc_df_with_peers = proc_df_with_peers[
                proc_df_with_peers["CERT"] != ms_combined_cert
            ].copy()
            rolling8q_df = rolling8q_df[
                rolling8q_df["CERT"] != ms_combined_cert
            ].copy()
            print(f"REPORT_VIEW={report_view}: excluded MS_COMBINED_CERT ({ms_combined_cert}) from all-bank outputs")

        # --- B6.1: Standard scatters ---
        std_scatter_df = rolling8q_df.copy()
        for c in ["NPL_to_Gross_Loans_Rate", "TTM_NCO_Rate", "TTM_Past_Due_Rate"]:
            if c in std_scatter_df.columns:
                std_scatter_df[c] = pd.to_numeric(std_scatter_df[c], errors="coerce")

        # Inject alias points: 99999 = All Peers (90003), 99998 = Core PB (90001)
        has_90001 = 90001 in set(std_scatter_df["CERT"].dropna().astype(int))
        has_90003 = 90003 in set(std_scatter_df["CERT"].dropna().astype(int))

        if has_90001 or has_90003:
            alias_map_std = {}
            if has_90003:
                alias_map_std[99999] = 90003
            if has_90001:
                alias_map_std[99998] = 90001
            std_scatter_df = build_plot_df_with_alias(std_scatter_df, alias_map_std)
        else:
            # Source latest composite values from FDIC_Data
            latest_date = proc_df_with_peers["REPDTE"].max()
            for alias_cert, source_cert in [(99999, 90003), (99998, 90001)]:
                src = proc_df_with_peers[
                    (proc_df_with_peers["CERT"] == source_cert) &
                    (proc_df_with_peers["REPDTE"] == latest_date)
                ].copy()
                if not src.empty:
                    src["CERT"] = alias_cert
                    std_scatter_df = pd.concat([std_scatter_df, src], ignore_index=True)

        s1_path = scatter_dir / f"{base}_scatter_nco_vs_npl_standard_{stamp}.png"
        plot_scatter_dynamic(
            df=std_scatter_df,
            x_col="NPL_to_Gross_Loans_Rate",
            y_col="TTM_NCO_Rate",
            subject_cert=subject_bank_cert,
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
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
            save_path=str(s1_path),
        )
        print(f"✓ Standard scatter saved: {s1_path}")

        s2_path = scatter_dir / f"{base}_scatter_pd_vs_npl_standard_{stamp}.png"
        plot_scatter_dynamic(
            df=std_scatter_df,
            x_col="NPL_to_Gross_Loans_Rate",
            y_col="TTM_Past_Due_Rate",
            subject_cert=subject_bank_cert,
            use_alt_peer_avg=False,
            show_peers_avg_label=True,
            show_idb_label=True,
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
            save_path=str(s2_path),
        )
        print(f"✓ Standard scatter saved: {s2_path}")

        # --- B6.2: Normalized scatters ---
        norm_scatter_df = rolling8q_df.copy()
        # Determine x column
        norm_x = None
        for cand in ["Norm_Nonaccrual_Rate", "Norm_NPL_to_Gross_Loans_Rate"]:
            if cand in norm_scatter_df.columns:
                norm_x = cand
                break
        if norm_x and "Norm_NCO_Rate" in norm_scatter_df.columns:
            for c in [norm_x, "Norm_NCO_Rate"]:
                norm_scatter_df[c] = pd.to_numeric(norm_scatter_df[c], errors="coerce")

            # Inject alias: 99999 = 90006, 99998 = 90004
            has_90004 = 90004 in set(norm_scatter_df["CERT"].dropna().astype(int))
            has_90006 = 90006 in set(norm_scatter_df["CERT"].dropna().astype(int))
            if has_90004 or has_90006:
                alias_map_norm = {}
                if has_90006:
                    alias_map_norm[99999] = 90006
                if has_90004:
                    alias_map_norm[99998] = 90004
                norm_scatter_df = build_plot_df_with_alias(norm_scatter_df, alias_map_norm)
            else:
                latest_date = proc_df_with_peers["REPDTE"].max()
                for alias_cert, source_cert in [(99999, 90006), (99998, 90004)]:
                    src = proc_df_with_peers[
                        (proc_df_with_peers["CERT"] == source_cert) &
                        (proc_df_with_peers["REPDTE"] == latest_date)
                    ].copy()
                    if not src.empty:
                        src["CERT"] = alias_cert
                        norm_scatter_df = pd.concat([norm_scatter_df, src], ignore_index=True)

            ns1_path = scatter_dir / f"{base}_scatter_nco_vs_npl_normalized_{stamp}.png"
            plot_scatter_dynamic(
                df=norm_scatter_df,
                x_col=norm_x,
                y_col="Norm_NCO_Rate",
                subject_cert=subject_bank_cert,
                use_alt_peer_avg=False,
                show_peers_avg_label=True,
                show_idb_label=True,
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
                save_path=str(ns1_path),
            )
            print(f"✓ Normalized scatter saved: {ns1_path}")

            # Optional second normalized scatter
            norm_y2 = None
            for cand in ["Norm_Past_Due_Rate", "Norm_Delinquency_Rate"]:
                if cand in norm_scatter_df.columns:
                    norm_y2 = cand
                    break
            if norm_y2:
                norm_scatter_df[norm_y2] = pd.to_numeric(norm_scatter_df[norm_y2], errors="coerce")
                ns2_path = scatter_dir / f"{base}_scatter_pd_vs_npl_normalized_{stamp}.png"
                plot_scatter_dynamic(
                    df=norm_scatter_df,
                    x_col=norm_x,
                    y_col=norm_y2,
                    subject_cert=subject_bank_cert,
                    use_alt_peer_avg=False,
                    show_peers_avg_label=True,
                    show_idb_label=True,
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
                    save_path=str(ns2_path),
                )
                print(f"✓ Normalized scatter (2nd) saved: {ns2_path}")
            else:
                print("WARNING: No Norm_Past_Due_Rate or Norm_Delinquency_Rate found — skipping 2nd normalized scatter")
        else:
            print("WARNING: Normalized scatter columns not available — skipping normalized scatters")

        # ------------------------------------------------------------------
        # FRED MACRO TABLE (Short Name -> Series ID via FRED_Descriptions)
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING FRED MACRO TABLE")
        print("-" * 60)
        try:
            fred_html, fred_df = build_fred_macro_table(excel_file, list(fred_short_names))
            fred_path = tables_dir / f"{base}_fred_table_{stamp}.html"
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
        print(f"Output directories:")
        print(f"  Charts:  {charts_dir}")
        print(f"  Scatter: {scatter_dir}")
        print(f"  Tables:  {tables_dir}")
        print(f"Subject bank CERT: {subject_bank_cert} (MSPBNA)")
        print(f"Report view: {report_view}")

    except Exception as e:
        import traceback
        print(f"ERROR: An unexpected error occurred: {e}")
        traceback.print_exc()
    finally:
        plt.close("all")








def create_credit_deterioration_chart_ppt(
    proc_df_with_peers: pd.DataFrame,
    subject_bank_cert: int = 19977,
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

    # ---------- palette / entities
    GOLD, BLUE, PURPLE = "#F7A81B", "#4C78A8", "#9C6FB6"
    default_entities = [subject_bank_cert, 99999, 99998]
    bar_entities  = bar_entities  or default_entities
    line_entities = line_entities or list(bar_entities)
    names  = {subject_bank_cert:"MSPBNA", 99999:"All Peers", 99998:"Peers (Ex. F&V)"}
    colors = {subject_bank_cert:GOLD,   99999:BLUE,       99998:PURPLE}

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

    # ---------- helpers
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

            # Try desired spot. If it would overlap later with line labels we'll still accept
            # because line labels will move, but if a *line point* is right on it, go to tip.
            add_fixed_bar_label(f"{v:.2%}", xpos, desired)

            # If the "above" choice landed inside the bar body and could be hidden by the bar,
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
    subject_cert: int = 19977,
    peer_avg_cert_primary: int = 99999,
    peer_avg_cert_alt: int = 99998,
    use_alt_peer_avg: bool = False,
    show_peers_avg_label: bool = True,
    show_idb_label: bool = True,
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
    GOLD, PEER, GUIDE = "#F7A81B", "#4C78A8", "#7F8C8D"

    def to_decimals_series(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        # Treat *_Rate / *to_Gross* as percent-points → convert if not already decimal
        if s.dropna().max() > 0.05:  # 0.05 pp threshold
            return s/100.0
        return s

    peers_cert = peer_avg_cert_alt if use_alt_peer_avg else peer_avg_cert_primary
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

    idb = df[df["CERT"] == subject_cert]
    peer_avg = df[df["CERT"] == peers_cert]
    others = df[~df["CERT"].isin([subject_cert, peer_avg_cert_primary, peer_avg_cert_alt])]

    Xo, Yo = to_decimals_series(others[x_col]), to_decimals_series(others[y_col])
    ax.scatter(Xo, Yo, s=42, alpha=0.9, color=PEER, edgecolor="white", linewidth=0.6, label="Peers")

    xi = yi = None
    if not idb.empty:
        xi = float(to_decimals_series(idb[x_col]).iloc[0]); yi = float(to_decimals_series(idb[y_col]).iloc[0])
        ax.scatter(xi, yi, s=80, color=GOLD, edgecolor="black", linewidth=0.7, label="MSPBNA")

    px = py = None
    if not peer_avg.empty:
        px = float(to_decimals_series(peer_avg[x_col]).iloc[0])
        py = float(to_decimals_series(peer_avg[y_col]).iloc[0])
        ax.scatter(px, py, s=90, color=PEER, marker="s", edgecolor="black", linewidth=0.7, label="_nolegend_")
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
        dx, dy = pick_offset(xi, yi); tag(xi, yi, "MSPBNA", (dx, dy), color="black", box=True)

    if identify_outliers and outliers_topn > 0:
        X_all = to_decimals_series(df[x_col]); Y_all = to_decimals_series(df[y_col])
        # distance to peer avg if available; else to sample mean
        cx = px if px is not None else float(X_all.mean())
        cy = py if py is not None else float(Y_all.mean())
        d = ((X_all - cx)**2 + (Y_all - cy)**2)**0.5
        mask_excl = df["CERT"].isin([peer_avg_cert_primary, peer_avg_cert_alt])
        cand = d[~mask_excl].sort_values(ascending=False)

        # keep N outliers; if MSPBNA in top set, ensure we add only one other
        top_idx = list(cand.index[:outliers_topn+1])  # +1 to allow for MSPBNA
        if not idb.empty and int(idb.index[0]) in top_idx:
            top_idx = [i for i in top_idx if i != int(idb.index[0])][:1]  # only one other
        else:
            top_idx = top_idx[:outliers_topn]

        for i in top_idx:
            ox = float(to_decimals_series(pd.Series([df.loc[i, x_col]])).iloc[0])
            oy = float(to_decimals_series(pd.Series([df.loc[i, y_col]])).iloc[0])
            label = short_name(df.loc[i, "NAME"]) if "NAME" in df.columns else str(df.loc[i, "CERT"])
            dx, dy = pick_offset(ox, oy); tag(ox, oy, label, (dx, dy), color="black", box=True)

    # smart axes + square
    all_x = pd.concat([Xo, pd.Series([xi]) if xi is not None else pd.Series(dtype=float),
                            pd.Series([px]) if px is not None else pd.Series(dtype=float)], ignore_index=True).dropna()
    all_y = pd.concat([Yo, pd.Series([yi]) if yi is not None else pd.Series(dtype=float),
                            pd.Series([py]) if py is not None else pd.Series(dtype=float)], ignore_index=True).dropna()
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