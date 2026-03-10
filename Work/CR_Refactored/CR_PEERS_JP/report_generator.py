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
    mspbna_cert = int(os.getenv("MSPBNA_CERT", "19977"))
    msbna_cert = int(os.getenv("MSBNA_CERT", "19977"))
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
# ALIAS HELPER (B3)
# ==================================================================================

def build_plot_df_with_alias(df, alias_map):
    frames = []
    for alias_cert, source_cert in alias_map.items():
        subset = df[df['CERT'] == source_cert].copy()
        if not subset.empty:
            subset['CERT'] = alias_cert
            frames.append(subset)
    if frames:
        return pd.concat([df] + frames, ignore_index=True)
    return df


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
    """
    entities_to_plot = [subject_bank_cert]

    if show_both_peer_groups:
        entities_to_plot.extend([99999, 99998])
        colors = {
            subject_bank_cert: '#F7A81B',
            99999: '#5B9BD5',
            99998: '#70AD47'
        }
        entity_names = {
            subject_bank_cert: "MSPBNA",
            99999: "All Peers",
            99998: "Peers (Ex. F&V)"
        }
    else:
        entities_to_plot.append(99998)
        colors = {
            subject_bank_cert: '#F7A81B',
            99998: '#5B9BD5'
        }
        entity_names = {
            subject_bank_cert: "MSPBNA",
            99998: "Peers (Ex. F&V)"
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
            bars = ax.bar(bar_positions[cert], nco_rate, alpha=0.7, color=colors[cert],
                         label=f'{label} TTM NCO Rate', width=bar_width)

    ax2 = ax.twinx()

    line_styles = ['-', '--', '-.']
    for i, cert in enumerate(entities_to_plot):
        entity_data = chart_df[chart_df['CERT'] == cert].reset_index(drop=True)
        if not entity_data.empty:
            label = entity_names.get(cert, f"CERT {cert}")
            npl_rate = entity_data['NPL_to_Gross_Loans_Rate'].fillna(0) / 100
            line_style = line_styles[i % len(line_styles)]
            ax2.plot(x_positions, npl_rate, color=colors[cert],
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


def generate_html_email_table(df: pd.DataFrame, report_date: datetime,
                              col_labels: Optional[Dict[str, str]] = None) -> str:
    """
    Generates a properly formatted HTML table for email.
    col_labels can override column header names. Keys in df should match.
    """
    formatted_date = report_date.strftime('%B %d, %Y') if hasattr(report_date, 'strftime') else str(report_date)

    # Determine column names from df
    # Expected columns: Metric, MSPBNA, then peer columns, then diff columns
    peer_cols = [c for c in df.columns if c not in ('Metric', 'MSPBNA') and not c.startswith('Diff')]
    diff_cols = [c for c in df.columns if c.startswith('Diff')]

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
                max-width: 1100px;
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
                <p>MSPBNA vs Peer Groups Analysis | Report Date: {formatted_date}</p>
            </div>

            <table>
                <thead>
                    <tr>
                        <th style="width: 25%;">Credit Metric</th>
                        <th style="width: 10%;">MSPBNA</th>
    """

    for pc in peer_cols:
        display = col_labels.get(pc, pc) if col_labels else pc
        html += f'<th style="width: 12%;">{display}</th>\n'

    for dc in diff_cols:
        display = col_labels.get(dc, dc) if col_labels else dc
        html += f'<th style="width: 12%;">{display}</th>\n'

    html += """
                    </tr>
                </thead>
                <tbody>
    """

    for _, row in df.iterrows():
        html += f"""
                    <tr>
                        <td class="metric-name">{row['Metric']}</td>
                        <td class="mspbna-value">{row['MSPBNA']}</td>
        """
        for pc in peer_cols:
            html += f'<td>{row[pc]}</td>\n'
        for dc in diff_cols:
            diff_class = get_diff_class(str(row[dc]))
            html += f'<td class="{diff_class}">{row[dc]}</td>\n'
        html += "</tr>"

    html += """
                </tbody>
            </table>

            <div class="legend">
                <strong>Legend:</strong><br>
                &bull; <span style="color: #d32f2f; font-weight: 600;">Red values</span>: MSPBNA performs worse than peer group (higher is worse for most credit metrics)<br>
                &bull; <span style="color: #388e3c; font-weight: 600;">Green values</span>: MSPBNA performs better than peer group (lower is better for most credit metrics)<br>
                &bull; All values are as of the latest available quarter
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
    subject_bank_cert: int,
    peer_certs: Dict[int, str] = None,
    table_label: str = "Standard",
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    """
    Build the email HTML table for a given set of peer groups.
    peer_certs maps CERT -> display name, e.g. {90001: "Core PB", 90002: "MSPBNA+Wealth", 90003: "All Peers"}.
    """
    if peer_certs is None:
        peer_certs = {90001: "Core PB", 90002: "MSPBNA+Wealth", 90003: "All Peers"}

    important_metrics = {
        "ASSET": "Assets",
        "LNLS": "Gross Loans",
        "CI_to_Capital_Risk": "C&I to Capital Risk (%)",
        "CRE_Concentration_Capital_Risk": "CRE Concentration Risk (%)",
        "MSPBNA_CRE_Growth_TTM": "CRE Growth TTM (%)",
        "MSPBNA_CRE_Growth_36M": "CRE Growth 36M (%)",
        "TTM_NCO_Rate": "TTM NCO Rate (%)",
        "NPL_to_Gross_Loans_Rate": "NPL to Gross Loans (%)",
        "Allowance_to_Gross_Loans_Rate": "Allowance to Gross Loans (%)",
        "TTM_Past_Due_Rate": "TTM Past Due Rate (%)",
        "TTM_PD30_Rate": "TTM Past Due (30-90 Days) Rate (%)",
        "TTM_PD90_Rate": "TTM Past Due (>90 Days) Rate (%)",
    }

    # Also try IDB_ prefixed columns as fallback
    for code in list(important_metrics.keys()):
        if code.startswith("MSPBNA_"):
            fallback = code.replace("MSPBNA_", "IDB_")
            important_metrics[fallback] = important_metrics[code]

    latest_date = proc_df_with_peers["REPDTE"].max()
    latest_data = proc_df_with_peers[proc_df_with_peers["REPDTE"] == latest_date]

    subj = latest_data[latest_data["CERT"] == subject_bank_cert]
    if subj.empty:
        print(f"Missing data for subject bank CERT {subject_bank_cert}")
        return None, None
    subj = subj.iloc[0]

    peer_rows = {}
    for cert, name in peer_certs.items():
        pr = latest_data[latest_data["CERT"] == cert]
        if not pr.empty:
            peer_rows[cert] = pr.iloc[0]

    if not peer_rows:
        print("Missing data for all peer groups")
        return None, None

    rows = []
    seen_display = set()
    for code, disp in important_metrics.items():
        if disp in seen_display:
            continue
        if code not in subj.index:
            continue
        seen_display.add(disp)

        v_subj = subj.get(code, np.nan)
        row_data = {"Metric": disp, "MSPBNA": None}

        if code in CURRENCY_MM_CODES:
            row_data["MSPBNA"] = _fmt_money_millions(v_subj)
        else:
            row_data["MSPBNA"] = _fmt_percent_auto(v_subj)

        for cert, name in peer_certs.items():
            pr = peer_rows.get(cert)
            v_peer = pr.get(code, np.nan) if pr is not None else np.nan
            d = (v_subj - v_peer) if not pd.isna(v_peer) else np.nan

            if code in CURRENCY_MM_CODES:
                row_data[name] = _fmt_money_millions(v_peer)
                row_data[f"Diff vs {name}"] = _fmt_money_millions_with_sign(d)
            else:
                row_data[name] = _fmt_percent_auto(v_peer)
                row_data[f"Diff vs {name}"] = _fmt_percent_auto(d)

        rows.append(row_data)

    df = pd.DataFrame(rows)
    html = generate_html_email_table(df, latest_date)
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
        peer_certs = [90001, 90002, 90003]

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
    df: pd.DataFrame,
    subject_cert: int,
    peer_certs: Optional[Dict[int, str]] = None,
) -> Optional[str]:
    """
    Wide HTML table showing all individual banks + peer-group averages side-by-side.
    Each row is a metric; columns are MSPBNA then each peer group.
    """
    if peer_certs is None:
        peer_certs = {90001: "Core PB", 90002: "MSPBNA+Wealth", 90003: "All Peers"}

    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date]

    subj = latest[latest["CERT"] == subject_cert]
    if subj.empty:
        print("  Skipped detailed peer table: no subject bank data")
        return None
    subj = subj.iloc[0]

    peer_rows = {}
    for cert, name in peer_certs.items():
        pr = latest[latest["CERT"] == cert]
        if not pr.empty:
            peer_rows[name] = pr.iloc[0]

    if not peer_rows:
        print("  Skipped detailed peer table: no peer data")
        return None

    metrics = {
        "ASSET": ("Total Assets", True),
        "LNLS": ("Gross Loans", True),
        "TTM_NCO_Rate": ("TTM NCO Rate", False),
        "NPL_to_Gross_Loans_Rate": ("NPL to Gross Loans", False),
        "Allowance_to_Gross_Loans_Rate": ("ALLL / Gross Loans", False),
        "TTM_Past_Due_Rate": ("TTM Past Due Rate", False),
        "TTM_PD30_Rate": ("Past Due 30-90 Days", False),
        "TTM_PD90_Rate": ("Past Due >90 Days", False),
        "CRE_Concentration_Capital_Risk": ("CRE Concentration Risk", False),
        "CI_to_Capital_Risk": ("C&I to Capital Risk", False),
        "Norm_NCO_Rate": ("Norm NCO Rate", False),
        "Norm_Nonaccrual_Rate": ("Norm Nonaccrual Rate", False),
    }

    col_headers = ["MSPBNA"] + list(peer_rows.keys())
    diff_headers = [f"Diff vs {n}" for n in peer_rows.keys()]

    html = f"""<html><head><meta charset="utf-8">{_TABLE_CSS}</head><body>
<div class="tbl-container">
<h2>Detailed Peer Analysis</h2>
<p class="sub">MSPBNA vs Peer Groups | {latest_date.strftime('%B %d, %Y')}</p>
<table><thead><tr><th>Credit Metric</th>"""
    for h in col_headers:
        html += f"<th>{h}</th>"
    for h in diff_headers:
        html += f"<th>{h}</th>"
    html += "</tr></thead><tbody>\n"

    for code, (display, is_money) in metrics.items():
        if code not in subj.index:
            continue
        sv = _safe_val(subj, code)
        fmt_s = _fmt_money_millions(sv) if is_money else _fmt_percent_auto(sv)
        html += f'<tr><td class="metric-name">{display}</td><td class="mspbna-value">{fmt_s}</td>'

        diffs = []
        for name, pr in peer_rows.items():
            pv = _safe_val(pr, code)
            fmt_p = _fmt_money_millions(pv) if is_money else _fmt_percent_auto(pv)
            html += f"<td>{fmt_p}</td>"
            d = (sv - pv) if (not pd.isna(sv) and not pd.isna(pv)) else np.nan
            if is_money:
                diffs.append(("neutral", _fmt_money_millions_with_sign(d)))
            else:
                dc = _trend_class(d)
                diffs.append((dc, _fmt_percent_auto(d)))
        for cls, val in diffs:
            html += f'<td class="{cls}">{val}</td>'
        html += "</tr>\n"

    html += "</tbody></table></div></body></html>"
    return html


def generate_normalized_comparison_table(
    df: pd.DataFrame,
    subject_cert: int,
) -> Optional[str]:
    """
    Side-by-side table comparing Standard vs Normalized metrics for the subject bank
    and the All Peers composite.
    """
    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date]

    subj = latest[latest["CERT"] == subject_cert]
    if subj.empty:
        print("  Skipped normalized comparison table: no subject bank data")
        return None
    subj = subj.iloc[0]

    # All Peers standard (90003) and normalized (90006)
    peer_std = latest[latest["CERT"] == 90003]
    peer_norm = latest[latest["CERT"] == 90006]
    peer_std = peer_std.iloc[0] if not peer_std.empty else None
    peer_norm = peer_norm.iloc[0] if not peer_norm.empty else None

    pairs = [
        ("NCO Rate", "TTM_NCO_Rate", "Norm_NCO_Rate"),
        ("Nonaccrual / NPL Rate", "NPL_to_Gross_Loans_Rate", "Norm_Nonaccrual_Rate"),
        ("Past Due Rate", "TTM_Past_Due_Rate", "Norm_Past_Due_Rate"),
        ("ALLL / Gross Loans", "Allowance_to_Gross_Loans_Rate", "Norm_Allowance_Rate"),
    ]

    html = f"""<html><head><meta charset="utf-8">{_TABLE_CSS}</head><body>
<div class="tbl-container">
<h2>Standard vs Normalized Metrics</h2>
<p class="sub">Side-by-Side Comparison | {latest_date.strftime('%B %d, %Y')}</p>
<table><thead>
<tr><th rowspan="2">Metric</th>
<th colspan="2" style="background:#2980b9">MSPBNA</th>
<th colspan="2" style="background:#27ae60">All Peers</th></tr>
<tr><th>Standard</th><th>Normalized</th><th>Standard</th><th>Normalized</th></tr>
</thead><tbody>\n"""

    for display, std_col, norm_col in pairs:
        sv_std = _safe_val(subj, std_col)
        sv_norm = _safe_val(subj, norm_col)
        pv_std = _safe_val(peer_std, std_col) if peer_std is not None else np.nan
        pv_norm = _safe_val(peer_norm, norm_col) if peer_norm is not None else np.nan

        # Skip row if neither standard nor normalized column exists in the data
        if std_col not in subj.index and norm_col not in subj.index:
            continue

        html += f'<tr><td class="metric-name">{display}</td>'
        html += f'<td class="mspbna-value">{_fmt_percent_auto(sv_std)}</td>'
        html += f'<td class="mspbna-value">{_fmt_percent_auto(sv_norm)}</td>'
        html += f"<td>{_fmt_percent_auto(pv_std)}</td>"
        html += f"<td>{_fmt_percent_auto(pv_norm)}</td></tr>\n"

    html += "</tbody></table></div></body></html>"
    return html


def generate_ratio_components_table(
    df: pd.DataFrame,
    subject_cert: int,
    is_normalized: bool = False,
    peer_cert: int = 90003,
) -> Optional[str]:
    """
    Recreates the legacy 'Ratio Components' table showing numerator/denominator
    breakdowns for key credit ratios.
    """
    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date]

    subj = latest[latest["CERT"] == subject_cert]
    peer = latest[latest["CERT"] == (90006 if is_normalized else peer_cert)]
    if subj.empty:
        print(f"  Skipped ratio components ({'Norm' if is_normalized else 'Std'}): no subject data")
        return None
    subj = subj.iloc[0]
    peer = peer.iloc[0] if not peer.empty else None

    if is_normalized:
        ratios = [
            ("Norm NCO Rate", "Norm_NCO_TTM", "Norm_Gross_Loans", "Norm_NCO_Rate"),
            ("Norm Nonaccrual Rate", "Norm_Nonaccrual_Bal", "Norm_Gross_Loans", "Norm_Nonaccrual_Rate"),
            ("Norm Allowance Rate", "Norm_ALLL", "Norm_Gross_Loans", "Norm_Allowance_Rate"),
            ("Norm Past Due Rate", "Norm_PastDue_TTM", "Norm_Gross_Loans", "Norm_Past_Due_Rate"),
        ]
    else:
        ratios = [
            ("TTM NCO Rate", "NTLNLS_TTM", "LNLS", "TTM_NCO_Rate"),
            ("NPL to Gross Loans", "NPL_Total", "LNLS", "NPL_to_Gross_Loans_Rate"),
            ("ALLL / Gross Loans", "ALLL", "LNLS", "Allowance_to_Gross_Loans_Rate"),
            ("TTM Past Due Rate", "PastDue_TTM", "LNLS", "TTM_Past_Due_Rate"),
            ("Resi NCO Rate", "RIC_Resi_NCO_TTM", "RIC_Resi_Bal", "RIC_Resi_NCO_Rate"),
            ("CRE NCO Rate", "RIC_CRE_NCO_TTM", "RIC_CRE_Bal", "RIC_CRE_NCO_Rate"),
            ("C&I NCO Rate", "RIC_Comm_NCO_TTM", "RIC_Comm_Bal", "RIC_Comm_NCO_Rate"),
        ]

    label = "Normalized" if is_normalized else "Standard"
    html = f"""<html><head><meta charset="utf-8">{_TABLE_CSS}</head><body>
<div class="tbl-container">
<h2>Ratio Components — {label}</h2>
<p class="sub">Numerator / Denominator Breakdown | {latest_date.strftime('%B %d, %Y')}</p>
<table><thead><tr>
<th>Ratio Name</th><th>Formula (Num)</th><th>Value (Num)</th>
<th>Formula (Denom)</th><th>Value (Denom)</th>
<th>Subject Ratio</th><th>Peer Ratio</th>
</tr></thead><tbody>\n"""

    for rname, num_col, den_col, ratio_col in ratios:
        # Skip entirely if neither numerator nor ratio column exists
        if num_col not in subj.index and ratio_col not in subj.index:
            continue

        sv_num = _safe_val(subj, num_col)
        sv_den = _safe_val(subj, den_col)
        sv_ratio = _safe_val(subj, ratio_col)
        pv_ratio = _safe_val(peer, ratio_col) if peer is not None else np.nan

        fmt_num = _fmt_money_millions(sv_num) if not pd.isna(sv_num) else "N/A"
        fmt_den = _fmt_money_millions(sv_den) if not pd.isna(sv_den) else "N/A"

        html += f'<tr><td class="metric-name">{rname}</td>'
        html += f"<td>{num_col}</td><td>{fmt_num}</td>"
        html += f"<td>{den_col}</td><td>{fmt_den}</td>"
        html += f'<td class="mspbna-value">{_fmt_percent_auto(sv_ratio)}</td>'
        html += f"<td>{_fmt_percent_auto(pv_ratio)}</td></tr>\n"

    html += "</tbody></table></div></body></html>"
    return html


def generate_segment_focus_table(
    df: pd.DataFrame,
    subject_cert: int,
    segment_name: str,
    msbna_cert: Optional[int] = None,
) -> Optional[str]:
    """
    Recreates the legacy 'Segment Focus' table (e.g., CRE Segment, Resi Segment).
    Shows only metrics relevant to the given segment, with columns for
    MSPBNA, MSBNA (optional), Core PB, and All Peers, plus a Trend column.
    """
    segment_metrics = {
        "CRE": {
            "RIC_CRE_Nonaccrual_Rate": "CRE Nonaccrual Rate",
            "RIC_CRE_NCO_Rate": "CRE NCO Rate",
            "RIC_CRE_PastDue_Rate": "CRE Past Due Rate",
            "RIC_CRE_ACL_Share": "CRE Share of ACL",
            "RIC_CRE_Loan_Share": "CRE Share of Loans",
            "CRE_Concentration_Capital_Risk": "CRE Concentration Risk",
            "MSPBNA_CRE_Growth_TTM": "CRE Growth TTM",
            "MSPBNA_CRE_Growth_36M": "CRE Growth 36M",
            "RIC_CRE_Years_of_Reserves": "CRE Years of Reserves",
        },
        "Resi": {
            "RIC_Resi_Nonaccrual_Rate": "Resi Nonaccrual Rate",
            "RIC_Resi_NCO_Rate": "Resi NCO Rate",
            "RIC_Resi_PastDue_Rate": "Resi Past Due Rate",
            "RIC_Resi_ACL_Share": "Resi Share of ACL",
            "RIC_Resi_Loan_Share": "Resi Share of Loans",
            "RIC_Resi_Years_of_Reserves": "Resi Years of Reserves",
        },
    }

    metrics = segment_metrics.get(segment_name, {})
    if not metrics:
        print(f"  Skipped segment table: unknown segment '{segment_name}'")
        return None

    latest_date = df["REPDTE"].max()
    latest = df[df["REPDTE"] == latest_date]

    subj = latest[latest["CERT"] == subject_cert]
    if subj.empty:
        print(f"  Skipped {segment_name} segment table: no subject data")
        return None
    subj = subj.iloc[0]

    # Check if there's a prior quarter for trend calc
    dates_sorted = sorted(df["REPDTE"].dropna().unique())
    prior_date = dates_sorted[-2] if len(dates_sorted) >= 2 else None
    subj_prior = None
    if prior_date is not None:
        sp = df[(df["CERT"] == subject_cert) & (df["REPDTE"] == prior_date)]
        if not sp.empty:
            subj_prior = sp.iloc[0]

    # Load MSBNA cert from env if not passed
    if msbna_cert is None:
        msbna_cert = int(os.getenv("MSBNA_CERT", "0"))

    entity_map = {"MSPBNA": subject_cert}
    if msbna_cert and msbna_cert != subject_cert:
        entity_map["MSBNA"] = msbna_cert
    entity_map["Core PB"] = 90001
    entity_map["All Peers"] = 90003

    entity_rows = {}
    for name, cert in entity_map.items():
        r = latest[latest["CERT"] == cert]
        if not r.empty:
            entity_rows[name] = r.iloc[0]

    # Filter to metrics actually present in data
    available_metrics = {k: v for k, v in metrics.items() if k in subj.index}
    if not available_metrics:
        print(f"  Skipped {segment_name} segment table: no matching columns in data")
        return None

    col_names = list(entity_rows.keys())
    html = f"""<html><head><meta charset="utf-8">{_TABLE_CSS}</head><body>
<div class="tbl-container">
<h2>{segment_name} Segment Focus</h2>
<p class="sub">Segment-Level Credit Metrics | {latest_date.strftime('%B %d, %Y')}</p>
<table><thead><tr><th>Metric</th>"""
    for cn in col_names:
        html += f"<th>{cn}</th>"
    html += "<th>Trend (QoQ)</th></tr></thead><tbody>\n"

    for code, display in available_metrics.items():
        html += f'<tr><td class="metric-name">{display}</td>'
        for cn in col_names:
            er = entity_rows.get(cn)
            v = _safe_val(er, code) if er is not None else np.nan
            cls = "mspbna-value" if cn == "MSPBNA" else ""
            html += f'<td class="{cls}">{_fmt_percent_auto(v)}</td>'

        # Trend: compare subject current vs prior quarter
        curr_v = _safe_val(subj, code)
        prev_v = _safe_val(subj_prior, code) if subj_prior is not None else np.nan
        if not pd.isna(curr_v) and not pd.isna(prev_v):
            diff = float(curr_v) - float(prev_v)
            tcls = _trend_class(diff)
            arrow = "+" if diff > 0 else ""
            html += f'<td class="{tcls}">{arrow}{diff:.2f}%</td>'
        else:
            html += '<td class="neutral">N/A</td>'
        html += "</tr>\n"

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
    if not all([id_col, date_col, val_col]):
        raise ValueError("FRED Data columns not recognized. Need Series*, *date*, *value* columns.")
    fred = fred.rename(columns={id_col:"SeriesID", date_col:"DATE", val_col:"VALUE"})
    fred["DATE"] = pd.to_datetime(fred["DATE"], errors="coerce")
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
    - writes charts to Peers/charts/, scatters to Peers/scatter/, tables to Peers/tables/,
    - generates normalized bar+line credit chart, standard+normalized scatters,
      standard+normalized HTML tables, and the FRED macro table.
    """
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

    print("=" * 80)
    print("MSPBNA PERFORMANCE REPORT GENERATOR")
    print("=" * 80)

    cfg = load_config()
    subject_bank_cert = cfg["subject_bank_cert"]

    # 1) Find latest processed Excel and set output roots
    excel_file = find_latest_excel_file(cfg["output_dir"])
    if not excel_file:
        print("ERROR: No Excel files found in output/. Run the pipeline first.")
        return

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

    stamp = datetime.now().strftime("%Y%m%d")
    base = Path(excel_file).stem

    try:
        # ------------------------------------------------------------------
        # LOAD SHEETS
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

        # ------------------------------------------------------------------
        # B6: 3-GROUP HTML TABLES (STANDARD + NORMALIZED) -> tables_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING HTML TABLES (STANDARD + NORMALIZED)")
        print("-" * 60)

        # STANDARD table: MSPBNA_CERT + 90001 (Core PB), 90002 (MSPBNA+Wealth), 90003 (All Peers)
        std_peer_certs = {90001: "Core PB", 90002: "MSPBNA+Wealth", 90003: "All Peers"}
        # B7: Exclude MS_COMBINED_CERT from All Banks listing if applicable
        if REPORT_VIEW == "MSPBNA_WEALTH_NORM":
            std_peer_certs = {k: v for k, v in std_peer_certs.items() if k != MS_COMBINED_CERT}

        std_html, std_df = generate_credit_metrics_email_table(
            proc_df_with_peers, subject_bank_cert,
            peer_certs=std_peer_certs,
            table_label="Standard",
        )
        if std_html:
            std_path = tables_dir / f"{base}_standard_table_{stamp}.html"
            with open(std_path, "w", encoding="utf-8") as f:
                f.write(std_html)
            print(f"  Standard table saved: {std_path}")
            if std_df is not None:
                print(f"  Table contains {len(std_df)} credit metrics")

        # NORMALIZED table: MSPBNA_CERT + 90004 (Core PB Norm), 90005 (MSPBNA+Wealth Norm), 90006 (All Peers Norm)
        norm_peer_certs = {90004: "Core PB Norm", 90005: "MSPBNA+Wealth Norm", 90006: "All Peers Norm"}
        if REPORT_VIEW == "MSPBNA_WEALTH_NORM":
            norm_peer_certs = {k: v for k, v in norm_peer_certs.items() if k != MS_COMBINED_CERT}

        norm_html, norm_df = generate_credit_metrics_email_table(
            proc_df_with_peers, subject_bank_cert,
            peer_certs=norm_peer_certs,
            table_label="Normalized",
        )
        if norm_html:
            norm_path = tables_dir / f"{base}_normalized_table_{stamp}.html"
            with open(norm_path, "w", encoding="utf-8") as f:
                f.write(norm_html)
            print(f"  Normalized table saved: {norm_path}")
            if norm_df is not None:
                print(f"  Table contains {len(norm_df)} credit metrics")

        # ------------------------------------------------------------------
        # STANDARD CREDIT DETERIORATION CHART -> charts_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING STANDARD CREDIT DETERIORATION CHART")
        print("-" * 60)
        std_chart_path = str(charts_dir / f"{base}_standard_credit_chart_{stamp}.png")
        create_credit_deterioration_chart_ppt(
            proc_df_with_peers=proc_df_with_peers,
            subject_bank_cert=subject_bank_cert,
            start_date=start_date,
            bar_metric="TTM_NCO_Rate",
            line_metric="NPL_to_Gross_Loans_Rate",
            custom_title="TTM NCO Rate (bars) vs NPL to Gross Loans Rate (lines)",
            save_path=std_chart_path,
        )
        print(f"  Standard chart saved: {std_chart_path}")

        # ------------------------------------------------------------------
        # NORMALIZED CREDIT DETERIORATION CHART -> charts_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING NORMALIZED CREDIT DETERIORATION CHART")
        print("-" * 60)

        norm_df_plot = build_plot_df_with_alias(proc_df_with_peers, {99999: 90006, 99998: 90004})

        # Determine line metric with fallback
        norm_line_metric = "Norm_Nonaccrual_Rate"
        if norm_line_metric not in norm_df_plot.columns:
            norm_line_metric = "Norm_NPL_to_Gross_Loans_Rate"

        norm_chart_path = str(charts_dir / f"{base}_normalized_credit_chart_{stamp}.png")

        fig, ax = create_credit_deterioration_chart_ppt(
            proc_df_with_peers=norm_df_plot,
            subject_bank_cert=subject_bank_cert,
            start_date=start_date,
            bar_metric="Norm_NCO_Rate",
            line_metric=norm_line_metric,
            bar_entities=[subject_bank_cert, 99999, 99998],
            line_entities=[subject_bank_cert, 99999, 99998],
            figsize=credit_figsize,
            title_size=title_size,
            axis_label_size=axis_label_size,
            tick_size=tick_size,
            tag_size=tag_size,
            legend_fontsize=legend_fontsize,
            economist_style=True,
            custom_title=credit_title or "Norm NCO Rate (bars) vs Norm Nonaccrual Rate (lines)",
            save_path=norm_chart_path,
        )
        print(f"  Normalized chart saved: {norm_chart_path}")

        # ------------------------------------------------------------------
        # B5: DUAL SCATTERS (STANDARD + NORMALIZED) -> scatter_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING SCATTER PLOTS (STANDARD + NORMALIZED)")
        print("-" * 60)

        # Ensure numeric columns
        for c in ["NPL_to_Gross_Loans_Rate", "TTM_NCO_Rate", "TTM_Past_Due_Rate",
                   "Norm_Nonaccrual_Rate", "Norm_NCO_Rate"]:
            if c in rolling8q_df.columns:
                rolling8q_df[c] = pd.to_numeric(rolling8q_df[c], errors="coerce")

        # -- Standard scatters --
        std_scatter_df = build_plot_df_with_alias(rolling8q_df, {99999: 90003, 99998: 90001})

        s1_path = scatter_dir / f"{base}_scatter_nco_vs_npl_{stamp}.png"
        plot_scatter_dynamic(
            df=std_scatter_df,
            x_col="NPL_to_Gross_Loans_Rate",
            y_col="TTM_NCO_Rate",
            subject_cert=subject_bank_cert,
            use_alt_peer_avg=False,
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
            save_path=str(s1_path),
        )
        print(f"  Standard scatter (NCO vs NPL) saved: {s1_path}")

        s2_path = scatter_dir / f"{base}_scatter_pd_vs_npl_{stamp}.png"
        plot_scatter_dynamic(
            df=std_scatter_df,
            x_col="NPL_to_Gross_Loans_Rate",
            y_col="TTM_Past_Due_Rate",
            subject_cert=subject_bank_cert,
            use_alt_peer_avg=False,
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
            save_path=str(s2_path),
        )
        print(f"  Standard scatter (PD vs NPL) saved: {s2_path}")

        # -- Normalized scatter --
        norm_scatter_df = build_plot_df_with_alias(rolling8q_df, {99999: 90006, 99998: 90004})

        s3_path = scatter_dir / f"{base}_scatter_norm_nco_vs_nonaccrual_{stamp}.png"
        plot_scatter_dynamic(
            df=norm_scatter_df,
            x_col="Norm_Nonaccrual_Rate",
            y_col="Norm_NCO_Rate",
            subject_cert=subject_bank_cert,
            use_alt_peer_avg=False,
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
            save_path=str(s3_path),
        )
        print(f"  Normalized scatter (Norm NCO vs Norm Nonaccrual) saved: {s3_path}")

        # ------------------------------------------------------------------
        # FRED MACRO TABLE -> tables_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING FRED MACRO TABLE")
        print("-" * 60)
        try:
            fred_html, fred_df = build_fred_macro_table(excel_file, list(fred_short_names))
            fred_path = tables_dir / f"{base}_fred_table_{stamp}.html"
            with open(fred_path, "w", encoding="utf-8") as f:
                f.write(fred_html)
            print(f"  FRED macro table saved: {fred_path}")
        except Exception as e:
            print(f"  Skipped FRED table: {e}")

        # ------------------------------------------------------------------
        # DETAILED / LEGACY HTML TABLES -> tables_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING DETAILED HTML TABLES")
        print("-" * 60)

        # 1. Detailed Peer Analysis (Wide)
        detail_html = generate_detailed_peer_table(proc_df_with_peers, subject_bank_cert)
        if detail_html:
            dp = tables_dir / f"{base}_detailed_peer_table_{stamp}.html"
            with open(dp, "w", encoding="utf-8") as f:
                f.write(detail_html)
            print(f"  Detailed peer table saved: {dp}")

        # 2. Normalized Comparison (Side-by-side)
        normcmp_html = generate_normalized_comparison_table(proc_df_with_peers, subject_bank_cert)
        if normcmp_html:
            nc = tables_dir / f"{base}_normalized_comparison_{stamp}.html"
            with open(nc, "w", encoding="utf-8") as f:
                f.write(normcmp_html)
            print(f"  Normalized comparison table saved: {nc}")

        # 3. Ratio Components (Standard)
        rc_std_html = generate_ratio_components_table(
            proc_df_with_peers, subject_bank_cert, is_normalized=False
        )
        if rc_std_html:
            rcs = tables_dir / f"{base}_ratio_components_standard_{stamp}.html"
            with open(rcs, "w", encoding="utf-8") as f:
                f.write(rc_std_html)
            print(f"  Ratio components (standard) saved: {rcs}")

        # 4. Ratio Components (Normalized)
        rc_norm_html = generate_ratio_components_table(
            proc_df_with_peers, subject_bank_cert, is_normalized=True
        )
        if rc_norm_html:
            rcn = tables_dir / f"{base}_ratio_components_normalized_{stamp}.html"
            with open(rcn, "w", encoding="utf-8") as f:
                f.write(rc_norm_html)
            print(f"  Ratio components (normalized) saved: {rcn}")

        # 5. CRE Segment Focus
        cre_html = generate_segment_focus_table(
            proc_df_with_peers, subject_bank_cert, segment_name="CRE"
        )
        if cre_html:
            crp = tables_dir / f"{base}_segment_CRE_{stamp}.html"
            with open(crp, "w", encoding="utf-8") as f:
                f.write(cre_html)
            print(f"  CRE segment table saved: {crp}")

        # 6. Resi Segment Focus
        resi_html = generate_segment_focus_table(
            proc_df_with_peers, subject_bank_cert, segment_name="Resi"
        )
        if resi_html:
            rsp = tables_dir / f"{base}_segment_Resi_{stamp}.html"
            with open(rsp, "w", encoding="utf-8") as f:
                f.write(resi_html)
            print(f"  Resi segment table saved: {rsp}")

        # ------------------------------------------------------------------
        # SEGMENT-LEVEL CHARTS -> charts_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING SEGMENT-LEVEL CHARTS")
        print("-" * 60)

        # 1. Portfolio Mix
        mix_path = str(charts_dir / f"{base}_portfolio_mix_{stamp}.png")
        fig = plot_portfolio_mix(proc_df_with_peers, subject_bank_cert, save_path=mix_path)
        if fig:
            print(f"  Portfolio mix chart saved: {mix_path}")

        # 2. Problem-Asset Attribution
        attr_path = str(charts_dir / f"{base}_problem_asset_attribution_{stamp}.png")
        fig = plot_problem_asset_attribution(proc_df_with_peers, subject_bank_cert, save_path=attr_path)
        if fig:
            print(f"  Problem-asset attribution chart saved: {attr_path}")

        # 3. Reserve Allocation vs Risk
        reserve_path = str(charts_dir / f"{base}_reserve_risk_allocation_{stamp}.png")
        fig = plot_reserve_risk_allocation(proc_df_with_peers, subject_bank_cert, save_path=reserve_path)
        if fig:
            print(f"  Reserve-risk allocation chart saved: {reserve_path}")

        # 4. Migration Ladder
        ladder_path = str(charts_dir / f"{base}_migration_ladder_{stamp}.png")
        fig = plot_migration_ladder(proc_df_with_peers, subject_bank_cert, save_path=ladder_path)
        if fig:
            print(f"  Migration ladder chart saved: {ladder_path}")

        # ------------------------------------------------------------------
        # ROADMAP CHARTS -> charts_dir
        # ------------------------------------------------------------------
        print("\n" + "-" * 60)
        print("GENERATING ROADMAP CHARTS")
        print("-" * 60)

        # 5. Years-of-Reserves by Segment
        yor_path = str(charts_dir / f"{base}_years_of_reserves_{stamp}.png")
        fig = plot_years_of_reserves(proc_df_with_peers, subject_bank_cert, save_path=yor_path)
        if fig:
            print(f"  Years-of-reserves chart saved: {yor_path}")

        # 6. Growth vs Deterioration Quadrant
        gvd_path = str(charts_dir / f"{base}_growth_vs_deterioration_{stamp}.png")
        fig = plot_growth_vs_deterioration(proc_df_with_peers, subject_bank_cert, save_path=gvd_path)
        if fig:
            print(f"  Growth-vs-deterioration chart saved: {gvd_path}")

        # 7. Risk-Adjusted Return Frontier
        rar_path = str(charts_dir / f"{base}_risk_adjusted_return_{stamp}.png")
        fig = plot_risk_adjusted_return(proc_df_with_peers, subject_bank_cert, save_path=rar_path)
        if fig:
            print(f"  Risk-adjusted return chart saved: {rar_path}")

        # 8. Concentration vs Capital Sensitivity
        cvc_path = str(charts_dir / f"{base}_concentration_vs_capital_{stamp}.png")
        fig = plot_concentration_vs_capital(proc_df_with_peers, subject_bank_cert, save_path=cvc_path)
        if fig:
            print(f"  Concentration-vs-capital chart saved: {cvc_path}")

        # 9. Liquidity / Draw-Risk Overlay
        liq_path = str(charts_dir / f"{base}_liquidity_overlay_{stamp}.png")
        fig = plot_liquidity_overlay(proc_df_with_peers, subject_bank_cert, save_path=liq_path)
        if fig:
            print(f"  Liquidity overlay chart saved: {liq_path}")

        # 10. Macro Overlay on Credit Trend
        macro_path = str(charts_dir / f"{base}_macro_overlay_{stamp}.png")
        fig = plot_macro_overlay(proc_df_with_peers, subject_bank_cert, excel_file, save_path=macro_path)
        if fig:
            print(f"  Macro overlay chart saved: {macro_path}")

        # ------------------------------------------------------------------
        # SUMMARY
        # ------------------------------------------------------------------
        print("\n" + "=" * 80)
        print("REPORT GENERATION COMPLETE")
        print("=" * 80)
        print(f"Source file: {excel_file}")
        print(f"Charts directory: {charts_dir}")
        print(f"Scatter directory: {scatter_dir}")
        print(f"Tables directory: {tables_dir}")
        print(f"Subject bank CERT: {subject_bank_cert}")
        print(f"Report view: {REPORT_VIEW}")

    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")
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

    GOLD, BLUE, PURPLE = "#F7A81B", "#4C78A8", "#9C6FB6"
    default_entities = [subject_bank_cert, 99999, 99998]
    bar_entities  = bar_entities  or default_entities
    line_entities = line_entities or list(bar_entities)
    names  = {subject_bank_cert:"MSPBNA", 99999:"All Peers", 99998:"Peers (Ex. F&V)"}
    colors = {subject_bank_cert:GOLD,   99999:BLUE,       99998:PURPLE}

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
    for c in bar_entities:
        vals = series_for(c, bar_metric)
        b = ax.bar(x + offsets[c], vals, width=bar_w, color=colors[c], alpha=0.92,
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

    for c in line_entities:
        s = series_for(c, line_metric)
        for k in idx_to_label:
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
    GOLD, PEER, GUIDE = "#F7A81B", "#4C78A8", "#7F8C8D"

    def to_decimals_series(s: pd.Series) -> pd.Series:
        s = pd.to_numeric(s, errors="coerce")
        if s.dropna().max() > 0.05:
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

    mspbna = df[df["CERT"] == subject_cert]
    peer_avg = df[df["CERT"] == peers_cert]
    others = df[~df["CERT"].isin([subject_cert, peer_avg_cert_primary, peer_avg_cert_alt])]

    Xo, Yo = to_decimals_series(others[x_col]), to_decimals_series(others[y_col])
    ax.scatter(Xo, Yo, s=42, alpha=0.9, color=PEER, edgecolor="white", linewidth=0.6, label="Peers")

    xi = yi = None
    if not mspbna.empty:
        xi = float(to_decimals_series(mspbna[x_col]).iloc[0]); yi = float(to_decimals_series(mspbna[y_col]).iloc[0])
        ax.scatter(xi, yi, s=80, color=GOLD, edgecolor="black", linewidth=0.7, label="MSPBNA")

    px = py = None
    if not peer_avg.empty:
        px = float(to_decimals_series(peer_avg[x_col]).iloc[0])
        py = float(to_decimals_series(peer_avg[y_col]).iloc[0])
        ax.scatter(px, py, s=90, color=PEER, marker="s", edgecolor="black", linewidth=0.7, label="_nolegend_")
        ax.axvline(px, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)
        ax.axhline(py, linestyle="--", linewidth=1.2, color=GUIDE, alpha=0.95)

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

    if show_mspbna_label and (xi is not None):
        dx, dy = pick_offset(xi, yi); tag(xi, yi, "MSPBNA", (dx, dy), color="black", box=True)

    if identify_outliers and outliers_topn > 0:
        X_all = to_decimals_series(df[x_col]); Y_all = to_decimals_series(df[y_col])
        cx = px if px is not None else float(X_all.mean())
        cy = py if py is not None else float(Y_all.mean())
        d = ((X_all - cx)**2 + (Y_all - cy)**2)**0.5
        mask_excl = df["CERT"].isin([peer_avg_cert_primary, peer_avg_cert_alt])
        cand = d[~mask_excl].sort_values(ascending=False)

        top_idx = list(cand.index[:outliers_topn+1])
        if not mspbna.empty and int(mspbna.index[0]) in top_idx:
            top_idx = [i for i in top_idx if i != int(mspbna.index[0])][:1]
        else:
            top_idx = top_idx[:outliers_topn]

        for i in top_idx:
            ox = float(to_decimals_series(pd.Series([df.loc[i, x_col]])).iloc[0])
            oy = float(to_decimals_series(pd.Series([df.loc[i, y_col]])).iloc[0])
            label = short_name(df.loc[i, "NAME"]) if "NAME" in df.columns else str(df.loc[i, "CERT"])
            dx, dy = pick_offset(ox, oy); tag(ox, oy, label, (dx, dy), color="black", box=True)

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

    colors = ["#F7A81B", "#4C78A8", "#70AD47", "#9C6FB6"]
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
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1f}%"))
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
    ax.bar(x - width / 2, acl_vals, width, label="Share of ACL", color="#E74C3C", alpha=0.85)
    ax.bar(x + width / 2, loan_vals, width, label="Share of Loans", color="#4C78A8", alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels(seg_labels, rotation=30, ha="right", fontsize=11)
    ax.set_title("Reserve Allocation vs Risk Exposure", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.set_ylabel("Share (%)", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1f}%"))
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
    """Line chart showing the early-warning pipeline progression over time."""
    series_map = {
        "TTM_PD30_Rate": ("Past Due 30-90d", "#3498DB"),
        "TTM_PD90_Rate": ("Past Due 90d+", "#F39C12"),
        "Norm_Nonaccrual_Rate": ("Nonaccrual", "#E74C3C"),
        "TTM_NCO_Rate": ("NCO", "#2C3E50"),
    }
    available = {k: v for k, v in series_map.items() if k in df.columns}
    if not available:
        print("  Skipped migration ladder: no pipeline columns found")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped migration ladder: no subject bank data")
        return None

    subj = subj.sort_values("REPDTE")
    for c in available:
        subj[c] = pd.to_numeric(subj[c], errors="coerce")

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")

    for col, (label, color) in available.items():
        ax.plot(subj["REPDTE"], subj[col], label=label, color=color,
                linewidth=2.2, marker="o", markersize=4)

    ax.set_title("Early-Warning Migration Ladder", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.set_xlabel("Reporting Period", fontsize=13, fontweight="bold")
    ax.set_ylabel("Rate", fontsize=13, fontweight="bold")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2%}"))
    ax.legend(loc="upper left", frameon=True, fontsize=11)
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

    latest_date = df["REPDTE"].max()
    subj = df[(df["CERT"] == subject_bank_cert) & (df["REPDTE"] == latest_date)]
    if subj.empty:
        print("  Skipped years-of-reserves: no subject bank data")
        return None
    subj = subj.iloc[0]

    # Also get All Peers (90003) for comparison
    peer = df[(df["CERT"] == 90003) & (df["REPDTE"] == latest_date)]
    peer = peer.iloc[0] if not peer.empty else None

    segments, subj_vals, peer_vals = [], [], []
    for col, seg_label in reserve_cols.items():
        sv = pd.to_numeric(subj.get(col, np.nan), errors="coerce")
        if pd.notna(sv):
            segments.append(seg_label.replace("_", " "))
            subj_vals.append(float(sv))
            pv = pd.to_numeric(peer.get(col, np.nan), errors="coerce") if peer is not None else np.nan
            peer_vals.append(float(pv) if pd.notna(pv) else 0.0)

    if not segments:
        print("  Skipped years-of-reserves: all values N/A")
        return None

    fig, ax = plt.subplots(figsize=(12, max(5, len(segments) * 0.8 + 2)))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    y = np.arange(len(segments))
    # Lollipop: horizontal stems + dots
    ax.hlines(y, 0, subj_vals, color="#F7A81B", linewidth=2.5, zorder=2)
    ax.scatter(subj_vals, y, color="#F7A81B", s=100, zorder=3, label="MSPBNA")
    if any(v > 0 for v in peer_vals):
        ax.scatter(peer_vals, y, color="#4C78A8", s=80, marker="D", zorder=3, label="All Peers")

    ax.set_yticks(y)
    ax.set_yticklabels(segments, fontsize=12)
    ax.set_xlabel("Years of Reserves", fontsize=13, fontweight="bold")
    ax.set_title("Years of Reserves by Segment", fontsize=18, fontweight="bold", color="#2B2B2B")
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

    # Exclude composite CERTs from the scatter cloud
    composite = {90001, 90002, 90003, 90004, 90005, 90006, 99998, 99999}
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    ax.scatter(peers[growth_col], peers[nco_col], s=50, alpha=0.7, color="#4C78A8",
               edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[growth_col], subj[nco_col], s=120, color="#F7A81B",
                   edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")

    # Quadrant lines at medians
    mx = latest[growth_col].median()
    my = latest[nco_col].median()
    ax.axvline(mx, linestyle="--", color="#7F8C8D", alpha=0.7, linewidth=1)
    ax.axhline(my, linestyle="--", color="#7F8C8D", alpha=0.7, linewidth=1)

    ax.set_xlabel(growth_col.replace("_", " "), fontsize=13, fontweight="bold")
    ax.set_ylabel("TTM NCO Rate", fontsize=13, fontweight="bold")
    ax.set_title("Growth vs Deterioration Quadrant", fontsize=18, fontweight="bold", color="#2B2B2B")
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

    composite = {90001, 90002, 90003, 90004, 90005, 90006, 99998, 99999}
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]

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
                   alpha=0.55, color="#4C78A8", edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[x_col].values, subj[y_col].values,
                   s=bubble_size(subj[size_col]).values,
                   color="#F7A81B", edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")

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

    composite = {90001, 90002, 90003, 90004, 90005, 90006, 99998, 99999}
    peers = latest[~latest["CERT"].isin(composite | {subject_bank_cert})]
    subj = latest[latest["CERT"] == subject_bank_cert]

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    ax.scatter(peers[x_col], peers[y_col], s=50, alpha=0.7, color="#4C78A8",
               edgecolor="white", linewidth=0.5, label="Peers")
    if not subj.empty:
        ax.scatter(subj[x_col].values, subj[y_col].values, s=120, color="#F7A81B",
                   edgecolor="black", linewidth=0.8, zorder=5, label="MSPBNA")

    # Quadrant lines at medians
    mx = latest[x_col].median()
    my = latest[y_col].median()
    ax.axvline(mx, linestyle="--", color="#7F8C8D", alpha=0.7, linewidth=1)
    ax.axhline(my, linestyle="--", color="#7F8C8D", alpha=0.7, linewidth=1)

    # Quadrant labels
    xlims, ylims = ax.get_xlim(), ax.get_ylim()
    ax.text(xlims[1], ylims[1], "High CRE + High C&I", ha="right", va="top",
            fontsize=9, color="#7F8C8D", style="italic")
    ax.text(xlims[0], ylims[0], "Low CRE + Low C&I", ha="left", va="bottom",
            fontsize=9, color="#7F8C8D", style="italic")

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
        "Loans_to_Deposits": ("Loans / Deposits", "#F7A81B", "-"),
        "Liquidity_Ratio": ("Liquidity Ratio", "#4C78A8", "--"),
        "HQLA_Ratio": ("HQLA Ratio", "#70AD47", "-."),
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
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.1f}%"))
    ax.set_title("Liquidity / Draw-Risk Overlay", fontsize=18, fontweight="bold", color="#2B2B2B")
    ax.legend(loc="upper left", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


def plot_macro_overlay(
    df: pd.DataFrame,
    subject_bank_cert: int,
    excel_file: str,
    save_path: Optional[str] = None,
) -> Optional[plt.Figure]:
    """Dual-axis chart: Norm_NCO_Rate on left axis, key FRED macro series on right axis."""
    nco_col = "Norm_NCO_Rate"
    if nco_col not in df.columns:
        print("  Skipped macro overlay: Norm_NCO_Rate not in data")
        return None

    subj = df[df["CERT"] == subject_bank_cert].copy()
    if subj.empty:
        print("  Skipped macro overlay: no subject bank data")
        return None
    subj = subj.sort_values("REPDTE")
    subj[nco_col] = pd.to_numeric(subj[nco_col], errors="coerce")

    # Load FRED data
    try:
        fred, desc = _load_fred_tables(excel_file)
    except Exception as e:
        print(f"  Skipped macro overlay: {e}")
        return None

    # Pick a macro series: prefer Fed Funds, fall back to Unemployment, then any available
    target_names = ["Fed Funds", "Unemployment", "All Loans Delinquency Rate"]
    sel = None
    chosen_name = None
    for tn in target_names:
        match = desc[desc["ShortName"] == tn]
        if not match.empty:
            sid = match.iloc[0]["SeriesID"]
            candidate = fred[fred["SeriesID"] == sid].copy()
            if not candidate.empty:
                sel = candidate
                chosen_name = tn
                break
    if sel is None:
        # Fall back to any available series
        if not fred.empty:
            first_sid = fred["SeriesID"].iloc[0]
            sel = fred[fred["SeriesID"] == first_sid].copy()
            match = desc[desc["SeriesID"] == first_sid]
            chosen_name = match.iloc[0]["ShortName"] if not match.empty else first_sid
        else:
            print("  Skipped macro overlay: no FRED data available")
            return None

    sel = sel.sort_values("DATE")
    sel["VALUE"] = pd.to_numeric(sel["VALUE"], errors="coerce")

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_alpha(0)
    ax.set_facecolor("none")
    _economist_ax(ax)

    ax.plot(subj["REPDTE"], subj[nco_col], color="#F7A81B", linewidth=2.5,
            marker="o", markersize=4, label="MSPBNA Norm NCO Rate", zorder=3)
    ax.set_ylabel("Norm NCO Rate", fontsize=13, fontweight="bold", color="#F7A81B")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:.2%}"))

    ax2 = ax.twinx()
    ax2.plot(sel["DATE"], sel["VALUE"], color="#4C78A8", linewidth=2, linestyle="--",
             label=chosen_name, alpha=0.85)
    ax2.set_ylabel(chosen_name, fontsize=13, fontweight="bold", color="#4C78A8")
    for sp in ["top"]:
        ax2.spines[sp].set_visible(False)

    ax.set_xlabel("Date", fontsize=13, fontweight="bold")
    ax.set_title(f"Macro Overlay: Norm NCO Rate vs {chosen_name}",
                 fontsize=18, fontweight="bold", color="#2B2B2B")

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper left", frameon=True, fontsize=11)
    plt.tight_layout()

    if save_path:
        Path(os.path.dirname(save_path)).mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=300, bbox_inches="tight", transparent=True)
    return fig


# ==================================================================================
# SCRIPT EXECUTION
# ==================================================================================

if __name__ == "__main__":
    generate_reports()
