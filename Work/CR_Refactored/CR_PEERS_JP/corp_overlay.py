#!/usr/bin/env python3
"""
Corp-Safe Overlay Module
========================

Dedicated module for joining local Bank_Performance_Dashboard output with
internal loan-level extracts. Produces corp-safe artifacts (HTML tables +
optional matplotlib charts) in a standalone workflow.

Responsibilities:
  * Schema contracts — validate required/optional columns in loan file
  * Local-output ingestion — find and read the latest dashboard Excel
  * Internal loan-file ingestion — read and validate loan-level CSV/Excel
  * Join logic — merge peer composition from dashboard with internal mix
  * Optional geography enrichment hooks (Census, BEA, HUD/Case-Shiller)
  * Corp-safe chart/table output — 4 required artifacts

Input A: Bank_Performance_Dashboard_YYYYMMDD.xlsx (latest in output/)
Input B: Internal loan-level extract (CSV or Excel) with contract columns.

Required columns (fail loudly if absent):
  * loan_id — unique loan identifier
  * current_balance — outstanding balance ($)
  * product_type — loan product classification

At least ONE geo field required:
  * zip_code, msa, or county

Optional columns (graceful degradation):
  * risk_rating, delinquency_status, nonaccrual_flag
  * segment, portfolio, collateral_type

Artifacts produced:
  1. loan_balance_by_product.png  — descending bar chart by product_type
  2. top10_geography_by_balance.png — top 10 by MSA > ZIP > county
  3. internal_credit_flags_summary.html — delinquency/nonaccrual/risk distribution
  4. peer_vs_internal_mix_bridge.html — local-output composition vs internal mix
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from logging_utils import (
    CsvLogger,
    build_artifact_filename,
    get_run_date_str,
    setup_csv_logging,
)
from rendering_mode import (
    ARTIFACT_REGISTRY,
    ArtifactManifest,
    RenderMode,
    select_mode,
    should_produce,
)


# =====================================================================
# Schema Contract
# =====================================================================

REQUIRED_COLUMNS = frozenset({"loan_id", "current_balance", "product_type"})
GEO_COLUMNS = ("msa", "zip_code", "county")  # priority order for geo resolution
OPTIONAL_COLUMNS = frozenset({
    "risk_rating", "delinquency_status", "nonaccrual_flag",
    "segment", "portfolio", "collateral_type",
})

# Dashboard composition metrics to extract for the bridge table
_DASHBOARD_COMPOSITION_METRICS = [
    "SBL_Composition",
    "RIC_CRE_Loan_Share",
    "RIC_Resi_Loan_Share",
    "Fund_Finance_Composition",
]


class LoanFileContractError(Exception):
    """Raised when the loan file does not satisfy the schema contract."""
    pass


def validate_loan_file(df: pd.DataFrame) -> Dict[str, Any]:
    """Validate the loan-level DataFrame against the schema contract.

    Returns a diagnostics dict with keys:
      - valid: bool
      - missing_required: list of missing required columns
      - geo_field: the resolved geo column name (best available), or None
      - available_optional: list of optional columns present
      - missing_optional: list of optional columns absent
      - row_count: int
      - unique_loans: int

    Raises LoanFileContractError if required columns or all geo fields are missing.
    """
    cols = set(c.lower().strip() for c in df.columns)
    # Normalize columns to lowercase
    df.columns = [c.lower().strip() for c in df.columns]

    missing_req = REQUIRED_COLUMNS - cols
    if missing_req:
        raise LoanFileContractError(
            f"Missing required columns: {sorted(missing_req)}. "
            f"Required: {sorted(REQUIRED_COLUMNS)}"
        )

    # Resolve best geo column
    geo_field = None
    for g in GEO_COLUMNS:
        if g in cols:
            geo_field = g
            break
    if geo_field is None:
        raise LoanFileContractError(
            f"No geographic column found. At least one of {GEO_COLUMNS} is required."
        )

    avail_opt = sorted(OPTIONAL_COLUMNS & cols)
    miss_opt = sorted(OPTIONAL_COLUMNS - cols)

    return {
        "valid": True,
        "missing_required": [],
        "geo_field": geo_field,
        "available_optional": avail_opt,
        "missing_optional": miss_opt,
        "row_count": len(df),
        "unique_loans": df["loan_id"].nunique(),
    }


# =====================================================================
# Input Ingestion
# =====================================================================

def find_latest_dashboard(directory: str = "output") -> Optional[str]:
    """Find the latest Bank_Performance_Dashboard_*.xlsx in directory."""
    try:
        base = Path(__file__).parent.resolve()
    except Exception:
        base = Path.cwd().resolve()

    out = base / directory
    if not out.exists():
        return None

    files = []
    for p in out.glob("Bank_Performance_Dashboard_*.xlsx"):
        try:
            files.append((p.stat().st_mtime, p))
        except OSError:
            pass
    if not files:
        return None
    return str(sorted(files, key=lambda t: t[0], reverse=True)[0][1])


def load_dashboard_composition(excel_path: str) -> Dict[str, float]:
    """Extract composition metrics from the dashboard's Summary_Dashboard sheet.

    Returns a dict of metric_name -> latest value for MSPBNA (CERT 34221).
    """
    subject_cert = int(os.getenv("MSPBNA_CERT", "34221"))
    try:
        summary = pd.read_excel(excel_path, sheet_name="Summary_Dashboard")
    except Exception:
        try:
            summary = pd.read_excel(excel_path, sheet_name="FDIC_Data")
        except Exception:
            return {}

    if "CERT" not in summary.columns:
        return {}

    subj = summary[summary["CERT"] == subject_cert]
    if subj.empty:
        return {}

    # Get the latest row
    if "REPDTE" in subj.columns:
        subj = subj.sort_values("REPDTE", ascending=False)
    latest = subj.iloc[0]

    result = {}
    for metric in _DASHBOARD_COMPOSITION_METRICS:
        if metric in latest.index and pd.notna(latest[metric]):
            result[metric] = float(latest[metric])
    return result


def load_loan_file(path: str) -> pd.DataFrame:
    """Load a loan-level CSV or Excel file. Normalizes column names to lowercase."""
    p = Path(path)
    if p.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    elif p.suffix.lower() in (".csv", ".tsv"):
        sep = "\t" if p.suffix.lower() == ".tsv" else ","
        df = pd.read_csv(path, sep=sep)
    else:
        raise ValueError(f"Unsupported file format: {p.suffix}. Use .csv, .tsv, .xlsx, or .xls")
    df.columns = [c.lower().strip() for c in df.columns]
    return df


# =====================================================================
# Optional Enrichment Hooks
# =====================================================================

def _resolve_census_key() -> Optional[str]:
    """Resolve Census API key. Returns None if unavailable."""
    return os.getenv("CENSUS_API_KEY")


def _resolve_bea_key() -> Optional[str]:
    """Resolve BEA API key with alias fallback. Returns None if unavailable."""
    return os.getenv("BEA_API_KEY") or os.getenv("BEA_USER_ID")


def enrich_geography(
    df: pd.DataFrame,
    geo_field: str,
    census_key: Optional[str] = None,
    bea_key: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Optionally enrich loan data with external geographic context.

    This is a hook point — enrichment only runs when API keys are available
    and the relevant libraries can reach external services. The workflow
    MUST run without enrichment.

    Parameters
    ----------
    df : DataFrame
        Loan data with geo_field column
    geo_field : str
        Name of the geographic column (msa, zip_code, or county)
    census_key : str or None
        Census API key (optional)
    bea_key : str or None
        BEA API key (optional)

    Returns
    -------
    (enriched_df, diagnostics)
        enriched_df has the same rows, possibly with added columns.
        diagnostics reports what enrichment was attempted/skipped.
    """
    diag: Dict[str, Any] = {
        "census_attempted": False,
        "census_success": False,
        "bea_attempted": False,
        "bea_success": False,
        "case_shiller_attempted": False,
        "case_shiller_success": False,
        "columns_added": [],
    }

    # Census enrichment (population, income by geography)
    if census_key and geo_field in ("zip_code", "county"):
        diag["census_attempted"] = True
        try:
            # Hook point: implement Census API call here when needed
            # For now, skip — workflow must run without internet
            pass
        except Exception:
            pass

    # BEA enrichment (GDP, employment by MSA/county)
    if bea_key and geo_field in ("msa", "county"):
        diag["bea_attempted"] = True
        try:
            # Hook point: implement BEA API call here when needed
            pass
        except Exception:
            pass

    # Case-Shiller enrichment (HPI context via existing mapper)
    if geo_field == "zip_code":
        diag["case_shiller_attempted"] = True
        try:
            from case_shiller_zip_mapper import map_zip_to_metro
            df = df.copy()
            df["case_shiller_metro"] = df[geo_field].astype(str).str.zfill(5).map(
                lambda z: map_zip_to_metro(z)
            )
            matched = df["case_shiller_metro"].notna().sum()
            if matched > 0:
                diag["case_shiller_success"] = True
                diag["columns_added"].append("case_shiller_metro")
        except ImportError:
            pass
        except Exception:
            pass

    return df, diag


# =====================================================================
# Artifact 1: Loan Balance by Product (bar chart)
# =====================================================================

def generate_loan_balance_by_product(
    df: pd.DataFrame,
    save_path: Optional[str] = None,
) -> Any:
    """Descending bar chart of current_balance by product_type.

    Parameters
    ----------
    df : DataFrame with 'product_type' and 'current_balance' columns
    save_path : file path for PNG output

    Returns
    -------
    matplotlib Figure, or None if matplotlib unavailable
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    agg = (
        df.groupby("product_type")["current_balance"]
        .sum()
        .sort_values(ascending=False)
    )

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(range(len(agg)), agg.values, color="#5B9BD5")
    ax.set_yticks(range(len(agg)))
    ax.set_yticklabels(agg.index, fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Current Balance ($)", fontsize=10)
    ax.set_title("Loan Balance by Product Type", fontsize=12, fontweight="bold",
                 color="#002F6C")

    # Format x-axis as $B or $M
    max_val = agg.max()
    if max_val >= 1e9:
        ax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e9:.1f}B")
        )
    elif max_val >= 1e6:
        ax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e6:.0f}M")
        )

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


# =====================================================================
# Artifact 2: Top 10 Geography by Balance (bar chart)
# =====================================================================

def generate_top10_geography(
    df: pd.DataFrame,
    geo_field: str,
    save_path: Optional[str] = None,
) -> Any:
    """Top 10 geographies by aggregate current_balance.

    Parameters
    ----------
    df : DataFrame with geo_field and 'current_balance' columns
    geo_field : column name for geography (msa, zip_code, or county)
    save_path : file path for PNG output

    Returns
    -------
    matplotlib Figure, or None if matplotlib unavailable
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    agg = (
        df.groupby(geo_field)["current_balance"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    geo_labels = {
        "msa": "MSA",
        "zip_code": "ZIP Code",
        "county": "County",
    }
    geo_display = geo_labels.get(geo_field, geo_field)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(range(len(agg)), agg.values, color="#70AD47")
    ax.set_yticks(range(len(agg)))
    ax.set_yticklabels(agg.index.astype(str), fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel("Current Balance ($)", fontsize=10)
    ax.set_title(f"Top 10 {geo_display} by Loan Balance", fontsize=12,
                 fontweight="bold", color="#002F6C")

    max_val = agg.max() if len(agg) > 0 else 0
    if max_val >= 1e9:
        ax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e9:.1f}B")
        )
    elif max_val >= 1e6:
        ax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: f"${x/1e6:.0f}M")
        )

    fig.tight_layout()
    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig


# =====================================================================
# Artifact 3: Internal Credit Flags Summary (HTML table)
# =====================================================================

def generate_credit_flags_summary(
    df: pd.DataFrame,
    available_optional: List[str],
) -> str:
    """HTML summary of delinquency/nonaccrual/risk-rating distribution.

    Produces a reduced table if optional columns are absent.

    Parameters
    ----------
    df : loan-level DataFrame
    available_optional : list of optional columns present in df

    Returns
    -------
    HTML string
    """
    date_str = datetime.now().strftime("%B %d, %Y")
    total_balance = df["current_balance"].sum()
    total_loans = len(df)

    sections = []

    # Risk Rating distribution
    if "risk_rating" in available_optional:
        rr = (
            df.groupby("risk_rating")
            .agg(count=("loan_id", "count"), balance=("current_balance", "sum"))
            .sort_values("balance", ascending=False)
        )
        rows = ""
        for rating, row in rr.iterrows():
            pct_count = row["count"] / total_loans * 100 if total_loans else 0
            pct_bal = row["balance"] / total_balance * 100 if total_balance else 0
            rows += (
                f"<tr><td>{rating}</td>"
                f"<td>{int(row['count']):,}</td>"
                f"<td>{pct_count:.1f}%</td>"
                f"<td>${row['balance']/1e6:,.1f}M</td>"
                f"<td>{pct_bal:.1f}%</td></tr>"
            )
        sections.append(
            f"<h4>Risk Rating Distribution</h4>"
            f"<table><thead><tr><th>Rating</th><th>Count</th><th>% of Loans</th>"
            f"<th>Balance</th><th>% of Balance</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    # Delinquency status distribution
    if "delinquency_status" in available_optional:
        dq = (
            df.groupby("delinquency_status")
            .agg(count=("loan_id", "count"), balance=("current_balance", "sum"))
            .sort_values("balance", ascending=False)
        )
        rows = ""
        for status, row in dq.iterrows():
            pct_count = row["count"] / total_loans * 100 if total_loans else 0
            pct_bal = row["balance"] / total_balance * 100 if total_balance else 0
            rows += (
                f"<tr><td>{status}</td>"
                f"<td>{int(row['count']):,}</td>"
                f"<td>{pct_count:.1f}%</td>"
                f"<td>${row['balance']/1e6:,.1f}M</td>"
                f"<td>{pct_bal:.1f}%</td></tr>"
            )
        sections.append(
            f"<h4>Delinquency Status Distribution</h4>"
            f"<table><thead><tr><th>Status</th><th>Count</th><th>% of Loans</th>"
            f"<th>Balance</th><th>% of Balance</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    # Nonaccrual flag distribution
    if "nonaccrual_flag" in available_optional:
        na_df = df.copy()
        na_df["nonaccrual_flag"] = na_df["nonaccrual_flag"].fillna("Unknown")
        na = (
            na_df.groupby("nonaccrual_flag")
            .agg(count=("loan_id", "count"), balance=("current_balance", "sum"))
            .sort_values("balance", ascending=False)
        )
        rows = ""
        for flag, row in na.iterrows():
            pct_count = row["count"] / total_loans * 100 if total_loans else 0
            pct_bal = row["balance"] / total_balance * 100 if total_balance else 0
            rows += (
                f"<tr><td>{flag}</td>"
                f"<td>{int(row['count']):,}</td>"
                f"<td>{pct_count:.1f}%</td>"
                f"<td>${row['balance']/1e6:,.1f}M</td>"
                f"<td>{pct_bal:.1f}%</td></tr>"
            )
        sections.append(
            f"<h4>Nonaccrual Flag Distribution</h4>"
            f"<table><thead><tr><th>Flag</th><th>Count</th><th>% of Loans</th>"
            f"<th>Balance</th><th>% of Balance</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )

    # Summary section (always present)
    summary_section = (
        f"<h4>Portfolio Summary</h4>"
        f"<table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>"
        f"<tr><td>Total Loans</td><td>{total_loans:,}</td></tr>"
        f"<tr><td>Total Balance</td><td>${total_balance/1e6:,.1f}M</td></tr>"
        f"<tr><td>Unique Products</td><td>{df['product_type'].nunique()}</td></tr>"
    )
    if "risk_rating" in available_optional:
        summary_section += f"<tr><td>Distinct Risk Ratings</td><td>{df['risk_rating'].nunique()}</td></tr>"
    if "nonaccrual_flag" in available_optional:
        na_count = df["nonaccrual_flag"].fillna("").astype(str).str.lower().isin(
            ["y", "yes", "true", "1", "nonaccrual"]
        ).sum()
        na_pct = na_count / total_loans * 100 if total_loans else 0
        summary_section += f"<tr><td>Nonaccrual Loans</td><td>{na_count:,} ({na_pct:.2f}%)</td></tr>"
    summary_section += "</tbody></table>"

    if not sections:
        sections.append(
            "<p><em>No optional credit-quality columns (risk_rating, "
            "delinquency_status, nonaccrual_flag) found in loan file. "
            "Showing portfolio summary only.</em></p>"
        )

    body = "\n".join(sections)

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .container {{ padding: 20px; max-width: 1200px; margin: 0 auto; }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        h4 {{ color: #002F6C; margin-top: 20px; margin-bottom: 8px; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 11px; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; text-align: center; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; }}
        td:first-child {{ text-align: left; font-weight: bold; color: #2c3e50; }}
        em {{ color: #888; }}
    </style></head><body>
    <div class="container">
        <h3>Internal Credit Flags Summary</h3>
        <p class="date-header">{date_str}</p>
        {summary_section}
        {body}
    </div></body></html>"""

    return html


# =====================================================================
# Artifact 4: Peer vs Internal Mix Bridge (HTML table)
# =====================================================================

def generate_peer_vs_internal_bridge(
    df: pd.DataFrame,
    dashboard_composition: Dict[str, float],
    geo_field: str,
) -> str:
    """Juxtapose peer-report composition with internal loan product/geo mix.

    Parameters
    ----------
    df : loan-level DataFrame
    dashboard_composition : dict from load_dashboard_composition()
    geo_field : resolved geo column name

    Returns
    -------
    HTML string
    """
    date_str = datetime.now().strftime("%B %d, %Y")
    total_balance = df["current_balance"].sum()

    # Internal product mix
    product_mix = (
        df.groupby("product_type")["current_balance"]
        .sum()
        .sort_values(ascending=False)
    )
    product_shares = (product_mix / total_balance * 100) if total_balance else product_mix * 0

    # Internal geo mix (top 5)
    geo_mix = (
        df.groupby(geo_field)["current_balance"]
        .sum()
        .sort_values(ascending=False)
        .head(5)
    )
    geo_shares = (geo_mix / total_balance * 100) if total_balance else geo_mix * 0

    # Build product rows
    product_rows = ""
    for prod, share in product_shares.items():
        bal = product_mix[prod]
        product_rows += (
            f"<tr><td>{prod}</td>"
            f"<td>${bal/1e6:,.1f}M</td>"
            f"<td>{share:.1f}%</td></tr>"
        )

    # Build peer composition rows
    _METRIC_DISPLAY = {
        "SBL_Composition": "SBL Share",
        "RIC_CRE_Loan_Share": "CRE Loan Share",
        "RIC_Resi_Loan_Share": "Resi Loan Share",
        "Fund_Finance_Composition": "Fund Finance Share",
    }
    peer_rows = ""
    if dashboard_composition:
        for metric, val in dashboard_composition.items():
            display = _METRIC_DISPLAY.get(metric, metric)
            # Values from dashboard are fractions (0-1) or percentages
            if abs(val) < 1.0:
                formatted = f"{val * 100:.2f}%"
            else:
                formatted = f"{val:.2f}%"
            peer_rows += f"<tr><td>{display}</td><td>{formatted}</td></tr>"
    else:
        peer_rows = (
            "<tr><td colspan='2'><em>Dashboard composition not available. "
            "Run MSPBNA_CR_Normalized.py first.</em></td></tr>"
        )

    # Build geo rows
    geo_labels = {"msa": "MSA", "zip_code": "ZIP Code", "county": "County"}
    geo_display = geo_labels.get(geo_field, geo_field)
    geo_rows = ""
    for geo, share in geo_shares.items():
        bal = geo_mix[geo]
        geo_rows += (
            f"<tr><td>{geo}</td>"
            f"<td>${bal/1e6:,.1f}M</td>"
            f"<td>{share:.1f}%</td></tr>"
        )

    html = f"""<html><head><style>
        body {{ font-family: Arial, sans-serif; background-color: transparent; }}
        .container {{ padding: 20px; max-width: 1200px; margin: 0 auto; }}
        h3 {{ color: #002F6C; margin-bottom: 5px; text-align: center; }}
        h4 {{ color: #002F6C; margin-top: 20px; margin-bottom: 8px; }}
        p.date-header {{ margin-top: 0; font-weight: bold; color: #555; text-align: center; }}
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; font-size: 11px; }}
        th {{ background-color: #002F6C; color: white; padding: 8px; border: 1px solid #2c3e50; text-align: center; }}
        td {{ padding: 6px; text-align: center; border: 1px solid #e0e0e0; }}
        td:first-child {{ text-align: left; font-weight: bold; color: #2c3e50; }}
        .section-header {{ background-color: #E6F3FF; font-weight: bold; color: #002F6C; }}
    </style></head><body>
    <div class="container">
        <h3>Peer vs Internal Portfolio Mix Bridge</h3>
        <p class="date-header">{date_str}</p>

        <h4>MSPBNA Peer-Report Composition (from Dashboard)</h4>
        <table>
            <thead><tr><th>Metric</th><th>MSPBNA Value</th></tr></thead>
            <tbody>{peer_rows}</tbody>
        </table>

        <h4>Internal Loan Product Mix</h4>
        <table>
            <thead><tr><th>Product Type</th><th>Balance</th><th>Share</th></tr></thead>
            <tbody>{product_rows}</tbody>
        </table>

        <h4>Internal Geographic Concentration (Top 5 by {geo_display})</h4>
        <table>
            <thead><tr><th>{geo_display}</th><th>Balance</th><th>Share</th></tr></thead>
            <tbody>{geo_rows}</tbody>
        </table>

        <p style="font-size:10px; color:#888; text-align:center;">
            Total Internal Portfolio Balance: ${total_balance/1e6:,.1f}M
            &nbsp;|&nbsp; Total Loans: {len(df):,}
        </p>
    </div></body></html>"""

    return html


# =====================================================================
# Orchestrator — run all artifacts
# =====================================================================

def run_corp_overlay(
    loan_file_path: str,
    dashboard_path: Optional[str] = None,
    output_dir: str = "output/Peers/corp_overlay",
    render_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """Run the full corp-overlay workflow.

    Parameters
    ----------
    loan_file_path : str
        Path to the internal loan-level CSV or Excel file.
    dashboard_path : str or None
        Path to the dashboard Excel. If None, auto-discovers latest.
    output_dir : str
        Directory for output artifacts.
    render_mode : str or None
        Render mode override. Default: resolve from env.

    Returns
    -------
    dict with keys: manifest, contract_diagnostics, enrichment_diagnostics,
                    artifacts_produced, errors
    """
    mode = select_mode(render_mode)
    manifest = ArtifactManifest(mode)
    csv_log = setup_csv_logging("corp_overlay", log_dir="logs")
    errors: List[str] = []

    result: Dict[str, Any] = {
        "manifest": manifest,
        "contract_diagnostics": {},
        "enrichment_diagnostics": {},
        "artifacts_produced": [],
        "errors": errors,
    }

    try:
        # --- Create output directory ---
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        date_str = get_run_date_str()
        stem = f"corp_overlay_{date_str}"

        csv_log.info(f"Corp overlay started, mode={mode.value}", event_type="CONFIG",
                     phase="startup")

        # --- Load and validate loan file ---
        print(f"Loading loan file: {loan_file_path}")
        csv_log.info(f"Loading loan file: {loan_file_path}", event_type="FILE_DISCOVERED",
                     phase="ingestion")
        loan_df = load_loan_file(loan_file_path)
        contract = validate_loan_file(loan_df)
        result["contract_diagnostics"] = contract
        geo_field = contract["geo_field"]
        available_opt = contract["available_optional"]

        print(f"  Loan file validated: {contract['row_count']} rows, "
              f"{contract['unique_loans']} unique loans, geo={geo_field}")
        csv_log.log_df_shape("loan_file", contract["row_count"],
                             len(loan_df.columns), phase="ingestion")

        # --- Load dashboard composition ---
        dash_path = dashboard_path or find_latest_dashboard()
        dash_comp: Dict[str, float] = {}
        if dash_path:
            print(f"Loading dashboard: {dash_path}")
            csv_log.info(f"Dashboard: {dash_path}", event_type="FILE_DISCOVERED",
                         phase="ingestion")
            dash_comp = load_dashboard_composition(dash_path)
            print(f"  Dashboard metrics loaded: {list(dash_comp.keys())}")
        else:
            print("  WARNING: No dashboard file found. Bridge table will show N/A for peer data.")
            csv_log.warning("No dashboard file found", event_type="VALIDATION_WARNING",
                            phase="ingestion")

        # --- Optional enrichment ---
        census_key = _resolve_census_key()
        bea_key = _resolve_bea_key()
        loan_df, enrich_diag = enrich_geography(loan_df, geo_field, census_key, bea_key)
        result["enrichment_diagnostics"] = enrich_diag
        if any(enrich_diag.get(f"{src}_success") for src in ("census", "bea", "case_shiller")):
            print(f"  Enrichment columns added: {enrich_diag['columns_added']}")

        # --- Artifact 1: loan_balance_by_product.png ---
        art_name = "loan_balance_by_product"
        if should_produce(art_name, mode, manifest):
            try:
                path = str(out / f"{stem}_{art_name}.png")
                fig = generate_loan_balance_by_product(loan_df, save_path=path)
                if fig is not None:
                    manifest.record_generated(art_name, path)
                    csv_log.log_file_written(path, phase="output", component=art_name)
                    result["artifacts_produced"].append(path)
                    print(f"  {art_name} saved: {path}")
                else:
                    manifest.record_failed(art_name, "matplotlib unavailable")
            except Exception as exc:
                manifest.record_failed(art_name, str(exc)[:200])
                errors.append(f"{art_name}: {exc}")

        # --- Artifact 2: top10_geography_by_balance.png ---
        art_name = "top10_geography_by_balance"
        if should_produce(art_name, mode, manifest):
            try:
                path = str(out / f"{stem}_{art_name}.png")
                fig = generate_top10_geography(loan_df, geo_field, save_path=path)
                if fig is not None:
                    manifest.record_generated(art_name, path)
                    csv_log.log_file_written(path, phase="output", component=art_name)
                    result["artifacts_produced"].append(path)
                    print(f"  {art_name} saved: {path}")
                else:
                    manifest.record_failed(art_name, "matplotlib unavailable")
            except Exception as exc:
                manifest.record_failed(art_name, str(exc)[:200])
                errors.append(f"{art_name}: {exc}")

        # --- Artifact 3: internal_credit_flags_summary.html ---
        art_name = "internal_credit_flags_summary"
        if should_produce(art_name, mode, manifest):
            try:
                path = str(out / f"{stem}_{art_name}.html")
                html = generate_credit_flags_summary(loan_df, available_opt)
                if html:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(html)
                    manifest.record_generated(art_name, path)
                    csv_log.log_file_written(path, phase="output", component=art_name)
                    result["artifacts_produced"].append(path)
                    print(f"  {art_name} saved: {path}")
                else:
                    manifest.record_failed(art_name, "generator returned empty")
            except Exception as exc:
                manifest.record_failed(art_name, str(exc)[:200])
                errors.append(f"{art_name}: {exc}")

        # --- Artifact 4: peer_vs_internal_mix_bridge.html ---
        art_name = "peer_vs_internal_mix_bridge"
        if should_produce(art_name, mode, manifest):
            try:
                path = str(out / f"{stem}_{art_name}.html")
                html = generate_peer_vs_internal_bridge(loan_df, dash_comp, geo_field)
                if html:
                    with open(path, "w", encoding="utf-8") as f:
                        f.write(html)
                    manifest.record_generated(art_name, path)
                    csv_log.log_file_written(path, phase="output", component=art_name)
                    result["artifacts_produced"].append(path)
                    print(f"  {art_name} saved: {path}")
                else:
                    manifest.record_failed(art_name, "generator returned empty")
            except Exception as exc:
                manifest.record_failed(art_name, str(exc)[:200])
                errors.append(f"{art_name}: {exc}")

        # --- Print manifest summary ---
        print("\n" + manifest.summary_table())

    except LoanFileContractError as exc:
        errors.append(f"Contract validation failed: {exc}")
        csv_log.error(str(exc), event_type="VALIDATION_ERROR", phase="ingestion")
        print(f"ERROR: {exc}")
    except Exception as exc:
        errors.append(f"Unexpected error: {exc}")
        csv_log.log_exception(exc, phase="output")
        print(f"ERROR: {exc}")
    finally:
        csv_log.shutdown()

    return result


# =====================================================================
# MSA-Level Macro Panel — dynamic geographic macro context
# =====================================================================

# FRED series patterns for MSA-level macro data.
# Actual series IDs are constructed dynamically per MSA.
_MSA_MACRO_SERIES = {
    "case_shiller_yoy": {
        "label": "Case-Shiller HPI YoY %",
        "unit": "%",
        "note": "Year-over-year house price growth",
    },
    "gdp_yoy": {
        "label": "GDP YoY %",
        "unit": "%",
        "note": "Year-over-year GDP growth (BEA)",
    },
    "unemployment_chg": {
        "label": "Unemployment Rate Chg (pp)",
        "unit": "pp",
        "note": "Year-over-year change in unemployment rate (percentage points)",
    },
}


def select_top_msas(
    loan_df: pd.DataFrame,
    geo_field: str = "msa",
    top_n: int = 5,
) -> List[str]:
    """Select top-N MSAs by aggregate loan balance from the internal loan file.

    Parameters
    ----------
    loan_df : DataFrame with geo_field and current_balance columns
    geo_field : column to aggregate by (default: msa)
    top_n : number of top MSAs to return

    Returns
    -------
    List of MSA identifiers, sorted descending by balance
    """
    if geo_field not in loan_df.columns or "current_balance" not in loan_df.columns:
        return []

    work = loan_df[[geo_field, "current_balance"]].copy()
    work["current_balance"] = pd.to_numeric(work["current_balance"], errors="coerce")
    work = work.dropna(subset=[geo_field, "current_balance"])
    if work.empty:
        return []

    agg = (
        work.groupby(geo_field)["current_balance"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
    )
    return list(agg.index)


def build_msa_macro_panel(
    msas: List[str],
    case_shiller_df: Optional[pd.DataFrame] = None,
    bea_gdp_df: Optional[pd.DataFrame] = None,
    unemployment_df: Optional[pd.DataFrame] = None,
    save_path: Optional[str] = None,
) -> Any:
    """Build a multi-panel macro chart for selected MSAs.

    Each MSA gets a small-multiple row with up to 3 subplots:
      1. Case-Shiller HPI YoY % (house price growth)
      2. GDP YoY % (economic growth from BEA)
      3. Unemployment Rate Change in percentage points (NOT %, NOT mislabeled)

    Critical unit rules:
      - House price and GDP growth are in % (e.g., +5.2%)
      - Unemployment change is in percentage points (e.g., +0.3 pp)
      - Y-axis labels MUST reflect the correct unit

    Parameters
    ----------
    msas : list of MSA identifiers (from select_top_msas)
    case_shiller_df : DataFrame with columns [msa, date, hpi_yoy_pct]
    bea_gdp_df : DataFrame with columns [msa, date, gdp_yoy_pct]
    unemployment_df : DataFrame with columns [msa, date, unemp_rate_chg_pp]
    save_path : file path for PNG output

    Returns
    -------
    matplotlib Figure, or None if no data or matplotlib unavailable
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    if not msas:
        print("  Skipped MSA macro panel: no MSAs selected")
        return None

    # Determine which data panels are available
    panels = []
    if case_shiller_df is not None and not case_shiller_df.empty:
        panels.append(("hpi_yoy_pct", "Case-Shiller HPI YoY (%)", "%", case_shiller_df))
    if bea_gdp_df is not None and not bea_gdp_df.empty:
        panels.append(("gdp_yoy_pct", "GDP YoY (%)", "%", bea_gdp_df))
    if unemployment_df is not None and not unemployment_df.empty:
        panels.append(("unemp_rate_chg_pp", "Unemployment Rate Chg (pp)", "pp", unemployment_df))

    if not panels:
        print("  Skipped MSA macro panel: no macro data available")
        return None

    n_msas = len(msas)
    n_panels = len(panels)

    fig, axes = plt.subplots(
        n_msas, n_panels, figsize=(5 * n_panels, 3 * n_msas),
        squeeze=False,
    )
    fig.patch.set_alpha(0)

    # Color palette for MSA panels
    _MSA_COLORS = ["#5B9BD5", "#70AD47", "#ED7D31", "#FFC000", "#A855F7"]

    for row_idx, msa in enumerate(msas):
        for col_idx, (value_col, title, unit, panel_df) in enumerate(panels):
            ax = axes[row_idx, col_idx]
            ax.set_facecolor("none")

            # Filter data for this MSA
            msa_col = "msa" if "msa" in panel_df.columns else panel_df.columns[0]
            date_col = "date" if "date" in panel_df.columns else panel_df.columns[1]
            msa_data = panel_df[panel_df[msa_col].astype(str) == str(msa)].copy()
            if msa_data.empty:
                ax.text(0.5, 0.5, "No data", transform=ax.transAxes,
                        ha="center", va="center", fontsize=10, color="#888888")
                ax.set_title(f"{msa}" if row_idx == 0 else "", fontsize=10)
                continue

            msa_data[date_col] = pd.to_datetime(msa_data[date_col], errors="coerce")
            msa_data = msa_data.sort_values(date_col)

            color = _MSA_COLORS[row_idx % len(_MSA_COLORS)]
            ax.plot(msa_data[date_col], msa_data[value_col],
                    color=color, linewidth=1.5)
            ax.axhline(0, color="#888888", linewidth=0.5, linestyle="--", alpha=0.5)

            # Labels
            if row_idx == 0:
                ax.set_title(title, fontsize=11, fontweight="bold", color="#2B2B2B")
            if col_idx == 0:
                ax.set_ylabel(str(msa), fontsize=10, fontweight="bold", rotation=0,
                              labelpad=50, ha="right")
            else:
                ax.set_ylabel("")

            # Y-axis unit label
            ax.yaxis.set_major_formatter(
                plt.FuncFormatter(
                    lambda x, _, u=unit: f"{x:.1f}{u}" if u == "%" else f"{x:+.2f} {u}"
                )
            )

            # Clean up
            for sp in ["top", "right"]:
                ax.spines[sp].set_visible(False)
            ax.tick_params(axis="x", labelsize=8, rotation=30)
            ax.tick_params(axis="y", labelsize=8)
            ax.grid(True, alpha=0.3, color="#D0D0D0")

    fig.suptitle("MSA Macro Backdrop — Top Exposures",
                 fontsize=14, fontweight="bold", color="#2B2B2B", y=1.02)
    fig.tight_layout()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(save_path, dpi=150, bbox_inches="tight", transparent=True)
        plt.close(fig)
    return fig
