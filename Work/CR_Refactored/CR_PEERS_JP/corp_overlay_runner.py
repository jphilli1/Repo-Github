#!/usr/bin/env python3
"""
Corp-Safe Overlay Runner — Separate CLI Entrypoint
====================================================

Standalone script that runs the corp overlay workflow.
NOT integrated into report_generator.py or MSPBNA_CR_Normalized.py.

Usage:
    python corp_overlay_runner.py <loan_file> [options]

    # Required argument:
    #   loan_file       Path to internal loan-level CSV or Excel file

    # Options:
    #   --dashboard     Path to dashboard Excel (default: auto-discover latest)
    #   --output-dir    Output directory (default: output/Peers/corp_overlay)
    #   --mode          Render mode: full_local or corp_safe (default: from env/full_local)

Examples:
    # Basic usage — auto-discovers dashboard, full_local mode
    python corp_overlay_runner.py data/internal_loans.csv

    # Explicit dashboard and corp_safe mode
    python corp_overlay_runner.py data/loans.xlsx --dashboard output/Bank_Performance_Dashboard_20260312.xlsx --mode corp_safe

    # Via environment variable
    export REPORT_MODE=corp_safe
    python corp_overlay_runner.py data/loans.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Corp-Safe Overlay: join local dashboard with internal loan data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Required loan file columns: loan_id, current_balance, product_type\n"
            "Required geo column (at least one): msa, zip_code, county\n"
            "Optional: risk_rating, delinquency_status, nonaccrual_flag, "
            "segment, portfolio, collateral_type"
        ),
    )
    parser.add_argument(
        "loan_file",
        help="Path to internal loan-level CSV or Excel file",
    )
    parser.add_argument(
        "--dashboard",
        default=None,
        help="Path to Bank_Performance_Dashboard_*.xlsx (default: auto-discover)",
    )
    parser.add_argument(
        "--output-dir",
        default="output/Peers/corp_overlay",
        help="Output directory for artifacts (default: output/Peers/corp_overlay)",
    )
    parser.add_argument(
        "--mode",
        default=None,
        choices=["full_local", "corp_safe"],
        help="Render mode (default: from REPORT_MODE env var or full_local)",
    )

    args = parser.parse_args()

    # Validate loan file exists
    loan_path = Path(args.loan_file)
    if not loan_path.exists():
        print(f"ERROR: Loan file not found: {loan_path}")
        return 1

    # Import here to keep startup fast and avoid import-time side effects
    from corp_overlay import run_corp_overlay

    print(f"{'='*60}")
    print(f"Corp-Safe Overlay Workflow")
    print(f"{'='*60}")

    result = run_corp_overlay(
        loan_file_path=str(loan_path),
        dashboard_path=args.dashboard,
        output_dir=args.output_dir,
        render_mode=args.mode,
    )

    if result["errors"]:
        print(f"\nErrors encountered ({len(result['errors'])}):")
        for err in result["errors"]:
            print(f"  - {err}")
        return 1

    counts = result["manifest"].counts()
    print(f"\nDone. Generated: {counts['generated']}, "
          f"Skipped: {counts['skipped']}, Failed: {counts['failed']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
