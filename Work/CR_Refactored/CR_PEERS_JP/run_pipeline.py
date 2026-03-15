#!/usr/bin/env python3
"""
CR_PEERS_JP Pipeline Runner
=============================

Runs the full MSPBNA Credit Risk pipeline sequentially:
  Step 1: MSPBNA_CR_Normalized.py  (data fetch + processing → Excel dashboard)
  Step 2: report_generator.py      (charts, scatters, HTML tables)

Usage:
    python run_pipeline.py                    # Both steps, full_local mode
    python run_pipeline.py --mode corp_safe   # Both steps, corp_safe mode
    python run_pipeline.py --step 2           # Step 2 only (assumes Step 1 already ran)
    python run_pipeline.py --force            # Continue Step 2 even if Step 1 fails
"""

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path


def _load_env():
    """Load .env from project root if python-dotenv is available."""
    env_path = Path(__file__).parent.resolve() / ".env"
    if env_path.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
            print(f"[pipeline] Loaded .env from {env_path}")
        except ImportError:
            print("[pipeline] python-dotenv not installed — skipping .env load")


def _run_step(label: str, cmd: list, env: dict) -> tuple:
    """Run a pipeline step via subprocess. Returns (exit_code, elapsed_seconds)."""
    print(f"\n{'='*72}")
    print(f"  {label}")
    print(f"{'='*72}\n")
    t0 = time.time()
    result = subprocess.run(cmd, env=env, cwd=str(Path(__file__).parent.resolve()))
    elapsed = time.time() - t0
    status = "SUCCESS" if result.returncode == 0 else f"FAILED (exit code {result.returncode})"
    print(f"\n[pipeline] {label}: {status} ({elapsed:.1f}s)")
    return result.returncode, elapsed


def main():
    parser = argparse.ArgumentParser(description="CR_PEERS_JP Pipeline Runner")
    parser.add_argument("--mode", choices=["full_local", "corp_safe"],
                        default="full_local", help="Render mode for report_generator")
    parser.add_argument("--step", choices=["1", "2", "both"],
                        default="both", help="Which step(s) to run")
    parser.add_argument("--force", action="store_true",
                        help="Continue to Step 2 even if Step 1 fails")
    args = parser.parse_args()

    _load_env()

    python = sys.executable
    env = os.environ.copy()
    env["REPORT_MODE"] = args.mode

    results = {}

    # Step 1: Data Fetch & Processing
    if args.step in ("1", "both"):
        rc, elapsed = _run_step(
            "STEP 1: Data Fetch & Processing (MSPBNA_CR_Normalized.py)",
            [python, os.path.join("src", "data_processing", "MSPBNA_CR_Normalized.py")],
            env,
        )
        results["Step 1"] = {"exit_code": rc, "elapsed": elapsed}

        if rc != 0 and args.step == "both" and not args.force:
            print(f"\n[pipeline] Step 1 failed. Use --force to continue to Step 2 anyway.")
            _print_summary(results)
            sys.exit(rc)

    # Step 2: Report Generation
    if args.step in ("2", "both"):
        rc, elapsed = _run_step(
            "STEP 2: Report Generation (report_generator.py)",
            [python, os.path.join("src", "reporting", "report_generator.py"), args.mode],
            env,
        )
        results["Step 2"] = {"exit_code": rc, "elapsed": elapsed}

    _print_summary(results)

    # Exit with worst exit code
    worst = max((v["exit_code"] for v in results.values()), default=0)
    sys.exit(worst)


def _print_summary(results: dict):
    """Print a combined pipeline summary."""
    print(f"\n{'='*72}")
    print("  PIPELINE SUMMARY")
    print(f"{'='*72}")
    total_time = 0
    for step, info in results.items():
        status = "OK" if info["exit_code"] == 0 else f"FAILED ({info['exit_code']})"
        print(f"  {step}: {status}  ({info['elapsed']:.1f}s)")
        total_time += info["elapsed"]
    print(f"  Total elapsed: {total_time:.1f}s")
    print(f"{'='*72}\n")


if __name__ == "__main__":
    main()
