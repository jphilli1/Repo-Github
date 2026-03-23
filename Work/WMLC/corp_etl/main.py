"""WMLC ETL Pipeline — Entry Point.

Reads config.yaml, loads all input files, computes intermediate tags,
evaluates WMLC flags, and outputs tagged CSV.

Usage:
    python corp_etl/main.py
"""

import os
import sys
import time
import logging
import yaml
import pandas as pd
from datetime import datetime

# Ensure the repo root is on sys.path so imports work
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from corp_etl.readers.base_reader import read_base_extract
from corp_etl.readers.lal_credit_reader import read_lal_credit
from corp_etl.readers.loan_reserve_reader import read_loan_reserve_report
from corp_etl.readers.dar_tracker_reader import read_dar_tracker
from corp_etl.taggers.intermediate_tags import apply_intermediate_tags
from corp_etl.taggers.wmlc_tagger import apply_wmlc_flags


def setup_logging(output_dir="./output"):
    """Configure dual logging: DEBUG to file, INFO to console."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(output_dir, f"etl_run_{timestamp}.log")

    logger = logging.getLogger("wmlc_etl")
    logger.setLevel(logging.DEBUG)

    # Remove existing handlers (prevents duplicates on re-run)
    logger.handlers.clear()

    # File handler — DEBUG level (everything)
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s.%(funcName)s | %(message)s"
    ))

    # Console handler — INFO level (summary only)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)-8s | %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Log file: {log_file}")
    return logger, log_file


def load_config(logger):
    """Load config.yaml from the corp_etl directory."""
    config_path = os.path.join(SCRIPT_DIR, "config.yaml")
    logger.info(f"Loading config: {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    logger.debug(f"Config contents: {config}")
    return config


def resolve_path(relative_path):
    """Resolve a path relative to the repo root."""
    return os.path.normpath(os.path.join(REPO_ROOT, relative_path.lstrip("./")))


def check_file_exists(path, label, logger):
    """Check if a file exists and log its status."""
    if os.path.exists(path):
        size = os.path.getsize(path)
        logger.info(f"{label}: {path} ({size:,} bytes)")
        return True
    else:
        logger.error(f"{label}: FILE NOT FOUND — {path}")
        return False


def main():
    start_time = time.time()

    # 0. Setup logging
    output_dir = os.path.join(REPO_ROOT, "output")
    logger, log_file = setup_logging(output_dir)

    logger.info("=" * 60)
    logger.info("WMLC ETL Pipeline")
    logger.info("=" * 60)

    # 1. Load config
    config = load_config(logger)
    inputs = config["input_files"]
    output_cfg = config["output"]

    # 2. Check all input files exist
    logger.info("--- Checking Input Files ---")
    base_path = resolve_path(inputs["base_extract"]["path"])
    check_file_exists(base_path, "Base extract", logger)

    lal_cfg = inputs["lal_credit"]
    lal_path = resolve_path(lal_cfg["path"])
    lal_exists = check_file_exists(lal_path, "LAL_Credit", logger)

    lrr_cfg = inputs["loan_reserve_report"]
    lrr_path = resolve_path(lrr_cfg["path"])
    lrr_exists = check_file_exists(lrr_path, "Loan_Reserve_Report", logger)

    dar_cfg = inputs["dar_tracker"]
    dar_path = resolve_path(dar_cfg["path"])
    dar_exists = check_file_exists(dar_path, "DAR_Tracker", logger)

    # 3. Read all input files (with graceful degradation)
    logger.info("--- Reading Input Files ---")
    base_df = read_base_extract(base_path)

    lal_df = pd.DataFrame()
    if lal_exists:
        try:
            lal_df = read_lal_credit(
                lal_path,
                skip_rows=lal_cfg.get("skip_rows", 3),
                key_column=lal_cfg.get("key_column", "Account Number"),
            )
        except Exception as e:
            logger.error(f"Failed to read LAL_Credit: {e}", exc_info=True)
            logger.warning("Continuing without LAL_Credit — NTC/CSE tags will be incomplete")
    else:
        logger.warning("Continuing without LAL_Credit — NTC/CSE tags will be incomplete")

    lrr_df = pd.DataFrame()
    if lrr_exists:
        try:
            lrr_df = read_loan_reserve_report(
                lrr_path,
                skip_rows=lrr_cfg.get("skip_rows", 6),
                key_column=lrr_cfg.get("key_column", "Facility Account Number"),
            )
        except Exception as e:
            logger.error(f"Failed to read Loan_Reserve_Report: {e}", exc_info=True)
            logger.warning("Continuing without Loan_Reserve_Report — TL NTC tags will be incomplete")
    else:
        logger.warning("Continuing without Loan_Reserve_Report — TL NTC tags will be incomplete")

    dar_df = pd.DataFrame()
    if dar_exists:
        try:
            dar_df = read_dar_tracker(
                dar_path,
                skip_rows=dar_cfg.get("skip_rows", 0),
                key_column=dar_cfg.get("key_column", "Facility ID"),
            )
        except Exception as e:
            logger.error(f"Failed to read DAR_Tracker: {e}", exc_info=True)
            logger.warning("Continuing without DAR_Tracker — is_office tags will be incomplete")
    else:
        logger.warning("Continuing without DAR_Tracker — is_office tags will be incomplete")

    # 4. Compute intermediate tags
    logger.info("--- Computing Intermediate Tags ---")
    tagged_df = apply_intermediate_tags(base_df, lal_df, lrr_df, dar_df)

    # 5. Evaluate WMLC flags
    logger.info("--- Evaluating WMLC Flags ---")
    tagged_df = apply_wmlc_flags(tagged_df)

    # 6. Write output
    output_path = resolve_path(output_cfg["tagged_file"])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    tagged_df.to_csv(output_path, index=False)
    file_size = os.path.getsize(output_path)
    logger.info(f"Output written: {output_path} ({len(tagged_df)} rows, "
                f"{len(tagged_df.columns)} columns, {file_size:,} bytes)")
    logger.info(f"New columns added: IS_NTC, IS_OFFICE, HAS_CREDIT_POLICY_EXCEPTION, "
                f"WMLC_FLAGS, WMLC_FLAG_COUNT, WMLC_QUALIFIED")
    logger.debug(f"Output dtypes:\n{tagged_df.dtypes.to_string()}")

    # 7. Verify output columns
    required_new_cols = [
        "IS_NTC", "IS_OFFICE", "HAS_CREDIT_POLICY_EXCEPTION",
        "WMLC_FLAGS", "WMLC_FLAG_COUNT", "WMLC_QUALIFIED",
    ]
    missing = [c for c in required_new_cols if c not in tagged_df.columns]
    if missing:
        logger.error(f"Missing output columns: {missing}")
        return 1
    else:
        logger.info("All required output columns present.")

    # 8. Summary
    elapsed = time.time() - start_time
    wmlc_count = int(tagged_df["WMLC_QUALIFIED"].sum())
    all_flags = set()
    for flags_str in tagged_df["WMLC_FLAGS"].dropna():
        if flags_str:
            all_flags.update(f.strip() for f in str(flags_str).split("|") if f.strip())

    logger.info("=" * 60)
    logger.info("WMLC ETL Pipeline — COMPLETE")
    logger.info(f"  {len(tagged_df)} loans processed, {wmlc_count} WMLC-qualified, "
                f"{len(all_flags)} distinct flag types")
    logger.info(f"  Runtime: {elapsed:.1f} seconds")
    logger.info(f"  Log file: {log_file}")
    logger.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
