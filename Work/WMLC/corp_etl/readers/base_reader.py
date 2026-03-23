"""Reader for the base loan extract (CSV or XLSX)."""

import logging
import pandas as pd
from corp_etl.file_utils import read_file, normalize_columns, pad12, clean_key_column

logger = logging.getLogger("wmlc_etl.readers.base")


def read_base_extract(path):
    """Read loan_extract and normalise columns + key fields.

    Args:
        path: file path to the CSV or XLSX.

    Returns:
        pandas DataFrame with normalized column names and
        clean key columns (ACCOUNT_NUMBER, FACILITY_ID, TL_FACILITY_DIGITS12).
    """
    logger.info(f"Reading base extract: {path}")
    df = read_file(path, skip_rows=0)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.debug(f"Raw columns: {list(df.columns)}")

    # Normalize column names
    df, changed = normalize_columns(df)

    # Convert numeric columns back to proper types
    numeric_cols = [
        "SBL_PERC", "BALANCE", "CREDIT_LIMIT", "AMT_ORIGINAL_COMT",
        "CREDIT_LII", "BASE_COMMIT", "LATEST_COMMIT", "COMMIT_DELTA",
        "NEW_COMMITMENT_AMOUNT", "CREDIT_LII_COMMITMENT_FLOOR",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Boolean columns
    if "IS_LAL_NFP" in df.columns:
        df["IS_LAL_NFP"] = df["IS_LAL_NFP"].astype(str).str.lower().map(
            {"true": True, "false": False, "1": True, "0": False}
        ).fillna(False)

    # Format and clean key columns using shared pad12 (handles nan/none/empty)
    for key_col in ["ACCOUNT_NUMBER", "FACILITY_ID", "TL_FACILITY_DIGITS12"]:
        if key_col in df.columns:
            df[key_col] = df[key_col].apply(pad12)

    # Clean key columns: ensure padded "nan" strings become real None
    for key_col in ["ACCOUNT_NUMBER", "FACILITY_ID", "TL_FACILITY_DIGITS12"]:
        if key_col in df.columns:
            df[key_col] = clean_key_column(df[key_col], logger_name=key_col)

    # Fill missing FACILITY_ID from TL_FACILITY_DIGITS12
    if "FACILITY_ID" in df.columns and "TL_FACILITY_DIGITS12" in df.columns:
        before_nulls = df["FACILITY_ID"].isna().sum()
        df["FACILITY_ID"] = df["FACILITY_ID"].fillna(df["TL_FACILITY_DIGITS12"])
        after_nulls = df["FACILITY_ID"].isna().sum()
        logger.info(f"FACILITY_ID fillna: {before_nulls} nulls before, {after_nulls} after "
                    f"(filled {before_nulls - after_nulls} from TL_FACILITY_DIGITS12)")

    logger.debug(f"First 3 rows:\n{df.head(3).to_string()}")
    return df
