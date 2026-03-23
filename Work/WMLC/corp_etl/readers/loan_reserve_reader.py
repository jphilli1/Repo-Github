"""Reader for Loan_Reserve_Report (.xlsx or .csv).

Skip rows per config (default 6).
Key format: pad12 (zero-pad to 12 digits).
"""

import logging
from corp_etl.file_utils import read_file, normalize_columns, pad12

logger = logging.getLogger("wmlc_etl.readers.loan_reserve")


def read_loan_reserve_report(path, skip_rows=6, key_column="Facility Account Number"):
    """Read Loan_Reserve_Report and normalise the account key.

    Returns:
        DataFrame with normalised 'ACCOUNT_NUMBER_KEY' column.
    """
    logger.info(f"Reading Loan_Reserve_Report: {path} (skip_rows={skip_rows})")
    df = read_file(path, skip_rows=skip_rows)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.debug(f"Raw columns: {list(df.columns)}")

    df, changed = normalize_columns(df)

    from corp_etl.column_matcher import find_column
    key_col = find_column(df, key_column)
    if key_col is None:
        logger.warning(f"Key column '{key_column}' not found. Columns: {list(df.columns)}")
        df["ACCOUNT_NUMBER_KEY"] = None
        return df

    df["ACCOUNT_NUMBER_KEY"] = df[key_col].apply(pad12)
    valid_keys = df["ACCOUNT_NUMBER_KEY"].notna().sum()
    unique_keys = df["ACCOUNT_NUMBER_KEY"].nunique()
    logger.info(f"Key column '{key_col}' found, {valid_keys} valid keys, {unique_keys} unique")

    if valid_keys < len(df):
        logger.warning(f"{len(df) - valid_keys} keys were null/nan and could not be formatted")

    logger.debug(f"First 3 rows:\n{df.head(3).to_string()}")
    return df
