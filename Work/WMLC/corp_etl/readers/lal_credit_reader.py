"""Reader for LAL_Credit (.xlsx or .csv).

Skip rows per config (default 3).
Key format: strip_dash_pad12  ("001-234567" -> "000001234567").
"""

import logging
from corp_etl.file_utils import read_file, normalize_columns, strip_dash_pad12

logger = logging.getLogger("wmlc_etl.readers.lal_credit")


def read_lal_credit(path, skip_rows=3, key_column="Account Number"):
    """Read LAL_Credit file and normalise the account key.

    Returns:
        DataFrame with normalised 'ACCOUNT_NUMBER_KEY' column.
    """
    logger.info(f"Reading LAL_Credit: {path} (skip_rows={skip_rows})")
    df = read_file(path, skip_rows=skip_rows)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    logger.debug(f"Raw columns: {list(df.columns)}")

    df, changed = normalize_columns(df)

    # Find the key column (now normalized)
    from corp_etl.column_matcher import find_column
    key_col = find_column(df, key_column)
    if key_col is None:
        logger.warning(f"Key column '{key_column}' not found after normalization. "
                       f"Columns: {list(df.columns)}")
        df["ACCOUNT_NUMBER_KEY"] = None
        return df

    df["ACCOUNT_NUMBER_KEY"] = df[key_col].apply(strip_dash_pad12)
    valid_keys = df["ACCOUNT_NUMBER_KEY"].notna().sum()
    unique_keys = df["ACCOUNT_NUMBER_KEY"].nunique()
    logger.info(f"Key column '{key_col}' found, {valid_keys} valid keys, {unique_keys} unique")

    if valid_keys < len(df):
        logger.warning(f"{len(df) - valid_keys} keys were null/nan and could not be formatted")

    logger.debug(f"First 3 rows:\n{df.head(3).to_string()}")
    return df
