"""WMLC flag tagger — evaluates all 16 flags per specs/WMLC_LOGIC.md.

All flags are evaluated independently. A loan can carry multiple flags.
Results stored as pipe-delimited string in WMLC_FLAGS.
All column references use NORMALIZED (UPPERCASE) names.
All thresholds use >= (greater than or equal to).
"""

import logging
import pandas as pd

logger = logging.getLogger("wmlc_etl.taggers.wmlc")

TL_BUCKETS = {
    "TL Aircraft", "TL CRE", "TL Life Insurance", "TL Multicollateral",
    "TL Other Secured", "TL PHA", "TL SBL Diversified", "TL SBL Highly Conc.",
    "TL Unsecured",
}
LAL_BUCKETS = {"LAL Diversified", "LAL Highly Conc.", "LAL NFPs"}
ALL_LAL_TL = LAL_BUCKETS | TL_BUCKETS

FLAG_NAMES = [
    "NTC > $50MM",
    "Non-Pass Originations >$0MM",
    "TL-CRE >$75MM",
    "TL-CRE Office >$10MM",
    "TL-SBL-D >$300MM",
    "TL-SBL-C >$100MM",
    "TL-LIC >$100MM",
    "TL-Alts HF/PE >$35MM",
    "TL-Alts Private Shares >$35MM",
    "TL-Alts Unsecured >$35MM",
    "TL-Alts PAF >$50MM",
    "TL-Alts Fine Art >$50MM",
    "TL-Alts Other Secured >$50MM",
    "LAL-D >$300MM",
    "LAL-C >$100MM",
    "RESI >$10MM",
]


def _collateral_contains(collateral_str, keyword):
    """Case-insensitive CONTAINS check on collateral description."""
    if pd.isna(collateral_str) or not collateral_str:
        return False
    return keyword.lower() in str(collateral_str).lower()


def evaluate_flags(row):
    """Evaluate all 16 WMLC flags for a single loan row.

    All thresholds use >= (greater than or equal to).

    Returns:
        list of flag name strings that the row qualifies for.
    """
    flags = []
    bucket = str(row.get("PRODUCT_BUCKET", ""))
    credit = float(row.get("CREDIT_LII", 0) or 0)
    is_ntc = bool(row.get("IS_NTC", False))
    is_office = bool(row.get("IS_OFFICE", False))
    focus = str(row.get("FOCUS_LIST", "")).strip()
    new_camp = str(row.get("NEW_CAMP_YN", "")).strip().upper()
    collateral = str(row.get("TXT_MSTR_FACIL_COLLATERAL_DESC", ""))
    sbl_pct = 0.0
    try:
        sbl_pct = float(row.get("SBL_PERC", 0) or 0)
    except (ValueError, TypeError):
        pass

    # Flag 1: NTC >= $50MM
    if bucket in ALL_LAL_TL and is_ntc and credit >= 50_000_000:
        flags.append("NTC > $50MM")

    # Flag 2: Non-Pass Originations >$0MM (any amount)
    if focus == "Non-Pass" and new_camp == "Y":
        flags.append("Non-Pass Originations >$0MM")

    # Flag 3: TL-CRE >= $75MM
    if bucket == "TL CRE" and credit >= 75_000_000:
        flags.append("TL-CRE >$75MM")

    # Flag 4: TL-CRE Office >= $10MM
    if bucket == "TL CRE" and credit >= 10_000_000 and is_office:
        flags.append("TL-CRE Office >$10MM")

    # Flag 5: TL-SBL-D >= $300MM (standalone or multicollateral with marketable sec + SBL < 50%)
    cond5a = bucket == "TL SBL Diversified" and credit >= 300_000_000
    cond5b = (bucket == "TL Multicollateral" and credit >= 300_000_000
              and _collateral_contains(collateral, "Marketable Sec")
              and sbl_pct < 50.0)
    if cond5a or cond5b:
        flags.append("TL-SBL-D >$300MM")

    # Flag 6: TL-SBL-C >= $100MM (standalone or multicollateral with marketable sec + SBL >= 50%)
    cond6a = bucket == "TL SBL Highly Conc." and credit >= 100_000_000
    cond6b = (bucket == "TL Multicollateral" and credit >= 100_000_000
              and _collateral_contains(collateral, "Marketable Sec")
              and sbl_pct >= 50.0)
    if cond6a or cond6b:
        flags.append("TL-SBL-C >$100MM")

    # Flag 7: TL-LIC >= $100MM
    if bucket == "TL Life Insurance" and credit >= 100_000_000:
        flags.append("TL-LIC >$100MM")

    # Flag 8: TL-Alts HF/PE >= $35MM (any TL bucket including multicollateral)
    if bucket in TL_BUCKETS and _collateral_contains(collateral, "Hedge") and credit >= 35_000_000:
        flags.append("TL-Alts HF/PE >$35MM")

    # Flag 9: TL-Alts Private Shares >= $35MM (standalone PHA or multicollateral at $35MM)
    cond9a = bucket == "TL PHA" and credit >= 35_000_000
    cond9b = (bucket == "TL Multicollateral" and credit >= 35_000_000
              and _collateral_contains(collateral, "Privately Held"))
    if cond9a or cond9b:
        flags.append("TL-Alts Private Shares >$35MM")

    # Flag 10: TL-Alts Unsecured >= $35MM (standalone or multicollateral at $35MM)
    cond10a = bucket == "TL Unsecured" and credit >= 35_000_000
    cond10b = (bucket == "TL Multicollateral" and credit >= 35_000_000
               and _collateral_contains(collateral, "Unsecured"))
    if cond10a or cond10b:
        flags.append("TL-Alts Unsecured >$35MM")

    # Flag 11: TL-Alts PAF >= $50MM (any TL bucket including multicollateral)
    if bucket in TL_BUCKETS and _collateral_contains(collateral, "Aircraft") and credit >= 50_000_000:
        flags.append("TL-Alts PAF >$50MM")

    # Flag 12: TL-Alts Fine Art >= $50MM (any TL bucket including multicollateral)
    if bucket in TL_BUCKETS and _collateral_contains(collateral, "Fine Art") and credit >= 50_000_000:
        flags.append("TL-Alts Fine Art >$50MM")

    # Flag 13: TL-Alts Other Secured >= $50MM (standalone or multicollateral)
    cond13a = bucket == "TL Other Secured" and credit >= 50_000_000
    cond13b = (bucket == "TL Multicollateral" and credit >= 50_000_000
               and _collateral_contains(collateral, "Other"))
    if cond13a or cond13b:
        flags.append("TL-Alts Other Secured >$50MM")

    # Flag 14: LAL-D >= $300MM
    if bucket == "LAL Diversified" and credit >= 300_000_000:
        flags.append("LAL-D >$300MM")

    # Flag 15: LAL-C >= $100MM
    if bucket == "LAL Highly Conc." and credit >= 100_000_000:
        flags.append("LAL-C >$100MM")

    # Flag 16: RESI >= $10MM
    if bucket == "RESI" and credit >= 10_000_000:
        flags.append("RESI >$10MM")

    return flags


def apply_wmlc_flags(df):
    """Apply all 16 WMLC flags to the DataFrame.

    Adds columns: WMLC_FLAGS, WMLC_FLAG_COUNT, WMLC_QUALIFIED.
    """
    logger.info("Evaluating WMLC flags...")

    all_flags = df.apply(evaluate_flags, axis=1)

    df["WMLC_FLAGS"] = all_flags.apply(lambda fl: "|".join(fl) if fl else "")
    df["WMLC_FLAG_COUNT"] = all_flags.apply(len)
    df["WMLC_QUALIFIED"] = df["WMLC_FLAG_COUNT"] > 0

    # Per-flag summary
    logger.info("=== WMLC Flag Summary ===")
    zero_flags = []
    for flag_name in FLAG_NAMES:
        count = all_flags.apply(lambda fl: flag_name in fl).sum()
        logger.info(f"  {flag_name:40s} {count:4d} loans")
        if count == 0:
            zero_flags.append(flag_name)

    total_qualified = int(df["WMLC_QUALIFIED"].sum())
    pct = 100.0 * total_qualified / len(df) if len(df) > 0 else 0
    multi = int((df["WMLC_FLAG_COUNT"] > 1).sum())

    logger.info(f"{'':40s} {'':4s} -----")
    logger.info(f"  {'Total WMLC-qualified':40s} {total_qualified:4d} loans ({pct:.1f}%)")
    logger.info(f"  {'Multi-flag loans':40s} {multi:4d} loans")

    for flag_name in zero_flags:
        logger.warning(f"Flag '{flag_name}' matched 0 loans — check input files and column mappings")

    if total_qualified == 0:
        logger.warning("No loans qualified for WMLC — verify input files and column mappings")

    multi_df = df[df["WMLC_FLAG_COUNT"] > 1].head(5)
    if len(multi_df) > 0:
        logger.debug(f"First 5 multi-flag loans:\n"
                     f"{multi_df[['ACCOUNT_NUMBER', 'PRODUCT_BUCKET', 'CREDIT_LII', 'WMLC_FLAGS']].to_string()}")

    return df
