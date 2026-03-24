"""Compute intermediate tags: IS_NTC, IS_OFFICE, HAS_CREDIT_POLICY_EXCEPTION.

Logic follows specs/WMLC_LOGIC.md exactly.
All column references use NORMALIZED (UPPERCASE) names.

Join key reference:
  LAL_Credit          ACCOUNT_NUMBER_KEY  →  base ACCOUNT_NUMBER
  Loan_Reserve_Report ACCOUNT_NUMBER_KEY  →  base TL_FACILITY_DIGITS12
  DAR_Tracker         FACILITY_ID_KEY     →  base TL_FACILITY_DIGITS12
"""

import logging
import pandas as pd

logger = logging.getLogger("wmlc_etl.taggers.intermediate")

LAL_BUCKETS = {"LAL Diversified", "LAL Highly Conc.", "LAL NFPs"}
TL_BUCKETS = {
    "TL Aircraft", "TL CRE", "TL Life Insurance", "TL Multicollateral",
    "TL Other Secured", "TL PHA", "TL SBL Diversified", "TL SBL Highly Conc.",
    "TL Unsecured",
}


def compute_is_ntc(base_df, lal_credit_df, loan_reserve_df):
    """Compute IS_NTC for every row in base_df.

    LAL buckets: is_lal_nfp == True OR account in LAL_Credit (charity/operating)
                 Join: LAL_Credit.ACCOUNT_NUMBER_KEY → base.ACCOUNT_NUMBER
    TL buckets:  facility in Loan_Reserve_Report (corp + entity type)
                 Join: LRR.ACCOUNT_NUMBER_KEY → base.TL_FACILITY_DIGITS12
    RESI:        always False
    """
    result = pd.Series(False, index=base_df.index)

    # --- LAL NTC: via is_lal_nfp boolean ---
    lal_mask = base_df["PRODUCT_BUCKET"].isin(LAL_BUCKETS)
    lal_nfp_count = 0
    if "IS_LAL_NFP" in base_df.columns:
        nfp_mask = lal_mask & (base_df["IS_LAL_NFP"] == True)
        result |= nfp_mask
        lal_nfp_count = int(nfp_mask.sum())

    # --- LAL NTC: via LAL_Credit external file ---
    lal_credit_count = 0
    if lal_credit_df is not None and len(lal_credit_df) > 0:
        # Build set of accounts where Operating Company or Charity == "Yes"
        op_mask = lal_credit_df.get("OPERATING_COMPANY", pd.Series(dtype=str)).astype(str).str.strip().str.lower() == "yes"
        # Handle the normalized column name for "Charity/Non-Profit Organization"
        charity_col = None
        for c in lal_credit_df.columns:
            if "CHARITY" in c and "ORGANIZATION" in c:
                charity_col = c
                break
        charity_mask = pd.Series(False, index=lal_credit_df.index)
        if charity_col:
            charity_mask = lal_credit_df[charity_col].astype(str).str.strip().str.lower() == "yes"

        ntc_rows = lal_credit_df[op_mask | charity_mask]
        lal_ntc_accounts = set(ntc_rows["ACCOUNT_NUMBER_KEY"].dropna())

        logger.info(f"LAL_Credit: {len(lal_credit_df)} rows, {op_mask.sum()} Operating Co, "
                    f"{charity_mask.sum()} Charity, {len(lal_ntc_accounts)} unique NTC accounts")

        # Join: LAL_Credit.ACCOUNT_NUMBER_KEY → base.ACCOUNT_NUMBER
        lal_ext_match = lal_mask & base_df["ACCOUNT_NUMBER"].isin(lal_ntc_accounts) & ~result
        result |= lal_ext_match
        lal_credit_count = int(lal_ext_match.sum())

        total_lal = int(lal_mask.sum())
        matched_lal = int(base_df.loc[lal_mask, "ACCOUNT_NUMBER"].isin(
            set(lal_credit_df["ACCOUNT_NUMBER_KEY"].dropna())).sum())
        logger.info(f"LAL_Credit join rate: {matched_lal}/{total_lal} LAL loans found in LAL_Credit "
                    f"({matched_lal/total_lal*100:.1f}%)" if total_lal > 0 else "No LAL loans")
    else:
        logger.warning("LAL_Credit data unavailable — skipping LAL-based NTC tagging from external file")

    # --- TL NTC: via Loan_Reserve_Report ---
    tl_ntc_count = 0
    if loan_reserve_df is not None and len(loan_reserve_df) > 0:
        # Condition A: Purpose Code contains "Corp" (substring, case-insensitive)
        purpose_mask = loan_reserve_df.get("PURPOSE_CODE_DESCRIPTION", pd.Series(dtype=str)).astype(str).str.contains(
            "corp", case=False, na=False)

        # Condition B: Account Relationship contains ANY entity type (OR, substring, case-insensitive)
        # Expanded to catch guarantor-type and institutional entity variants
        entity_pattern = (
            "organization|limited liability|corporation|limited partnership"
            "|guarantor|trust|estate|joint venture|partnership|association"
        )
        entity_mask = loan_reserve_df.get("ACCOUNT_RELATIONSHIP_CODE_DESCRIPTION", pd.Series(dtype=str)).astype(str).str.contains(
            entity_pattern, case=False, na=False, regex=True)
        logger.info(f"Loan Reserve Report: entity pattern matches {entity_mask.sum()} rows "
                    f"(expanded to include guarantor/trust/estate variants)")

        ntc_mask = purpose_mask & entity_mask
        ntc_facilities = set(loan_reserve_df.loc[ntc_mask, "ACCOUNT_NUMBER_KEY"].dropna())

        logger.info(f"Loan Reserve Report: {len(loan_reserve_df)} total rows")
        logger.info(f"  Purpose Code 'Corp' matches: {purpose_mask.sum()}")
        logger.info(f"  Entity type matches: {entity_mask.sum()}")
        logger.info(f"  Both conditions (NTC): {ntc_mask.sum()} rows, {len(ntc_facilities)} unique facilities")

        # Join: LRR.ACCOUNT_NUMBER_KEY → base.TL_FACILITY_DIGITS12
        tl_mask = base_df["PRODUCT_BUCKET"].isin(TL_BUCKETS)
        if "TL_FACILITY_DIGITS12" in base_df.columns:
            tl_ntc_match = tl_mask & base_df["TL_FACILITY_DIGITS12"].isin(ntc_facilities)
            result |= tl_ntc_match
            tl_ntc_count = int(tl_ntc_match.sum())

            total_tl = int(tl_mask.sum())
            all_lrr_keys = set(loan_reserve_df["ACCOUNT_NUMBER_KEY"].dropna())
            matched_tl = int(base_df.loc[tl_mask, "TL_FACILITY_DIGITS12"].isin(all_lrr_keys).sum())
            logger.info(f"LRR join rate: {matched_tl}/{total_tl} TL loans found in LRR "
                        f"({matched_tl/total_tl*100:.1f}%)" if total_tl > 0 else "No TL loans")
        else:
            logger.warning("TL_FACILITY_DIGITS12 column missing — cannot join Loan Reserve Report")

        if tl_ntc_count == 0:
            logger.warning("TL NTC matched 0 loans — check LRR FACILITY_ACCOUNT_NUMBER format vs TL_FACILITY_DIGITS12")
    else:
        logger.warning("Loan_Reserve_Report data unavailable — skipping TL NTC tagging")

    logger.info(f"IS_NTC: {int(result.sum())} loans tagged as NTC "
                f"({100.0 * result.sum() / len(base_df):.1f}%)")
    logger.info(f"  LAL NTC (via IS_LAL_NFP): {lal_nfp_count}")
    logger.info(f"  LAL NTC (via LAL_Credit charity/operating): {lal_credit_count}")
    logger.info(f"  TL NTC (via Loan Reserve Report): {tl_ntc_count}")

    return result


def compute_is_office(base_df, dar_tracker_df):
    """Compute IS_OFFICE: TL_FACILITY_DIGITS12 found in DAR_Tracker where PROPERTY_TYPE contains 'office'.

    Join: DAR_Tracker.FACILITY_ID_KEY → base.TL_FACILITY_DIGITS12
    """
    if dar_tracker_df is not None and len(dar_tracker_df) > 0:
        # Use str.contains for case-insensitive match on PROPERTY_TYPE
        office_mask = dar_tracker_df.get("PROPERTY_TYPE", pd.Series(dtype=str)).astype(str).str.contains(
            "office", case=False, na=False)
        office_facilities = set(dar_tracker_df.loc[office_mask, "FACILITY_ID_KEY"].dropna())

        logger.info(f"DAR Tracker: {len(dar_tracker_df)} rows, {office_mask.sum()} office rows, "
                    f"{len(office_facilities)} unique office facility IDs")

        # Join: DAR_Tracker.FACILITY_ID_KEY → base.TL_FACILITY_DIGITS12
        if "TL_FACILITY_DIGITS12" in base_df.columns:
            result = base_df["TL_FACILITY_DIGITS12"].isin(office_facilities)
            matched = int(result.sum())

            # Log overall join rate
            all_dar_keys = set(dar_tracker_df["FACILITY_ID_KEY"].dropna())
            total = len(base_df)
            join_matched = int(base_df["TL_FACILITY_DIGITS12"].isin(all_dar_keys).sum())
            logger.info(f"DAR join rate: {join_matched}/{total} base loans found in DAR "
                        f"({join_matched/total*100:.1f}%)")
        else:
            logger.warning("TL_FACILITY_DIGITS12 column missing — cannot join DAR Tracker")
            result = pd.Series(False, index=base_df.index)
            matched = 0

        logger.info(f"IS_OFFICE: {matched} loans tagged as CRE Office "
                    f"({matched/len(base_df)*100:.1f}%)")
        if matched == 0:
            logger.warning("IS_OFFICE matched 0 loans — check DAR Tracker key format vs TL_FACILITY_DIGITS12")
        return result
    else:
        logger.warning("DAR_Tracker data unavailable — skipping IS_OFFICE tagging")
        return pd.Series(False, index=base_df.index)


def compute_has_credit_policy_exception(base_df, lal_credit_df):
    """Compute HAS_CREDIT_POLICY_EXCEPTION from LAL_Credit.

    Join: LAL_Credit.ACCOUNT_NUMBER_KEY → base.ACCOUNT_NUMBER
    """
    exception_cols = [
        "BANK_LEVEL_LIMITGUIDELINE_EXCEPTION",
        "CREDIT_REPORT_RAC_EXCEPTION",
        "FIRM_LEVEL_LIMITGUIDELINE_EXCEPTION",
        "SIGNIFICANT_CREDIT_STANDARD_EXCEPTION",
    ]

    if lal_credit_df is not None and len(lal_credit_df) > 0:
        # Vectorized: check if any exception column == "Yes"
        exception_mask = pd.Series(False, index=lal_credit_df.index)
        for col in exception_cols:
            if col in lal_credit_df.columns:
                exception_mask |= lal_credit_df[col].astype(str).str.strip().str.lower() == "yes"

        exception_accounts = set(lal_credit_df.loc[exception_mask, "ACCOUNT_NUMBER_KEY"].dropna())
        logger.debug(f"Credit policy exception lookup: {len(exception_accounts)} accounts")

        # Join: LAL_Credit.ACCOUNT_NUMBER_KEY → base.ACCOUNT_NUMBER
        result = base_df["ACCOUNT_NUMBER"].isin(exception_accounts)
        matched = int(result.sum())
        logger.info(f"HAS_CREDIT_POLICY_EXCEPTION: {matched} loans "
                    f"({matched/len(base_df)*100:.1f}%)")
        return result
    else:
        logger.warning("LAL_Credit data unavailable — skipping credit policy exception tagging")
        return pd.Series(False, index=base_df.index)


def expand_new_camp_yn(df):
    """Expand NEW_CAMP_YN: Y if existing Y, or NEW_COMMITMENT_AMOUNT > 0, or valid reason."""
    original_count = int((df["NEW_CAMP_YN"] == "Y").sum())

    nca = pd.to_numeric(df.get("NEW_COMMITMENT_AMOUNT", pd.Series(dtype=float)), errors="coerce").fillna(0)
    ncr = df.get("NEW_COMMITMENT_REASON", pd.Series(dtype=str)).fillna("").astype(str)
    ncr_valid = (ncr != "") & (ncr.str.upper() != "NONE") & (ncr.str.upper() != "NAN")

    expanded = (df["NEW_CAMP_YN"] == "Y") | (nca > 0) | ncr_valid
    df["NEW_CAMP_YN"] = expanded.map({True: "Y", False: "N"})

    new_count = int((df["NEW_CAMP_YN"] == "Y").sum())
    logger.info(f"NEW_CAMP_YN expanded: {original_count} -> {new_count} "
                f"(+{new_count - original_count} from commitment amount/reason)")
    logger.info(f"  NEW_COMMITMENT_AMOUNT > 0: {int((nca > 0).sum())} loans")
    logger.info(f"  NEW_COMMITMENT_REASON valid: {int(ncr_valid.sum())} loans")
    return df


_BUCKET_THRESHOLDS = [
    (1_000_000_000, "$1,000,000,000", 1000000000),
    (750_000_000, "$750,000,000", 750000000),
    (700_000_000, "$700,000,000", 700000000),
    (600_000_000, "$600,000,000", 600000000),
    (500_000_000, "$500,000,000", 500000000),
    (400_000_000, "$400,000,000", 400000000),
    (350_000_000, "$350,000,000", 350000000),
    (300_000_000, "$300,000,000", 300000000),
    (250_000_000, "$250,000,000", 250000000),
    (200_000_000, "$200,000,000", 200000000),
    (175_000_000, "$175,000,000", 175000000),
    (150_000_000, "$150,000,000", 150000000),
    (125_000_000, "$125,000,000", 125000000),
    (100_000_000, "$100,000,000", 100000000),
    (75_000_000, "$75,000,000", 75000000),
    (50_000_000, "$50,000,000", 50000000),
    (40_000_000, "$40,000,000", 40000000),
    (35_000_000, "$35,000,000", 35000000),
    (30_000_000, "$30,000,000", 30000000),
    (25_000_000, "$25,000,000", 25000000),
    (20_000_000, "$20,000,000", 20000000),
    (15_000_000, "$15,000,000", 15000000),
    (10_000_001, "$10,000,001", 10000001),
    (1, "$1", 1),
]


def _assign_bucket(credit_lii):
    """Assign commitment bucket label and floor from credit_lii value."""
    try:
        val = float(credit_lii)
    except (ValueError, TypeError):
        return "$1", 1
    for floor_val, label, floor_int in _BUCKET_THRESHOLDS:
        if val >= floor_val:
            return label, floor_int
    return "$1", 1


def compute_bucket_columns(df):
    """Always (re)compute CREDIT_LII_COMMITMENT_BUCKET and _FLOOR from CREDIT_LII."""
    credit_lii = pd.to_numeric(df["CREDIT_LII"], errors="coerce").fillna(0)
    buckets = credit_lii.apply(_assign_bucket)
    df["CREDIT_LII_COMMITMENT_BUCKET"] = buckets.apply(lambda x: x[0])
    df["CREDIT_LII_COMMITMENT_FLOOR"] = buckets.apply(lambda x: x[1])
    logger.info("Computed CREDIT_LII_COMMITMENT_BUCKET/FLOOR from CREDIT_LII")
    logger.debug(f"Bucket distribution:\n{df['CREDIT_LII_COMMITMENT_BUCKET'].value_counts().to_string()}")
    return df


def apply_intermediate_tags(base_df, lal_credit_df, loan_reserve_df, dar_tracker_df):
    """Apply all three intermediate tags to the base DataFrame."""
    logger.info("Computing intermediate tags...")
    df = base_df.copy()

    # Always compute bucket columns to guarantee consistency
    df = compute_bucket_columns(df)

    # Expand NEW_CAMP_YN before any downstream logic
    df = expand_new_camp_yn(df)

    df["IS_NTC"] = compute_is_ntc(df, lal_credit_df, loan_reserve_df)
    df["IS_OFFICE"] = compute_is_office(df, dar_tracker_df)
    df["HAS_CREDIT_POLICY_EXCEPTION"] = compute_has_credit_policy_exception(df, lal_credit_df)

    logger.debug(f"Sample tagged rows (IS_NTC=True):\n"
                 f"{df[df['IS_NTC']].head(3)[['ACCOUNT_NUMBER', 'PRODUCT_BUCKET', 'IS_NTC', 'CREDIT_LII']].to_string()}")

    return df
