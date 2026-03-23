"""
Sub-Agent 1: Proxy Data Generator for WMLC Dashboard Pipeline.

Produces:
  proxy_data/loan_extract.csv       (500+ rows)
  proxy_data/LAL_Credit.xlsx        (80+ data rows, 3 skip rows)
  proxy_data/Loan_Reserve_Report.xlsx (100+ data rows, 6 skip rows)
  proxy_data/DAR_Tracker.xlsx       (60+ data rows, no skip rows)
"""

import os
import random
import string
import datetime
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "proxy_data")
os.makedirs(OUT_DIR, exist_ok=True)

random.seed(42)
np.random.seed(42)

PRODUCT_BUCKETS = [
    "LAL Diversified",
    "LAL Highly Conc.",
    "LAL NFPs",
    "RESI",
    "TL Aircraft",
    "TL CRE",
    "TL Life Insurance",
    "TL Multicollateral",
    "TL Other Secured",
    "TL PHA",
    "TL SBL Diversified",
    "TL SBL Highly Conc.",
    "TL Unsecured",
]

LAL_BUCKETS = {"LAL Diversified", "LAL Highly Conc.", "LAL NFPs"}
TL_BUCKETS = {
    "TL Aircraft", "TL CRE", "TL Life Insurance", "TL Multicollateral",
    "TL Other Secured", "TL PHA", "TL SBL Diversified",
    "TL SBL Highly Conc.", "TL Unsecured",
}

BUCKET_WEIGHTS = {
    "LAL Diversified": 0.16,
    "LAL Highly Conc.": 0.06,
    "LAL NFPs": 0.05,
    "RESI": 0.10,
    "TL Aircraft": 0.04,
    "TL CRE": 0.10,
    "TL Life Insurance": 0.05,
    "TL Multicollateral": 0.06,
    "TL Other Secured": 0.05,
    "TL PHA": 0.06,
    "TL SBL Diversified": 0.12,
    "TL SBL Highly Conc.": 0.08,
    "TL Unsecured": 0.07,
}

# Bucket ladder -----------------------------------------------------------
BUCKET_LADDER = [
    (1_000_000_000, "$1,000,000,000", 1_000_000_000),
    (750_000_000,   "$750,000,000",   750_000_000),
    (700_000_000,   "$700,000,000",   700_000_000),
    (600_000_000,   "$600,000,000",   600_000_000),
    (500_000_000,   "$500,000,000",   500_000_000),
    (400_000_000,   "$400,000,000",   400_000_000),
    (350_000_000,   "$350,000,000",   350_000_000),
    (300_000_000,   "$300,000,000",   300_000_000),
    (250_000_000,   "$250,000,000",   250_000_000),
    (200_000_000,   "$200,000,000",   200_000_000),
    (175_000_000,   "$175,000,000",   175_000_000),
    (150_000_000,   "$150,000,000",   150_000_000),
    (125_000_000,   "$125,000,000",   125_000_000),
    (100_000_000,   "$100,000,000",   100_000_000),
    (75_000_000,    "$75,000,000",    75_000_000),
    (50_000_000,    "$50,000,000",    50_000_000),
    (40_000_000,    "$40,000,000",    40_000_000),
    (35_000_000,    "$35,000,000",    35_000_000),
    (30_000_000,    "$30,000,000",    30_000_000),
    (25_000_000,    "$25,000,000",    25_000_000),
    (20_000_000,    "$20,000,000",    20_000_000),
    (15_000_000,    "$15,000,000",    15_000_000),
    (10_000_001,    "$10,000,001",    10_000_001),
    (1,             "$1",             1),
]


def assign_bucket(credit_lii: float):
    for floor_val, label, floor_int in BUCKET_LADDER:
        if credit_lii >= floor_val:
            return label, floor_int
    return "$1", 1


# ---------------------------------------------------------------------------
# Name pools
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "James", "Robert", "John", "Michael", "David", "William", "Richard", "Joseph",
    "Thomas", "Charles", "Christopher", "Daniel", "Matthew", "Anthony", "Mark",
    "Donald", "Steven", "Paul", "Andrew", "Joshua", "Kenneth", "Kevin", "Brian",
    "George", "Timothy", "Ronald", "Edward", "Jason", "Jeffrey", "Ryan",
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
    "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
    "Ashley", "Kimberly", "Emily", "Donna", "Michelle", "Dorothy", "Carol",
    "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
    "Campbell", "Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans",
    "Turner", "Diaz", "Parker", "Cruz", "Edwards", "Collins", "Reyes",
    "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers", "Gutierrez",
    "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey", "Reed", "Kelly",
    "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks",
]
COMPANY_SUFFIXES = [
    "Holdings LLC", "Capital Partners", "Ventures", "Trust", "Foundation",
    "Enterprises", "Group", "Associates", "Properties", "Investments",
    "Management", "Family Office", "Advisors", "Corp", "Partners LP",
]

COLLATERAL_GENERIC = [
    "Diversified Marketable Securities Portfolio",
    "U.S. Treasury & Government Agency Securities",
    "Large Cap Equity Portfolio",
    "Investment Grade Fixed Income Portfolio",
    "Balanced Growth Portfolio",
    "S&P 500 Index Portfolio",
    "Multi-Asset Class Portfolio",
    "Cash & Cash Equivalents",
    "Municipal Bond Portfolio",
    "Blue Chip Equity Portfolio",
    "Global Equity and Fixed Income Portfolio",
    "Investment Grade Corporate Bond Portfolio",
    "Real Estate Holdings",
    "Residential Property",
    "Commercial Real Estate",
    "Life Insurance Policy",
    "Whole Life Insurance",
    "Universal Life Insurance Policy",
]

COLLATERAL_HEDGE = [
    "Hedge Fund Class A Interests",
    "Hedge Fund LP Shares - Multi-Strategy",
    "Hedge Fund Interests - Global Macro",
    "Hedge Fund Units - Long/Short Equity",
]

COLLATERAL_PRIVATE = [
    "Privately Held Equity Shares - Tech Startup",
    "Privately Held Company Stock",
    "Privately Held LLC Membership Interest",
    "Privately Held Corporate Shares",
]

COLLATERAL_AIRCRAFT = [
    "Aircraft - 2022 Gulfstream G650",
    "Aircraft - Bombardier Global 7500",
    "Aircraft - Cessna Citation Longitude",
]

COLLATERAL_FINEART = [
    "Fine Art Collection - Contemporary",
    "Fine Art - Impressionist Paintings",
    "Fine Art Portfolio - Modern",
]

COLLATERAL_UNSECURED = [
    "Unsecured - Personal Guarantee",
    "Unsecured - Signature Loan",
    "Unsecured Line of Credit",
]

COLLATERAL_OTHER = [
    "Other Secured - Yacht",
    "Other Secured - Wine Collection",
    "Other Secured - Precious Metals",
    "Other Secured - Collectible Vehicles",
]

SUB_PRODUCTS = [
    "Securities Based Lending", "Tailored Lending", "Liquidity Access Line",
    "Pledged Asset Line", "Custom Credit", "Residential Mortgage",
    "Jumbo Mortgage", "Commercial Mortgage", "Asset-Based Lending",
    "Purpose Loan", "Non-Purpose Loan",
]

NTC_ENTITY_TYPES = [
    "Organization (Acct owner)",
    "Limited Liability Company",
    "Corporation (CEO, etc.)",
    "Limited Partnership",
]

PROPERTY_TYPES = [
    "Office", "OFFICE", "Retail", "Industrial", "Multifamily",
    "Mixed Use", "Hotel", "Warehouse", "Medical Office",
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def rand_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def rand_company():
    return f"{random.choice(LAST_NAMES)} {random.choice(COMPANY_SUFFIXES)}"


def rand_acct_12():
    return str(random.randint(1, 999_999_999_999)).zfill(12)


def acct_to_key(acct: str) -> str:
    """Convert 12-digit acct to ###-###### format (first 3 - next 6)."""
    raw = acct.lstrip("0") or "0"
    padded = raw.zfill(9)
    return f"{padded[:3]}-{padded[3:9]}"


def rand_date(start_year=2018, end_year=2025):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    delta = (end - start).days
    return start + datetime.timedelta(days=random.randint(0, delta))


def rand_credit_lii():
    """Log-normal distribution centred around $15M, fat tail to $1B+."""
    val = np.random.lognormal(mean=16.5, sigma=1.5)
    return max(1.0, round(val, 2))


def collateral_for_bucket(bucket: str) -> str:
    if bucket == "TL Aircraft":
        return random.choice(COLLATERAL_AIRCRAFT + COLLATERAL_GENERIC[:3])
    if bucket == "TL PHA":
        return random.choice(COLLATERAL_PRIVATE + COLLATERAL_GENERIC[:3])
    if bucket == "TL Unsecured":
        return random.choice(COLLATERAL_UNSECURED + COLLATERAL_GENERIC[:2])
    if bucket == "TL Other Secured":
        return random.choice(COLLATERAL_OTHER + COLLATERAL_GENERIC[:2])
    if bucket == "TL Life Insurance":
        return random.choice(["Life Insurance Policy", "Whole Life Insurance",
                              "Universal Life Insurance Policy"])
    if bucket == "TL Multicollateral":
        pool = (COLLATERAL_GENERIC + COLLATERAL_HEDGE[:1] +
                COLLATERAL_PRIVATE[:1] + COLLATERAL_OTHER[:1])
        return random.choice(pool)
    if bucket == "TL CRE":
        return random.choice(["Commercial Real Estate", "Real Estate Holdings",
                              "CRE Portfolio - Office/Retail",
                              "CRE Multi-Tenant Property"])
    return random.choice(COLLATERAL_GENERIC)


# ---------------------------------------------------------------------------
# Generate bulk rows for loan_extract
# ---------------------------------------------------------------------------
N_BULK = 500


def generate_bulk_rows(n: int) -> list[dict]:
    rows = []
    bucket_list = list(BUCKET_WEIGHTS.keys())
    bucket_probs = [BUCKET_WEIGHTS[b] for b in bucket_list]

    for i in range(n):
        acct = rand_acct_12()
        facility = rand_acct_12()
        bucket = np.random.choice(bucket_list, p=bucket_probs)
        credit_lii = rand_credit_lii()
        bucket_label, bucket_floor = assign_bucket(credit_lii)
        balance = round(credit_lii * random.uniform(0.2, 0.95), 2)
        credit_limit = round(credit_lii * random.uniform(1.0, 1.3), 2)
        is_new = random.random() < 0.40
        is_non_pass = random.random() < 0.05

        row = {
            "tl_facility_digits12": facility,
            "facility_id": facility,
            "account_number": acct,
            "key_acct": acct_to_key(acct),
            "borrower": rand_name(),
            "name": rand_name(),
            "sub_product_norm": random.choice(SUB_PRODUCTS),
            "product_bucket": bucket,
            "is_lal_nfp": bucket == "LAL NFPs" and random.random() < 0.6,
            "focus_list": "Non-Pass" if is_non_pass else "",
            "txt_mstr_facil_collateral_desc": collateral_for_bucket(bucket),
            "SBL_PERC": round(random.uniform(0, 100), 2) if "SBL" in bucket else 0.0,
            "book_date": rand_date(),
            "effective_date": rand_date(),
            "origination_date": rand_date(2015, 2024),
            "balance": balance,
            "credit_limit": credit_limit,
            "amt_original_comt": round(credit_lii * random.uniform(0.8, 1.2), 2),
            "credit_lii": credit_lii,
            "NEW_CAMP_YN": "Y" if is_new else "N",
            "NEW_CAMP_REASON": random.choice(["New", "Increase Facility"]) if is_new else "",
            "base_commit": round(credit_lii * random.uniform(0.7, 1.0), 2),
            "latest_commit": credit_lii,
            "commit_delta": round(credit_lii * random.uniform(-0.1, 0.3), 2),
            "new_commitment_amount": credit_lii if is_new else 0.0,
            "new_commitment_reason": random.choice(["New", "Increase Facility"]) if is_new else "",
            "credit_lii_commitment_bucket": bucket_label,
            "credit_lii_commitment_floor": bucket_floor,
        }
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Seeded rows — 3+ per flag to guarantee coverage
# ---------------------------------------------------------------------------
def generate_seeded_rows() -> list[dict]:
    """Create rows that definitely trigger each of the 15 WMLC flags."""
    seeded = []

    def make_row(bucket, credit_lii, overrides=None):
        acct = rand_acct_12()
        facility = rand_acct_12()
        bucket_label, bucket_floor = assign_bucket(credit_lii)
        balance = round(credit_lii * 0.6, 2)
        r = {
            "tl_facility_digits12": facility,
            "facility_id": facility,
            "account_number": acct,
            "key_acct": acct_to_key(acct),
            "borrower": rand_name(),
            "name": rand_name(),
            "sub_product_norm": random.choice(SUB_PRODUCTS),
            "product_bucket": bucket,
            "is_lal_nfp": False,
            "focus_list": "",
            "txt_mstr_facil_collateral_desc": collateral_for_bucket(bucket),
            "SBL_PERC": round(random.uniform(0, 100), 2) if "SBL" in bucket else 0.0,
            "book_date": rand_date(),
            "effective_date": rand_date(),
            "origination_date": rand_date(2015, 2024),
            "balance": balance,
            "credit_limit": round(credit_lii * 1.1, 2),
            "amt_original_comt": round(credit_lii * 0.9, 2),
            "credit_lii": credit_lii,
            "NEW_CAMP_YN": "N",
            "NEW_CAMP_REASON": "",
            "base_commit": round(credit_lii * 0.8, 2),
            "latest_commit": credit_lii,
            "commit_delta": 0.0,
            "new_commitment_amount": 0.0,
            "new_commitment_reason": "",
            "credit_lii_commitment_bucket": bucket_label,
            "credit_lii_commitment_floor": bucket_floor,
        }
        if overrides:
            r.update(overrides)
        return r

    # --- Flag 1: NTC > $50MM (LAL path — is_lal_nfp) ---
    for amt in [55_000_000, 60_000_000, 75_000_000]:
        seeded.append(make_row("LAL NFPs", amt, {"is_lal_nfp": True}))

    # Flag 1 (TL path via Loan_Reserve_Report — handled by external file linking below)
    # We create TL rows that will be linked in Loan_Reserve_Report
    flag1_tl_rows = []
    for amt in [55_000_000, 65_000_000, 80_000_000]:
        r = make_row("TL SBL Diversified", amt)
        flag1_tl_rows.append(r)
        seeded.append(r)

    # --- Flag 2: Non-Pass Originations ---
    for _ in range(4):
        seeded.append(make_row(
            random.choice(PRODUCT_BUCKETS), rand_credit_lii(),
            {"focus_list": "Non-Pass", "NEW_CAMP_YN": "Y",
             "NEW_CAMP_REASON": "New"}
        ))

    # --- Flag 3: TL-CRE >$75MM ---
    for amt in [80_000_000, 90_000_000, 100_000_000]:
        seeded.append(make_row("TL CRE", amt))

    # --- Flag 4: TL-CRE Office >$10MM (needs is_office via DAR_Tracker) ---
    flag4_rows = []
    for amt in [15_000_000, 25_000_000, 50_000_000]:
        r = make_row("TL CRE", amt)
        flag4_rows.append(r)
        seeded.append(r)

    # --- Flag 5: TL-SBL-D >$300MM ---
    for amt in [310_000_000, 350_000_000, 400_000_000]:
        seeded.append(make_row("TL SBL Diversified", amt))

    # --- Flag 6: TL-SBL-C >$100MM ---
    for amt in [110_000_000, 125_000_000, 150_000_000]:
        seeded.append(make_row("TL SBL Highly Conc.", amt))

    # --- Flag 7: TL-LIC >$100MM ---
    for amt in [110_000_000, 130_000_000, 200_000_000]:
        seeded.append(make_row("TL Life Insurance", amt))

    # --- Flag 8: TL-Alts HF/PE >$35MM ---
    for amt in [40_000_000, 50_000_000, 70_000_000]:
        seeded.append(make_row(
            random.choice(list(TL_BUCKETS)), amt,
            {"txt_mstr_facil_collateral_desc": random.choice(COLLATERAL_HEDGE)}
        ))

    # --- Flag 9: TL-Alts Private Shares >$35MM ---
    # Path A: TL PHA > $35MM
    for amt in [40_000_000, 45_000_000, 55_000_000]:
        seeded.append(make_row("TL PHA", amt))
    # Path B: TL Multicollateral > $50MM + "Privately Held"
    for amt in [55_000_000, 60_000_000, 70_000_000]:
        seeded.append(make_row(
            "TL Multicollateral", amt,
            {"txt_mstr_facil_collateral_desc": random.choice(COLLATERAL_PRIVATE)}
        ))

    # --- Flag 10: TL-Alts Unsecured >$35MM ---
    # Path A: TL Unsecured > $35MM
    for amt in [40_000_000, 50_000_000, 60_000_000]:
        seeded.append(make_row("TL Unsecured", amt))
    # Path B: TL Multicollateral > $50MM + "Unsecured"
    for amt in [55_000_000, 60_000_000, 70_000_000]:
        seeded.append(make_row(
            "TL Multicollateral", amt,
            {"txt_mstr_facil_collateral_desc": random.choice(COLLATERAL_UNSECURED)}
        ))

    # --- Flag 11: TL-Alts PAF >$50MM ---
    for amt in [55_000_000, 60_000_000, 80_000_000]:
        seeded.append(make_row(
            random.choice(list(TL_BUCKETS)), amt,
            {"txt_mstr_facil_collateral_desc": random.choice(COLLATERAL_AIRCRAFT)}
        ))

    # --- Flag 12: TL-Alts Fine Art >$50MM ---
    for amt in [55_000_000, 60_000_000, 75_000_000]:
        seeded.append(make_row(
            random.choice(list(TL_BUCKETS)), amt,
            {"txt_mstr_facil_collateral_desc": random.choice(COLLATERAL_FINEART)}
        ))

    # --- Flag 13: TL-Alts Other Secured >$50MM ---
    # Path A: TL Other Secured > $50MM
    for amt in [55_000_000, 65_000_000, 80_000_000]:
        seeded.append(make_row("TL Other Secured", amt))
    # Path B: TL Multicollateral > $50MM + "Other"
    for amt in [55_000_000, 60_000_000, 70_000_000]:
        seeded.append(make_row(
            "TL Multicollateral", amt,
            {"txt_mstr_facil_collateral_desc": random.choice(COLLATERAL_OTHER)}
        ))

    # --- Flag 14: LAL-D >$300MM ---
    for amt in [310_000_000, 400_000_000, 500_000_000]:
        seeded.append(make_row("LAL Diversified", amt))

    # --- Flag 15: LAL-C >$100MM ---
    for amt in [110_000_000, 150_000_000, 200_000_000]:
        seeded.append(make_row("LAL Highly Conc.", amt))

    # --- Multi-flag rows (5+) ---
    # TL CRE >$75MM + Office + NTC
    for amt in [80_000_000, 90_000_000]:
        r = make_row("TL CRE", amt)
        flag4_rows.append(r)  # will also link in DAR_Tracker as Office
        seeded.append(r)

    # LAL NFPs NTC > $50MM + LAL-C > $100MM doesn't combine (different buckets)
    # Instead: TL SBL-D >$300MM + NTC >$50MM + HF/PE >$35MM
    for amt in [310_000_000, 350_000_000]:
        r = make_row("TL SBL Diversified", amt, {
            "txt_mstr_facil_collateral_desc": "Hedge Fund Class A Interests"
        })
        flag1_tl_rows.append(r)
        seeded.append(r)

    # Non-Pass + NEW (flag 2) on a TL CRE >$75MM (flag 3)
    r = make_row("TL CRE", 80_000_000, {
        "focus_list": "Non-Pass", "NEW_CAMP_YN": "Y", "NEW_CAMP_REASON": "New"
    })
    seeded.append(r)

    # LAL NFPs NTC >$50MM (flag 1) that is also Non-Pass (flag 2)
    r = make_row("LAL NFPs", 60_000_000, {
        "is_lal_nfp": True, "focus_list": "Non-Pass",
        "NEW_CAMP_YN": "Y", "NEW_CAMP_REASON": "New"
    })
    seeded.append(r)

    # NTC via LAL_Credit (Operating Company) — need rows for LAL buckets
    flag1_lal_credit_rows = []
    for amt in [55_000_000, 70_000_000, 80_000_000]:
        r = make_row("LAL Diversified", amt, {"is_lal_nfp": False})
        flag1_lal_credit_rows.append(r)
        seeded.append(r)

    return seeded, flag4_rows, flag1_tl_rows, flag1_lal_credit_rows


# ---------------------------------------------------------------------------
# Build main loan_extract
# ---------------------------------------------------------------------------
print("Generating bulk rows ...")
bulk = generate_bulk_rows(N_BULK)
print("Generating seeded rows ...")
seeded, flag4_rows, flag1_tl_rows, flag1_lal_credit_rows = generate_seeded_rows()

all_rows = bulk + seeded
df = pd.DataFrame(all_rows)

# Ensure bucket consistency
for idx, row in df.iterrows():
    lbl, flr = assign_bucket(row["credit_lii"])
    df.at[idx, "credit_lii_commitment_bucket"] = lbl
    df.at[idx, "credit_lii_commitment_floor"] = flr

print(f"Total loan_extract rows: {len(df)}")
print(f"\nProduct bucket distribution:\n{df['product_bucket'].value_counts().to_string()}")
print(f"\nCredit LII range: ${df['credit_lii'].min():,.0f} — ${df['credit_lii'].max():,.0f}")
print(f"NEW_CAMP_YN='Y': {(df['NEW_CAMP_YN']=='Y').sum()} ({(df['NEW_CAMP_YN']=='Y').mean()*100:.1f}%)")
print(f"Non-Pass: {(df['focus_list']=='Non-Pass').sum()}")
print(f"is_lal_nfp=True: {df['is_lal_nfp'].sum()}")

csv_path = os.path.join(OUT_DIR, "loan_extract.csv")
df.to_csv(csv_path, index=False)
print(f"\nWrote {csv_path}")


# ---------------------------------------------------------------------------
# LAL_Credit.xlsx — 80+ data rows, 3 skip rows
# ---------------------------------------------------------------------------
print("\nGenerating LAL_Credit.xlsx ...")

# Gather LAL account numbers from loan_extract for linking
lal_accts = df[df["product_bucket"].isin(LAL_BUCKETS)]["account_number"].unique().tolist()
random.shuffle(lal_accts)

lal_rows = []

# Include seeded NTC-qualifying rows (Operating Company / Charity)
for r in flag1_lal_credit_rows:
    lal_rows.append({
        "Account Number": acct_to_key(r["account_number"]),
        "Operating Company": "Yes",
        "Charity/Non-Profit Organization": "No",
        "Bank Level Limit/Guideline Exception": random.choice(["Yes", "No"]),
        "Credit Report RAC Exception": "No",
        "Firm Level Limit/Guideline Exception": "No",
        "Significant Credit Standard Exception": "No",
    })

# NFP accounts
nfp_accts = df[(df["product_bucket"] == "LAL NFPs") & (df["is_lal_nfp"] == True)][
    "account_number"
].unique().tolist()
for acct in nfp_accts[:5]:
    lal_rows.append({
        "Account Number": acct_to_key(acct),
        "Operating Company": "No",
        "Charity/Non-Profit Organization": "Yes",
        "Bank Level Limit/Guideline Exception": "No",
        "Credit Report RAC Exception": "No",
        "Firm Level Limit/Guideline Exception": "No",
        "Significant Credit Standard Exception": "No",
    })

# Fill to 85+ with other LAL accounts (with some exceptions)
used = {r["Account Number"] for r in lal_rows}
for acct in lal_accts:
    if len(lal_rows) >= 90:
        break
    key = acct_to_key(acct)
    if key in used:
        continue
    used.add(key)
    has_exc = random.random() < 0.15
    lal_rows.append({
        "Account Number": key,
        "Operating Company": "No",
        "Charity/Non-Profit Organization": "No",
        "Bank Level Limit/Guideline Exception": "Yes" if has_exc else "No",
        "Credit Report RAC Exception": "Yes" if (has_exc and random.random() < 0.5) else "No",
        "Firm Level Limit/Guideline Exception": "Yes" if random.random() < 0.05 else "No",
        "Significant Credit Standard Exception": "Yes" if random.random() < 0.05 else "No",
    })

lal_df = pd.DataFrame(lal_rows)
print(f"LAL_Credit data rows: {len(lal_df)}")

# Write with 3 skip rows
lal_path = os.path.join(OUT_DIR, "LAL_Credit.xlsx")
with pd.ExcelWriter(lal_path, engine="openpyxl") as writer:
    # Write filler rows first
    filler = pd.DataFrame([
        ["LAL Credit Exception Report - Confidential"],
        [""],
        [f"Generated: {datetime.date.today().isoformat()}"],
    ])
    filler.to_excel(writer, sheet_name="Sheet1", index=False, header=False, startrow=0)
    # Write data starting at row 3 (0-indexed), which is row 4 in Excel
    lal_df.to_excel(writer, sheet_name="Sheet1", index=False, startrow=3)
print(f"Wrote {lal_path}")


# ---------------------------------------------------------------------------
# Loan_Reserve_Report.xlsx — 100+ data rows, 6 skip rows
# ---------------------------------------------------------------------------
print("\nGenerating Loan_Reserve_Report.xlsx ...")

tl_accts = df[df["product_bucket"].isin(TL_BUCKETS)]["account_number"].unique().tolist()
random.shuffle(tl_accts)

lrr_rows = []

# Seeded NTC-qualifying TL rows (Corp + entity type)
for r in flag1_tl_rows:
    lrr_rows.append({
        "Facility Account Number": r["account_number"],
        "Purpose Code Description": "Corp General Purpose",
        "Account Relationship Code Description": random.choice(NTC_ENTITY_TYPES),
    })

# Fill to 110+
used_lrr = {r["Facility Account Number"] for r in lrr_rows}
for acct in tl_accts:
    if len(lrr_rows) >= 115:
        break
    if acct in used_lrr:
        continue
    used_lrr.add(acct)
    is_corp = random.random() < 0.25
    lrr_rows.append({
        "Facility Account Number": acct,
        "Purpose Code Description": "Corp General Purpose" if is_corp else random.choice([
            "Personal Investment", "Real Estate Purchase", "Business Expansion",
            "Working Capital", "Equipment Purchase",
        ]),
        "Account Relationship Code Description": random.choice(
            NTC_ENTITY_TYPES if is_corp else [
                "Individual", "Joint Account", "Trust (Revocable)",
                "Trust (Irrevocable)", "Estate",
            ]
        ),
    })

lrr_df = pd.DataFrame(lrr_rows)
print(f"Loan_Reserve_Report data rows: {len(lrr_df)}")

lrr_path = os.path.join(OUT_DIR, "Loan_Reserve_Report.xlsx")
with pd.ExcelWriter(lrr_path, engine="openpyxl") as writer:
    filler = pd.DataFrame([
        ["Loan Reserve Report - Internal Use Only"],
        ["Risk Management Division"],
        [""],
        [f"Report Date: {datetime.date.today().isoformat()}"],
        [""],
        ["--- End Header ---"],
    ])
    filler.to_excel(writer, sheet_name="Sheet1", index=False, header=False, startrow=0)
    lrr_df.to_excel(writer, sheet_name="Sheet1", index=False, startrow=6)
print(f"Wrote {lrr_path}")


# ---------------------------------------------------------------------------
# DAR_Tracker.xlsx — 60+ data rows, no skip rows
# ---------------------------------------------------------------------------
print("\nGenerating DAR_Tracker.xlsx ...")

cre_facilities = df[df["product_bucket"] == "TL CRE"]["facility_id"].unique().tolist()
random.shuffle(cre_facilities)

dar_rows = []

# Seeded Office rows for Flag 4
for r in flag4_rows:
    dar_rows.append({
        "Facility ID": r["facility_id"],
        "Property_Type": random.choice(["Office", "OFFICE"]),
        "Property_Name": f"{random.choice(LAST_NAMES)} Tower",
        "Address": f"{random.randint(100, 9999)} {random.choice(LAST_NAMES)} Ave, New York, NY",
    })

# Fill with CRE facilities first, then synthetic ones to reach 65+
used_dar = {r["Facility ID"] for r in dar_rows}
for fac in cre_facilities:
    if len(dar_rows) >= 75:
        break
    if fac in used_dar:
        continue
    used_dar.add(fac)
    dar_rows.append({
        "Facility ID": fac,
        "Property_Type": random.choice(PROPERTY_TYPES),
        "Property_Name": f"{random.choice(LAST_NAMES)} {random.choice(['Plaza', 'Center', 'Park', 'Building', 'Complex'])}",
        "Address": f"{random.randint(100, 9999)} {random.choice(LAST_NAMES)} St, {random.choice(['New York, NY', 'Chicago, IL', 'Los Angeles, CA', 'Dallas, TX', 'Miami, FL'])}",
    })
# If still under 65, add synthetic facility IDs (won't link but fill the file)
while len(dar_rows) < 65:
    fac = rand_acct_12()
    if fac in used_dar:
        continue
    used_dar.add(fac)
    dar_rows.append({
        "Facility ID": fac,
        "Property_Type": random.choice(PROPERTY_TYPES),
        "Property_Name": f"{random.choice(LAST_NAMES)} {random.choice(['Tower', 'Campus', 'Square', 'Mall', 'Depot'])}",
        "Address": f"{random.randint(100, 9999)} {random.choice(LAST_NAMES)} Blvd, {random.choice(['Boston, MA', 'Houston, TX', 'Seattle, WA', 'Denver, CO', 'Atlanta, GA'])}",
    })

dar_df = pd.DataFrame(dar_rows)
print(f"DAR_Tracker data rows: {len(dar_df)}")

dar_path = os.path.join(OUT_DIR, "DAR_Tracker.xlsx")
dar_df.to_excel(dar_path, index=False)
print(f"Wrote {dar_path}")


# ---------------------------------------------------------------------------
# Summary validation
# ---------------------------------------------------------------------------
print("\n" + "=" * 70)
print("VALIDATION SUMMARY")
print("=" * 70)

files = {
    "loan_extract.csv": (csv_path, 500),
    "LAL_Credit.xlsx": (lal_path, 80),
    "Loan_Reserve_Report.xlsx": (lrr_path, 100),
    "DAR_Tracker.xlsx": (dar_path, 60),
}

for name, (path, min_rows) in files.items():
    exists = os.path.exists(path)
    if name.endswith(".csv"):
        count = len(pd.read_csv(path))
    else:
        count = "N/A (skip-row file)"
    print(f"  {name}: exists={exists}, rows={count}, min={min_rows}")

print(f"\nAll 13 product_buckets present: {len(df['product_bucket'].unique()) == 13}")
print(f"  Unique buckets: {sorted(df['product_bucket'].unique())}")

print(f"\ncredit_lii range: ${df['credit_lii'].min():,.0f} to ${df['credit_lii'].max():,.0f}")
for threshold in [10_000_000, 35_000_000, 50_000_000, 75_000_000, 100_000_000, 300_000_000]:
    count = (df["credit_lii"] > threshold).sum()
    print(f"  credit_lii > ${threshold:,.0f}: {count} rows")

print(f"\nNEW_CAMP_YN='Y': {(df['NEW_CAMP_YN']=='Y').sum()} / {len(df)} = "
      f"{(df['NEW_CAMP_YN']=='Y').mean()*100:.1f}%")
print(f"Non-Pass: {(df['focus_list']=='Non-Pass').sum()}")
print(f"Non-Pass + NEW_CAMP_YN='Y': "
      f"{((df['focus_list']=='Non-Pass') & (df['NEW_CAMP_YN']=='Y')).sum()}")

# Check linking
lal_credit_accts = set(lal_df["Account Number"].values)
lrr_accts = set(lrr_df["Facility Account Number"].values)
dar_facs = set(dar_df["Facility ID"].values)

base_lal_keys = set(df[df["product_bucket"].isin(LAL_BUCKETS)]["key_acct"].values)
base_tl_accts = set(df[df["product_bucket"].isin(TL_BUCKETS)]["account_number"].values)
base_cre_facs = set(df[df["product_bucket"] == "TL CRE"]["facility_id"].values)

lal_overlap = len(lal_credit_accts & base_lal_keys)
lrr_overlap = len(lrr_accts & base_tl_accts)
dar_overlap = len(dar_facs & base_cre_facs)

print(f"\nExternal file linkage:")
print(f"  LAL_Credit accounts matching loan_extract LAL key_accts: {lal_overlap}")
print(f"  Loan_Reserve_Report accounts matching loan_extract TL accts: {lrr_overlap}")
print(f"  DAR_Tracker facilities matching loan_extract TL CRE facilities: {dar_overlap}")

print(f"\nDAR_Tracker Office rows: {(dar_df['Property_Type'].str.lower() == 'office').sum()}")

print("\n=== DATA GENERATION COMPLETE ===")
