"""Generate an enhanced WMLC Tracker proxy file for Tracker Analytics.

Creates proxy_data/WMLC_Tracker.xlsx with realistic columns:
  - Tracker File V, Relationship Name, Product Area, Product Type
  - Deal Type (weighted random)
  - WMLC Date, Transaction Size (MM) Gross, Transaction Size Net
  - 18 WMLC flag columns (sparsely populated)
  - FA-MSSB sub-columns: FA-MSSB-D, FA-MSSB-C, FA-MSSB-ESOP, FA-OLD LIMIT
  - DD-MSSB sub-columns: DD-MSSB-DIRECT, DD-MSSB-INDIRECT
"""

import os
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Product areas and their associated product types
PRODUCT_CONFIG = {
    "LAL":     ["LAL Diversified", "LAL Highly Conc.", "LAL NFPs"],
    "TL-CRE":  ["TL CRE"],
    "TL-LIQ":  ["TL SBL Diversified", "TL SBL Highly Conc."],
    "TL-LIC":  ["TL Life Insurance"],
    "TL-ALTS": ["TL Aircraft", "TL PHA", "TL Multicollateral", "TL Other Secured",
                 "TL Unsecured", "TL Fine Art"],
}

EXCLUDED_AREAS = ["DD-MSSB", "FA-MSSB", "PSL"]

# Deal Type values with weights: New Deal ~30%, Renewal ~25%, Amendment ~20%,
# Increase ~10%, Modification ~10%, Extension ~5%
DEAL_TYPES = ["New Deal", "Renewal", "Amendment", "Increase", "Modification", "Extension"]
DEAL_TYPE_WEIGHTS = [0.30, 0.25, 0.20, 0.10, 0.10, 0.05]

# FA-MSSB sub-columns
FA_SUB_COLUMNS = ["FA-MSSB-D", "FA-MSSB-C", "FA-MSSB-ESOP", "FA-OLD LIMIT"]

# DD-MSSB sub-columns
DD_SUB_COLUMNS = ["DD-MSSB-DIRECT", "DD-MSSB-INDIRECT"]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel",
    "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark", "Margaret",
    "Steven", "Sandra", "Paul", "Ashley", "Andrew", "Dorothy", "Joshua",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
]

ENTITY_SUFFIXES = [
    " LLC", " Inc.", " LP", " Corp", " Trust", " Holdings", " Group",
    " Partners", " Capital", " Fund", " Foundation", "",
]

WMLC_FLAG_COLUMNS = [
    "NTC >$50MM",
    "Non-Pass Originations >$0MM",
    "TL-CRE >$75MM",
    "TL-CRE Office >$10MM",
    "TL-CRE Non-Recourse >$0MM",
    "TL-CRE Partial Recourse >$25MM",
    "TL-SBL D >$300MM",
    "TL-SBL C >$100MM",
    "TL-LIC >$100MM",
    "TL-Alts HF/PE >$35MM",
    "TL-Alts Private Shares >$35MM",
    "TL-Alts Unsecured >$35MM",
    "TL-Alts PAF >$50MM",
    "TL-Alts Fine Art >$50MM",
    "TL-Alts Other Secured >$50MM",
    "TL Non-SBL/IBL >$25MM",
    "LAL D >$300MM",
    "LAL C >$100MM",
]

# Which product areas typically trigger which flags
FLAG_PRODUCT_AFFINITY = {
    "NTC >$50MM":                      ["LAL", "TL-LIQ", "TL-ALTS"],
    "Non-Pass Originations >$0MM":     ["LAL", "TL-CRE", "TL-LIQ", "TL-ALTS"],
    "TL-CRE >$75MM":                   ["TL-CRE"],
    "TL-CRE Office >$10MM":            ["TL-CRE"],
    "TL-CRE Non-Recourse >$0MM":       ["TL-CRE"],
    "TL-CRE Partial Recourse >$25MM":  ["TL-CRE"],
    "TL-SBL D >$300MM":                ["TL-LIQ"],
    "TL-SBL C >$100MM":                ["TL-LIQ"],
    "TL-LIC >$100MM":                  ["TL-LIC"],
    "TL-Alts HF/PE >$35MM":           ["TL-ALTS"],
    "TL-Alts Private Shares >$35MM":  ["TL-ALTS"],
    "TL-Alts Unsecured >$35MM":       ["TL-ALTS"],
    "TL-Alts PAF >$50MM":             ["TL-ALTS"],
    "TL-Alts Fine Art >$50MM":        ["TL-ALTS"],
    "TL-Alts Other Secured >$50MM":   ["TL-ALTS"],
    "TL Non-SBL/IBL >$25MM":          ["TL-ALTS", "TL-LIC"],
    "LAL D >$300MM":                   ["LAL"],
    "LAL C >$100MM":                   ["LAL"],
}


def random_name():
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    suffix = random.choice(ENTITY_SUFFIXES)
    if random.random() < 0.4:
        return f"{first} {last}"
    return f"{first} {last}{suffix}"


def random_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def main():
    rows = []
    n_rows = 120
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2026, 3, 15)

    valid_areas = list(PRODUCT_CONFIG.keys())

    for i in range(n_rows):
        # 85% valid product areas, 15% excluded
        if random.random() < 0.15:
            prod_area = random.choice(EXCLUDED_AREAS)
            prod_type = ""
        else:
            prod_area = random.choice(valid_areas)
            prod_type = random.choice(PRODUCT_CONFIG[prod_area])

        wmlc_date = random_date(start_date, end_date)
        gross = round(random.uniform(10, 500) * random.choice([1, 1, 1, 2, 5]), 1)
        net = round(gross * random.uniform(0.5, 0.95), 1)

        # Deal Type — weighted random
        deal_type = random.choices(DEAL_TYPES, weights=DEAL_TYPE_WEIGHTS, k=1)[0]

        row = {
            "Tracker File V": random_name(),
            "Relationship Name": random_name() if random.random() < 0.7 else "",
            "Product Area": prod_area,
            "Product Type": prod_type,
            "Deal Type": deal_type,
            "WMLC Date": wmlc_date.strftime("%m/%d/%Y"),
            "Transaction Size (MM) Gross": gross,
            "Transaction Size Net": net,
        }

        # Populate flag columns (sparse)
        for flag in WMLC_FLAG_COLUMNS:
            affinity_areas = FLAG_PRODUCT_AFFINITY.get(flag, [])
            if prod_area in affinity_areas and random.random() < 0.25:
                row[flag] = "Y"
            else:
                row[flag] = ""

        # FA-MSSB sub-columns: only for FA-MSSB product area rows
        if prod_area == "FA-MSSB":
            # Sparsely set 1-2 FA sub-columns to "Y"
            n_fa_flags = random.choice([1, 1, 1, 2])
            chosen_fa = random.sample(FA_SUB_COLUMNS, n_fa_flags)
            for fa_col in FA_SUB_COLUMNS:
                row[fa_col] = "Y" if fa_col in chosen_fa else ""
        else:
            for fa_col in FA_SUB_COLUMNS:
                row[fa_col] = ""

        # DD-MSSB sub-columns: only for DD-MSSB product area rows
        if prod_area == "DD-MSSB":
            # Sparsely set one DD sub-column to "Y"
            chosen_dd = random.choice(DD_SUB_COLUMNS)
            for dd_col in DD_SUB_COLUMNS:
                row[dd_col] = "Y" if dd_col == chosen_dd else ""
        else:
            for dd_col in DD_SUB_COLUMNS:
                row[dd_col] = ""

        rows.append(row)

    df = pd.DataFrame(rows)

    # Ensure dates are parsed
    df["WMLC Date"] = pd.to_datetime(df["WMLC Date"])

    output_path = os.path.join(REPO, "proxy_data", "WMLC_Tracker.xlsx")
    df.to_excel(output_path, index=False, engine="openpyxl")

    print(f"Generated {len(df)} rows -> {output_path}")
    print(f"Product Areas: {df['Product Area'].value_counts().to_dict()}")
    print(f"Deal Types: {df['Deal Type'].value_counts().to_dict()}")
    print(f"Date range: {df['WMLC Date'].min()} to {df['WMLC Date'].max()}")

    # Flag population stats
    for flag in WMLC_FLAG_COLUMNS:
        count = (df[flag] == "Y").sum()
        if count > 0:
            print(f"  {flag}: {count}")

    # FA/DD sub-column stats
    for col in FA_SUB_COLUMNS + DD_SUB_COLUMNS:
        count = (df[col] == "Y").sum()
        if count > 0:
            print(f"  {col}: {count}")


if __name__ == "__main__":
    main()
