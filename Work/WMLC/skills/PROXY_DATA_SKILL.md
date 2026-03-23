# SKILL: Proxy Data Generation

## Objective
Generate realistic proxy loan portfolio data that exercises all WMLC flag paths.

## Tools
- Python 3.x with pandas, openpyxl, random, datetime
- numpy for distributions (if available, otherwise use random)

## Key Principles

### Realistic Distributions
- credit_lii: Use a log-normal distribution. Most loans should be $1M-$50M,
  with a fat tail extending to $500M+. This mirrors real WM portfolios.
- product_bucket: Weight toward LAL Diversified (~20%), TL SBL Diversified (~15%),
  TL CRE (~10%). Remaining 11 buckets share the rest.
- Borrower names: Use a pool of 200+ realistic names. Avoid "Test Borrower 1".

### WMLC Flag Coverage
You MUST ensure proxy data triggers EVERY one of the 15 WMLC flags at least 3 times.
This means deliberately crafting rows that meet each flag's conditions.

Strategy: After generating the bulk data randomly, append 45+ "seeded" rows
(3 per flag) that are specifically designed to trigger each flag. This guarantees
coverage even if the random distribution misses some thresholds.

Seeded row examples:
- Flag "TL-CRE >$75MM": product_bucket="TL CRE", credit_lii=80,000,000
- Flag "NTC > $50MM": is_lal_nfp=True, product_bucket="LAL NFPs", credit_lii=60,000,000
- Flag "TL-Alts HF/PE >$35MM": product_bucket="TL Unsecured",
  txt_mstr_facil_collateral_desc="Hedge Fund Class A Interests", credit_lii=40,000,000

### External File Alignment
- LAL_Credit: account_numbers in this file MUST match account_numbers in the base
  extract for LAL product_buckets. Include rows with "Yes" in exception columns.
- Loan_Reserve_Report: facility_account_numbers MUST match some TL-bucket loans.
  Include rows with "Corp" in Purpose Code and qualifying entity types.
- DAR_Tracker: facility_ids MUST match some TL CRE loans.
  Include "Office" and "OFFICE" (mixed case) in Property_Type.

### Account Number Formatting
- Base extract: 12-digit zero-padded string (e.g., "000012345678")
- LAL_Credit: format "###-######" (e.g., "001-234567") — the ETL strips dashes and pads
- Loan_Reserve_Report: 12-digit zero-padded (matches base directly)
- DAR_Tracker: variable length — ETL pads to 12

### Bucket Ladder Consistency
credit_lii_commitment_bucket and credit_lii_commitment_floor MUST be derived
deterministically from credit_lii. Write a helper function:

```python
def assign_bucket(credit_lii):
    thresholds = [
        (1_000_000_000, "$1,000,000,000", 1000000000),
        (750_000_000, "$750,000,000", 750000000),
        (700_000_000, "$700,000,000", 700000000),
        # ... all 24 buckets
        (1, "$1", 1),
    ]
    for floor_val, label, floor_int in thresholds:
        if credit_lii >= floor_val:
            return label, floor_int
    return "$1", 1
```

### Skip Row Handling for External Files
- LAL_Credit: First 3 rows must be filler text (e.g., "Report Header", blank, date)
  Row 4 = actual column headers. Data starts row 5.
- Loan_Reserve_Report: First 6 rows filler. Row 7 = headers. Data starts row 8.
- DAR_Tracker: No skip rows. Row 1 = headers.

## Output Checklist
- [ ] proxy_data/loan_extract.csv — 500+ rows, all columns, correct dtypes
- [ ] proxy_data/LAL_Credit.xlsx — 80+ data rows, 3 skip rows, matching accounts
- [ ] proxy_data/Loan_Reserve_Report.xlsx — 100+ data rows, 6 skip rows, matching facilities
- [ ] proxy_data/DAR_Tracker.xlsx — 60+ data rows, no skip rows, matching facilities
- [ ] At least 3 rows trigger each of the 15 WMLC flags
- [ ] At least 5 rows trigger MULTIPLE flags simultaneously
