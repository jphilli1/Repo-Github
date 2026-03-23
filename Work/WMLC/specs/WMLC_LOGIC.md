# WMLC Flag Logic Specification

## Overview

WMLC (Wealth Management Lending Committee) flags identify loans that must be brought to committee
based on product type, size, and other characteristics. Flags are **multi-label** — a single loan
can carry multiple WMLC flags simultaneously.

A loan is considered `wmlc_qualified = True` if it carries **one or more** WMLC flags.

---

## External Input Files

| File | Skip Rows | Key Column | Key Format | Purpose |
|------|-----------|------------|------------|---------|
| `LAL_Credit` | 3 | `Account Number` | `###-######` → strip `-`, zero-pad to 12 | CSE exceptions, NTC (charity/operating co) |
| `Loan_Reserve_Report` | 6 | `Facility Account Number` | 12-digit zero-padded | NTC (corp entity filter) |
| `DAR_Tracker` | 0 | `Facility ID` | Zero-pad to 12 | CRE Office flag (`Property_Type`) |

> **Note:** `privately_share_report` is **excluded** from WMLC logic per design decision.

---

## Intermediate Tags (computed before WMLC flags)

### `is_ntc` (Non-Traditional Client)

**For LAL product_buckets** (`LAL Diversified`, `LAL Highly Conc.`, `LAL NFPs`):
```
is_ntc = True IF any of:
  (a) base.is_lal_nfp == True
  (b) account_number found in LAL_Credit WHERE
      "Operating Company" == "Yes"
      OR "Charity/Non-Profit Organization" == "Yes"
```

**For TL product_buckets** (all TL-prefixed buckets):
```
is_ntc = True IF account_number found in Loan_Reserve_Report WHERE
  "Purpose Code Description" CONTAINS "Corp" (case-insensitive)
  AND "Account Relationship Code Description" CONTAINS ANY OF:
    - "Organization (Acct owner)"
    - "Limited Liability Company"
    - "Corporation (CEO, etc.)"
    - "Limited Partnership"
```

**For RESI:** `is_ntc = False` (not applicable)

### `is_office` (CRE Office Property)

```
is_office = True IF facility_id found in DAR_Tracker WHERE
  "Property_Type" matches "office" (CASE-INSENSITIVE)
```

### `has_credit_policy_exception` (from LAL_Credit)

```
has_credit_policy_exception = True IF account_number found in LAL_Credit WHERE
  "Bank Level Limit/Guideline Exception" == "Yes"
  OR "Credit Report RAC Exception" == "Yes"
  OR "Firm Level Limit/Guideline Exception" == "Yes"
  OR "Significant Credit Standard Exception" == "Yes"
```

> This column is added to the base table for informational/audit purposes.
> It is **not** used as a WMLC trigger (CSE flag excluded from scope).

---

## TL Product Bucket Set

The following product_buckets are classified as "TL" for collateral-text-based flag evaluation:

```python
TL_BUCKETS = {
    "TL Aircraft",
    "TL CRE",
    "TL Life Insurance",
    "TL Multicollateral",
    "TL Other Secured",
    "TL PHA",
    "TL SBL Diversified",
    "TL SBL Highly Conc.",
    "TL Unsecured",
}
```

---

## WMLC Flag Definitions

All flags evaluated independently. A loan receives **every** flag it qualifies for.
All string comparisons on `txt_mstr_facil_collateral_desc` are **case-insensitive** and use **CONTAINS** logic.

### Flag 1: `NTC > $50MM`

```
product_bucket IN (all LAL + all TL buckets)
AND is_ntc == True
AND credit_lii > 50,000,000
```

### Flag 2: `Non-Pass Originations >$0MM`

```
focus_list == "Non-Pass"
AND NEW_CAMP_YN == "Y"
```

> No dollar threshold — any non-pass new commitment qualifies.

### Flag 3: `TL-CRE >$75MM`

```
product_bucket == "TL CRE"
AND credit_lii > 75,000,000
```

### Flag 4: `TL-CRE Office >$10MM`

```
product_bucket == "TL CRE"
AND credit_lii > 10,000,000
AND is_office == True
```

### Flag 5: `TL-SBL-D >$300MM`

```
product_bucket == "TL SBL Diversified"
AND credit_lii > 300,000,000
```

### Flag 6: `TL-SBL-C >$100MM`

```
product_bucket == "TL SBL Highly Conc."
AND credit_lii > 100,000,000
```

### Flag 7: `TL-LIC >$100MM`

```
product_bucket == "TL Life Insurance"
AND credit_lii > 100,000,000
```

### Flag 8: `TL-Alts HF/PE >$35MM`

```
product_bucket IN TL_BUCKETS
AND txt_mstr_facil_collateral_desc CONTAINS "Hedge"
AND credit_lii > 35,000,000
```

### Flag 9: `TL-Alts Private Shares >$35MM`

```
(product_bucket == "TL PHA" AND credit_lii > 35,000,000)
OR
(product_bucket == "TL Multicollateral"
  AND credit_lii > 50,000,000
  AND txt_mstr_facil_collateral_desc CONTAINS "Privately Held")
```

### Flag 10: `TL-Alts Unsecured >$35MM`

```
(product_bucket == "TL Unsecured" AND credit_lii > 35,000,000)
OR
(product_bucket == "TL Multicollateral"
  AND credit_lii > 50,000,000
  AND txt_mstr_facil_collateral_desc CONTAINS "Unsecured")
```

### Flag 11: `TL-Alts PAF >$50MM`

```
product_bucket IN TL_BUCKETS
AND txt_mstr_facil_collateral_desc CONTAINS "Aircraft"
AND credit_lii > 50,000,000
```

### Flag 12: `TL-Alts Fine Art >$50MM`

```
product_bucket IN TL_BUCKETS
AND txt_mstr_facil_collateral_desc CONTAINS "Fine Art"
AND credit_lii > 50,000,000
```

### Flag 13: `TL-Alts Other Secured >$50MM`

```
(product_bucket == "TL Other Secured" AND credit_lii > 50,000,000)
OR
(product_bucket == "TL Multicollateral"
  AND credit_lii > 50,000,000
  AND txt_mstr_facil_collateral_desc CONTAINS "Other")
```

### Flag 14: `LAL-D >$300MM`

```
product_bucket == "LAL Diversified"
AND credit_lii > 300,000,000
```

### Flag 15: `LAL-C >$100MM`

```
product_bucket == "LAL Highly Conc."
AND credit_lii > 100,000,000
```

---

## Output Columns Added to Base Table

| Column | Datatype | Description |
|--------|----------|-------------|
| `is_ntc` | boolean | Non-Traditional Client tag |
| `is_office` | boolean | CRE Office property tag (from DAR Tracker) |
| `has_credit_policy_exception` | boolean | Credit policy exception (from LAL_Credit) |
| `wmlc_flags` | string | Pipe-delimited list of all matched flags, e.g. `"TL-CRE >$75MM\|NTC > $50MM"` |
| `wmlc_flag_count` | int | Count of matched flags |
| `wmlc_qualified` | boolean | `True` if `wmlc_flag_count > 0` |

---

## Quick Reference: Flag → Key Conditions

| # | Flag Name | product_bucket Constraint | Threshold | Extra Condition |
|---|-----------|--------------------------|-----------|-----------------|
| 1 | NTC > $50MM | LAL + TL | > $50MM | `is_ntc == True` |
| 2 | Non-Pass Originations | Any | None | `focus_list == "Non-Pass"` AND `NEW_CAMP_YN == "Y"` |
| 3 | TL-CRE >$75MM | TL CRE | > $75MM | — |
| 4 | TL-CRE Office >$10MM | TL CRE | > $10MM | `is_office == True` |
| 5 | TL-SBL-D >$300MM | TL SBL Diversified | > $300MM | — |
| 6 | TL-SBL-C >$100MM | TL SBL Highly Conc. | > $100MM | — |
| 7 | TL-LIC >$100MM | TL Life Insurance | > $100MM | — |
| 8 | TL-Alts HF/PE >$35MM | TL_BUCKETS | > $35MM | collateral CONTAINS "Hedge" |
| 9 | TL-Alts Private Shares | TL PHA / TL Multi | > $35MM / $50MM | see logic above |
| 10 | TL-Alts Unsecured | TL Unsecured / TL Multi | > $35MM / $50MM | see logic above |
| 11 | TL-Alts PAF >$50MM | TL_BUCKETS | > $50MM | collateral CONTAINS "Aircraft" |
| 12 | TL-Alts Fine Art >$50MM | TL_BUCKETS | > $50MM | collateral CONTAINS "Fine Art" |
| 13 | TL-Alts Other Secured | TL Other Secured / TL Multi | > $50MM | see logic above |
| 14 | LAL-D >$300MM | LAL Diversified | > $300MM | — |
| 15 | LAL-C >$100MM | LAL Highly Conc. | > $100MM | — |
