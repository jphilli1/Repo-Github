# UBS CRE Risk Metric Mapping Audit

**Date:** 2026-03-19
**Scope:** Nonaccrual, NCO, and Delinquency (PD30/PD90) mapping for CRE segment
**Subject Entity:** UBS Bank USA (FDIC CERT 57565 / RSSD 3212149)
**Filing Type:** FFIEC 031 (internationally active — uses RCFD consolidated codes)

---

## Executive Summary

Five bugs identified in CRE risk metric mapping. One causes **active data loss** for all banks (P3REMULT delinquency never fetched). Two cause **semantic numerator/denominator misalignment** in CRE rates. Two are lower-severity code defects.

| Bug | Severity | Impact | Affected Metric |
|-----|----------|--------|-----------------|
| #1 `P3LREMUL` typo | **CRITICAL** | Multifamily PD30 data never fetched from FDIC API | `RIC_CRE_PD30`, `RIC_CRE_Inv_PD30` |
| #2 `FDIC_FALLBACK_MAP` dormant alias | LOW | Map defined but never applied; no current impact | None (dormant) |
| #3 `RIC_CRE_ACL` duplicate code | MODERATE | No RCON fallback for CRE ACL — silent failure on 041 filers | `RIC_CRE_ACL`, `RIC_CRE_ACL_Coverage` |
| #4 Nonaccrual numerator/denominator mismatch | **HIGH** | CRE nonaccrual includes OO CRE; balance excludes it | `RIC_CRE_Nonaccrual_Rate` |
| #5 NCO numerator/denominator mismatch | **HIGH** | CRE NCO includes OO CRE; balance excludes it | `RIC_CRE_NCO_Rate` (via TTM) |

---

## Live FDIC API Validation (UBS CERT 57565)

All CRE fields **are reported** by UBS Bank USA. The issue is not field availability — it's how our code fetches and maps them.

### CRE Balances (Q4 2025 / Q3 2025)

| Field | 20251231 | 20250930 | Description |
|-------|----------|----------|-------------|
| LNREMULT | 232,826 | 548,962 | Multifamily |
| LNRENROT | 1,248,363 | 1,621,046 | Non-owner-occ nonfarm |
| LNRENROW | 117,575 | 117,586 | Owner-occupied nonfarm |
| LNRECONS | 3,048 | 2,852 | Construction |

### CRE Nonaccrual

| Field | 20251231 | 20250930 | Description |
|-------|----------|----------|-------------|
| NAREMULT | 0 | 50,995 | Multifamily NA |
| NARENROT | 0 | 32,186 | Non-owner-occ NA |
| NARENROW | 0 | 0 | Owner-occupied NA |
| NARENRES | 0 | 32,186 | Nonfarm nonresidential total NA |

### CRE NCO (YTD Cumulative)

| Field | 20251231 | 20250930 | Description |
|-------|----------|----------|-------------|
| NTREMULT | 34,353 | 27,769 | Multifamily NCO YTD |
| NTRENROT | 10,864 | 0 | Non-owner-occ NCO YTD |
| NTRENROW | 0 | 0 | Owner-occupied NCO YTD |
| NTRENRES | 10,864 | 0 | Nonfarm nonresidential total NCO YTD |

### CRE Delinquency

| Field | 20251231 | 20250930 | Description |
|-------|----------|----------|-------------|
| P3REMULT | 0 | 49,412 | Multifamily PD30 |
| P3RENROT | 0 | 0 | Non-owner-occ PD30 |
| P9REMULT | 0 | 0 | Multifamily PD90 |
| P9RENROT | 0 | 0 | Non-owner-occ PD90 |

---

## Bug #1: `P3LREMUL` Typo in Fetch List (CRITICAL)

**Location:** `MSPBNA_CR_Normalized.py`, line 475

```python
# Current (WRONG):
"P3RECONS", "P3LREMUL", "P3RENROT", "P3RENROW", "P3REAG", "P3RENRES",

# Should be:
"P3RECONS", "P3REMULT", "P3RENROT", "P3RENROW", "P3REAG", "P3RENRES",
```

**Evidence:** FDIC API returns `null` for `P3LREMUL` and `0`/`49412` for `P3REMULT`:
```json
{"P3LREMUL": null, "P3REMULT": 0}   // Q4 2025
{"P3LREMUL": null, "P3REMULT": 49412} // Q3 2025
```

**Impact:** `P3REMULT` column is never populated from the FDIC API. When the code reaches `sum_cols(df_processed, ['P3REMULT', 'P3RENROT'])` at line 2646, `P3REMULT` is either missing from the dataframe or zero (from FFIEC bulk loader if available). This means:
- `RIC_CRE_Inv_PD30` understates multifamily delinquency
- `RIC_CRE_PD30` (via `resolve_cre_metric(df, 'P3')`) is also wrong
- All banks are affected, not just UBS
- The FFIEC bulk loader may partially compensate if it has the field, but the FDIC API path is broken

**Note:** The corresponding P9 field `P9REMULT` at line 482 is spelled correctly.

---

## Bug #2: `FDIC_FALLBACK_MAP` Dormant Alias (LOW)

**Location:** `MSPBNA_CR_Normalized.py`, line 374

```python
"P9RENRES": ["P9RENROT"], # Legacy Nonfarm NA -> Non-Owner Nonfarm NA
```

**Issue:** This alias would **overwrite** real `P9RENRES` data (nonfarm nonresidential 90+ PD total) with `P9RENROT` (non-owner-occupied only). However, the `FDIC_FALLBACK_MAP` dict is defined but **never consumed** — no code iterates over it to apply the fallback resolution. The comment at line 464 references it, but no implementation exists.

**Impact:** None currently. But if the fallback map is ever activated, this alias would corrupt `P9RENRES`.

---

## Bug #3: `RIC_CRE_ACL` Duplicate RCFD Code (MODERATE)

**Location:** `MSPBNA_CR_Normalized.py`, line 2625

```python
# Current (WRONG):
df_processed['RIC_CRE_ACL'] = best_of(df_processed, ['RCFDJJ13', 'RCFDJJ13'])

# Should be:
df_processed['RIC_CRE_ACL'] = best_of(df_processed, ['RCFDJJ13', 'RCONJJ13'])
```

**Impact:** For FFIEC 031 filers (like UBS), `RCFDJJ13` is the correct consolidated code and this works. But for **FFIEC 041 filers** (domestic-only banks), `RCFDJJ13` may not be populated. The intended RCON fallback `RCONJJ13` is never tried because the list has a duplicate. This affects the All Peers group (CERT 628, 3511, 7213, 3510) if any file 041.

**Note:** Line 2624 (`RIC_CRE_Cost`) correctly uses `['RCFDJJ05', 'RCONJJ05']`, confirming this is a typo.

---

## Bug #4: CRE Nonaccrual Numerator/Denominator Mismatch (HIGH)

**Location:** `MSPBNA_CR_Normalized.py`, lines 2655 and 2628-2637

### The Problem

`RIC_CRE_Nonaccrual` is computed via `resolve_cre_metric(df, 'NA')`:

```python
def resolve_cre_metric(df, prefix):
    mf  = NAREMULT        # Multifamily
    row = NARENROW        # Owner-occupied nonfarm
    res = NARENRES        # Nonfarm nonresidential (total/legacy)
    rot = NARENROT        # Non-owner-occupied nonfarm
    return mf + max(rot, row + res)   # <-- INCLUDES OWNER-OCCUPIED
```

But the denominator for `RIC_CRE_Nonaccrual_Rate` is `RIC_CRE_Cost` (RCFDJJ05), and the related balance `CRE_Investment_Pure_Balance` is defined as:

```python
CRE_Investment_Pure_Balance = LNREMULT + LNRENROT  # <-- EXCLUDES OWNER-OCCUPIED
```

### Concrete Example (UBS Q3 2025)

```
Numerator (resolve_cre_metric 'NA'):
  NAREMULT=50,995 + max(NARENROT=32,186, NARENROW=0 + NARENRES=32,186)
  = 50,995 + 32,186 = 83,181

Denominator (CRE_Investment_Pure_Balance):
  LNREMULT=548,962 + LNRENROT=1,621,046 = 2,170,008

Rate = 83,181 / 2,170,008 = 3.83%
```

If owner-occupied is excluded from numerator to match:
```
NAREMULT=50,995 + NARENROT=32,186 = 83,181  (same in this case because ROW=0)
```

In this specific quarter the values happen to be the same (because NARENROW=0 and NARENRES=NARENROT). But in quarters where NARENROW > 0 (e.g., for other peer banks), the numerator would be inflated relative to the denominator.

### The `max()` Logic Concern

The `max(rot, row + res)` construct is designed to handle a legacy FDIC API behavior where NARENRES was the total nonfarm nonresidential (= owner-occ + non-owner-occ), while NARENROT was the non-owner-occ subcategory. But:

1. If the FDIC API now reports BOTH NARENROT and NARENROW separately, the `max()` takes whichever is larger, which could include owner-occupied exposure
2. For the **Investment CRE** rate (which excludes owner-occupied by definition), the numerator should strictly be `NAREMULT + NARENROT`

---

## Bug #5: CRE NCO Numerator/Denominator Mismatch (HIGH)

**Location:** `MSPBNA_CR_Normalized.py`, lines 2763-2767

Same pattern as Bug #4, but for NCOs:

```python
cre_q_nco = df_processed['NTREMULT_Q'] + np.maximum(
    df_processed['NTRENROT_Q'],
    (df_processed.get('NTRENROW_Q', 0) + df_processed.get('NTRENRES_Q', 0))
)
```

This includes owner-occupied CRE NCOs in the "CRE NCO" figure, but the denominator (`CRE_Investment_Pure_Balance`) excludes owner-occupied. This inflates `RIC_CRE_NCO_Rate`.

### Concrete Example (UBS Q4 2025 YTD)

```
NTREMULT = 34,353  (multifamily)
NTRENROT = 10,864  (non-owner-occ)
NTRENROW = 0       (owner-occupied)
NTRENRES = 10,864  (nonfarm total)

max(10,864, 0 + 10,864) = 10,864
CRE NCO = 34,353 + 10,864 = 45,217

This happens to be correct for UBS (NTRENROW=0), but for banks
with non-zero NTRENROW, the numerator would be inflated.
```

---

## Recommended Fixes

### Fix #1: P3LREMUL → P3REMULT (line 475)

```python
# Line 475: Fix typo
"P3RECONS", "P3REMULT", "P3RENROT", "P3RENROW", "P3REAG", "P3RENRES",
```

### Fix #2: Remove dormant fallback alias (line 374)

```python
# Line 374: Remove or correct - P9RENRES is a legitimate field, not a legacy alias
# DELETE: "P9RENRES": ["P9RENROT"],
```

### Fix #3: RIC_CRE_ACL RCON fallback (line 2625)

```python
df_processed['RIC_CRE_ACL'] = best_of(df_processed, ['RCFDJJ13', 'RCONJJ13'])
```

### Fix #4 & #5: Align CRE risk numerators with investment CRE denominator

For the **Investment CRE** segment (which excludes owner-occupied), the numerators should be:

```python
# Line 2655: Use investment CRE numerator, NOT resolve_cre_metric
df_processed['RIC_CRE_Nonaccrual'] = sum_cols(df_processed, ['NAREMULT', 'NARENROT'])

# Line 2656-2657: Same for PD30/PD90
df_processed['RIC_CRE_PD30'] = sum_cols(df_processed, ['P3REMULT', 'P3RENROT'])
df_processed['RIC_CRE_PD90'] = sum_cols(df_processed, ['P9REMULT', 'P9RENROT'])

# Lines 2763-2767: Same for NCO quarterly
cre_q_nco = df_processed['NTREMULT_Q'] + df_processed['NTRENROT_Q']
```

**Note:** The `resolve_cre_metric` function and `RIC_CRE_TopHouse_*` columns (lines 2640-2642) should be **retained** as the broad "top-of-house CRE" aggregate. But `RIC_CRE_Nonaccrual` / `RIC_CRE_PD30` / `RIC_CRE_PD90` (used for rates and reporting) should use the clean investment CRE definition.

### Alternative: If `resolve_cre_metric` is intentionally broad

If the intent is for `RIC_CRE_*` to include all nonfarm nonresidential (owner-occ + non-owner-occ), then the **denominator** needs to change to match:

```python
# Alternative: Broaden the denominator to include owner-occupied
CRE_Broad_Balance = LNREMULT + LNRENROT + LNRENROW
```

But this contradicts the `CRE_Investment_Pure_Balance` definition (line 2859-2864) and the normalization taxonomy in `docs/claude/06-normalization-and-peer-groups.md`.

---

## Appendix: Field Name Reference

| FDIC API Field | Call Report Item | Description |
|---------------|-----------------|-------------|
| P3REMULT | RC-N 1.d | Multifamily PD 30-89 days |
| P3RENROT | RC-N 1.e(2) | Non-owner-occ nonfarm NR PD 30-89 |
| P3RENROW | RC-N 1.e(1) | Owner-occ nonfarm NR PD 30-89 |
| P3RENRES | RC-N 1.e | Nonfarm NR total PD 30-89 (legacy) |
| P9REMULT | RC-N 1.d | Multifamily PD 90+ days |
| P9RENROT | RC-N 1.e(2) | Non-owner-occ nonfarm NR PD 90+ |
| NAREMULT | RC-N 1.d | Multifamily nonaccrual |
| NARENROT | RC-N 1.e(2) | Non-owner-occ nonfarm NR nonaccrual |
| NARENROW | RC-N 1.e(1) | Owner-occ nonfarm NR nonaccrual |
| NARENRES | RC-N 1.e | Nonfarm NR total nonaccrual (legacy) |
| NTREMULT | RI-B 1.d | Multifamily net charge-offs |
| NTRENROT | RI-B 1.e(2) | Non-owner-occ nonfarm NR NCO |
| NTRENROW | RI-B 1.e(1) | Owner-occ nonfarm NR NCO |
| NTRENRES | RI-B 1.e | Nonfarm NR total NCO (legacy) |
| P3LREMUL | ❌ INVALID | Does not exist in FDIC API (returns null) |
| P3REMULT | ✅ CORRECT | Multifamily PD 30-89 (correct field name) |
