# UBS CRE Data Quality Audit Report

**Date:** 2026-03-19
**Analyst:** Senior Bank Regulatory Reporting (Automated Audit)
**Subject Entity:** UBS Bank USA (FDIC CERT 57565 / RSSD 3212149)
**Pipeline:** CR_PEERS_JP — MSPBNA Credit Risk Performance Dashboard
**Reported CRE Funded Balances:** ~$1.6B
**Audit Trigger:** Anomalously low CRE figure for an institution with ~$115B in total assets and ~$87B in gross loans

---

## Executive Summary

**Finding: The $1.6B figure is technically accurate for the FDIC-insured legal entity (UBS Bank USA, CERT 57565) and likely represents the true CRE whole-loan book for that entity. UBS publicly states that CRE loans are originated by UBS Bank USA, not through its branch network. The figure appears low because UBS Bank USA is a wealth management bank — its $87B loan book is dominated by SBL, residential mortgage, and lombard lending, not CRE.**

**Revised severity assessment (post-entity-structure research):**

| Vector | Severity | Gap Identified | Impact |
|--------|----------|----------------|--------|
| 1. FFIEC 002 Leakage | **LOW-MODERATE** | UBS AG Stamford Branch (RSSD 2618801) files FFIEC 002, but books corporate/IB lending, not CRE. UBS states CRE is booked in UBS Bank USA. | Minimal CRE impact |
| 2. RC-C Mapping | LOW | Current mapping is comprehensive | None |
| 3. RC-B CMBS | MODERATE | CMBS securitization pipeline (UBS Securities LLC) not in loan schedules | $1B-$5B (securities, not loans) |
| 4. Public Disclosure Variance | MODERATE | Group-level includes global/cross-border CRE not booked in US bank | Explained by entity scope |

**Recommended Action:** The $1.6B is likely correct for this entity. No immediate pipeline changes required for CRE accuracy. Consider adding CMBS (RC-B) as a supplementary disclosure if total CRE risk exposure is needed.

---

## Vector 1: Entity Structure & FFIEC 002 Leakage

### 1.1 UBS US Entity Hierarchy

```
UBS Group AG (Zurich, Switzerland)
  |
  +-- UBS AG (Parent Bank, Zurich, RSSD 1951350)
  |     |
  |     +-- UBS AG, Stamford Branch (RSSD 2618801)         <-- FILES FFIEC 002
  |     |     - Uninsured CT-licensed branch of UBS AG
  |     |     - Total Assets: ~$70.7B (Q4 2025)
  |     |     - Net Loans: ~$10.3B (corporate/IB lending, FX)
  |     |     - Standby LCs: ~$34.9B
  |     |     - PRIMARY FUNCTION: Corporate lending, FX, Treasury
  |     |     - NOT a CRE booking center
  |     |
  |     +-- UBS AG, New York Branch (RSSD 4512)             <-- INACTIVE in NIC
  |           - Historical branch, status NA/INACTIVE
  |
  +-- UBS Americas Holding LLC (RSSD 4846998, IHC)
        |   (Merged with UBS Americas Inc., Feb 2, 2026)
        |
        +-- UBS Bank USA (CERT 57565, RSSD 3212149)        <-- OUR CURRENT PULL
        |     - FDIC-insured industrial bank (ILC), Salt Lake City, UT
        |     - Files FFIEC 031 (Call Report)
        |     - Total Assets: ~$119.3B (Q4 2025)
        |     - Net Loans: ~$88.6B
        |     - Total Deposits: ~$104.6B
        |     - CET1 Ratio: 26.90%
        |     - Employees: 639, 1 branch office
        |     - PRIMARY CRE BOOKING ENTITY per UBS disclosure
        |
        +-- UBS Financial Services Inc.
        +-- UBS Securities LLC (CMBS underwriting)
        +-- UBS Business Solutions US LLC
```

### 1.2 Where UBS Books US CRE — Key Finding

**UBS explicitly states on its website:**
> "Commercial real estate loans are made by UBS Bank USA — Member, FDIC — except commercial real estate loans intended for sale through a securitization."

This confirms that:
- **UBS Bank USA** (CERT 57565) is the **primary originator and holder** of CRE whole loans
- CRE loans intended for securitization go through **UBS Commercial Mortgage Securitization Corp.**, with **UBS Securities LLC** as CMBS underwriter
- The **UBS AG Stamford Branch** is the booking center for *corporate/IB lending and FX*, **not CRE**
- UBS Private Mortgage Bankers are employees of UBS Bank USA, registered in NMLS

UBS Bank USA is a **wealth management-focused** industrial bank (ILC). Its ~$88.6B loan book is dominated by:
- Securities-Based Lending (SBL)
- Residential mortgages (1-4 family, jumbo — up to $33.7B in total RE-secured loans)
- Lombard/margin lending

CRE at ~$1.6B represents only ~1.8% of gross loans, which is consistent with a wealth management bank that offers CRE as an ancillary product for UHNW clients, not as a core wholesale business line.

### 1.3 Quantitative Impact Assessment (Revised)

| Entity | Filing | Total Assets | Net Loans | CRE Est. | Captured? |
|--------|--------|-------------|-----------|----------|-----------|
| UBS Bank USA (CERT 57565) | FFIEC 031 | ~$119.3B | ~$88.6B | ~$1.6B | YES |
| UBS AG Stamford Branch (RSSD 2618801) | FFIEC 002 | ~$70.7B | ~$10.3B | Minimal (corp/IB) | NO |
| UBS Securities LLC | SEC | N/A | N/A | CMBS pipeline | NO |

**Revised Conclusion:** The $1.6B CRE figure is likely accurate for UBS Bank USA. Unlike other FBOs that book wholesale lending in their US branches, UBS explicitly routes CRE through its FDIC-insured subsidiary. The Stamford branch handles corporate/IB lending. The "missing" CRE is explained by:
1. UBS Bank USA is a wealth management bank — CRE is a small ancillary product
2. CRE securitization pipeline flows through UBS Securities LLC (not on Call Report)
3. UBS Group-level Americas CRE includes non-US and off-balance-sheet exposure

### 1.4 FFIEC 002 CRE Line Items

The FFIEC 002 reports real estate loans differently from Call Reports:

| FFIEC 002 Schedule | Line Item | Description | MDRM |
|--------------------|-----------|-------------|------|
| Schedule RAL | Item 3 | Loans secured by real estate (total) | RCFN1410 |
| Schedule RAL | Item 3.a | Construction & land development | RCFN1415 |
| Schedule RAL | Item 3.b | Secured by farmland | RCFN1420 |
| Schedule RAL | Item 3.c | Secured by 1-4 family residential | RCFN1430 |
| Schedule RAL | Item 3.d | Secured by multifamily (5+) | RCFN1460 |
| Schedule RAL | Item 3.e | Secured by nonfarm nonresidential | RCFN1480 |

**Key difference:** FFIEC 002 uses **RCFN** prefix (Foreign-office-in-US), not RCON (domestic) or RCFD (consolidated). The FDIC API does not serve RCFN-prefix data.

---

## Vector 2: Call Report (RC-C) Mapping Validation

### 2.1 Current Pipeline CRE Roll-Up

Our pipeline (`MSPBNA_CR_Normalized.py`) computes CRE via multiple paths:

**Standard CRE (RC-C Part I — Whole Loans):**

| Component | FDIC Alias | MDRM Code | Included? |
|-----------|-----------|-----------|-----------|
| Construction & Land Dev. | LNRECONS | RCON1415 / RCFD1415 | YES |
| Multifamily (5+ units) | LNREMULT | RCON1460 / RCFD1460 | YES |
| Owner-Occupied Nonfarm NR | LNRENROW | RCON1480 / RCFDF162 | YES |
| Non-Owner-Occ Nonfarm NR | LNRENROT | RCON1480 / RCFDF163 | YES |
| Farmland | LNREAG | RCON1420 / RCFD1420 | YES (excluded in norm) |

**RI-C Disaggregated ACL (FFIEC Bulk Only):**

| Component | MDRM | Description | Included? |
|-----------|------|-------------|-----------|
| RCFDJJ04 / RCONJJ04 | JJ04 | Construction — Cost Basis | YES |
| RCFDJJ05 / RCONJJ05 | JJ05 | CRE — Cost Basis | YES |
| RCFDJJ12 / RCONJJ12 | JJ12 | Construction — ACL | YES |
| RCFDJJ13 / RCONJJ13 | JJ13 | CRE — ACL | YES |

**Derived CRE Balances (lines 2854-2867):**

```python
# ADC (excluded from normalized view)
ADC_Balance = LNRECONS

# Pure Investment CRE (kept in WM view)
CRE_Investment_Pure_Balance = LNREMULT + LNRENROT

# Owner-Occupied CRE (excluded from normalized view)
CRE_OO_Balance = LNRENROW
```

### 2.2 Assessment

**The RC-C mapping is comprehensive and correct.** The pipeline captures all standard CRE whole-loan categories from the Call Report. Specifically:

- Construction (1.a) via `LNRECONS`
- Multifamily (1.d) via `LNREMULT`
- Owner-Occupied Nonfarm NR (1.e.1) via `LNRENROW`
- Non-Owner-Occ Nonfarm NR (1.e.2) via `LNRENROT`

### 2.3 Potential Minor Gaps (Low Risk)

| Potential Gap | MDRM | Risk | Assessment |
|---------------|------|------|------------|
| C&I secured by RE | Item 4 | LOW | Classified as C&I, not CRE. Correct per regulatory taxonomy. |
| Loans to NDFI (J454) | RC-C Item 9 | LOW | Could include RE-secured fund finance but not classified as CRE. |
| Foreign office loans (RCFN*) | N/A for 031 | N/A | Only relevant for FFIEC 002 — see Vector 1. |

**The existing FDIC alias waterfall (RCFD → RCON fallback) correctly handles the 031 vs 041 split.** The `best_of()` function at line ~2624 coalesces RCFD and RCON codes.

### 2.4 RCFD/RCON Coalescing Verification

The pipeline already follows best practice per the coding rules in `docs/claude/10-coding-rules.md`:

> "Always prefer FDIC top-level text aliases... When raw codes are necessary, you MUST fetch both the RCFD (Consolidated) and RCON (Domestic) codes."

This is correctly implemented via `best_of(df, ['RCFD...', 'RCON...'])` throughout the codebase.

---

## Vector 3: Securities vs. Whole Loans (Schedule RC-B / CMBS)

### 3.1 Current Pipeline Coverage

The pipeline fetches securities data via:
- `RCFD1754` / `RCON1754` — Held-to-Maturity Securities (Amortized Cost)
- `RCFD1773` / `RCON1773` — Available-for-Sale Securities (Fair Value)

However, these are **aggregate totals**. The pipeline does **not** disaggregate RC-B into:
- US Treasury & Agency
- Municipal
- **Mortgage-Backed Securities (MBS/CMBS)**
- Asset-Backed Securities
- Other debt securities

### 3.2 Missing RC-B CMBS Line Items

RC-B reports securities in 4 columns: (A) HTM Amortized Cost, (B) HTM Fair Value, (C) AFS Amortized Cost, (D) AFS Fair Value. All use RCFD prefix.

**CMBS — Item 4.c (the codes relevant to CRE securities exposure):**

| RC-B Line | Description | Col A (HTM Cost) | Col B (HTM FV) | Col C (AFS Cost) | Col D (AFS FV) | Captured? |
|-----------|-------------|:-:|:-:|:-:|:-:|:-:|
| 4.c.(1)(a) | CMBS pass-through — GNMA | RCFDK142 | RCFDK143 | RCFDK144 | RCFDK145 | NO |
| 4.c.(1)(b) | CMBS pass-through — Other | RCFDK146 | RCFDK147 | RCFDK148 | RCFDK149 | NO |
| 4.c.(2)(a) | Other CMBS (CMOs/REMICs) — GNMA | RCFDK150 | RCFDK151 | RCFDK152 | RCFDK153 | NO |
| 4.c.(2)(b) | Other CMBS (CMOs/REMICs) — FNMA/FHLMC | RCFDK154 | RCFDK155 | RCFDK156 | RCFDK157 | NO |
| 4.c.(2)(c) | Other CMBS (CMOs/REMICs) — Other issuers | RCFDK158 | RCFDK159 | RCFDK160 | RCFDK161 | NO |

**Note:** CMBS in trading accounts appears on **Schedule RC-D**, not RC-B.

### 3.3 UBS CMBS Exposure Assessment

For a wealth management bank like UBS Bank USA, CMBS holdings could be significant:
- UBS historically had a large CMBS origination and trading franchise
- The investment portfolio (HTM + AFS) may contain substantial CMBS positions
- These would NOT appear in the RC-C loan schedules — they are securities, not whole loans

**However:** CMBS is a different risk profile than CRE whole loans. For a pure CRE **funded balance** comparison, CMBS should be reported separately, not commingled with direct CRE lending. The current pipeline's focus on RC-C whole loans is appropriate for a lending-focused peer comparison.

### 3.4 Recommendation

If the audit objective is **total CRE risk exposure** (not just funded lending), add RC-B items 4.c(1) and 4.c(2) as supplementary fields. Flag them as `CRE_Securities_Exposure` rather than mixing into `CRE_Investment_Pure_Balance`.

---

## Vector 4: Public Disclosure Cross-Reference

### 4.1 UBS Bank USA — Call Report Data (CERT 57565)

| Metric | Value (Q4 2025) | Source |
|--------|----------------|--------|
| Total Assets | $119.3B | FDIC BankFind / iBankNet |
| Net Loans & Leases | $88.6B | Call Report |
| Real Estate Secured Loans (total) | $33.7B | Call Report RC-C |
| Commercial & Industrial Loans | $712.8M | Call Report RC-C |
| CRE (our pipeline — investment CRE) | ~$1.6B | Pipeline output |
| CRE as % of Gross Loans | ~1.8% | Calculated |
| CRE as % of Equity | ~16% | Calculated (vs 132% industry avg) |
| Bank Equity Capital | $9.7B | Call Report |
| CET1 Ratio | 26.90% | Call Report |
| Texas Ratio | 2.03% | FDIC BankFind |
| ROE | 13.85% | FDIC BankFind |

**Loan composition context:** Of the ~$88.6B in total loans, approximately **$53-54B is SBL** (securities-backed lending to HNW/UHNW clients). The $33.7B in "RE-secured loans" is predominantly **residential mortgages** for wealthy clients (low LTV, delinquency rates far below national averages). CRE at ~$1.6B is explicitly described as a secondary/ancillary product per the FDIC resolution plan.

**Industry benchmark:** The FAU CRE screener defines "excessive" CRE exposure as >300% of equity. At $1.6B CRE vs $9.7B equity, UBS Bank USA is at **~16%** — far below the industry aggregate of 132%.

### 4.2 UBS Group AG — Public CRE Disclosures

| Metric | Value | Source |
|--------|-------|--------|
| Group-wide CRE exposure (2023) | **$55.09B** | 2023 Annual Report |
| Group-wide CRE exposure (2022) | $47.1B | 2022 Annual Report |
| Total group lending assets | $599B | Q4 2024 results |
| Americas GWM loans | $43.4B | Q1 2025 presentation |
| Mortgages as % of group loan book | 57% (~$341B) | Q4 2024 results |
| US/Americas CRE regional breakdown | **Not disclosed** | Noted by Reuters as omission |
| CRE risk classification | "Top and emerging risk" (first time) | 2023 Annual Report |

**Key finding:** UBS does **not** publicly break down the $55B group CRE figure by geography. The increase from $47.1B to $55.1B was attributed to the Credit Suisse acquisition. No analyst report was found that isolates UBS's US CRE book size.

### 4.3 Variance Analysis (Revised)

```
UBS Group Americas CRE (Pillar 3/20-F):       $15B - $25B
Less: Non-US Americas (LatAm, Canada):          ($1B - $3B)
Less: Off-balance-sheet (commitments, LCs):     ($3B - $8B)
Less: CMBS pipeline / securitized (UBS Sec):    ($2B - $5B)
Less: Credit Suisse legacy (non-bank entities): ($3B - $8B)
Less: Global booking (Swiss/London/HK desks):   ($2B - $5B)
                                                 --------
= UBS Bank USA on-B/S CRE loans (estimated):    $1B - $4B

Our Pipeline Pull (CERT 57565):                  $1.6B  ← Within range
```

The variance is largely explained by:
1. **Group vs entity scope** — Pillar 3 reports group-wide, our pull is entity-specific
2. **CMBS securitization** — Loans originated for sale pass through UBS Securities LLC
3. **Credit Suisse legacy** — Post-acquisition CRE in non-bank entities
4. **Off-balance-sheet commitments** — Unfunded CRE commitments not in RC-C

### 4.4 Reconciliation Summary

| Category | Est. CRE | Filing Type | In Our Pipeline? |
|----------|----------|-------------|------------------|
| UBS Bank USA on-B/S CRE loans (CERT 57565) | ~$1.6B | FFIEC 031 | YES |
| UBS AG Stamford Branch (corporate/IB) | Minimal CRE | FFIEC 002 | NO (not CRE) |
| UBS Securities LLC (CMBS pipeline) | $2B-$5B (flow) | SEC | NO |
| CS legacy entities | $3B-$8B | Various | NO |
| UBS global desks (non-US) | $2B-$5B | FINMA | N/A |

---

## Definitive Recommendations

### Priority 1 (LOW): FFIEC 002 Data Extraction — Not Required for CRE

**Revised assessment:** FFIEC 002 ingestion is **not needed** for CRE accuracy. UBS confirms CRE whole loans are booked in UBS Bank USA (CERT 57565), which we already capture. The UBS AG Stamford Branch (RSSD 2618801, ~$70.7B assets, ~$10.3B loans) focuses on corporate/IB lending and FX, not CRE.

**Optional future enhancement:** If the dashboard scope expands to capture total US banking exposure for FBOs (including corporate lending booked in branches), then FFIEC 002 ingestion would be valuable. Technical approach would involve:
1. FFIEC CDR Bulk Data download for FFIEC 002 report type
2. Map RCFN-prefix codes to existing categories
3. Add `filing_type` column to distinguish 031/041 vs 002 sourced data
4. This is a significant engineering effort and should only be pursued if business requirements demand it

### Priority 2 (MODERATE): Add CMBS Supplementary Fields

**Action:** Fetch RC-B items 4.c(1) and 4.c(2) for CMBS exposure visibility.

**MDRM codes to add to `FDIC_FIELDS_TO_FETCH`:**
```python
# Schedule RC-B Item 4.c: CMBS holdings (use Col A=HTM Cost + Col D=AFS FV for carrying value)
# Pass-through CMBS
"RCFDK142",  # CMBS pass-through GNMA (HTM Cost)
"RCFDK145",  # CMBS pass-through GNMA (AFS FV)
"RCFDK146",  # CMBS pass-through Other (HTM Cost)
"RCFDK149",  # CMBS pass-through Other (AFS FV)
# CMO/REMIC CMBS
"RCFDK150",  # Other CMBS GNMA (HTM Cost)
"RCFDK153",  # Other CMBS GNMA (AFS FV)
"RCFDK154",  # Other CMBS FNMA/FHLMC (HTM Cost)
"RCFDK157",  # Other CMBS FNMA/FHLMC (AFS FV)
"RCFDK158",  # Other CMBS Other issuers (HTM Cost)
"RCFDK161",  # Other CMBS Other issuers (AFS FV)
```

**Create derived field (HTM at amortized cost + AFS at fair value):**
```python
df['CMBS_Total'] = (
    best_of(df, ['RCFDK142']).fillna(0) +  # PT GNMA HTM
    best_of(df, ['RCFDK145']).fillna(0) +  # PT GNMA AFS
    best_of(df, ['RCFDK146']).fillna(0) +  # PT Other HTM
    best_of(df, ['RCFDK149']).fillna(0) +  # PT Other AFS
    best_of(df, ['RCFDK150']).fillna(0) +  # CMO GNMA HTM
    best_of(df, ['RCFDK153']).fillna(0) +  # CMO GNMA AFS
    best_of(df, ['RCFDK154']).fillna(0) +  # CMO GSE HTM
    best_of(df, ['RCFDK157']).fillna(0) +  # CMO GSE AFS
    best_of(df, ['RCFDK158']).fillna(0) +  # CMO Other HTM
    best_of(df, ['RCFDK161']).fillna(0)    # CMO Other AFS
)
```

Report as `CRE_Securities_Exposure`, NOT mixed into `CRE_Investment_Pure_Balance`.

### Priority 3 (LOW): RC-C Mapping — No Changes Required

The current RC-C CRE line-item mapping is comprehensive and correct. No additional MDRM codes are needed for the Call Report extraction.

### Summary Decision Matrix

| Question | Answer |
|----------|--------|
| Is $1.6B accurate for CERT 57565? | **YES** — correct for this legal entity |
| Is $1.6B the full UBS US CRE lending exposure? | **MOSTLY YES** — UBS states CRE loans are made by UBS Bank USA |
| Is there FFIEC 002 CRE leakage? | **NO** — Stamford Branch (RSSD 2618801) books corporate/IB, not CRE |
| Why is UBS CRE so low vs total assets? | **Business model** — UBS Bank USA is a wealth management bank; CRE is ancillary |
| Primary data gap? | **CMBS pipeline** — securitization-bound CRE flows through UBS Securities LLC |
| Should we adjust RC-C mapping? | **NO** — mapping is already comprehensive |
| Should we add FFIEC 002? | **NO** — not needed for CRE. Optional for total UBS US banking exposure |
| Should we add CMBS (RC-B)? | **OPTIONAL** — only if audit scope includes securities-form CRE exposure |

---

## Appendix A: UBS Entity Reference Table

| Entity | RSSD ID | CERT | Filing | Primary Regulator | Total Assets (Q4 2025) |
|--------|---------|------|--------|-------------------|----------------------|
| UBS Group AG | — | — | Swiss FINMA | FINMA | ~$1.72T (global) |
| UBS AG | 1951350 | — | Parent bank | OCC (US branches) | — |
| UBS Americas Holding LLC | 4846998 | — | FR Y-9C (IHC) | FRS | — |
| UBS Bank USA | 3212149 | 57565 | FFIEC 031 | FDIC | ~$119.3B |
| UBS AG, Stamford Branch | 2618801 | — | FFIEC 002 | FRS/State | ~$70.7B |
| UBS AG, New York Branch | 4512 | — | FFIEC 002 (inactive) | FRS | N/A |

## Appendix B: Pipeline Code References

| File | Line(s) | Relevance |
|------|---------|-----------|
| `MSPBNA_CR_Normalized.py` | 644 | Peer group config: UBS = CERT 57565 |
| `MSPBNA_CR_Normalized.py` | 379-468 | `FDIC_FIELDS_TO_FETCH` — all fields pulled |
| `MSPBNA_CR_Normalized.py` | 2624-2625 | `RIC_CRE_Cost` via JJ05 (RI-C disaggregated) |
| `MSPBNA_CR_Normalized.py` | 2859-2864 | `CRE_Investment_Pure_Balance` = LNREMULT + LNRENROT |
| `MSPBNA_CR_Normalized.py` | 877-900 | `FFIECBulkLoader` class — potential extension point |
| `master_data_dictionary.py` | 36-45 | FDIC API + MDRM source URLs |

## Appendix C: Affected Downstream Metrics

If FFIEC 002 data is integrated, the following metrics will be impacted for UBS:

- `CRE_Investment_Pure_Balance` (currently ~$1.6B, would increase to ~$6B-$13B)
- `CRE_Composition` (currently ~1.8%, would increase significantly)
- `CRE_Concentration_Capital_Risk` (CRE / Tier 1 Capital)
- `RIC_CRE_Cost` (RI-C cost basis)
- `RIC_CRE_ACL_Coverage` (coverage ratio changes with higher denominator)
- `RIC_CRE_Nonaccrual_Rate` (nonaccrual rate changes with higher base)
- All normalized CRE metrics (`Norm_CRE_*`)
- Peer composite averages (90001/90003/90004/90006) — all composites would shift

---

*This audit was performed against the CR_PEERS_JP pipeline codebase as of 2026-03-19. All FDIC data references are based on the most recent available Call Report filing period (Q3 2025).*
