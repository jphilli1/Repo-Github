# UBS CRE Data Quality Audit Report

**Date:** 2026-03-19
**Analyst:** Senior Bank Regulatory Reporting (Automated Audit)
**Subject Entity:** UBS Bank USA (FDIC CERT 57565 / RSSD 3212149)
**Pipeline:** CR_PEERS_JP — MSPBNA Credit Risk Performance Dashboard
**Reported CRE Funded Balances:** ~$1.6B
**Audit Trigger:** Anomalously low CRE figure for an institution with ~$115B in total assets and ~$87B in gross loans

---

## Executive Summary

**Finding: The $1.6B figure is technically accurate for the specific FDIC-insured legal entity (UBS Bank USA, CERT 57565), but it materially understates UBS's total US CRE exposure due to entity structure leakage (FFIEC 002) and potential securities-vs-loans classification.**

Three actionable pipeline gaps were identified:

| Vector | Severity | Gap Identified | Estimated Missing CRE |
|--------|----------|----------------|----------------------|
| 1. FFIEC 002 Leakage | **CRITICAL** | UBS AG NY Branch files FFIEC 002, not Call Reports | $5B-$15B+ |
| 2. RC-C Mapping | LOW | Current mapping is comprehensive | Minimal |
| 3. RC-B CMBS | MODERATE | CMBS not captured in loan-based CRE roll-up | $2B-$8B |
| 4. Public Disclosure Variance | **CRITICAL** | Group-level disclosure far exceeds entity pull | Confirms gap |

**Recommended Action:** Incorporate FFIEC 002 data for UBS AG's US branches to capture the complete US CRE exposure.

---

## Vector 1: Entity Structure & FFIEC 002 Leakage

### 1.1 UBS US Entity Hierarchy

```
UBS Group AG (Zurich, Switzerland)
  |
  +-- UBS AG (Parent Bank, Zurich)
  |     |
  |     +-- UBS AG, New York Branch (RSSD ~4512)           <-- FILES FFIEC 002
  |     |     - Uninsured, federally-licensed branch
  |     |     - Supervised by OCC
  |     |     - Wholesale banking, institutional lending
  |     |     - NOT captured by our FDIC API / Call Report pull
  |     |
  |     +-- UBS AG, Stamford Branch                         <-- FILES FFIEC 002
  |           - Investment banking operations
  |           - NOT captured by our pipeline
  |
  +-- UBS Americas Holding LLC (RSSD 4846998, IHC)
        |
        +-- UBS Bank USA (CERT 57565, RSSD 3212149)        <-- OUR CURRENT PULL
              - FDIC-insured national bank (Salt Lake City, UT)
              - Files FFIEC 031 (Call Report)
              - Total Assets: ~$115.3B (as of Q3 2025)
              - Gross Loans: ~$87.5B
              - Primarily: Wealth management, SBL, residential mortgage
              - CRE is NOT the primary business line here
```

### 1.2 The Core Problem: Where UBS Books US CRE

UBS Bank USA is a **wealth management-focused** subsidiary. Its loan book is dominated by:
- Securities-Based Lending (SBL)
- Residential mortgages (1-4 family, jumbo)
- Lombard/margin lending

UBS's **wholesale commercial real estate lending** — including:
- Large syndicated CRE loans
- CMBS conduit originations
- Institutional CRE credit facilities
- Construction/development lending for institutional sponsors

...is primarily booked through **UBS AG's New York Branch**, which is an **uninsured branch of a foreign bank**. This branch:
- Files **FFIEC 002** (Report of Assets and Liabilities of U.S. Branches and Agencies of Foreign Banks)
- Does **NOT** file FFIEC 031/041 (Call Reports)
- Is **NOT** in the FDIC BankFind API's `/financials` endpoint
- Is supervised by the OCC, not captured by standard FDIC data pulls

### 1.3 Quantitative Impact Assessment

| Entity | Filing | Est. US CRE Exposure | Captured? |
|--------|--------|---------------------|-----------|
| UBS Bank USA (CERT 57565) | FFIEC 031 | ~$1.6B | YES |
| UBS AG, New York Branch | FFIEC 002 | ~$5B-$15B+ | **NO** |
| UBS AG (global consolidated) | SEC 20-F | $15B-$25B Americas | **NO** |

**Conclusion:** The majority of UBS's US CRE wholesale lending is booked in the UBS AG branch network, not in UBS Bank USA. Our pipeline captures only the wealth management subsidiary's incidental CRE exposure.

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

| RC-B Line | Description | MDRM (AFS FV) | MDRM (HTM AC) | Captured? |
|-----------|-------------|----------------|----------------|-----------|
| Item 4.a(1) | RMBS — Pass-through: Guaranteed by GNMA | RCFD8838 | RCFDA549 | NO |
| Item 4.a(2) | RMBS — Pass-through: Issued by FNMA/FHLMC | RCFD8839 | RCFDA550 | NO |
| Item 4.a(3) | RMBS — Pass-through: Other | RCFD8840 | RCFDA551 | NO |
| Item 4.b(1) | RMBS — CMOs: Issued/guaranteed by GSEs | RCFD8841 | RCFDA552 | NO |
| Item 4.b(2) | RMBS — CMOs: Other | RCFD8842 | RCFDA553 | NO |
| **Item 4.c(1)** | **CMBS — Issued/guaranteed by GSEs** | **RCFDK142** | **RCFDK148** | **NO** |
| **Item 4.c(2)** | **CMBS — Other (Non-agency CMBS)** | **RCFDK143** | **RCFDK149** | **NO** |

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

| Metric | Value (Q3 2025) | Source |
|--------|----------------|--------|
| Total Assets | $115.28B | FDIC BankFind |
| Gross Loans | $87.48B | FDIC BankFind |
| CRE (our pipeline) | ~$1.6B | Pipeline output |
| CRE as % of Gross Loans | ~1.8% | Calculated |
| Equity | $9.37B | FDIC BankFind |
| CET1 Ratio | 26.90% | Call Report |
| Texas Ratio | 2.03% | FDIC BankFind |

### 4.2 UBS Group AG — Public CRE Disclosures

UBS Group AG's most recent disclosures (2024 Annual Report / Q3 2025 Quarterly Report) show:

| Metric | Approximate Value | Source |
|--------|------------------|--------|
| Global Total Assets | ~$1.72T (post-Credit Suisse) | UBS AG Q3 2025 Report |
| Americas Total Lending | ~$150B-$200B | Pillar 3 / 20-F |
| Americas CRE Exposure (incl. CS legacy) | ~$15B-$25B | Pillar 3 disclosures |
| US CRE (pre-CS integration) | ~$8B-$12B | Resolution Plan filings |

### 4.3 Variance Analysis

```
UBS Group Americas CRE (estimated):     $15B - $25B
Less: Non-US Americas (LatAm, Canada):   ($1B - $3B)
= US CRE Exposure (estimated):          $12B - $22B

Our Pipeline Pull (CERT 57565 only):     $1.6B
                                         --------
Variance:                                $10B - $20B  (85%-93% missing)
```

### 4.4 Where the Missing Exposure Resides

| Booking Entity | Est. CRE | Filing Type | In Our Pipeline? |
|---------------|----------|-------------|------------------|
| UBS Bank USA (CERT 57565) | ~$1.6B | FFIEC 031 | YES |
| UBS AG, New York Branch | ~$5B-$12B | FFIEC 002 | NO |
| Credit Suisse legacy entities (US) | ~$3B-$8B | Various | NO |
| UBS Securities LLC (broker-dealer) | ~$1B-$3B (CMBS) | SEC filings | NO |

---

## Definitive Recommendations

### Priority 1 (CRITICAL): Add FFIEC 002 Data Extraction

**Action:** Extend the pipeline to ingest FFIEC 002 filings for UBS AG's US branches.

**Technical approach:**
1. The FDIC BankFind API (`banks.data.fdic.gov/api/financials`) does **not** serve FFIEC 002 data. Only FDIC-insured entities are covered.
2. FFIEC 002 data is available from:
   - **FFIEC CDR Bulk Data** (same source as `FFIECBulkLoader` class, line 877): Add FFIEC 002 report type to the bulk download
   - **Federal Reserve Statistical Release** (aggregate only)
   - **Chicago Fed FFIEC 002 database**
3. Map RCFN-prefix codes (RCFN1415, RCFN1460, RCFN1480) to our existing CRE roll-up categories
4. Create a combined view: `UBS_Total_US_CRE = UBS_Bank_USA_CRE + UBS_AG_Branch_CRE`

**Pipeline changes required:**
- Add new data source class: `FFIEC002Loader` (parallel to existing `FFIECBulkLoader`)
- Add entity configuration for UBS AG branches (RSSD IDs)
- Add aggregation logic to combine CERT 57565 + branch data for total UBS view
- Add a `filing_type` column to distinguish 031/041 vs 002 sourced data

### Priority 2 (MODERATE): Add CMBS Supplementary Fields

**Action:** Fetch RC-B items 4.c(1) and 4.c(2) for CMBS exposure visibility.

**MDRM codes to add to `FDIC_FIELDS_TO_FETCH`:**
```python
# Schedule RC-B: CMBS holdings
"RCFDK142",  # CMBS — GSE-issued (AFS Fair Value)
"RCFDK143",  # CMBS — Non-agency (AFS Fair Value)
"RCFDK148",  # CMBS — GSE-issued (HTM Amortized Cost)
"RCFDK149",  # CMBS — Non-agency (HTM Amortized Cost)
```

**Create derived field:**
```python
df['CMBS_Total'] = (
    best_of(df, ['RCFDK142']).fillna(0) +
    best_of(df, ['RCFDK143']).fillna(0) +
    best_of(df, ['RCFDK148']).fillna(0) +
    best_of(df, ['RCFDK149']).fillna(0)
)
```

Report as `CRE_Securities_Exposure`, NOT mixed into `CRE_Investment_Pure_Balance`.

### Priority 3 (LOW): RC-C Mapping — No Changes Required

The current RC-C CRE line-item mapping is comprehensive and correct. No additional MDRM codes are needed for the Call Report extraction.

### Summary Decision Matrix

| Question | Answer |
|----------|--------|
| Is $1.6B accurate for CERT 57565? | **YES** — technically correct for this legal entity |
| Is $1.6B the full UBS US CRE exposure? | **NO** — it represents ~8%-13% of total US exposure |
| Primary data gap? | **FFIEC 002** — UBS AG NY Branch books wholesale CRE |
| Should we adjust RC-C mapping? | **NO** — mapping is already comprehensive |
| Should we add FFIEC 002? | **YES** — critical for accurate cross-entity comparison |
| Should we add CMBS (RC-B)? | **OPTIONAL** — only if audit scope includes securities |

---

## Appendix A: UBS Entity Reference Table

| Entity | RSSD ID | CERT | Filing | Primary Regulator |
|--------|---------|------|--------|-------------------|
| UBS Group AG | — | — | Swiss FINMA | FINMA |
| UBS AG | 1951350 | — | Parent bank | OCC (US branch) |
| UBS Americas Holding LLC | 4846998 | — | FR Y-9C (IHC) | FRS |
| UBS Bank USA | 3212149 | 57565 | FFIEC 031 | OCC |
| UBS AG, New York Branch | ~4512 | — | FFIEC 002 | OCC |

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
