# 06 — Normalization & Peer Groups

## Peer Groupings

There are **4 peer groups** (2 standard + 2 normalized). Composite CERTs are assigned via `base_dummy_cert + display_order`:

| Table Type | Peer 1 | Peer 2 |
|---|---|---|
| **Standard** | 90001 — Core PB | 90003 — All Peers |
| **Normalized** | 90004 — Core PB Norm | 90006 — All Peers Norm |

The former MSPBNA+Wealth groups (90002/90005) were removed as duplicate cert membership. `validate_peer_group_uniqueness()` enforces that no two groups sharing the same `use_normalized` flag may have identical sorted cert lists.

**Subject bank excluded from wealth composites**: Core PB composites (90001/90004) average **external peers only** (GS + UBS). MSPBNA is NOT a member of its own peer group — including it would make "Delta vs Wealth Peers" self-inclusive and understate the delta. All Peers composites (90003/90006) similarly exclude MSPBNA (they use MSBNA + G-SIBs).

**Cross-mode duplication by design**: 90001/90004 and 90003/90006 share identical member CERTs. The distinction is `use_normalized`: standard composites NaN-out `Norm_*` columns; normalized composites NaN-out standard rate columns. Hard validation (`validate_peer_group_uniqueness()`) ensures no two groups within the SAME `use_normalized` mode share identical cert lists.

**Peer_Group_Definitions sheet**: An Excel sheet documents all 4 peer group definitions with member CERTs, use cases, and display order.

## MS Combined Entity

- CERT `88888` (MS Combined Entity) must be **filtered out** of HTML table listings when `REPORT_VIEW == "MSPBNA_WEALTH_NORM"`.
- Load via `MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))`.

## Scatter & Chart Composite Handling

- **Active composite regime** is defined at the top of `report_generator.py`:
  - `ACTIVE_STANDARD_COMPOSITES = {"core_pb": 90001, "all_peers": 90003}`
  - `ACTIVE_NORMALIZED_COMPOSITES = {"core_pb": 90004, "all_peers": 90006}`
  - `INACTIVE_LEGACY_COMPOSITES = {90002, 90005, 99998, 99999}`
- All chart/table builders MUST use these canonical constants. **Never** hardcode 99998, 99999, 90002, or 90005 as peer-average selectors.
- `ALL_COMPOSITE_CERTS` includes both active and legacy CERTs for scatter-dot exclusion only.
- `plot_scatter_dynamic()` defaults: `peer_avg_cert_primary=90003, peer_avg_cert_alt=90001`. Normalized call sites pass `90006/90004` explicitly.
- If an active composite is missing from data, the chart/table MUST skip (not silently substitute a legacy CERT). Use `validate_composite_cert_regime()` for preflight checks.
- The former `build_plot_df_with_alias()` function has been removed — it appended duplicate rows that contaminated scatter plots. Do not re-introduce it.

## Coverage vs Share vs x-Multiple Label Rule (Non-Negotiable)

Every ratio-component row label **must** match its denominator type:

| Denominator Type | Correct Label Term | Display Format | Examples |
|---|---|---|---|
| Exposure / loan base (Gross_Loans, RIC_CRE_Cost, Wealth_Resi_Balance, CRE_Investment_Pure_Balance) | **"Coverage"** or **"Ratio"** | % | `RIC_CRE_ACL / RIC_CRE_Cost` → "CRE ACL Coverage" |
| ACL pool (Total_ACL, Norm_ACL_Balance) | **"Share"** or **"% of ACL"** | % | `RIC_CRE_ACL / Total_ACL` → "CRE % of ACL" |
| Nonaccrual / NPL base (RIC_CRE_Nonaccrual, RIC_Resi_Nonaccrual) | **"NPL Coverage"** | x-multiple | `RIC_CRE_ACL / RIC_CRE_Nonaccrual` → "CRE NPL Coverage" (1.23x) |

**If denominator is `Total_ACL` or `Norm_ACL_Balance`, the label must NEVER contain "Coverage".**

## `_METRIC_FORMAT_TYPE` Maintenance Rule

`_METRIC_FORMAT_TYPE` in `report_generator.py` is the **explicit registry** for x-multiple formatted metrics. Rules:

1. Only **NPL coverage** metrics (denominator = nonaccrual or past-due) belong in this dict.
2. Any **new** NPL coverage metric MUST be added here explicitly — there is no auto-detection.
3. Loan-coverage and share-of-ACL metrics must **NEVER** be added — they default to percent.
4. Current entries: `RIC_CRE_Risk_Adj_Coverage`, `RIC_Resi_Risk_Adj_Coverage`, `RIC_Comm_Risk_Adj_Coverage`.

---

## Top-Down Normalization with Over-Exclusion Detection

Normalized metrics use `calc_normalized_residual(total, excluded, label, tolerance_pct=0.05)` which returns:
- `final_value`: residual (total - excluded), with minor over-exclusions (<=5%) clipped to 0 and material ones set to NaN
- `severity`: one of `ok`, `minor_clip`, `material_nan`
- 15 diagnostics columns are written to the `Normalization_Diagnostics` sheet

## Normalized Ratio Components

`generate_ratio_components_table(is_normalized=True)` uses:
- **Delinquency numerator**: `_Norm_Total_Past_Due` (synthesized as `Norm_PD30 + Norm_PD90`)
- **ACL numerator**: `Norm_ACL_Balance` (not `Total_ACL`)
- **Risk-adjusted denominator**: `Norm_Risk_Adj_Gross_Loans` (= `Norm_Gross_Loans - SBL_Balance`)
- **Resi ACL Coverage**: `RIC_Resi_ACL / Wealth_Resi_Balance`

## Supported Exclusion Stack (Normalized Universe)

The normalized exclusion engine removes 7 categories from gross totals. Each category must have internally consistent balance, NCO, nonaccrual, and past-due exclusion mappings.

| Category | Balance | NCO (YTD) | Nonaccrual | PD30 | PD90 | Notes |
|---|---|---|---|---|---|---|
| **C&I** | LNCI | NTCI/RIAD4638 (net) | NACI | P3CI | P9CI | Balance-gated NCO |
| **NDFI** | J454 | **0.0** | **0.0** | **0.0** | **0.0** | Balance only; all risk numerators unsupported |
| **ADC** | LNRECONS | NTLS/RIAD4658-4659 (net) | NARECONS | P3RECONS | P9RECONS | Balance-gated NCO |
| **Credit Card** | RCFDB538 | RIADB514-B515 (net) | RCFDB575 | P3CRCD | P9CRCD | Balance-gated NCO |
| **Auto** | LNAUTO | RIADK205-K206 (net) | RCFDK213 | P3AUTO | P9AUTO | Balance-gated NCO |
| **Ag** | LNAG | RIAD4635-4645 (net) | RCFD5341 | P3AG | P9AG | Balance-gated NCO; PD audit-flagged if absent |
| **OO CRE** | LNRENROW | NTRENROW (net) | NARENROW | P3RENROW | P9RENROW | Balance-gated NCO |

## Balance-Gating for Excluded NCO Categories

Six excluded NCO categories (C&I, Credit Card, Auto, Ag, ADC, OO CRE) are balance-gated: if the excluded balance for a category is zero, the excluded NCO is forced to zero regardless of MDRM field values.

| Category | Balance Column | NCO Column | Flag Column |
|---|---|---|---|
| C&I | `Excl_CI_Balance` | `Excl_CI_NCO_YTD` | `_CI_NCO_Gated` |
| Credit Card | `Excl_CreditCard_Balance` | `Excl_CC_NCO_YTD` | `_CC_NCO_Gated` |
| Auto | `Excl_Auto_Balance` | `Excl_Auto_NCO_YTD` | `_Auto_NCO_Gated` |
| Ag | `Excl_Ag_Balance` | `Excl_Ag_NCO_YTD` | `_Ag_NCO_Gated` |
| ADC | `Excl_ADC_Balance` | `Excl_ADC_NCO_YTD` | `_ADC_NCO_Gated` |
| OO CRE | `Excl_OO_CRE_Balance` | `Excl_OO_CRE_NCO_YTD` | `_OO_CRE_NCO_Gated` |

**NCO ceiling constraint**: `Excluded_NCO_TTM` is capped at `Total_NCO_TTM`. If the ceiling fires in production, investigate the specific exclusion category.

## Structured Audit Flags

| Flag | Meaning |
|---|---|
| `_audit_unsupported_ndfi_pdna` | Always True — NDFI PD/NA numerators set to 0.0 (no CR-only mapping) |
| `_audit_ndfi_nco_unsupported` | Always True — NDFI NCO set to 0.0 (no valid direct CR field) |
| `_audit_ag_pd_fallback_to_zero` | True when both P3AG and P9AG are zero (fields absent for this bank) |
| `_audit_resi_balance_fallback_used` | True when RC-C components were zero and LNRERES fallback was used |
| `category_balance_zero_but_nco_nonzero_flag` | True when any category had zero balance but nonzero raw NCO (gated to 0) |

## Normalized Composite Minimum Coverage

Normalized composites (90004/90006) must have >= 50% non-NaN contributor share per critical metric, otherwise the composite metric is NaN'd out. The `Composite_Coverage_Audit` sheet documents per-group/metric contributor counts, coverage percentages, and NaN-out decisions.

**Critical normalized metrics for coverage checks:**
- `Norm_NCO_Rate`, `Norm_Nonaccrual_Rate`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Gross_Loans`

---

## Normalized Segment Taxonomy (Loan-Segmentation Alignment)

Aligned to Call Report schedule RC-C.

| Segment | Balance Definition | Notes |
|---|---|---|
| **Wealth Resi** | RC-C components: open-end/revolving (1797) + closed-end first liens (5367) + closed-end junior liens (5368). Fallback: **LNRERES alone** (includes HELOC/open-end). LNRELOC is NOT added to avoid double-counting. | Numerators (NCO, PD, NA) still use split fields (NTRERES+NTRELOC, etc.). `Wealth_Resi_TTM_NCO_Rate` uses `RIC_Resi_NCO_TTM` (true TTM, not raw YTD). |
| **C&I Exclusion** | `Excl_CI_Balance = LNCI` (or best_of RCON1763/RCFD1763) directly. SBL is NOT subtracted — SBL lives under RC-C item 9, not item 4. | |
| **NDFI / Fund Finance** | J454 retained for balance exclusion. **J458/J459/J460 removed** — not Call Report-consistent for PD/NA. Segment-level PD/NA set to **NaN** (coverage gap). Excluded NDFI PD/NA set to **0.0** (not subtracted). `_NDFI_PD_NA_NotIsolatable = True` audit flag added. | LIMITATION: CR-only PD/NA mapping is unsupported for NDFI. |
| **Agricultural NCO** | Charge-offs (RIAD4655) minus recoveries (RIAD4665) = ag production loan NCOs (RI-B Item 3). NTAG/NTREAG as fallback. | RIAD4635/4645 were total-portfolio items, not ag-specific. |
| **Agricultural PD** | Primary: RCON1594/RCFD1594 (30-89 days) and RCON1597/RCFD1597 (90+ days). Fallback: P3AG/P9AG/P3AGR/P9AGR. | RCON2746/2747 were "All other loans" PD, not agricultural. |
| **ADC NCO** | Charge-offs (RIAD3582) minus recoveries (RIAD3583) = construction & land development loan NCOs (RI-B Item 1.a). NTRECONS as fallback. | NTLS/RIAD4658/4659 were total-portfolio items, not construction-specific. |
| **CRE Investment Pure** | `LNREMULT + LNRENROT` only. **LNREOTH removed** from the calculation path. | Owner-occupied CRE excluded separately. |
| **Tailored Lending** | **Unsupported** in Call Report-only mode. No proxy from fine art, aircraft, bespoke HNW unsecured, J451, or other CR proxies. Internal product tags would be required. | |

## Known Limitations (Segment Taxonomy)

- **NDFI credit quality**: Call Report does not publish NDFI-specific delinquency/nonaccrual. J458/J459/J460 were removed. RC-N item 7 aggregates loans in RC-C item 9 without NDFI-only disaggregation. Segment-level PD/NA are NaN (coverage gap). Excluded NDFI PD/NA are 0.0 (not subtracted from normalization).
- **Tailored Lending**: Cannot be inferred from Call Report data. Requires internal product tags. No proxy math is present.
- **Ag PD fallback**: If P3AG/P9AG are absent, the exclusion PD defaults to 0.0 via `best_of(...).fillna(0)`.

## Segment Support Boundaries

| Segment | Balance | Risk Metrics (NCO/PD/NA) | Boundary Notes |
|---|---|---|---|
| **SBL** | Supported (RCFD1545/RCON1545, fallback LNOTHER) | **Proxy only** — no SBL-specific NCO, PD, or NA fields in Call Report. | SBL balance is clean; SBL risk rates are NOT computed. |
| **Wealth Resi** | Supported (1-4 family incl. HELOC via LNRERES) | Supported via split MDRM fields | Jumbo is **not** separately identifiable in Call Report. |
| **CRE Investment** | Supported (LNREMULT + LNRENROT) | Supported (NTREMULT+NTRENROT, etc.) | Multifamily + non-owner-occupied nonfarm only. |
| **Tailored Lending** | **Not segmented** in Call Report | **Not available** | Requires internal product-level tags. |
| **NDFI / Fund Finance** | Supported (J454 balance only) | **Not available** — all set to 0.0 with audit flags | CR-only mode has no NDFI-specific NCO/PD/NA fields. |

---

## Additional Normalization Conventions

### Case-Shiller ZIP Enrichment

Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` env var (default `true`). When disabled, `build_case_shiller_zip_sheets()` returns audit with `SKIPPED` status. See `@docs/claude/10-coding-rules.md` for detailed HUD/Case-Shiller architecture.

### IDB Label Convention

Dictionary keys in `master_data_dictionary.py` must **never** use the `IDB_` prefix. All former `IDB_*` keys have been renamed. User-facing labels, CSS classes, and HTML headers must reference **MSPBNA**, never **IDB**.

### Curated Presentation Tabs

`Summary_Dashboard` and `Normalized_Comparison` use curated metric allowlists (`SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS` in `MSPBNA_CR_Normalized.py`). Only approved KPIs appear in presentation-facing tabs. Raw MDRM fields and internal pipeline columns are excluded. The full dataset remains available in the `FDIC_Data` sheet.

### Display Label Policy

All presentation tabs use `_get_metric_short_name(code)` to resolve display names from `FDIC_Metric_Descriptions` (via `MasterDataDictionary`). Columns: `Metric Code` (technical field name) + `Metric Name` (display label). Falls back to the code itself when no display label exists.

### Metric Role Classification

Metrics are classified as **evaluative** (risk/return/coverage — receives performance flags) or **descriptive** (size/balance/composition — no evaluative flags). `DESCRIPTIVE_METRICS` frozenset in `MSPBNA_CR_Normalized.py` lists all descriptive metrics. `_get_performance_flag()` returns blank for descriptive metrics.

### Norm_Provision_Rate Treatment

`Norm_Provision_Rate` is intentionally set to NaN — provision expense (`ELNATR`) is not segment-specific in call reports. It is excluded from `NORMALIZED_COMPARISON_METRICS` and should never be presented as a normal KPI.
