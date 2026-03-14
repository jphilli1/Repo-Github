# 06 — Normalization, Peer Groups & Data Enrichment

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

The normalized exclusion engine removes 7 categories from gross totals. Each category must have internally consistent balance, NCO, nonaccrual, and past-due exclusion mappings. NDFI credit-quality numerators are **not** directly isolated in Call Report-only mode and are set to zero rather than approximated.

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

Six excluded NCO categories (C&I, Credit Card, Auto, Ag, ADC, OO CRE) are balance-gated: if the excluded balance for a category is zero, the excluded NCO is forced to zero regardless of MDRM field values. This prevents misclassification propagation (e.g., MSPBNA showing $27K Auto NCO with zero Auto balance). Each gating decision produces a `_*_NCO_Gated` flag column. The `Exclusion_Component_Audit` sheet documents per-bank/quarter gating decisions, dominant exclusion categories, and flags where balance is zero but NCO was nonzero.

| Category | Balance Column | NCO Column | Flag Column |
|---|---|---|---|
| C&I | `Excl_CI_Balance` | `Excl_CI_NCO_YTD` | `_CI_NCO_Gated` |
| Credit Card | `Excl_CreditCard_Balance` | `Excl_CC_NCO_YTD` | `_CC_NCO_Gated` |
| Auto | `Excl_Auto_Balance` | `Excl_Auto_NCO_YTD` | `_Auto_NCO_Gated` |
| Ag | `Excl_Ag_Balance` | `Excl_Ag_NCO_YTD` | `_Ag_NCO_Gated` |
| ADC | `Excl_ADC_Balance` | `Excl_ADC_NCO_YTD` | `_ADC_NCO_Gated` |
| OO CRE | `Excl_OO_CRE_Balance` | `Excl_OO_CRE_NCO_YTD` | `_OO_CRE_NCO_Gated` |

## Structured Audit Flags

The following audit flags are persisted per row in the `Exclusion_Component_Audit` sheet:

| Flag | Meaning |
|---|---|
| `_audit_unsupported_ndfi_pdna` | Always True — NDFI PD/NA numerators set to 0.0 (no CR-only mapping) |
| `_audit_ndfi_nco_unsupported` | Always True — NDFI NCO set to 0.0 (no valid direct CR field) |
| `_audit_ag_pd_fallback_to_zero` | True when both P3AG and P9AG are zero (fields absent for this bank) |
| `_audit_resi_balance_fallback_used` | True when RC-C components were zero and LNRERES fallback was used |
| `category_balance_zero_but_nco_nonzero_flag` | True when any category had zero balance but nonzero raw NCO (gated to 0) |

## Normalized Composite Minimum Coverage

Normalized composites (90004/90006) must have ≥50% non-NaN contributor share per critical metric, otherwise the composite metric is NaN'd out. This prevents misleading composites when only 2 of 8 banks have usable normalized data. The `Composite_Coverage_Audit` sheet documents per-group/metric contributor counts, coverage percentages, and NaN-out decisions.

**Critical normalized metrics for coverage checks:**
- `Norm_NCO_Rate`, `Norm_Nonaccrual_Rate`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Gross_Loans`

**Important (2026-03-12 update)**: The primary cause of over-exclusion (RIAD4635/NTLS MDRM mapping errors) has been fixed. A ceiling constraint now caps `Excluded_NCO_TTM` at `Total_NCO_TTM`. Residual over-exclusion from minor mapping gaps is expected to be rare. If the ceiling constraint fires in production, investigate the specific exclusion category.

## Case-Shiller ZIP Enrichment

Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` env var (default `true`). When disabled, `build_case_shiller_zip_sheets()` returns audit with `SKIPPED` status. Uses county-level FIPS codes (S&P CoreLogic methodology) joined to HUD County-to-ZIP crosswalk (type=7) for exact geographic mapping. Maps 5-digit ZIP codes to 20 regional Case-Shiller metros via ~160 constituent counties.

---

## Normalized Segment Taxonomy (Loan-Segmentation Alignment)

The following rules govern the normalized segment definitions. They were aligned to Call Report schedule RC-C in the 2026-03-12 taxonomy cleanup.

| Segment | Balance Definition | Notes |
|---|---|---|
| **Wealth Resi** | RC-C components: open-end/revolving (1797) + closed-end first liens (5367) + closed-end junior liens (5368). Fallback: **LNRERES alone** (includes HELOC/open-end). LNRELOC is NOT added to avoid double-counting. | Numerators (NCO, PD, NA) still use split fields (NTRERES+NTRELOC, etc.). `Wealth_Resi_TTM_NCO_Rate` uses `RIC_Resi_NCO_TTM` (true TTM, not raw YTD). |
| **C&I Exclusion** | `Excl_CI_Balance = LNCI` (or best_of RCON1763/RCFD1763) directly. SBL is NOT subtracted — SBL lives under RC-C item 9, not item 4. | |
| **NDFI / Fund Finance** | J454 retained for balance exclusion. **J458/J459/J460 removed** — not Call Report-consistent for PD/NA. Segment-level PD/NA set to **NaN** (coverage gap). Excluded NDFI PD/NA set to **0.0** (not subtracted). `_NDFI_PD_NA_NotIsolatable = True` audit flag added. | LIMITATION: CR-only PD/NA mapping is unsupported for NDFI. RC-N item 7 aggregates RC-C item 9 categories. |
| **Agricultural NCO** | Charge-offs (RIAD4655) minus recoveries (RIAD4665) = ag production loan NCOs (RI-B Item 3). NTAG/NTREAG as fallback. | RIAD4635/4645 were total-portfolio items, not ag-specific. |
| **Agricultural PD** | Primary: RCON1594/RCFD1594 (30-89 days) and RCON1597/RCFD1597 (90+ days). Fallback: P3AG/P9AG/P3AGR/P9AGR. | RCON2746/2747 were "All other loans" PD, not agricultural. |
| **ADC NCO** | Charge-offs (RIAD3582) minus recoveries (RIAD3583) = construction & land development loan NCOs (RI-B Item 1.a). NTRECONS as fallback. | NTLS/RIAD4658/4659 were total-portfolio items, not construction-specific. |
| **CRE Investment Pure** | `LNREMULT + LNRENROT` only. **LNREOTH removed** from the calculation path. | Owner-occupied CRE excluded separately. |
| **Tailored Lending** | **Unsupported** in Call Report-only mode. No proxy from fine art, aircraft, bespoke HNW unsecured, J451, or other CR proxies. Internal product tags would be required. | |

### Known Limitations (Segment Taxonomy)

- **NDFI credit quality**: Call Report does not publish NDFI-specific delinquency/nonaccrual. J458/J459/J460 were removed (those are unused commitments, not PD/NA items). RC-N item 7 aggregates loans in RC-C item 9 (including J454 and 1545) without NDFI-only disaggregation. Segment-level PD/NA are NaN (coverage gap). Excluded NDFI PD/NA are 0.0 (not subtracted from normalization). `_NDFI_PD_NA_NotIsolatable = True` audit flag is set. To implement NDFI PD/NA, optional memorandum-item logic would be needed.
- **Tailored Lending**: Cannot be inferred from Call Report data. Requires internal product tags. No proxy math is present in this codebase.
- **Ag PD fallback**: If P3AG/P9AG are absent from the FDIC dataset for a given bank, the exclusion PD defaults to 0.0 via `best_of(...).fillna(0)`. This is preferred over mis-mapping RCON2746/2747.

### Segment Support Boundaries

This table documents what the Call Report can and cannot identify for each wealth-management loan segment. Presentation-facing labels must not overstate the precision available.

| Segment | Balance | Risk Metrics (NCO/PD/NA) | Boundary Notes |
|---|---|---|---|
| **SBL** | Supported (RCFD1545/RCON1545, fallback LNOTHER) | **Proxy only** — no SBL-specific NCO, PD, or NA fields in Call Report. "All Other Loans" numerators would mix SBL with other uncategorised lending. Not shown in presentation. | SBL balance is clean; SBL risk rates are NOT computed. |
| **Wealth Resi** | Supported (1-4 family incl. HELOC via LNRERES) | Supported via split MDRM fields (NTRERES+NTRELOC, P3RERES+P3RELOC, etc.) | Jumbo is **not** separately identifiable in Call Report. The segment covers all 1-4 family residential including HELOCs and open-end lines. |
| **CRE Investment** | Supported (LNREMULT + LNRENROT) | Supported (NTREMULT+NTRENROT, P3REMULT+P3RENROT, P9REMULT+P9RENROT, NAREMULT+NARENROT) | Multifamily + non-owner-occupied nonfarm only. Excludes ADC (construction) and owner-occupied CRE, which are separate segments. |
| **Tailored Lending** | **Not segmented** in Call Report | **Not available** | Fine art, aircraft, bespoke HNW unsecured, etc. cannot be identified from any Call Report schedule. Requires internal product-level tags. J451 (total to nondepository institutions) is NOT a tailored-lending proxy. |
| **NDFI / Fund Finance** | Supported (J454 balance only) | **Not available** — all set to 0.0 with audit flags | CR-only mode has no NDFI-specific NCO/PD/NA fields. |

---

## Peer Groupings

There are **4 peer groups** (2 standard + 2 normalized). Composite CERTs are assigned via `base_dummy_cert + display_order`:

| Table Type | Peer 1 | Peer 2 |
|---|---|---|
| **Standard** | 90001 — Core PB | 90003 — All Peers |
| **Normalized** | 90004 — Core PB Norm | 90006 — All Peers Norm |

The former MSPBNA+Wealth groups (90002/90005) were removed as duplicate cert membership. `validate_peer_group_uniqueness()` enforces that no two groups sharing the same `use_normalized` flag may have identical sorted cert lists.

**Cross-mode duplication by design**: 90001/90004 and 90003/90006 share identical member CERTs. The distinction is `use_normalized`: standard composites NaN-out `Norm_*` columns; normalized composites NaN-out standard rate columns. Hard validation (`validate_peer_group_uniqueness()`) ensures no two groups within the SAME `use_normalized` mode share identical cert lists.

**Peer_Group_Definitions sheet**: A new Excel sheet documents all 4 peer group definitions with member CERTs, use cases, and display order.

---

## Metric Registry & Validation Architecture

### Overview

Derived metrics are now formally registered in `metric_registry.py` using a `MetricSpec` dataclass. Each spec declares:

| Field | Purpose |
|---|---|
| `code` | Column name in the DataFrame (e.g., `Risk_Adj_Allowance_Coverage`) |
| `dependencies` | List of upstream columns required to compute this metric |
| `compute` | A lambda that recomputes the metric from raw columns (used for validation) |
| `unit` | Semantic type: `fraction`, `dollars`, `count`, `multiple`, `years` |
| `min_value` / `max_value` | Optional sanity bounds |
| `allow_negative` | Whether negative values are valid (e.g., NCO rates can go negative) |
| `consumers` | List of downstream report artifacts (charts/tables) that depend on this metric |
| `severity` | `high`, `medium`, or `low` — controls alerting priority |

### Report Consumer Map

The `REPORT_CONSUMER_MAP` dict (auto-built from specs) maps each metric code to its downstream consumers. This lets `report_generator.py` know which charts/tables are affected when a metric fails validation.

### Validation Engine

`run_upstream_validation_suite(df)` is called in `MSPBNA_CR_Normalized.py` just before the Excel write (after normalization diagnostics and composite coverage audit). For each registered metric, it:

1. **Recomputes** the metric from its declared formula (`spec.compute(df)`)
2. **Compares** the recomputed value against the stored column value
3. **Checks bounds** (min/max) and sign constraints
4. **Tags** each row with `CERT` and `REPDTE` for row-level tracing

Results are exported to the `Metric_Validation_Audit` sheet in the Excel dashboard. The call is wrapped in try/except — if the suite fails, the pipeline continues with an empty audit sheet.

### Dependency Graph

`build_reverse_dependency_map()` returns `{upstream_col: [derived_metrics]}`, enabling impact analysis: if an upstream column (e.g., `Gross_Loans`) is missing or corrupt, the graph shows exactly which derived metrics and downstream reports are affected.

### Semantic Validation Rules (A-E)

Beyond formula recomputation, `metric_registry.py` now includes 5 semantic validation rules via `run_semantic_validation(df)`:

| Rule | ID | Description |
|---|---|---|
| Over-Exclusion | A | Flags rows where `Excluded_*` exceeds `Total_*` |
| Flatline Anomaly | B | Flags normalized metrics that are constant across all entities in latest quarter |
| Duplicate Composite | C | Flags standard/normalized composites producing identical metric values |
| Output Contamination | D | Flags composite CERTs with metric values they shouldn't have (e.g., std composite with non-null Norm_* rates) |
| Consumer Linkage | E | Flags metrics with no declared downstream consumers (orphans) |

`run_full_validation_suite(df)` returns both the formula validation report and the semantic validation report.

### Normalization Diagnostics

The normalization pipeline no longer uses silent `.clip(lower=0)`. Instead:

| Column | Purpose |
|---|---|
| `_Norm_NCO_Residual` / `_Norm_NA_Residual` / `_Norm_Loans_Residual` | Raw subtraction residual (can be negative) |
| `_Flag_NCO_OverExclusion` / `_Flag_NA_OverExclusion` / `_Flag_Loans_OverExclusion` | Boolean (1 = exclusion > total) |
| `_*_OverExclusion_Pct` | Residual as % of total |
| `_Norm_*_Severity` | `ok` / `minor_clip` (< 5% over, clipped to 0) / `material_nan` (>= 5% over, set to NaN) |

### Preflight Validation

`validate_output_inputs()` in `report_generator.py` runs before any chart/table generation:
1. Checks subject bank CERT exists in data
2. Flags all-NaN or all-zero normalized metrics
3. **Blocks** on material over-exclusion severity for subject bank AND plotted peer-average composites (90004, 90006) — **scoped to latest plotted period only**. Historical material failures are logged as informational warnings but do NOT block.
4. **Blocks** if required composite CERTs are missing from data:
   - Standard: 90003 (All Peers), 90001 (Core PB)
   - Normalized: 90006 (All Peers Norm), 90004 (Core PB Norm)
5. Validates real peers exist for scatter plots
6. Runs semantic validation if metric_registry is available

**Period scoping**: The preflight determines `latest_repdte = proc_df["REPDTE"].max()` and only evaluates blocking severity checks against that single period. This prevents historical data-quality issues (which may have been present before balance-gating or upstream fixes) from blocking current-period report generation.

Suppressed charts use readable artifact names: `normalized_credit_chart`, `normalized_scatter_nco_vs_nonaccrual`, `standard_scatter_nco_vs_npl`, `standard_scatter_pd_vs_npl`.

---

## HUD USPS ZIP Crosswalk — Case-Shiller County-Level Enrichment

### Overview

`case_shiller_zip_mapper.py` maps the 20 regional Case-Shiller metro indexes to ZIP codes using **county-level FIPS codes** (per S&P CoreLogic methodology) joined to the HUD USPS County-to-ZIP Crosswalk API (type=7). This is an **internal reference mapping layer** — it does not alter FRED data or FDIC metrics. Its purpose is to enable downstream loan-level geographic tagging against Case-Shiller regions.

**Explicitly excluded**: U.S. National, Composite-10, and Composite-20 indexes (these have no geographic footprint to map).

### County-Level FIPS Mapping (S&P CoreLogic)

The `CASE_SHILLER_COUNTY_MAP` list contains the official county-level definitions from the S&P Case-Shiller "Index Geography" table. Each entry maps a 5-digit US FIPS county code to a Case-Shiller region. This replaces the former CBSA/CBSA-Division approximation approach with exact county-level definitions as specified by S&P.

**Key statistics:** ~160 counties across 20 metros, spanning 17 states + DC.

### HUD API Token Setup (One-Time)

1. Register at https://www.huduser.gov/hudapi/public/register
2. Receive your access token via email
3. Set the environment variable:
   ```bash
   export HUD_USER_TOKEN='eyJ...'
   ```
   Or add to `.env` in the script directory: `HUD_USER_TOKEN=eyJ...`
4. The token is a Bearer token used in the `Authorization` header

**Token discovery** uses `resolve_hud_token()` from `case_shiller_zip_mapper.py` with multi-source resolution: explicit argument -> `os.getenv` -> `.env` in script dir -> `.env` in cwd. The resolver returns `(token, diagnostics)` — diagnostics include `source_used`, masked prefix, paths checked, cwd, executable, and PID. The full token is never logged.

### HUD Response Parsing & Flattening (Two-Pass)

The HUD API response requires **two passes** to reach actual crosswalk data rows:

**Pass 1 — Top-level extraction** (`extract_hud_result_rows()`):

| Shape | Payload Structure | Extraction Path |
|---|---|---|
| A | `[row, row, ...]` | Direct — payload is already the row list |
| B | `{"results": [row, ...]}` | `payload["results"]` |
| C | `{"results": {"rows": [row, ...]}}` | `payload["results"]["rows"]` |
| D | `{"results": {"data": [row, ...]}}` | `payload["results"]["data"]` |
| E | `{"data": [row, ...]}` | `payload["data"]` |
| F | `{"data": {"results": [row, ...]}}` | Recursive dict search |

**Pass 2 — Wrapper row flattening** (`flatten_hud_rows()`):

The HUD county-to-ZIP API (type=7) returns Shape B where each "row" is actually a **wrapper object** with metadata fields and a nested `results` list containing the actual crosswalk rows. `flatten_hud_rows()` processes **each row independently**. Any row with a list-valued `results` key is a wrapper — it is exploded into its child rows with parent metadata propagated (child keys take precedence).

### Canonical HUD Crosswalk Fields

After canonicalization, all downstream code uses these standard column names:

| Canonical Name | Variants Accepted |
|---|---|
| `zip` | `zip`, `zip_code`, `zipcode`, `zip5`, `results.zip`, `results.zip_code` |
| `county_fips` | `county`, `county_fips`, `geoid`, `county_geoid`, `countyfips`, `fips`, `results.county`, `results.county_fips` |
| `res_ratio` | `res_ratio`, `residential_ratio`, `results.res_ratio` |
| `bus_ratio` | `bus_ratio`, `business_ratio`, `results.bus_ratio` |
| `oth_ratio` | `oth_ratio`, `other_ratio`, `results.oth_ratio` |
| `tot_ratio` | `tot_ratio`, `total_ratio` |

ZIP codes are zero-padded to 5 chars. County FIPS are zero-padded to 5 chars.

### Enrichment Status Codes

| Status | Meaning |
|---|---|
| `SKIPPED_DISABLED` | `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT=false` |
| `SKIPPED_NO_TOKEN` | Token not visible to current Python process |
| `SKIPPED_NO_REQUESTS` | `requests` library not installed |
| `FAILED_TOKEN_AUTH` | HTTP 401/403 from HUD API |
| `FAILED_HTTP` | Non-auth HTTP failures |
| `FAILED_PARSE` | HTTP success but response could not be flattened to usable ZIP/FIPS columns |
| `FAILED_EMPTY_RESPONSE` | API responded but all counties returned empty |
| `SUCCESS_NO_MATCHES` | HUD returned rows but none matched S&P county FIPS codes |
| `SUCCESS_NO_ZIPS` | Enrichment ran but produced zero ZIP rows |
| `SUCCESS_WITH_ZIPS` | Normal success |

### HUD API: County-to-ZIP (Type 7)

The HUD USPS Crosswalk API is called with `type=7` (county-to-ZIP) for each unique FIPS code in `CASE_SHILLER_COUNTY_MAP`. Results are joined strictly on 5-digit FIPS code.

**Request hardening**: All HUD requests use a `requests.Session()` with `Accept: application/json` and `User-Agent` headers. HTTP status codes are classified via `_classify_http_status()` into query-level failure constants.

**Misclassification prevention**: The orchestrator tracks `county_diagnostics` for every FIPS fetch. When all county requests fail HTTP, the enrichment status is `FAILED_HTTP` — it will NOT drift to `FAILED_EMPTY_RESPONSE`, `SUCCESS_NO_MATCHES`, or `SUCCESS_NO_ZIPS`.

**Smoke test**: `run_hud_smoke_test(fips_code, token)` runs a single HUD request for local debugging.

### Validation (7 Checks)

1. No non-metro regions leak into coverage
2. No blank ZIP codes
3. All ZIPs are 5-character zero-padded strings
4. Summary ZIP counts reconcile with detail rows
5. All FIPS codes in county map are valid 5-digit strings
6. All 20 metros have at least one ZIP row
7. HUD ratio columns are not entirely null
