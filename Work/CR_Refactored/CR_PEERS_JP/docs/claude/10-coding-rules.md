# 10 — Coding Rules & Technical Architecture

## Strict Coding Conventions (Non-Negotiable)

### ALWAYS UPDATE DOCS

Whenever you make architectural changes, add new charts, or change data pipelines, you **must** update the relevant `docs/claude/` files and `@docs/claude/99-changelog.md`.

### FDIC API Variables & FFIEC Waterfall

Always prefer FDIC top-level text aliases (e.g., `ASSET`, `NTCI`) over raw MDRM codes (`RCFD...`, `RIAD...`). When raw codes are necessary, you **MUST** fetch both the `RCFD` (Consolidated) and `RCON` (Domestic) codes. You must then coalesce them (`RCFD.fillna(RCON)`) to ensure apples-to-apples comparisons between international G-SIBs (FFIEC 031) and domestic-only banks (FFIEC 041/051). Never force internationally active banks to strictly use `RCON`, as this strips out their foreign office balances. Synthetic local variables must never be included in the raw API fetch request.

### Deprecated Functions

`generate_normalized_comparison_table()` in `report_generator.py` is **deprecated** — raises `NotImplementedError`. It produced a mixed standard-vs-normalized artifact violating the single-regime-per-artifact rule. Use the separate standard/normalized table generators instead.

### HUD Token — Attribute-Only Access

`DashboardConfig` is a plain Python class (not a dict). HUD token access rules:

- **Field**: `hud_user_token: Optional[str] = None` on `DashboardConfig`
- **Write**: `config.hud_user_token = _hud_token` (attribute assignment, **never** `config["..."]`)
- **Read**: `getattr(self.config, "hud_user_token", None)` (safe attribute access, **never** `.get()`)
- **Pass**: `build_case_shiller_zip_sheets(hud_user_token=self.config.hud_user_token)` (explicit kwarg)
- **Resolve**: Single call to `resolve_hud_token()` in `_validate_runtime_env()` — no competing resolution paths

Dict-style access (`config["key"]`, `config.get("key")`) will raise `TypeError` on `DashboardConfig` and is forbidden.

### Centralized Chart Palette (CHART_PALETTE)

All charts **must** use the centralized `CHART_PALETTE` dict and its convenience aliases (`_C_MSPBNA`, `_C_WEALTH`, `_C_ALL_PEERS`, `_C_PEER_CLOUD`, `_C_GUIDE`, etc.) defined at the top of `report_generator.py`. No chart should use arbitrary per-chart color choices.

| Key | Hex Color | Entity / Usage |
|---|---|---|
| `mspbna` | `#F7A81B` | Gold — Subject bank (MSPBNA) |
| `wealth_peers` | `#7B2D8E` | Purple — Wealth Peers / Core PB composite |
| `all_peers` | `#5B9BD5` | Blue — All Peers composite |
| `peer_cloud` | `#8FA8C8` | Muted blue-gray — individual peer dots in scatter |
| `guide` | `#6B7B8D` | Slate-gray — averages, crosshair guides, reference lines |
| `range_all` | `#D0D0D0` | Light gray — range bands (All Peers) |
| `range_wealth` | `#B8A0C8` | Muted purple-gray — range bands (Wealth Peers) |
| `text` | `#2B2B2B` | Dark — titles, axis labels |
| `grid` | `#D0D0D0` | Grid lines |

### Chart Annotation / Anti-Overlap Policy

`ChartAnnotationHelper` in `report_generator.py` provides a shared label placement engine:

**Priority tiers:**
1. **MSPBNA** — always labeled (TIER_SUBJECT = 1)
2. **Wealth Peers** — always labeled when plotted (TIER_WEALTH = 2)
3. **All Peers** — labeled only when explicitly plotted (TIER_ALL_PEERS = 3)
4. **Individual peers** — labeled only for outliers, edge-of-range, or specifically selected (TIER_INDIVIDUAL = 4)

**Rules:**
- Use ticker abbreviations only: MSPBNA, GS, UBS, C, JPM, BAC, WFC, MS
- Endpoint labels preferred over many internal labels on time-series charts
- Value labels on bar/line combos should be sparse and strategic
- Leader lines allowed on scatter plots for displaced labels
- Directional nudges used to avoid collisions; higher-priority labels get first placement

### Wealth Peers Comparator Inclusion Policy

Wealth Peers must appear as an explicit styled point/line/marker in these charts:
- `scatter_nco_vs_npl` — triangle marker, purple
- `scatter_pd_vs_npl` — triangle marker, purple
- `scatter_norm_nco_vs_nonaccrual` — triangle marker, purple
- `risk_adjusted_return` — bubble marker, purple
- `years_of_reserves` — triangle marker, purple
- `growth_vs_deterioration` — triangle marker, purple
- `concentration_vs_capital` — triangle marker, purple

All Peers remains as a reference marker/crosshair where appropriate. Do NOT clutter subject-only operational overlays (e.g., `portfolio_mix`, `liquidity_overlay`).

### CSS Class Naming

- Use `mspbna-row` and `mspbna-value` (not `idb-row` / `idb-value`).
- All user-facing labels, HTML headers, and filenames must reference **MSPBNA**, never **IDB**.

### Entity Display Label Policy

All table headers, chart legends, and HTML output must use **`resolve_display_label(cert, name)`** from `report_generator.py`. Never hardcode full bank names.

| Entity | Display Label | Rule |
|---|---|---|
| Subject bank (CERT 34221) | `MSPBNA` | Subject cert → fixed label |
| MSBNA (CERT 32992) | `MSBNA` | Secondary cert → fixed label |
| Goldman Sachs | `GS` | `_TICKER_MAP` lookup |
| UBS | `UBS` | `_TICKER_MAP` lookup |
| JPMorgan Chase | `JPM` | `_TICKER_MAP` lookup |
| Bank of America | `BAC` | `_TICKER_MAP` lookup |
| Citibank | `C` | `_TICKER_MAP` lookup |
| Wells Fargo | `WFC` | `_TICKER_MAP` lookup |
| Core PB composites (90001/90004) | `Wealth Peers` | `_COMPOSITE_LABELS` lookup |
| All Peers composites (90003/90006) | `All Peers` | `_COMPOSITE_LABELS` lookup |
| MS Combined (88888) | `MS Combined` | `_COMPOSITE_LABELS` lookup |

**Fallback**: Strip "National Association" / "N.A." suffixes and title-case. Last resort: `"CERT {cert}"`.

### Chart Metrics

**Standard chart:** Bar = `TTM_NCO_Rate`, Line = `NPL_to_Gross_Loans_Rate`
**Normalized chart:** Bar = `Norm_NCO_Rate`, Line = `Norm_Nonaccrual_Rate` (fallback: `Norm_NPL_to_Gross_Loans_Rate`)

Both standard and normalized credit-deterioration charts are generated.

### FRED Deduplication

- `series_ids` must always be deduplicated before async fetching to prevent `ValueError: cannot reindex on an axis with duplicate labels`.
- Use `list(dict.fromkeys(series_ids))` to preserve order while removing duplicates.
- The guard must exist both at construction time and at the entry point of `fetch_all_series_async()`.

### FRED Frequency Inference

- `infer_freq_from_index(idx)` infers series frequency from a `DatetimeIndex` when FRED metadata is missing.
- The helper is heuristic and fail-safe: tries `pd.infer_freq()`, then median-obs-per-year, then distinct-months-per-year, falls back to `("quarterly", "Q")`.
- **Never** use `Series.groupby(lambda x: x[0])` on a Series of tuples — pandas passes the integer index label. Use `DataFrame.groupby()` on named columns instead.

### FRED Series Validation

- FRED API returns HTTP 400 Bad Request for discontinued or mistyped series IDs. Always verify IDs against the FRED website.
- Known corrections: `CORCCACBS` (not `CORCCLACBS`), `RHVRUSQ156N` (not `RCVRUSQ156N`).
- Discontinued series must be removed: `GOLDAMGBD228NLBM` (discontinued), `STLFSI2` (replaced by STLFSI4).
- `USALOLITONOSTSAM` (OECD Leading Indicator) is valid but stale — last observation January 2024.
- `FREDDataFetcher._fetch_single_series` classifies failures as `FAIL_INVALID_ID` (HTTP 400), `FAIL_CONNECTION` (retried with backoff), or `FAIL_OTHER`.
- `fetch_all_series_async` logs a structured summary: total/fetched/failed counts, breakdown by failure type.

### Stock vs Flow Math Convention

| Type | Variables | Math Treatment |
|---|---|---|
| **Stock** (point-in-time) | Balances, Delinquency (PD30/PD90), Nonaccrual, ACL | Use directly. No TTM prefix. Delinquency metric is `Past_Due_Rate` (NOT `TTM_Past_Due_Rate`). |
| **Flow** (cumulative YTD) | NCO, Income, Provision, Interest Expense | Convert YTD → discrete quarterly via `ytd_to_discrete()`, then `rolling(4).sum()` for TTM. |

**Income-statement annualization**: Loan Yield and Provision Rate use `annualize_ytd()` which computes `YTD_value * (4.0 / quarter)`.

### Wealth-Focused vs Detailed Table Distinction

| Table | Columns | Composite Used | Purpose |
|---|---|---|---|
| **Executive Summary** | MSPBNA \| GS \| UBS \| Wealth Peers \| Delta | Core PB (90001 std / 90004 norm) | Wealth-focused |
| **Segment Focus** (CRE, Resi) | MSPBNA \| GS \| UBS \| Wealth Peers \| Delta | Core PB (90001 std / 90004 norm) | Segment drill-down |
| **Detailed Peer Table** | MSPBNA + all individual peers + composites | All Peers (90003 std / 90006 norm) | Full landscape |

- Executive and segment tables use **"Wealth Peers" = Core PB composite**. Do NOT include MSBNA or All Peers.
- Individual banks display as **tickers** (GS, UBS, JPM, etc.), not full names.
- GS and UBS are identified dynamically from bank NAME (via `_TICKER_MAP`), not hardcoded CERTs.

---

## Metric Registry & Validation Architecture

### MetricSpec

Each derived metric has a `MetricSpec` in `metric_registry.py` with: `name`, `unit` (rate, pct, multiple, dollars, count), `dependencies` (source columns), `consumer` (which chart/table uses it), `formula_doc` (human-readable formula).

### Validation Engine (6 Rules)

| Rule | Check |
|---|---|
| Rule A | All MetricSpec dependencies present in data |
| Rule B | No circular dependency chains |
| Rule C | All consumer references valid (match ARTIFACT_REGISTRY) |
| Rule D | No duplicate metric names |
| Rule E | MetricSemantic coverage (every MetricSpec has a semantic entry) |
| Rule F | Unsupported mappings flagged (NDFI PD/NA, Tailored Lending) |

### MetricSemantic

Each metric in `metric_semantics.py` has: `polarity` (FAVORABLE/ADVERSE/NEUTRAL), `display_format` (PERCENT/MULTIPLE/DOLLARS/COUNT), `group` (Coverage, Rates, Composition, etc.), `display_order`.

`ordered_metrics(metrics, semantics_dict)` sorts by GROUP_ORDER then display_order for consistent table/heatmap row ordering.

### Preflight Integration

`validate_output_inputs()` in `report_generator.py` runs MetricSpec + MetricSemantic checks before artifact generation. Warnings go to `PRECHECK_WARN` log events; errors to `PRECHECK_FAIL`. Critical failures add artifact names to `suppressed_charts`.

---

## FRED Expansion Layer

### Registry Design

Each series is registered as a `FREDSeriesSpec` with fields: `category`, `subcategory`, `series_id`, `display_name`, `freq`, `units`, `seasonal_adjustment`, `priority`, `use_case`, `chart_group`, `transformations`, `is_active`, `notes`, `sheet`, `discovery_source`.

**Priority levels:**
- **P1** — Series directly used in dashboard charts
- **P2** — Series used for overlays / regimes / alerts
- **P3** — Full discovered registry (stored but not shown by default)

### Modules

**Module A — SBL / Market-Collateral Proxy** (14 series, `FRED_SBL_Backdrop` sheet):
- Bank-system securities inventory (INVEST, SBCLCBM027SBOG family)
- Broker-dealer leverage / margin proxies (BOGZ1 family)
- Label as "market proxy" or "system-level SBL backdrop" — NOT as MSPBNA SBL comps

**Module B — Residential / Jumbo Mortgage** (19 series, `FRED_Residential_Jumbo` sheet):
- Jumbo rate (OBMMIJUMBO30YF), jumbo SLOOS demand/standards
- Large-bank residential balances (RRELCBM, H8B1221, CRLLCB, RHELCB families)
- Top-100-bank delinquency / charge-off (DRSFRMT100, CORSFRMT100)

**Module C — CRE Lending / Underwriting / Credit** (23 series, `FRED_CRE` sheet):
- CRE balance / growth (CREACB, CRELCB, H8B3219, CLDLCB families)
- CRE SLOOS standards/demand (9 series covering nonfarm, multifamily, C&LD)
- CRE credit quality (DRCRELEXF, CORCREXF, COMREPUSQ)

**Module D — Case-Shiller Collateral** (seed: 24 series, discovered: ~140+):
- Standard HPI: national, composites, key metros (SA + NSA)
- Tiered HPI: high/middle/low tiers for 16 markets
- Discovery uses `release_id=199159` (standard) and `release_id=345173` (tiered)

### Transforms

| Transform | Description |
|---|---|
| `pct_chg_mom` | Month-over-month percent change |
| `pct_chg_qoq_annualized` | QoQ change, annualized |
| `pct_chg_yoy` | Year-over-year percent change |
| `z_score_5y` / `z_score_10y` | Rolling z-score over trailing window |
| `rolling_3m_avg` / `rolling_12m_avg` | Rolling mean |
| `spread_vs_*` | Spread against a reference series |

### Named Spreads

| Spread | Components |
|---|---|
| Jumbo vs Conforming | `OBMMIJUMBO30YF - MORTGAGE30US` |
| CRE Demand vs Standards | `SUBLPDRCDNLGNQ - SUBLPDRCSNLGNQ` |
| Resi Growth vs Delinquency | `H8B1221NLGCQG - DRSFRMT100S` |
| High-Tier vs National HPI | YoY growth differential |

### Regime Flags

| Flag | Trigger |
|---|---|
| `REGIME__CRE_Tightening` | CRE standards > 0 AND demand < 0 |
| `REGIME__Jumbo_Tightening` | Jumbo standards > 0 AND demand < 0 |
| `REGIME__Resi_Stress` | Resi delinquency rising AND high-tier HPI decelerating |
| `REGIME__SBL_Deleveraging` | Securities-in-bank-credit falling AND broker-dealer margin weakening |

### FRED Expansion Excel Output Sheets

| Sheet | Contents |
|---|---|
| `FRED_SBL_Backdrop` | SBL proxy series + derived spreads / regime flags |
| `FRED_Residential_Jumbo` | Jumbo rates, SLOOS, residential balances, delinquency, charge-offs |
| `FRED_CRE` | CRE balances, CLD, SLOOS, delinquency, charge-offs, prices |
| `FRED_CaseShiller_Master` | Full discovered Case-Shiller registry |
| `FRED_CaseShiller_Selected` | Curated subset for dashboard visuals |
| `FRED_Expansion_Registry` | Full metadata registry (audit sheet) |
| `FRED_Expansion_Validation` | Validation results |

### Validation Checks

- **Duplicates**: No series_id appears more than once
- **Discontinued**: Excludes series with observation_end before 2024
- **Missing metadata**: Flags specs missing display_name, freq, units, or use_case
- **Stale releases**: Warns if last valid observation is > 6 months old
- **Orphan columns**: Data columns not present in registry

---

## HUD USPS ZIP Crosswalk — Case-Shiller County-Level Enrichment

### Overview

`case_shiller_zip_mapper.py` maps the 20 regional Case-Shiller metro indexes to ZIP codes using **county-level FIPS codes** (per S&P CoreLogic methodology) joined to the HUD USPS County-to-ZIP Crosswalk API (type=7).

**Explicitly excluded**: U.S. National, Composite-10, and Composite-20 indexes (no geographic footprint).

### County-Level FIPS Mapping

`CASE_SHILLER_COUNTY_MAP` contains the official county-level definitions from S&P Case-Shiller "Index Geography" table. ~160 counties across 20 metros, spanning 17 states + DC.

### HUD API Token Setup

1. Register at https://www.huduser.gov/hudapi/public/register
2. Receive your access token via email
3. Set: `export HUD_USER_TOKEN='eyJ...'` or add to `.env`

**Token discovery** uses `resolve_hud_token()` with multi-source resolution: explicit argument → `os.getenv` → `.env` in script dir → `.env` in cwd. Returns `(token, diagnostics)` — full token is never logged.

### HUD Response Parsing & Flattening (Two-Pass)

**Pass 1 — Top-level extraction** (`extract_hud_result_rows()`):

| Shape | Payload Structure | Extraction Path |
|---|---|---|
| A | `[row, row, ...]` | Direct |
| B | `{"results": [row, ...]}` | `payload["results"]` |
| C | `{"results": {"rows": [row, ...]}}` | `payload["results"]["rows"]` |
| D | `{"results": {"data": [row, ...]}}` | `payload["results"]["data"]` |
| E | `{"data": [row, ...]}` | `payload["data"]` |

**Pass 2 — Wrapper row flattening** (`flatten_hud_rows()`):

HUD county-to-ZIP API (type=7) returns wrapper objects with nested `results`. `flatten_hud_rows()` explodes wrappers into child rows with parent metadata propagated.

### Canonical HUD Crosswalk Fields

| Canonical Name | Variants Accepted |
|---|---|
| `zip` | `zip`, `zip_code`, `zipcode`, `zip5`, `results.zip`, `results.zip_code` |
| `county_fips` | `county`, `county_fips`, `geoid`, `county_geoid`, `countyfips`, `fips`, `results.county`, `results.county_fips` |
| `res_ratio` | `res_ratio`, `residential_ratio`, `results.res_ratio` |
| `bus_ratio` | `bus_ratio`, `business_ratio`, `results.bus_ratio` |
| `oth_ratio` | `oth_ratio`, `other_ratio`, `results.oth_ratio` |
| `tot_ratio` | `tot_ratio`, `total_ratio` |

ZIP codes and county FIPS are zero-padded to 5 chars.

### Enrichment Status Codes

| Status | Meaning |
|---|---|
| `SKIPPED_DISABLED` | `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT=false` |
| `SKIPPED_NO_TOKEN` | Token not visible to current Python process |
| `SKIPPED_NO_REQUESTS` | `requests` library not installed |
| `FAILED_TOKEN_AUTH` | HTTP 401/403 from HUD API |
| `FAILED_HTTP` | Non-auth HTTP failures |
| `FAILED_PARSE` | HTTP success but response could not be flattened |
| `FAILED_EMPTY_RESPONSE` | API responded but all counties returned empty |
| `SUCCESS_NO_MATCHES` | HUD returned rows but none matched S&P county FIPS |
| `SUCCESS_NO_ZIPS` | Enrichment ran but produced zero ZIP rows |
| `SUCCESS_WITH_ZIPS` | Normal success |

### Query-Level Failure Constants

| Constant | HTTP Code | Meaning |
|---|---|---|
| `QUERY_FAILED_TOKEN_AUTH` | 401, 403 | Bearer token invalid or expired |
| `QUERY_FAILED_HTTP_NOT_FOUND` | 404 | Endpoint or resource not found |
| `QUERY_FAILED_HTTP_BAD_REQUEST` | 400 | Malformed request parameters |
| `QUERY_FAILED_HTTP_RATE_LIMIT` | 429 | Rate limited by HUD API |
| `QUERY_FAILED_HTTP_SERVER` | 5xx | Server-side error |
| `QUERY_FAILED_HTTP_EXCEPTION` | N/A | Connection timeout, DNS failure, etc. |
| `QUERY_FAILED_PARSE` | 200 | Response parsed but has wrapper-only columns |
| `QUERY_SUCCESS` | 200 | Successful fetch with usable data |

### Request Hardening

All HUD requests use a `requests.Session()` with `Accept: application/json` and `User-Agent` headers. `fetch_hud_crosswalk()` returns `(DataFrame, diagnostics_dict)`. HTTP status codes are classified via `_classify_http_status()`.

**Misclassification prevention**: When all county requests fail HTTP, the enrichment status is `FAILED_HTTP` — not `FAILED_EMPTY_RESPONSE`.

**Smoke test**: `run_hud_smoke_test(fips_code, token)` runs a single HUD request for local debugging.

### HUD Output Sheets

| Sheet | Description |
|---|---|
| `CaseShiller_Zip_Coverage` | One row per (region, ZIP, county_fips) with county name, state, HUD ratios |
| `CaseShiller_Zip_Summary` | One row per region: ZIP count, unique county count, county/state lists |
| `CaseShiller_County_Map_Audit` | Full county-level FIPS mapping with `included_in_zip_output` flag |

## Strictly Enforced Business Logic (Skills)
When modifying code related to flow variables or peer reporting, you MUST strictly adhere to the rules defined in the following skill files:
- For TTM calculations, YTD de-accumulation, and zero-balance gating (especially NCOs): @.claude/skills/cr-flow-math/SKILL.md
- For 90002/90005 peer composites, percentage-point deltas, and `Past_Due_Rate` metric naming: @.claude/skills/cr-peer-reporting/SKILL.md
