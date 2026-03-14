# 10 — Coding Rules

These are **non-negotiable** for any agent editing this codebase.

## ALWAYS UPDATE CLAUDE.md

- Whenever you make architectural changes, add new charts, or change data pipelines, you **must** update the relevant `docs/claude/` memory file to review restrictions, track to-do items, identify potential gaps, and inform future coding agents.

## FDIC API Variables & FFIEC Waterfall

- Always prefer FDIC top-level text aliases (e.g., `ASSET`, `NTCI`) over raw MDRM codes (`RCFD...`, `RIAD...`). When raw codes are necessary, you **MUST** fetch both the `RCFD` (Consolidated) and `RCON` (Domestic) codes. You must then coalesce them (`RCFD.fillna(RCON)`) to ensure apples-to-apples comparisons between international G-SIBs (FFIEC 031) and domestic-only banks (FFIEC 041/051). Never force internationally active banks to strictly use `RCON`, as this strips out their foreign office balances. Synthetic local variables must never be included in the raw API fetch request.

## NO HARDCODING

- Subject bank CERTs **must** be loaded dynamically via `int(os.getenv("MSPBNA_CERT", "34221"))`.
- Never hardcode CERT numbers directly. Always use the config/env pattern. Production defaults: `MSPBNA_CERT=34221`, `MSBNA_CERT=32992`.
- The FRED API key must come from `os.getenv('FRED_API_KEY')` or `.env`.

## IMPORT SAFETY

- `MSPBNA_CR_Normalized.py` must be importable **without** setting environment variables, altering `cwd`, printing to stdout, or opening log files. All side effects live in `main()` (via `_validate_runtime_env()`, `setup_logging()`, `os.chdir()`).
- Module-level `MSPBNA_CERT` and `MSBNA_CERT` use `os.getenv()` with defaults for import safety. The hard validation (ValueError on missing vars) happens at runtime in `_validate_runtime_env()`.
- `csv_log` and `logger` are initialized to `None` at module level and set by `setup_logging()` inside `main()`.

## DEPRECATED FUNCTIONS

- `generate_normalized_comparison_table()` in `report_generator.py` is **deprecated** — raises `NotImplementedError`. It produced a mixed standard-vs-normalized artifact violating the single-regime-per-artifact rule. Use the separate standard/normalized table generators instead.

## SCATTER & CHART COMPOSITE HANDLING

- **Active composite regime** is defined at the top of `report_generator.py`:
  - `ACTIVE_STANDARD_COMPOSITES = {"core_pb": 90001, "all_peers": 90003}`
  - `ACTIVE_NORMALIZED_COMPOSITES = {"core_pb": 90004, "all_peers": 90006}`
  - `INACTIVE_LEGACY_COMPOSITES = {90002, 90005, 99998, 99999}`
- All chart/table builders MUST use these canonical constants. **Never** hardcode 99998, 99999, 90002, or 90005 as peer-average selectors.
- `ALL_COMPOSITE_CERTS` includes both active and legacy CERTs for scatter-dot exclusion only.
- `plot_scatter_dynamic()` defaults: `peer_avg_cert_primary=90003, peer_avg_cert_alt=90001`. Normalized call sites pass `90006/90004` explicitly.
- If an active composite is missing from data, the chart/table MUST skip (not silently substitute a legacy CERT). Use `validate_composite_cert_regime()` for preflight checks.
- The former `build_plot_df_with_alias()` function has been removed — it appended duplicate rows that contaminated scatter plots. Do not re-introduce it.

## MS COMBINED ENTITY

- CERT `88888` (MS Combined Entity) must be **filtered out** of HTML table listings when `REPORT_VIEW == "MSPBNA_WEALTH_NORM"`.
- Load via `MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))`.

## COVERAGE vs SHARE vs x-MULTIPLE LABEL RULE (Non-Negotiable)

Every ratio-component row label **must** match its denominator type:

| Denominator Type | Correct Label Term | Display Format | Examples |
|---|---|---|---|
| Exposure / loan base (Gross_Loans, RIC_CRE_Cost, Wealth_Resi_Balance, CRE_Investment_Pure_Balance) | **"Coverage"** or **"Ratio"** | % | `RIC_CRE_ACL / RIC_CRE_Cost` → "CRE ACL Coverage" |
| ACL pool (Total_ACL, Norm_ACL_Balance) | **"Share"** or **"% of ACL"** | % | `RIC_CRE_ACL / Total_ACL` → "CRE % of ACL" |
| Nonaccrual / NPL base (RIC_CRE_Nonaccrual, RIC_Resi_Nonaccrual) | **"NPL Coverage"** | x-multiple | `RIC_CRE_ACL / RIC_CRE_Nonaccrual` → "CRE NPL Coverage" (1.23x) |

**If denominator is `Total_ACL` or `Norm_ACL_Balance`, the label must NEVER contain "Coverage".**

## `_METRIC_FORMAT_TYPE` MAINTENANCE RULE

`_METRIC_FORMAT_TYPE` in `report_generator.py` is the **explicit registry** for x-multiple formatted metrics. Rules:

1. Only **NPL coverage** metrics (denominator = nonaccrual or past-due) belong in this dict.
2. Any **new** NPL coverage metric MUST be added here explicitly — there is no auto-detection.
3. Loan-coverage and share-of-ACL metrics must **NEVER** be added — they default to percent.
4. Current entries: `RIC_CRE_Risk_Adj_Coverage`, `RIC_Resi_Risk_Adj_Coverage`, `RIC_Comm_Risk_Adj_Coverage`.

## HUD TOKEN — ATTRIBUTE-ONLY ACCESS

`DashboardConfig` is a plain Python class (not a dict). HUD token access rules:

- **Field**: `hud_user_token: Optional[str] = None` on `DashboardConfig`
- **Write**: `config.hud_user_token = _hud_token` (attribute assignment, **never** `config["..."]`)
- **Read**: `getattr(self.config, "hud_user_token", None)` (safe attribute access, **never** `.get()`)
- **Pass**: `build_case_shiller_zip_sheets(hud_user_token=self.config.hud_user_token)` (explicit kwarg)
- **Resolve**: Single call to `resolve_hud_token()` in `_validate_runtime_env()` — no competing resolution paths

Dict-style access (`config["key"]`, `config.get("key")`) will raise `TypeError` on `DashboardConfig` and is forbidden.

## CENTRALIZED CHART PALETTE (CHART_PALETTE)

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

## CHART ANNOTATION / ANTI-OVERLAP POLICY

`ChartAnnotationHelper` in `report_generator.py` provides a shared label placement engine:

**Priority tiers:**
1. **MSPBNA** — always labeled (TIER_SUBJECT = 1)
2. **Wealth Peers** — always labeled when plotted (TIER_WEALTH = 2)
3. **All Peers** — labeled only when explicitly plotted (TIER_ALL_PEERS = 3)
4. **Individual peers** — labeled only for outliers, edge-of-range, or specifically selected (TIER_INDIVIDUAL = 4)

**Rules:**
- Use ticker abbreviations only: MSPBNA, GS, UBS, C, JPM, BAC, WFC, MS
- Endpoint labels preferred over many internal labels on time-series charts
- Value labels on bar/line combos should be sparse and strategic (latest quarter, major inflection points only)
- Leader lines allowed on scatter plots for displaced labels
- Directional nudges used to avoid collisions; higher-priority labels get first placement

## WEALTH PEERS COMPARATOR INCLUSION POLICY

Wealth Peers must appear as an explicit styled point/line/marker (not implied in the peer cloud) in these charts:
- `scatter_nco_vs_npl` — triangle marker, purple
- `scatter_pd_vs_npl` — triangle marker, purple
- `scatter_norm_nco_vs_nonaccrual` — triangle marker, purple
- `risk_adjusted_return` — bubble marker, purple
- `years_of_reserves` — triangle marker, purple
- `growth_vs_deterioration` — triangle marker, purple
- `concentration_vs_capital` — triangle marker, purple

All Peers remains as a reference marker/crosshair where appropriate. Do NOT clutter charts that are intentionally subject-only operational overlays (e.g., `portfolio_mix`, `liquidity_overlay`).

## CSS CLASS NAMING

- Use `mspbna-row` and `mspbna-value` (not `idb-row` / `idb-value`).
- All user-facing labels, HTML headers, and filenames must reference **MSPBNA**, never **IDB**.

## ENTITY DISPLAY LABEL POLICY

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

**Fallback**: If no ticker match, strip "National Association" / "N.A." suffixes and title-case. Last resort: `"CERT {cert}"`.

## CHART METRICS

**Standard chart:** Bar = `TTM_NCO_Rate`, Line = `NPL_to_Gross_Loans_Rate`
**Normalized chart:** Bar = `Norm_NCO_Rate`, Line = `Norm_Nonaccrual_Rate` (fallback: `Norm_NPL_to_Gross_Loans_Rate`)

Both standard and normalized credit-deterioration charts are generated.

## FRED Deduplication

- `series_ids` must always be deduplicated before async fetching to prevent `ValueError: cannot reindex on an axis with duplicate labels`.
- Use `list(dict.fromkeys(series_ids))` to preserve order while removing duplicates.
- The guard must exist both at construction time (when building `series_ids_to_fetch`) and at the entry point of `fetch_all_series_async()`.

## FRED Frequency Inference

- `infer_freq_from_index(idx)` (module-level in `MSPBNA_CR_Normalized.py`) infers series frequency from a `DatetimeIndex` when FRED metadata is missing or marked "Unknown".
- The helper is **heuristic and fail-safe**: it tries `pd.infer_freq()` first, then median-obs-per-year, then distinct-months-per-year, and falls back to `("quarterly", "Q")` on any unexpected failure.
- **Never** use `Series.groupby(lambda x: x[0])` on a Series of tuples — pandas passes the integer index label, not the tuple value. Use `DataFrame.groupby()` on named columns instead.
- The helper should never crash the FRED pipeline. All code paths are wrapped in `try/except`.

## FRED Series Validation

- The FRED API returns HTTP 400 Bad Request for discontinued or mistyped series IDs. Always verify IDs against the FRED website before adding them to `FRED_SERIES_TO_FETCH`.
- Known corrections: `CORCCACBS` (not `CORCCLACBS`), `RHVRUSQ156N` (not `RCVRUSQ156N`).
- Discontinued series must be removed, not left in the fetch list (e.g., `GOLDAMGBD228NLBM` was discontinued by FRED, `STLFSI2` discontinued 2022-01-07 and replaced by `STLFSI4`).
- Redundant series should be removed if covered by another ID (e.g., `DEPALL` removed in favor of `DPSACBW027SBOG`).
- `USALOLITONOSTSAM` (OECD Leading Indicator) is valid but stale — last observation January 2024. Monitor for potential functional discontinuation.
- `FREDDataFetcher._fetch_single_series` classifies failures as `FAIL_INVALID_ID` (HTTP 400), `FAIL_CONNECTION` (DNS/proxy/timeout — retried with backoff), or `FAIL_OTHER`. Connection errors are retried up to 3 times with exponential backoff (2s, 4s, 8s). HTTP 400 errors are not retried.
- `fetch_all_series_async` logs a structured summary at the end of the FRED fetch phase: total/fetched/failed counts, breakdown by failure type, and a list of missing macro chart series (from `MACRO_CORR_FRED_SERIES`).

## STOCK vs FLOW MATH CONVENTION

Call Report variables fall into two categories that require different math:

| Type | Variables | Math Treatment |
|---|---|---|
| **Stock** (point-in-time) | Balances, Delinquency (PD30/PD90), Nonaccrual, ACL | Use directly. No TTM prefix. Delinquency metric is `Past_Due_Rate` (NOT `TTM_Past_Due_Rate`). |
| **Flow** (cumulative YTD) | NCO, Income, Provision, Interest Expense | Convert YTD → discrete quarterly via `ytd_to_discrete()`, then `rolling(4).sum()` for TTM. |

**Income-statement annualization**: Loan Yield (`Loan_Yield_Proxy`) and Provision Rate (`Provision_to_Loans_Rate`) use `annualize_ytd()` which computes `YTD_value * (4.0 / quarter)`. This is the standard banking convention — it gives a current-period-only view without mixing in prior-year stale quarters.

**Module-level helpers** in `MSPBNA_CR_Normalized.py`:
- `ytd_to_discrete(df, col_name)` — YTD → discrete quarterly flows
- `annualize_ytd(df, col_name)` — YTD → annualized rate

## WEALTH-FOCUSED vs DETAILED TABLE DISTINCTION

Three table tiers serve different audiences:

| Table | Columns | Composite Used | Purpose |
|---|---|---|---|
| **Executive Summary** | MSPBNA \| GS \| UBS \| Wealth Peers \| Delta MSPBNA vs Wealth Peers | Core PB (90001 std / 90004 norm) | Wealth-focused peer comparison |
| **Segment Focus** (CRE, Resi) | MSPBNA \| GS \| UBS \| Wealth Peers \| Delta MSPBNA vs Wealth Peers | Core PB (90001 std / 90004 norm) | Segment-specific drill-down |
| **Detailed Peer Table** | MSPBNA + all individual peers + composites | All Peers (90003 std / 90006 norm) | Full peer landscape |

**Key rules:**
- Executive summary and segment tables use **"Wealth Peers" = Core PB composite** (90001/90004). Do NOT include MSBNA or All Peers.
- The detailed peer table is the **only** broad all-peer table.
- Both standard and normalized versions are generated as **separate artifacts**.
- Individual banks display as **tickers** (GS, UBS, JPM, BAC, C, WFC), not full names. Composites display as **descriptive labels** (Wealth Peers, All Peers). All labels are resolved via `resolve_display_label()` in `report_generator.py`.
- GS and UBS are identified dynamically from bank NAME in data (via `_TICKER_MAP`), not hardcoded CERTs.

## IDB Label Convention

Dictionary keys in `master_data_dictionary.py` must **never** use the `IDB_` prefix. All former `IDB_*` keys have been renamed (e.g., `IDB_CRE_Growth_TTM` → `CRE_Growth_TTM`). User-facing labels, CSS classes, and HTML headers must reference **MSPBNA**, never **IDB**.

## Curated Presentation Tabs

`Summary_Dashboard` and `Normalized_Comparison` use curated metric allowlists (`SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS` in `MSPBNA_CR_Normalized.py`). Only approved KPIs appear in these presentation-facing tabs. Raw MDRM fields and internal pipeline columns are excluded. The full dataset remains available in the `FDIC_Data` sheet.

## Display Label Policy

All presentation tabs use `_get_metric_short_name(code)` to resolve display names from `FDIC_Metric_Descriptions` (via `MasterDataDictionary`). Columns: `Metric Code` (technical field name) + `Metric Name` (display label). Falls back to the code itself when no display label exists. Debug logging is emitted for any fallback.

**Validation note**: Every metric in `SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS` must have a corresponding entry in `LOCAL_DERIVED_METRICS` (Tier 3) or resolve via MDRM/FDIC API (Tiers 1-2). Regression tests in `TestWorkbookLevelCuration` and `TestDisplayLabelCoverage` enforce this. Presentation-layer fixes are only considered complete once visible in the generated workbook — source-code-level intent alone is insufficient.

## Metric Role Classification

Metrics are classified as **evaluative** (risk/return/coverage — receives performance flags) or **descriptive** (size/balance/composition — no evaluative flags). `DESCRIPTIVE_METRICS` frozenset in `MSPBNA_CR_Normalized.py` lists all descriptive metrics. `_get_performance_flag()` returns blank for descriptive metrics to prevent misleading "Top Quartile" / "Bottom Quartile" flags on non-evaluative fields like ASSET or LNLS.

## Norm_Provision_Rate Treatment

`Norm_Provision_Rate` is intentionally set to NaN in the pipeline — provision expense (`ELNATR`) is not segment-specific in call reports, so a normalized rate would be semantically misleading. It is excluded from `NORMALIZED_COMPARISON_METRICS` (presentation tab), and dead-metric suppression in HTML tables catches any remaining instances. It should never be presented as a normal KPI or as a silent zero.
