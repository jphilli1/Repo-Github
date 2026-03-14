# 05 — Executive & Macro Artifacts

## Executive Chart Artifacts (16 Artifacts)

Implemented in `executive_charts.py`, integrated into `report_generator.py` Phase 8.

| Artifact | File | Mode | Type |
|---|---|---|---|
| `yoy_heatmap_standard_wealth` | `{stem}_yoy_heatmap_standard_wealth.html` | BOTH | HTML |
| `yoy_heatmap_standard_allpeers` | `{stem}_yoy_heatmap_standard_allpeers.html` | BOTH | HTML |
| `yoy_heatmap_normalized_wealth` | `{stem}_yoy_heatmap_normalized_wealth.html` | BOTH | HTML |
| `yoy_heatmap_normalized_allpeers` | `{stem}_yoy_heatmap_normalized_allpeers.html` | BOTH | HTML |
| `kri_bullet_standard` | `{stem}_kri_bullet_standard.png` | FULL_LOCAL_ONLY | PNG |
| `kri_bullet_standard_coverage` | `{stem}_kri_bullet_standard_coverage.png` | FULL_LOCAL_ONLY | PNG |
| `kri_bullet_normalized_rates` | `{stem}_kri_bullet_normalized_rates.png` | FULL_LOCAL_ONLY | PNG |
| `kri_bullet_normalized_composition` | `{stem}_kri_bullet_normalized_composition.png` | FULL_LOCAL_ONLY | PNG |
| `sparkline_standard_wealth` | `{stem}_sparkline_standard_wealth.html` | BOTH | HTML |
| `sparkline_standard_allpeers` | `{stem}_sparkline_standard_allpeers.html` | BOTH | HTML |
| `sparkline_normalized_wealth` | `{stem}_sparkline_normalized_wealth.html` | BOTH | HTML |
| `sparkline_normalized_allpeers` | `{stem}_sparkline_normalized_allpeers.html` | BOTH | HTML |
| `growth_vs_deterioration_bookwide` | `{stem}_growth_vs_deterioration_bookwide.png` | FULL_LOCAL_ONLY | PNG |
| `cumul_growth_loans_vs_acl_wealth` | `{stem}_cumul_growth_loans_vs_acl_wealth.png` | FULL_LOCAL_ONLY | PNG |
| `cumul_growth_loans_vs_acl_allpeers` | `{stem}_cumul_growth_loans_vs_acl_allpeers.png` | FULL_LOCAL_ONLY | PNG |

### Integration Patterns

- Heatmaps: 4 variants via `_heatmap_specs` loop — Standard/Normalized × Wealth Peers/All Peers. Each uses the appropriate composite CERT: Wealth = core_pb (90001/90004), All = all_peers (90003/90006). Title and column headers dynamically include peer group name.
- Sparklines: 4 variants via `_sparkline_specs` loop — Standard/Normalized × Wealth Peers/All Peers. Standard sparklines use `SPARKLINE_METRICS_STANDARD` (7 metrics), normalized use `SPARKLINE_METRICS_NORMALIZED` (5 metrics). Title and "vs Peers" column header show the peer group name.
- KRI bullet charts are **football-field** style with nested peer range bands
- All 4 KRI bullet charts use `_bullet_specs` loop with per-spec metric list, title, and composite CERTs
- Bullet charts pass `wealth_member_certs` and `all_peers_member_certs` for actual min/max range computation
- The obsolete single artifact names `yoy_heatmap_standard`, `yoy_heatmap_normalized`, `sparkline_summary`, `kri_bullet_chart`, `kri_bullet_normalized` are removed from the registry

### Sparkline Metric Lists

- `SPARKLINE_METRICS_STANDARD` (7): TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, Past_Due_Rate, RIC_CRE_Nonaccrual_Rate, RIC_CRE_ACL_Coverage
- `SPARKLINE_METRICS_NORMALIZED` (5): Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, Norm_Delinquency_Rate

### Football-Field KRI Chart Design (Nested Bands)

- **Outer lighter band (light gray #D0D0D0)**: All Peers min–max range across individual member CERTs
- **Inner darker band (muted purple-gray #B8A0C8)**: Wealth Peers min–max range across member CERTs
- **Gold diamond (#F7A81B)**: MSPBNA value
- **Median markers**: Vertical tick marks at median of each peer group
- **Edge labels**: Min/max peer tickers annotated at band edges (using `resolve_display_label()`)
- Fallback: if member CERTs not provided, falls back to composite CERT values as single-point bands

### Unit Family Separation (Never Mix % Rates with x-Multiples)

- `kri_bullet_standard` — 5 % rate metrics: TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Past_Due_Rate, Allowance_to_Gross_Loans_Rate, RIC_CRE_ACL_Coverage
- `kri_bullet_standard_coverage` — 2 x-multiple metrics: Risk_Adj_Allowance_Coverage, RIC_CRE_Risk_Adj_Coverage
- `kri_bullet_normalized_rates` — 5 rate metrics: Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage
- `kri_bullet_normalized_composition` — 4 composition metrics: Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share (Norm_Resi_ACL_Share removed — the data engine does not calculate segment-level ACL outside of CRE; the numerator `RIC_Resi_ACL` depends on `RCFDJJ14`/`RCONJJ14` which are frequently absent for non-residential-focused banks, producing systematic NaN)

### Bookwide Growth vs Deterioration

- `growth_vs_deterioration_bookwide` — X-axis: `Total_Loans_Growth_TTM` (pre-computed by MSPBNA_CR_Normalized.py as trailing-4Q growth on LNLS). Y-axis: TTM NCO Rate (fallback: NPL to Gross Loans Rate).
- Uses the pre-computed growth column directly — does NOT compute growth inline. The data engine creates `Total_Loans_Growth_TTM` via the `growth_targets` dict (key `'Total_Loans'` → column `'LNLS'`) and exports it automatically via the `*_Growth_TTM` wildcard capture at snapshot construction.
- Complementary to the existing CRE-focused `growth_vs_deterioration` chart.
- Same visual style: quadrant scatter with median crosshair lines, MSPBNA diamond, Wealth Peers triangle, All Peers square.

### Cumulative Growth: Target Loans vs CRE ACL

- 2 variants via `_cumul_specs` loop: MSPBNA vs Wealth Peers, MSPBNA vs All Peers
- **Target Loans** = CRE Balance (`RIC_CRE_Cost`) + RESI Balance (`Wealth_Resi_Balance` / `RIC_Resi_Cost` / `LNRERES` fallback)
- **Target ACL** = CRE ACL Balance (`RIC_CRE_ACL`)
- Indexed to Q4 2015 (2015-12-31): cumulative % growth = `(Current - Base) / Base`
- 4 lines per chart: MSPBNA Loans (solid gold), MSPBNA ACL (dashed gold), Peer Loans (solid peer color), Peer ACL (dashed peer color)
- Endpoint CAGR annotations: `(Final / Base)^(1/years) - 1`, with horizontal dashed reference lines
- Anti-overlap via `_nudge_cagr_labels()` (min_gap=3pp) ensures labels don't collide
- Entity colors: MSPBNA=#F7A81B (gold), Wealth Peers=#7B2D8E (purple), All Peers=#5B9BD5 (blue)

### Comparator Fallback Behavior

- Both peer groups have data → nested bands (outer All Peers + inner Wealth Peers)
- Single peer group only → single band for that group, other drawn as thin reference line
- Neither peer group available → metric row skipped entirely

---

## Deterministic Macro Chart Artifacts (3 Artifacts)

Implemented directly in `report_generator.py` (functions: `generate_macro_corr_heatmap`, `plot_macro_overlay_credit_stress`, `plot_macro_overlay_rates_housing`).

| Artifact | File | Mode | Type |
|---|---|---|---|
| `macro_corr_heatmap_lag1` | `{stem}_macro_corr_heatmap_lag1.html` | BOTH | HTML |
| `macro_overlay_credit_stress` | `{stem}_macro_overlay_credit_stress.png` | FULL_LOCAL_ONLY | PNG |
| `macro_overlay_rates_housing` | `{stem}_macro_overlay_rates_housing.png` | FULL_LOCAL_ONLY | PNG |

**Series selection is deterministic — no heuristic fallback:**
- `macro_corr_heatmap_lag1`: 8 internal metrics × 13 FRED series, Pearson correlation, +1Q lag
- `macro_overlay_credit_stress`: Left = Norm_NCO_Rate, Right = BAMLH0A0HYM2 + NFCI (z-scored)
- `macro_overlay_rates_housing`: Left = RIC_Resi_Nonaccrual_Rate (or Norm_Nonaccrual_Rate fallback), Right = FEDFUNDS + MORTGAGE30US + CSUSHPISA YoY%

The old heuristic `plot_macro_overlay()` that picked "Fed Funds", "Unemployment", "All Loans Delinquency Rate" with fallback to first available series has been deleted. The obsolete artifact name `macro_overlay` is removed from both the registry and report_generator.py. If required FRED series are not present in the workbook, the artifact skips gracefully with manifest logging.

### Required FRED Series (13)

| Category | Series ID | Short Name | Frequency |
|---|---|---|---|
| Rates/Curve | FEDFUNDS | Fed Funds Rate | Monthly |
| Rates/Curve | T10Y2Y | 10Y-2Y Spread | Daily |
| Credit/Stress | BAMLH0A0HYM2 | HY OAS | Daily |
| Credit/Stress | VIXCLS | VIX | Daily |
| Credit/Stress | NFCI | Chicago Fed NFCI | Weekly |
| Credit/Stress | STLFSI4 | St. Louis FSI | Weekly |
| Credit/Stress | DRTSCILM | C&I Standards (Large/Med) | Quarterly |
| Bank Health | DRALACBS | All Loans Delinquency | Quarterly |
| Bank Health | DRCRELEXFACBS | CRE Delinq (ex-farm) | Quarterly |
| Bank Health | DRSFRMACBS | 1-4 Resi Delinquency | Quarterly |
| Housing | MORTGAGE30US | 30Y Mortgage Rate | Weekly |
| Housing | HOUST | Housing Starts | Monthly |
| Housing | CSUSHPISA | Case-Shiller National (SA) | Monthly |

---

## FRED Expansion Layer — Registry-Driven Macro / Market Context

### Architecture

The FRED expansion is implemented as four modules:

| Module | Purpose |
|---|---|
| `fred_series_registry.py` | Central registry of all expanded FRED series (`FREDSeriesSpec` dataclass) |
| `fred_case_shiller_discovery.py` | Async release-table discovery for all Case-Shiller series |
| `fred_transforms.py` | Transforms (pct_chg, z-scores, rolling avgs), named spreads, regime flags |
| `fred_ingestion_engine.py` | Async fetcher, validation, sheet routing, Excel output |

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
- Tiered HPI: high/middle/low tiers for 16 markets (discovery via release API)
- Sales-pair counts: market liquidity context
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

### Excel Output Sheets

| Sheet | Contents |
|---|---|
| `FRED_SBL_Backdrop` | SBL proxy series + derived spreads / regime flags |
| `FRED_Residential_Jumbo` | Jumbo rates, SLOOS, residential balances, delinquency, charge-offs |
| `FRED_CRE` | CRE balances, CLD, SLOOS, delinquency, charge-offs, prices |
| `FRED_CaseShiller_Master` | Full discovered Case-Shiller registry |
| `FRED_CaseShiller_Selected` | Curated subset for dashboard visuals |
| `FRED_Expansion_Registry` | Full metadata registry (audit sheet) |
| `FRED_Expansion_Validation` | Validation results |

### First-Wave Charts (report_generator.py)

| Chart | Function | Key Series |
|---|---|---|
| SBL Backdrop | `plot_sbl_backdrop()` | SBCLCBM027SBOG + BOGZ1FU663067005A |
| Jumbo Conditions | `plot_jumbo_conditions()` | OBMMIJUMBO30YF + SUBLPDHMDJLGNQ + SUBLPDHMSKLGNQ |
| Resi Credit Cycle | `plot_resi_credit_cycle()` | DRSFRMT100S + CORSFRMT100S + H8B1221NLGCQG |
| CRE Cycle | `plot_cre_cycle()` | H8B3219NLGCMG + SUBLPDRCSNLGNQ + DRCRELEXFT100S |
| CS Collateral Panel | `plot_cs_collateral_panel()` | High-tier metros + national + sales-pair counts |

### Validation Checks

The ingestion engine validates:
- **Duplicates**: No series_id appears more than once in the registry
- **Discontinued**: Excludes series with observation_end before 2024
- **Missing metadata**: Flags specs missing display_name, freq, units, or use_case
- **Stale releases**: Warns if last valid observation is > 6 months old
- **Orphan columns**: Data columns not present in registry
