# 05 — Executive & Macro Artifact Catalog

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

**Integration pattern:**
- Heatmaps: 4 variants via `_heatmap_specs` loop — Standard/Normalized x Wealth Peers/All Peers. Each uses the appropriate composite CERT: Wealth = core_pb (90001/90004), All = all_peers (90003/90006). Title and column headers dynamically include peer group name.
- Sparklines: 4 variants via `_sparkline_specs` loop — Standard/Normalized x Wealth Peers/All Peers. Standard sparklines use `SPARKLINE_METRICS_STANDARD` (7 metrics), normalized use `SPARKLINE_METRICS_NORMALIZED` (5 metrics). Title and "vs Peers" column header show the peer group name.
- KRI bullet charts are **football-field** style with nested peer range bands.
- All 4 KRI bullet charts use `_bullet_specs` loop with per-spec metric list, title, and composite CERTs.
- Bullet charts pass `wealth_member_certs` and `all_peers_member_certs` for actual min/max range computation.
- The obsolete single artifact names `yoy_heatmap_standard`, `yoy_heatmap_normalized`, `sparkline_summary`, `kri_bullet_chart`, `kri_bullet_normalized` are removed from the registry.

### Sparkline Metric Lists

- `SPARKLINE_METRICS_STANDARD` (7): TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, Past_Due_Rate, RIC_CRE_Nonaccrual_Rate, RIC_CRE_ACL_Coverage
- `SPARKLINE_METRICS_NORMALIZED` (5): Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, Norm_Delinquency_Rate

### Football-Field KRI Chart Design (Nested Bands + Average Markers)

- **Outer lighter band (light gray #D0D0D0)**: All Peers min-max range across individual member CERTs
- **Inner darker band (muted purple-gray #B8A0C8)**: Wealth Peers min-max range across member CERTs
- **Gold diamond (#F7A81B)**: MSPBNA value
- **Purple triangle-up (#7B2D8E)**: Wealth PB Avg — mean of wealth peer member values (shown when >= 2 members)
- **Blue square (#5B9BD5)**: All Peers Avg — mean of all peer member values (shown when >= 2 members)
- **Median markers**: Vertical tick marks at median of each peer group
- **Edge labels**: Min/max peer tickers annotated at band edges (using `resolve_display_label()`)
- **Legend**: Up to 5 entries — MSPBNA diamond, Wealth PB Avg (Mean), All Peers Avg (Mean), Wealth Peers Range, All Peers Range. Avg entries are conditional on data availability.
- Fallback: if member CERTs not provided, falls back to composite CERT values as single-point bands

### Unit Family Separation (Never Mix % Rates with x-Multiples)

- `kri_bullet_standard` — 5 % rate metrics: TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Past_Due_Rate, Allowance_to_Gross_Loans_Rate, RIC_CRE_ACL_Coverage
- `kri_bullet_standard_coverage` — 2 x-multiple metrics: Risk_Adj_Allowance_Coverage, RIC_CRE_Risk_Adj_Coverage
- `kri_bullet_normalized_rates` — 5 rate metrics: Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage
- `kri_bullet_normalized_composition` — 4 composition metrics: Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share (Norm_Resi_ACL_Share removed — the data engine does not calculate segment-level ACL outside of CRE)

### Comparator Fallback Behavior

- Both peer groups have data → nested bands (outer All Peers + inner Wealth Peers)
- Single peer group only → single band for that group, other drawn as thin reference line
- Neither peer group available → metric row skipped entirely

### Bookwide Growth vs Deterioration

- `growth_vs_deterioration_bookwide` — X-axis: `Total_Loans_Growth_TTM` (pre-computed). Y-axis: TTM NCO Rate (fallback: NPL to Gross Loans Rate).
- Uses the pre-computed growth column directly — does NOT compute growth inline. The data engine creates `Total_Loans_Growth_TTM` via the `growth_targets` dict.
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

---

## Deterministic Macro Chart Artifacts (3 Artifacts)

Implemented directly in `report_generator.py` (functions: `generate_macro_corr_heatmap`, `plot_macro_overlay_credit_stress`, `plot_macro_overlay_rates_housing`).

| Artifact | File | Mode | Type |
|---|---|---|---|
| `macro_corr_heatmap_lag1` | `{stem}_macro_corr_heatmap_lag1.html` | BOTH | HTML |
| `macro_overlay_credit_stress` | `{stem}_macro_overlay_credit_stress.png` | FULL_LOCAL_ONLY | PNG |
| `macro_overlay_rates_housing` | `{stem}_macro_overlay_rates_housing.png` | FULL_LOCAL_ONLY | PNG |

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

**Series selection is deterministic — no heuristic fallback.** The old `plot_macro_overlay()` that picked random series has been deleted.

### Artifact Details

**macro_corr_heatmap_lag1.html:**
- Internal metric rows (8): Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_NCO_Rate, RIC_CRE_ACL_Coverage
- Macro columns (13): All 13 FRED series above
- Pearson correlation, trailing 20Q window
- Macro series lagged +1 quarter vs internal metrics
- Insufficient overlap (< 4 quarters) → N/A cell
- Color scale: green (counter-cyclical) → neutral → red (comovement)
- All series quarterly-aligned via `_fred_to_quarterly()` (last-observation-per-quarter resampling)

**macro_overlay_credit_stress.png:**
- Left axis: MSPBNA Norm_NCO_Rate (gold, solid)
- Right axis: BAMLH0A0HYM2 (blue, dashed) + NFCI (red, dashed), both z-scored
- Z-scoring: `(value - mean) / std` for readability when units differ

**macro_overlay_rates_housing.png:**
- Left axis: RIC_Resi_Nonaccrual_Rate (preferred) or Norm_Nonaccrual_Rate (fallback)
- Right axis: FEDFUNDS (solid), MORTGAGE30US (dashed), CSUSHPISA YoY % (dash-dot)
- CSUSHPISA transformed: `pct_change(4) * 100` (quarterly YoY %)
- Title dynamically names the actual plotted series

---

## FRED Expansion First-Wave Charts

| Chart | Function | Key Series |
|---|---|---|
| SBL Backdrop | `plot_sbl_backdrop()` | SBCLCBM027SBOG + BOGZ1FU663067005A |
| Jumbo Conditions | `plot_jumbo_conditions()` | OBMMIJUMBO30YF + SUBLPDHMDJLGNQ + SUBLPDHMSKLGNQ |
| Resi Credit Cycle | `plot_resi_credit_cycle()` | DRSFRMT100S + CORSFRMT100S + H8B1221NLGCQG |
| CRE Cycle | `plot_cre_cycle()` | H8B3219NLGCMG + SUBLPDRCSNLGNQ + DRCRELEXFT100S |
| CS Collateral Panel | `plot_cs_collateral_panel()` | High-tier metros + national + sales-pair counts |
