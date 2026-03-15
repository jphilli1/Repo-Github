# 08 — Corp-Safe Overlay Architecture

## Overview

`corp_overlay.py` is a **standalone module** that joins the local `Bank_Performance_Dashboard_YYYYMMDD.xlsx` output with an internal loan-level extract to produce corp-safe artifacts. It runs via its own CLI entrypoint (`corp_overlay_runner.py`) and is **NOT** integrated into `report_generator.py` or `MSPBNA_CR_Normalized.py`.

## Data Flow

```
Bank_Performance_Dashboard_YYYYMMDD.xlsx (Input A — from MSPBNA_CR_Normalized.py)
        │
        ├─── Summary_Dashboard sheet → peer composition metrics
        │
Internal Loan File (Input B — CSV or Excel)
        │
        ├─── Required: loan_id, current_balance, product_type, geo field
        │
        ▼
corp_overlay.py
        │
        ├──► output/Peers/corp_overlay/  loan_balance_by_product.png
        ├──► output/Peers/corp_overlay/  top10_geography_by_balance.png
        ├──► output/Peers/corp_overlay/  internal_credit_flags_summary.html
        └──► output/Peers/corp_overlay/  peer_vs_internal_mix_bridge.html
```

## Input Contract (Loan File)

| Column | Required | Description |
|---|---|---|
| `loan_id` | Yes | Unique loan identifier |
| `current_balance` | Yes | Outstanding balance ($) |
| `product_type` | Yes | Loan product classification |
| `msa` | At least one geo | Metropolitan Statistical Area code (preferred) |
| `zip_code` | At least one geo | 5-digit ZIP code |
| `county` | At least one geo | County FIPS or name |
| `risk_rating` | Optional | Internal risk rating |
| `delinquency_status` | Optional | Delinquency bucket (current, 30dpd, 60dpd, etc.) |
| `nonaccrual_flag` | Optional | Y/N nonaccrual indicator |
| `segment` | Optional | Business segment tag |
| `portfolio` | Optional | Portfolio identifier |
| `collateral_type` | Optional | Collateral classification |

**Validation:** `validate_loan_file()` raises `LoanFileContractError` on missing required columns or missing all geo fields. Column matching is case-insensitive.

**Geo priority:** MSA > zip_code > county. The first available is used for geographic aggregation.

## Optional Enrichment Hooks

| Source | Env Var | Resolution | Status |
|---|---|---|---|
| Census | `CENSUS_API_KEY` | Direct env lookup → None | Hook point (not yet implemented) |
| BEA | `BEA_API_KEY` → `BEA_USER_ID` | Canonical → alias → None | Hook point (not yet implemented) |
| Case-Shiller | (uses existing mapper) | `map_zip_to_metro()` from `case_shiller_zip_mapper.py` | Implemented (ZIP → metro tagging) |

All enrichment is **optional**. The workflow runs fully offline without any API keys or internet access.

## CLI Usage

```bash
# Basic — auto-discovers dashboard, full_local mode
python corp_overlay_runner.py data/internal_loans.csv

# Explicit dashboard and corp_safe mode
python corp_overlay_runner.py data/loans.xlsx \
    --dashboard output/Bank_Performance_Dashboard_20260312.xlsx \
    --mode corp_safe

# Via environment variable
export REPORT_MODE=corp_safe
python corp_overlay_runner.py data/loans.csv --output-dir custom/output/path
```

## Artifact Details

| Artifact | Description | Mode |
|---|---|---|
| `loan_balance_by_product.png` | Descending horizontal bar chart of `current_balance` aggregated by `product_type` | FULL_LOCAL_ONLY |
| `top10_geography_by_balance.png` | Top 10 geographies (by resolved geo field) ranked by aggregate balance | FULL_LOCAL_ONLY |
| `internal_credit_flags_summary.html` | Distribution tables for risk_rating, delinquency_status, nonaccrual_flag; reduced to portfolio summary if optional columns absent | BOTH |
| `peer_vs_internal_mix_bridge.html` | Side-by-side: MSPBNA peer-report composition (SBL/CRE/Resi shares from dashboard) vs internal loan product/geography mix | BOTH |

## Reduced-Mode Behavior

- No `risk_rating` → credit flags summary shows portfolio summary only, no rating distribution
- No `delinquency_status` → no delinquency section
- No `nonaccrual_flag` → no nonaccrual section
- No dashboard found → bridge table shows "not available" for peer composition
- corp_safe mode → PNG charts skipped, HTML tables produced

## MSA-Level Macro Panel (Superseded)

`corp_overlay.py` includes `select_top_msas()` and `build_msa_macro_panel()`. These are **superseded** by `report_generator.py::plot_msa_macro_panel()` which reads workbook sheets produced by `local_macro.py`. The corp_overlay utilities are retained for backward compatibility only. See `@docs/claude/07-local-macro.md` for the current local macro architecture.

## Known Limitations

- The bridge table extracts composition from `Summary_Dashboard` sheet only. If the dashboard uses a different sheet layout, `load_dashboard_composition()` falls back to `FDIC_Data`.
- Dashboard auto-discovery uses the same `Bank_Performance_Dashboard_*.xlsx` glob pattern as `report_generator.py`.
- Loan file must be CSV, TSV, or Excel (.xlsx/.xls). Other formats are rejected.
- MSA macro panel data quality depends on API availability (BEA, BLS, Census, HUD). When APIs are unavailable, the pipeline produces empty DataFrames and a `Local_Macro_Skip_Audit` sheet; the chart is skipped cleanly.
