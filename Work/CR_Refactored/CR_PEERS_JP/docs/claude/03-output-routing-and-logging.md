# 03 — Output Routing & Logging

## Data Flow

```
FDIC API + FRED API
        │
        ▼
MSPBNA_CR_Normalized.py
        │
        ▼
output/Bank_Performance_Dashboard_YYYYMMDD.xlsx
        │
        ▼
report_generator.py
        │
        ├──► output/Peers/charts/   (Standard + Normalized deterioration charts,
        │                           Portfolio Mix, Segment Attribution,
        │                           Reserve Allocation, Migration Ladder,
        │                           Years-of-Reserves, Growth vs Deterioration,
        │                           Risk-Adjusted Return, Concentration vs Capital,
        │                           Liquidity Overlay, Macro Overlay PNGs)
        ├──► output/Peers/scatter/  (Standard + Normalized scatter plot PNGs)
        └──► output/Peers/tables/   (Standard + Normalized HTML tables, FRED macro table)
```

## Excel Sheet Layout

| Sheet Name | Contents |
|---|---|
| `FDIC_Data` | Quarterly metrics for subject bank + all peer CERTs |
| `Averages_8Q*` | Rolling 8-quarter averages used for scatter plots |
| `FRED_Data` | Macroeconomic time-series (Fed Funds, VIX, Unemployment, etc.) |
| `FRED_Descriptions` | Series ID ↔ Short Name mapping for FRED data |
| `FDIC_Metric_Descriptions` | Human-readable descriptions of FDIC metrics |
| `FRED_SBL_Backdrop` | SBL proxy series + spreads / regime flags |
| `FRED_Residential_Jumbo` | Jumbo rates, SLOOS, resi balances, delinquency, charge-offs |
| `FRED_CRE` | CRE balances, CLD, SLOOS, delinquency, charge-offs, prices |
| `FRED_CaseShiller_Master` | Full Case-Shiller registry (all discovered series) |
| `FRED_CaseShiller_Selected` | Curated Case-Shiller subset for dashboard |
| `FRED_Expansion_Registry` | Full metadata registry audit sheet |
| `Normalization_Diagnostics` | Over-exclusion flags, residuals, severity per CERT/quarter |
| `Peer_Group_Definitions` | 4 peer group definitions with member CERTs and metadata |
| `Resi_Normalized_Audit` | Residential metric values, mapping/label status, latest quarter |
| `Exclusion_Component_Audit` | Per-bank/quarter excluded category balances, NCO, balance-gating flags, dominant category |
| `Composite_Coverage_Audit` | Per-group/metric contributor counts, coverage %, NaN-out decisions for normalized composites |
| `Normalization_Reconciliation_Sample` | Latest-period reconciliation: Total vs Excluded vs Normalized for NCO and Loans, plus severity flags |
| `CaseShiller_Zip_Coverage` | One row per (Case-Shiller region, ZIP code) with HUD crosswalk ratios |
| `CaseShiller_Zip_Summary` | One row per metro with aggregate ZIP counts and mapping metadata |
| `CaseShiller_County_Map_Audit` | County-level FIPS mapping rules per S&P CoreLogic methodology |

For FRED expansion sheets, see `@docs/claude/10-coding-rules.md` (FRED Expansion Layer).
For local macro sheets, see `@docs/claude/07-local-macro.md`.

## Output File Naming

**Date-only naming** (YYYYMMDD, no HHMMSS). Same-day reruns overwrite previous artifacts. The Excel dashboard filename is `Bank_Performance_Dashboard_YYYYMMDD.xlsx`. The stem (`Bank_Performance_Dashboard_YYYYMMDD`) already contains the date, so report artifacts append only the artifact descriptor — **no second date suffix**.

- Excel dashboard: `Bank_Performance_Dashboard_YYYYMMDD.xlsx`
- Charts/scatters/tables use the dashboard stem (which already includes the date):
  - `{stem}_standard_credit_chart.png`
  - `{stem}_normalized_credit_chart.png`
  - `{stem}_executive_summary_standard.html`
  - `{stem}_executive_summary_normalized.html`
  - `{stem}_detailed_peer_table_standard.html`
  - `{stem}_detailed_peer_table_normalized.html`
  - `{stem}_core_pb_peer_table_standard.html`
  - `{stem}_core_pb_peer_table_normalized.html`
  - `{stem}_ratio_components_standard.html`
  - `{stem}_ratio_components_normalized.html`
  - `{stem}_cre_segment_standard.html` / `{stem}_cre_segment_normalized.html`
  - `{stem}_resi_segment_standard.html` / `{stem}_resi_segment_normalized.html`
  - `{stem}_fred_table.html`
  - `{stem}_portfolio_mix.png`
  - `{stem}_problem_asset_attribution.png`
  - `{stem}_reserve_risk_allocation.png`
  - `{stem}_migration_ladder.png`
  - `{stem}_scatter_nco_vs_npl.png`
  - `{stem}_scatter_pd_vs_npl.png`
  - `{stem}_scatter_norm_nco_vs_nonaccrual.png`
  - `{stem}_scatter_cre_nco_vs_nonaccrual.png`
  - `{stem}_scatter_norm_acl_vs_delinquency.png`
  - `{stem}_years_of_reserves.png`
  - `{stem}_growth_vs_deterioration.png`
  - `{stem}_growth_vs_deterioration_bookwide.png`
  - `{stem}_risk_adjusted_return.png`
  - `{stem}_concentration_vs_capital.png`
  - `{stem}_liquidity_overlay.png`
  - `{stem}_yoy_heatmap_standard_wealth.html`
  - `{stem}_yoy_heatmap_standard_allpeers.html`
  - `{stem}_yoy_heatmap_normalized_wealth.html`
  - `{stem}_yoy_heatmap_normalized_allpeers.html`
  - `{stem}_kri_bullet_standard.png`
  - `{stem}_kri_bullet_standard_coverage.png`
  - `{stem}_kri_bullet_normalized_rates.png`
  - `{stem}_kri_bullet_normalized_composition.png`
  - `{stem}_sparkline_standard_wealth.html`
  - `{stem}_sparkline_standard_allpeers.html`
  - `{stem}_sparkline_normalized_wealth.html`
  - `{stem}_sparkline_normalized_allpeers.html`
  - `{stem}_macro_corr_heatmap_lag1.html`
  - `{stem}_macro_overlay_credit_stress.png`
  - `{stem}_macro_overlay_rates_housing.png`
  - `{stem}_cumul_growth_loans_vs_acl_wealth.png`
  - `{stem}_cumul_growth_loans_vs_acl_allpeers.png`

Where `{stem}` = `Bank_Performance_Dashboard_YYYYMMDD`

---

## CSV Structured Logging

Each script produces its own CSV log file in `logs/`, reset (overwritten) each run:

| Script | Log File |
|---|---|
| `MSPBNA_CR_Normalized.py` | `logs/MSPBNA_CR_Normalized_YYYYMMDD_log.csv` |
| `report_generator.py` | `logs/report_generator_YYYYMMDD_log.csv` |

**CSV Schema (15 columns):**

| Column | Description |
|---|---|
| `timestamp` | ISO 8601 with milliseconds |
| `run_date` | YYYYMMDD date string |
| `script_name` | Identifies the producing script |
| `run_id` | Unique 12-char hex ID per run |
| `level` | INFO, WARNING, ERROR |
| `phase` | Pipeline stage (startup, data_fetch, processing, output, shutdown, etc.) |
| `component` | Subsystem (fdic, fred, excel, peer_composite, etc.) |
| `function` | Function name (optional) |
| `line_no` | Source line number (optional) |
| `event_type` | Structured event category (see below) |
| `message` | Human-readable log message |
| `exception_type` | Exception class name (for EXCEPTION events) |
| `exception_message` | Exception string (for EXCEPTION events) |
| `traceback` | Full traceback (for EXCEPTION events) |
| `context_json` | JSON-encoded context dict (optional) |

**Event types:** `CONFIG`, `FILE_DISCOVERED`, `FILE_WRITTEN`, `DATAFRAME_SHAPE`, `VALIDATION_WARNING`, `VALIDATION_ERROR`, `EXCEPTION`, `STDOUT`, `STDERR`, `CHART_SKIPPED`, `TABLE_SKIPPED`, `METRIC_SUPPRESSED`, `PRECHECK_FAIL`, `PRECHECK_WARN`

**stdout/stderr mirroring:** All console output (`print()` calls and stderr) is tee'd into the CSV log as `STDOUT`/`STDERR` events via `TeeToLogger`. Console output is preserved — the tee adds CSV rows without suppressing visible output.

**Centralized utilities** in `logging_utils.py`:
- `get_run_date_str()` — returns YYYYMMDD date string
- `build_artifact_filename(prefix, suffix, ext, output_dir)` — builds date-stamped filenames
- `CsvLogger` — structured CSV writer with convenience methods (`info`, `warning`, `error`, `log_exception`, `log_file_written`, `log_df_shape`)
- `TeeToLogger` — stream wrapper for stdout/stderr capture
- `setup_csv_logging(script_name)` — one-call setup (creates logger, installs tee, logs startup)

**Safe logging lifecycle:**
- `CsvLogger.log()` is a no-op after close — never raises `ValueError`
- `TeeToLogger.write()` always writes to the original stream first; if the CSV logger is closed, CSV logging is silently skipped — `print()` after close never crashes
- `CsvLogger.close()` restores `sys.stdout`/`sys.stderr` to originals before closing the file
- `CsvLogger.shutdown()` logs a final CONFIG message, restores streams, closes file — preferred over bare `close()`
- Both `close()` and `shutdown()` are idempotent (safe to call multiple times)
- `setup_csv_logging()` unwraps existing `TeeToLogger` instances to prevent nested wrapping
- Logging failures never crash the pipeline or mask real application exceptions
- Each script has exactly **one** terminal shutdown path:
  - `MSPBNA_CR_Normalized.py`: `csv_log.shutdown()` in `main()`'s `finally` block, after all prints
  - `report_generator.py`: `csv_log.shutdown()` in `generate_reports()`'s `finally` block
