# CR_PEERS_JP — Credit Risk Performance Reporting Engine

## 1. Project Overview

This repository is an **automated Credit Risk Performance reporting engine** for MSPBNA (Morgan Stanley Private Bank, National Association). The pipeline:

1. **Fetches** raw call-report data from the FDIC API and macroeconomic time-series from the FRED API.
2. **Processes** the data into standard and normalized credit-quality metrics, computes peer-group composites, and builds rolling 8-quarter averages.
3. **Outputs** a consolidated Excel dashboard (`Bank_Performance_Dashboard_*.xlsx`) containing multiple sheets (Summary_Dashboard, Normalized_Comparison, Latest_Peer_Snapshot, Averages_8Q_All_Metrics, FDIC_Metric_Descriptions, Macro_Analysis, FDIC_Data, FRED_Data, FRED_Metadata, FRED_Descriptions, Data_Validation_Report, Normalization_Diagnostics, Peer_Group_Definitions, Exclusion_Component_Audit, Composite_Coverage_Audit, Metric_Validation_Audit, and optional Case-Shiller ZIP sheets).
4. **Generates reports**: PNG credit-deterioration charts, PNG scatter plots, and HTML comparison tables — all routed to structured subdirectories under `output/Peers/`.

The two core scripts are:

| Script | Role |
|---|---|
| `MSPBNA_CR_Normalized.py` | Data fetch, processing, normalization, and Excel dashboard creation |
| `report_generator.py` | Reads the dashboard Excel and produces charts, scatters, and HTML tables |
| `metric_registry.py` | Derived metric specs, validation engine, dependency graph |
| `fred_series_registry.py` | Central FRED series registry (SBL, Resi, CRE, Case-Shiller) |
| `fred_case_shiller_discovery.py` | Async Case-Shiller release-table discovery |
| `fred_transforms.py` | Transforms, spreads, z-scores, regime flags |
| `fred_ingestion_engine.py` | Async FRED fetcher, validation, sheet routing, Excel output |
| `test_regression.py` | Regression tests: scatter integrity, peer groups, over-exclusion, validation |
| `logging_utils.py` | Centralized CSV logging, date-only artifact naming, stdout/stderr tee capture |
| `case_shiller_zip_mapper.py` | HUD USPS ZIP Crosswalk enrichment for Case-Shiller metros |
| `corp_overlay.py` | Corp-safe overlay: loan-file ingestion, schema contracts, peer-vs-internal join, 4 artifacts |
| `corp_overlay_runner.py` | Standalone CLI entrypoint for corp overlay workflow (not in report_generator.py) |

---

## 2. Build & Execution Commands

Run the pipeline **sequentially** in a terminal (PowerShell on Windows):

```powershell
# Activate the virtual environment
.\venv\Scripts\Activate

# Install / update dependencies
pip install -r requirements.txt

# Step 1 — Data Fetch & Processing (produces the Excel dashboard)
python MSPBNA_CR_Normalized.py

# Step 2 — Report Generation (produces charts, scatters, HTML tables)
python report_generator.py
```

### Required Environment Variables

| Variable | Purpose | Example |
|---|---|---|
| `FRED_API_KEY` | FRED API authentication (required by Step 1) | `export FRED_API_KEY='abc123'` |
| `MSPBNA_CERT` | Subject bank CERT number (dynamic, no hardcoding) | `34221` |
| `MSBNA_CERT` | Secondary bank CERT number | `32992` |
| `SUBJECT_BANK_CERT` | Used by `MSPBNA_CR_Normalized.py` | `34221` |
| `MS_COMBINED_CERT` | MS Combined Entity CERT (default `88888`) | `88888` |
| `REPORT_VIEW` | Controls table filtering logic | `ALL_BANKS` or `MSPBNA_WEALTH_NORM` |
| `HUD_USER_TOKEN` | HUD USPS Crosswalk API bearer token (required for ZIP enrichment) | `export HUD_USER_TOKEN='eyJ...'` |
| `HUD_CROSSWALK_YEAR` | Optional crosswalk vintage year | `2025` |
| `HUD_CROSSWALK_QUARTER` | Optional crosswalk vintage quarter (1-4) | `4` |
| `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` | Enable/disable ZIP enrichment (default `true`) | `true` or `false` |
| `REPORT_MODE` | Render mode for report_generator (canonical). Values: `full_local`, `corp_safe` | `full_local` |
| `REPORT_RENDER_MODE` | Backward-compatible alias for `REPORT_MODE` | `full_local` |
| `BEA_API_KEY` | BEA API authentication (canonical, optional) | `export BEA_API_KEY='abc'` |
| `BEA_USER_ID` | Backward-compatible alias for `BEA_API_KEY` | `export BEA_USER_ID='abc'` |
| `CENSUS_API_KEY` | Census API authentication (optional) | `export CENSUS_API_KEY='abc'` |

These can be set in a `.env` file in the project root or exported in the shell.

**Env var alias resolution priority:**
- Render mode: explicit arg → `REPORT_MODE` → `REPORT_RENDER_MODE` → `full_local` (default)
- BEA key: `BEA_API_KEY` → `BEA_USER_ID` → `None` (optional, no force-fail)
- Census key: `CENSUS_API_KEY` → `None` (optional, no force-fail)

### Key Dependencies

`aiohttp`, `matplotlib`, `numpy`, `openpyxl`, `pandas`, `python-dotenv`, `requests`, `scipy`, `seaborn`, `tqdm`

---

## 3. Project Architecture & Output Routing

### Data Flow

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

### Excel Sheet Layout

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
| `CaseShiller_Zip_Coverage` | One row per (Case-Shiller region, ZIP code) with HUD crosswalk ratios |
| `CaseShiller_Zip_Summary` | One row per metro with aggregate ZIP counts and mapping metadata |
| `CaseShiller_County_Map_Audit` | County-level FIPS mapping rules per S&P CoreLogic methodology |

### Output File Naming

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
  - `{stem}_years_of_reserves.png`
  - `{stem}_growth_vs_deterioration.png`
  - `{stem}_risk_adjusted_return.png`
  - `{stem}_concentration_vs_capital.png`
  - `{stem}_liquidity_overlay.png`
  - `{stem}_yoy_heatmap_standard.html`
  - `{stem}_yoy_heatmap_normalized.html`
  - `{stem}_kri_bullet_standard.png`
  - `{stem}_kri_bullet_normalized_rates.png`
  - `{stem}_kri_bullet_normalized_composition.png`
  - `{stem}_sparkline_summary.html`
  - `{stem}_macro_corr_heatmap_lag1.html`
  - `{stem}_macro_overlay_credit_stress.png`
  - `{stem}_macro_overlay_rates_housing.png`

Where `{stem}` = `Bank_Performance_Dashboard_YYYYMMDD`

### CSV Structured Logging

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

---

## 3a. Dual-Mode Rendering Architecture

### Overview

`report_generator.py` supports two rendering modes, controlled by the `render_mode` parameter or the `REPORT_MODE` environment variable (with `REPORT_RENDER_MODE` as a backward-compatible alias):

| Mode | Description | Default? |
|---|---|---|
| `full_local` | All artifacts produced using matplotlib/seaborn/openpyxl. Equivalent to pre-refactor behaviour. | **Yes** |
| `corp_safe` | HTML tables only. All matplotlib-based charts and scatter plots are skipped gracefully with clear log messages. Designed for locked-down corporate environments without rich plotting libraries. | No |

### Mode Selection Priority

1. Explicit `render_mode` argument to `generate_reports()`
2. `REPORT_MODE` environment variable (canonical)
3. `REPORT_RENDER_MODE` environment variable (backward-compatible alias)
4. Default: `full_local`

### Key Modules

| Module | Role |
|---|---|
| `rendering_mode.py` | `RenderMode` enum, `select_mode()`, `ArtifactCapability`, `ArtifactManifest`, `ARTIFACT_REGISTRY`, `should_produce()` |
| `report_generator.py` | Consumes `rendering_mode` — each artifact block is guarded by `should_produce()` |

### Capability Matrix

Each artifact declares its availability:

- **`BOTH`** — available in `full_local` and `corp_safe`. All HTML table artifacts use this.
- **`FULL_LOCAL_ONLY`** — requires matplotlib/seaborn. All PNG chart/scatter artifacts use this.

When an artifact is not available in the current mode, `should_produce()`:
1. Records a skip in the `ArtifactManifest` with a human-readable reason
2. Prints `[SKIP] <reason>` to the console
3. Returns `False` so the caller skips the block

### Artifact Skip Semantics

Artifacts can be skipped for two independent reasons:
1. **Mode-based skip**: The artifact's `ArtifactAvailability` does not include the current `RenderMode`.
2. **Preflight suppression**: The `validate_output_inputs()` preflight adds artifact names to `suppressed_charts` (e.g., when a composite CERT has material over-exclusion).

Both reasons are logged in the manifest. Skipping is always intentional and never silent.

### Artifact Manifest

Every `generate_reports()` run produces an `ArtifactManifest` object (also returned to callers). The manifest tracks:
- **artifact name** — canonical identifier from `ARTIFACT_REGISTRY`
- **mode** — the render mode used
- **status** — `generated`, `skipped`, or `failed`
- **path** — file path for generated artifacts
- **skip_reason** — human-readable reason for skipped artifacts
- **error** — error message for failed artifacts

The manifest summary table is printed at the end of every run.

### CLI Usage

```bash
# Default (full_local) — identical to pre-refactor behaviour
python report_generator.py

# Explicit full_local
python report_generator.py full_local

# Corporate-safe mode (tables only, no matplotlib)
python report_generator.py corp_safe

# Via environment variable
export REPORT_MODE=corp_safe
python report_generator.py

# Or via backward-compatible alias
export REPORT_RENDER_MODE=corp_safe
python report_generator.py
```

### Adding New Artifacts

1. Register the artifact in `rendering_mode.py` using `_reg()`:
   ```python
   _reg("my_new_chart", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
        "Description of my new chart")
   ```
2. Guard the production block in `generate_reports()`:
   ```python
   if should_produce("my_new_chart", mode, manifest, suppressed_charts):
       # ... produce the artifact ...
       manifest.record_generated("my_new_chart", str(path))
   ```

### Canonical Rendering Abstraction Rule

**`rendering_mode.py` is the single canonical source** for all rendering abstractions. `report_generator.py` must NOT define its own copies of:
- `RenderMode` / `ReportMode`
- `ArtifactStatus`, `ArtifactSpec`, `ArtifactCapability`
- `ManifestEntry`, `ArtifactManifest`
- `ARTIFACT_REGISTRY`
- `should_produce()`
- `resolve_report_mode_for_generator()`

All of these must be imported from `rendering_mode.py`. The `_ReportContext` dataclass in `report_generator.py` is a lightweight internal carrier and is NOT a duplicate of any rendering-mode type.

### Executive Chart Artifacts (6 Artifacts)

Implemented in `executive_charts.py`, integrated into `report_generator.py` Phase 8.

| Artifact | File | Mode | Type |
|---|---|---|---|
| `yoy_heatmap_standard` | `{stem}_yoy_heatmap_standard.html` | BOTH | HTML |
| `yoy_heatmap_normalized` | `{stem}_yoy_heatmap_normalized.html` | BOTH | HTML |
| `kri_bullet_standard` | `{stem}_kri_bullet_standard.png` | FULL_LOCAL_ONLY | PNG |
| `kri_bullet_normalized_rates` | `{stem}_kri_bullet_normalized_rates.png` | FULL_LOCAL_ONLY | PNG |
| `kri_bullet_normalized_composition` | `{stem}_kri_bullet_normalized_composition.png` | FULL_LOCAL_ONLY | PNG |
| `sparkline_summary` | `{stem}_sparkline_summary.html` | BOTH | HTML |

**Integration pattern:**
- Heatmaps loop over `[False, True]` for standard/normalized variants
- Standard bullet chart is a single artifact; normalized is split into two (rates vs composition) with separate metric lists and axis scales
- Bullet charts use correct composite CERTs per variant: standard → 90001/90003, normalized → 90004/90006
- Sparkline passes `norm_peer_cert=90006` for Norm_ metric rows, `peer_cert=90003` for standard rows
- The obsolete single artifact names `kri_bullet_chart` and `kri_bullet_normalized` are removed from the registry

**Normalized bullet chart split:**
- `kri_bullet_normalized_rates` — 5 rate metrics: Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage
- `kri_bullet_normalized_composition` — 5 composition metrics: Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share

**Comparator fallback behavior:**
- Both comparators available → gray band between min and max
- Single comparator available → thin vertical reference marker (NOT collapse to subject value)
- Neither comparator available → metric row skipped entirely (NOT drawn with subject-to-subject band)

### Deterministic Macro Chart Artifacts (3 Artifacts)

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

### Remaining Risks

1. **Executive charts import guard**: `_HAS_EXECUTIVE_CHARTS` flag means if `executive_charts.py` fails to import (e.g., `metric_semantics.py` missing), all 5 executive artifacts silently skip. No manifest entry is recorded for the skip.
2. **FRED data dependency**: Macro chart artifacts depend on FRED_Data sheet being present in the workbook. If `MSPBNA_CR_Normalized.py` was run without FRED_API_KEY, macro charts silently return None.
3. **matplotlib tight_layout warning**: The `warnings.filterwarnings` suppression in `report_generator.py` masks a real twinx() incompatibility in macro overlay charts. The charts render correctly but may have suboptimal spacing.
4. **Normalized metric coverage**: Macro correlation heatmap rows use normalized metrics that may be NaN for some banks due to over-exclusion. N/A cells are shown correctly but reduce information density.

---

## 4. Strict Coding Conventions & Rules

These are **non-negotiable** for any agent editing this codebase:

### ALWAYS UPDATE CLAUDE.md

- Whenever you make architectural changes, add new charts, or change data pipelines, you **must** update this `CLAUDE.md` file to review restrictions, track to-do items, identify potential gaps, and inform future coding agents.

### FDIC API Variables & FFIEC Waterfall

- Always prefer FDIC top-level text aliases (e.g., `ASSET`, `NTCI`) over raw MDRM codes (`RCFD...`, `RIAD...`). When raw codes are necessary, you **MUST** fetch both the `RCFD` (Consolidated) and `RCON` (Domestic) codes. You must then coalesce them (`RCFD.fillna(RCON)`) to ensure apples-to-apples comparisons between international G-SIBs (FFIEC 031) and domestic-only banks (FFIEC 041/051). Never force internationally active banks to strictly use `RCON`, as this strips out their foreign office balances. Synthetic local variables must never be included in the raw API fetch request.

### NO HARDCODING

- Subject bank CERTs **must** be loaded dynamically via `int(os.getenv("MSPBNA_CERT", "34221"))`.
- Never hardcode CERT numbers directly. Always use the config/env pattern. Production defaults: `MSPBNA_CERT=34221`, `MSBNA_CERT=32992`.
- The FRED API key must come from `os.getenv('FRED_API_KEY')` or `.env`.

### IMPORT SAFETY

- `MSPBNA_CR_Normalized.py` must be importable **without** setting environment variables, altering `cwd`, printing to stdout, or opening log files. All side effects live in `main()` (via `_validate_runtime_env()`, `setup_logging()`, `os.chdir()`).
- Module-level `MSPBNA_CERT` and `MSBNA_CERT` use `os.getenv()` with defaults for import safety. The hard validation (ValueError on missing vars) happens at runtime in `_validate_runtime_env()`.
- `csv_log` and `logger` are initialized to `None` at module level and set by `setup_logging()` inside `main()`.

### DEPRECATED FUNCTIONS

- `generate_normalized_comparison_table()` in `report_generator.py` is **deprecated** — raises `NotImplementedError`. It produced a mixed standard-vs-normalized artifact violating the single-regime-per-artifact rule. Use the separate standard/normalized table generators instead.

### SCATTER & CHART COMPOSITE HANDLING

- **Active composite regime** is defined at the top of `report_generator.py`:
  - `ACTIVE_STANDARD_COMPOSITES = {"core_pb": 90001, "all_peers": 90003}`
  - `ACTIVE_NORMALIZED_COMPOSITES = {"core_pb": 90004, "all_peers": 90006}`
  - `INACTIVE_LEGACY_COMPOSITES = {90002, 90005, 99998, 99999}`
- All chart/table builders MUST use these canonical constants. **Never** hardcode 99998, 99999, 90002, or 90005 as peer-average selectors.
- `ALL_COMPOSITE_CERTS` includes both active and legacy CERTs for scatter-dot exclusion only.
- `plot_scatter_dynamic()` defaults: `peer_avg_cert_primary=90003, peer_avg_cert_alt=90001`. Normalized call sites pass `90006/90004` explicitly.
- If an active composite is missing from data, the chart/table MUST skip (not silently substitute a legacy CERT). Use `validate_composite_cert_regime()` for preflight checks.
- The former `build_plot_df_with_alias()` function has been removed — it appended duplicate rows that contaminated scatter plots. Do not re-introduce it.

### PEER GROUPINGS

There are **4 peer groups** (2 standard + 2 normalized). Composite CERTs are assigned via `base_dummy_cert + display_order`:

| Table Type | Peer 1 | Peer 2 |
|---|---|---|
| **Standard** | 90001 — Core PB | 90003 — All Peers |
| **Normalized** | 90004 — Core PB Norm | 90006 — All Peers Norm |

The former MSPBNA+Wealth groups (90002/90005) were removed as duplicate cert membership. `validate_peer_group_uniqueness()` enforces that no two groups sharing the same `use_normalized` flag may have identical sorted cert lists.

**Cross-mode duplication by design**: 90001/90004 and 90003/90006 share identical member CERTs. The distinction is `use_normalized`: standard composites NaN-out `Norm_*` columns; normalized composites NaN-out standard rate columns. Hard validation (`validate_peer_group_uniqueness()`) ensures no two groups within the SAME `use_normalized` mode share identical cert lists.

**Peer_Group_Definitions sheet**: A new Excel sheet documents all 4 peer group definitions with member CERTs, use cases, and display order.

### MS COMBINED ENTITY

- CERT `88888` (MS Combined Entity) must be **filtered out** of HTML table listings when `REPORT_VIEW == "MSPBNA_WEALTH_NORM"`.
- Load via `MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))`.

### COVERAGE vs SHARE vs x-MULTIPLE LABEL RULE (Non-Negotiable)

Every ratio-component row label **must** match its denominator type:

| Denominator Type | Correct Label Term | Display Format | Examples |
|---|---|---|---|
| Exposure / loan base (Gross_Loans, RIC_CRE_Cost, Wealth_Resi_Balance, CRE_Investment_Pure_Balance) | **"Coverage"** or **"Ratio"** | % | `RIC_CRE_ACL / RIC_CRE_Cost` → "CRE ACL Coverage" |
| ACL pool (Total_ACL, Norm_ACL_Balance) | **"Share"** or **"% of ACL"** | % | `RIC_CRE_ACL / Total_ACL` → "CRE % of ACL" |
| Nonaccrual / NPL base (RIC_CRE_Nonaccrual, RIC_Resi_Nonaccrual) | **"NPL Coverage"** | x-multiple | `RIC_CRE_ACL / RIC_CRE_Nonaccrual` → "CRE NPL Coverage" (1.23x) |

**If denominator is `Total_ACL` or `Norm_ACL_Balance`, the label must NEVER contain "Coverage".**

### `_METRIC_FORMAT_TYPE` MAINTENANCE RULE

`_METRIC_FORMAT_TYPE` in `report_generator.py` is the **explicit registry** for x-multiple formatted metrics. Rules:

1. Only **NPL coverage** metrics (denominator = nonaccrual or past-due) belong in this dict.
2. Any **new** NPL coverage metric MUST be added here explicitly — there is no auto-detection.
3. Loan-coverage and share-of-ACL metrics must **NEVER** be added — they default to percent.
4. Current entries: `RIC_CRE_Risk_Adj_Coverage`, `RIC_Resi_Risk_Adj_Coverage`, `RIC_Comm_Risk_Adj_Coverage`.

### HUD TOKEN — ATTRIBUTE-ONLY ACCESS

`DashboardConfig` is a plain Python class (not a dict). HUD token access rules:

- **Field**: `hud_user_token: Optional[str] = None` on `DashboardConfig`
- **Write**: `config.hud_user_token = _hud_token` (attribute assignment, **never** `config["..."]`)
- **Read**: `getattr(self.config, "hud_user_token", None)` (safe attribute access, **never** `.get()`)
- **Pass**: `build_case_shiller_zip_sheets(hud_user_token=self.config.hud_user_token)` (explicit kwarg)
- **Resolve**: Single call to `resolve_hud_token()` in `_validate_runtime_env()` — no competing resolution paths

Dict-style access (`config["key"]`, `config.get("key")`) will raise `TypeError` on `DashboardConfig` and is forbidden.

### CHARTING STYLES

All charts must use this color palette consistently:

| Entity | Hex Color | Description |
|---|---|---|
| **MSPBNA (Subject Bank)** | `#F7A81B` | Gold |
| **All Peers** | `#5B9BD5` or `#4C78A8` | Blue |
| **Core PB / Peers (Ex. F&V)** | `#70AD47` or `#9C6FB6` | Green or Purple |

**Do not alter** the internal matplotlib/seaborn plotting logic in `create_credit_deterioration_chart_ppt` or `plot_scatter_dynamic` without explicit permission from the project owner.

### CSS CLASS NAMING

- Use `mspbna-row` and `mspbna-value` (not `idb-row` / `idb-value`).
- All user-facing labels, HTML headers, and filenames must reference **MSPBNA**, never **IDB**.

### ENTITY DISPLAY LABEL POLICY

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

### CHART METRICS

**Standard chart:** Bar = `TTM_NCO_Rate`, Line = `NPL_to_Gross_Loans_Rate`
**Normalized chart:** Bar = `Norm_NCO_Rate`, Line = `Norm_Nonaccrual_Rate` (fallback: `Norm_NPL_to_Gross_Loans_Rate`)

Both standard and normalized credit-deterioration charts are generated.

### FRED Deduplication

- `series_ids` must always be deduplicated before async fetching to prevent `ValueError: cannot reindex on an axis with duplicate labels`.
- Use `list(dict.fromkeys(series_ids))` to preserve order while removing duplicates.
- The guard must exist both at construction time (when building `series_ids_to_fetch`) and at the entry point of `fetch_all_series_async()`.

### FRED Frequency Inference

- `infer_freq_from_index(idx)` (module-level in `MSPBNA_CR_Normalized.py`) infers series frequency from a `DatetimeIndex` when FRED metadata is missing or marked "Unknown".
- The helper is **heuristic and fail-safe**: it tries `pd.infer_freq()` first, then median-obs-per-year, then distinct-months-per-year, and falls back to `("quarterly", "Q")` on any unexpected failure.
- **Never** use `Series.groupby(lambda x: x[0])` on a Series of tuples — pandas passes the integer index label, not the tuple value. Use `DataFrame.groupby()` on named columns instead.
- The helper should never crash the FRED pipeline. All code paths are wrapped in `try/except`.

### FRED Series Validation

- The FRED API returns HTTP 400 Bad Request for discontinued or mistyped series IDs. Always verify IDs against the FRED website before adding them to `FRED_SERIES_TO_FETCH`.
- Known corrections: `CORCCACBS` (not `CORCCLACBS`), `RHVRUSQ156N` (not `RCVRUSQ156N`).
- Discontinued series must be removed, not left in the fetch list (e.g., `GOLDAMGBD228NLBM` was discontinued by FRED).
- Redundant series should be removed if covered by another ID (e.g., `DEPALL` removed in favor of `DPSACBW027SBOG`).

### STOCK vs FLOW MATH CONVENTION

Call Report variables fall into two categories that require different math:

| Type | Variables | Math Treatment |
|---|---|---|
| **Stock** (point-in-time) | Balances, Delinquency (PD30/PD90), Nonaccrual, ACL | Use directly. No TTM prefix. Delinquency metric is `Past_Due_Rate` (NOT `TTM_Past_Due_Rate`). |
| **Flow** (cumulative YTD) | NCO, Income, Provision, Interest Expense | Convert YTD → discrete quarterly via `ytd_to_discrete()`, then `rolling(4).sum()` for TTM. |

**Income-statement annualization**: Loan Yield (`Loan_Yield_Proxy`) and Provision Rate (`Provision_to_Loans_Rate`) use `annualize_ytd()` which computes `YTD_value * (4.0 / quarter)`. This is the standard banking convention — it gives a current-period-only view without mixing in prior-year stale quarters.

**Module-level helpers** in `MSPBNA_CR_Normalized.py`:
- `ytd_to_discrete(df, col_name)` — YTD → discrete quarterly flows
- `annualize_ytd(df, col_name)` — YTD → annualized rate

### WEALTH-FOCUSED vs DETAILED TABLE DISTINCTION

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

---

## 5. Common Errors & Troubleshooting

### DNS / Proxy Error: `[Errno 11001] getaddrinfo failed`

**Symptom:** Step 1 (`MSPBNA_CR_Normalized.py`) fails when trying to reach `banks.data.fdic.gov` or `api.stlouisfed.org`.

**Cause:** The corporate VPN or firewall is blocking outbound HTTPS requests to the FDIC and FRED APIs.

**Fix (PowerShell):**

```powershell
# Set proxy environment variables before running Step 1
$env:HTTP_PROXY  = "http://your-corporate-proxy:port"
$env:HTTPS_PROXY = "http://your-corporate-proxy:port"

# Then run the pipeline
python MSPBNA_CR_Normalized.py
```

Alternatively, disconnect from the VPN before running Step 1, then reconnect afterward.

### Missing FRED_API_KEY

**Symptom:** `ValueError: FRED_API_KEY not found`

**Fix:** Create a `.env` file in the project root:

```
FRED_API_KEY=your_key_here
SUBJECT_BANK_CERT=34221
```

Or export it directly: `export FRED_API_KEY='your_key_here'`

### No Excel File Found

**Symptom:** `report_generator.py` prints `ERROR: No Excel files found in output/`

**Fix:** Run Step 1 first (`python MSPBNA_CR_Normalized.py`). The report generator reads the latest `Bank_Performance_Dashboard_*.xlsx` from the `output/` directory.

### Missing Sheet in Excel

**Symptom:** `FileNotFoundError: 8Q average sheet not found`

**Fix:** Ensure `MSPBNA_CR_Normalized.py` completed successfully and the output Excel contains the `Averages_8Q*` sheet. Re-run Step 1 if needed.

---

## 6. Normalization Conventions

### Top-Down Normalization with Over-Exclusion Detection

Normalized metrics use `calc_normalized_residual(total, excluded, label, tolerance_pct=0.05)` which returns:
- `final_value`: residual (total - excluded), with minor over-exclusions (<=5%) clipped to 0 and material ones set to NaN
- `severity`: one of `ok`, `minor_clip`, `material_nan`
- 15 diagnostics columns are written to the `Normalization_Diagnostics` sheet

### Normalized Ratio Components

`generate_ratio_components_table(is_normalized=True)` uses:
- **Delinquency numerator**: `_Norm_Total_Past_Due` (synthesized as `Norm_PD30 + Norm_PD90`)
- **ACL numerator**: `Norm_ACL_Balance` (not `Total_ACL`)
- **Risk-adjusted denominator**: `Norm_Risk_Adj_Gross_Loans` (= `Norm_Gross_Loans - SBL_Balance`)
- **Resi ACL Coverage**: `RIC_Resi_ACL / Wealth_Resi_Balance`

### Supported Exclusion Stack (Normalized Universe)

The normalized exclusion engine removes 7 categories from gross totals. Each
category must have internally consistent balance, NCO, nonaccrual, and past-due
exclusion mappings. NDFI credit-quality numerators are **not** directly isolated
in Call Report-only mode and are set to zero rather than approximated.

| Category | Balance | NCO (YTD) | Nonaccrual | PD30 | PD90 | Notes |
|---|---|---|---|---|---|---|
| **C&I** | LNCI | NTCI/RIAD4638 (net) | NACI | P3CI | P9CI | Balance-gated NCO |
| **NDFI** | J454 | **0.0** | **0.0** | **0.0** | **0.0** | Balance only; all risk numerators unsupported |
| **ADC** | LNRECONS | NTLS/RIAD4658-4659 (net) | NARECONS | P3RECONS | P9RECONS | Balance-gated NCO |
| **Credit Card** | RCFDB538 | RIADB514-B515 (net) | RCFDB575 | P3CRCD | P9CRCD | Balance-gated NCO |
| **Auto** | LNAUTO | RIADK205-K206 (net) | RCFDK213 | P3AUTO | P9AUTO | Balance-gated NCO |
| **Ag** | LNAG | RIAD4635-4645 (net) | RCFD5341 | P3AG | P9AG | Balance-gated NCO; PD audit-flagged if absent |
| **OO CRE** | LNRENROW | NTRENROW (net) | NARENROW | P3RENROW | P9RENROW | Balance-gated NCO |

### Balance-Gating for Excluded NCO Categories

Six excluded NCO categories (C&I, Credit Card, Auto, Ag, ADC, OO CRE) are balance-gated: if the excluded balance for a category is zero, the excluded NCO is forced to zero regardless of MDRM field values. This prevents misclassification propagation (e.g., MSPBNA showing $27K Auto NCO with zero Auto balance). Each gating decision produces a `_*_NCO_Gated` flag column. The `Exclusion_Component_Audit` sheet documents per-bank/quarter gating decisions, dominant exclusion categories, and flags where balance is zero but NCO was nonzero.

| Category | Balance Column | NCO Column | Flag Column |
|---|---|---|---|
| C&I | `Excl_CI_Balance` | `Excl_CI_NCO_YTD` | `_CI_NCO_Gated` |
| Credit Card | `Excl_CreditCard_Balance` | `Excl_CC_NCO_YTD` | `_CC_NCO_Gated` |
| Auto | `Excl_Auto_Balance` | `Excl_Auto_NCO_YTD` | `_Auto_NCO_Gated` |
| Ag | `Excl_Ag_Balance` | `Excl_Ag_NCO_YTD` | `_Ag_NCO_Gated` |
| ADC | `Excl_ADC_Balance` | `Excl_ADC_NCO_YTD` | `_ADC_NCO_Gated` |
| OO CRE | `Excl_OO_CRE_Balance` | `Excl_OO_CRE_NCO_YTD` | `_OO_CRE_NCO_Gated` |

### Structured Audit Flags

The following audit flags are persisted per row in the `Exclusion_Component_Audit` sheet:

| Flag | Meaning |
|---|---|
| `_audit_unsupported_ndfi_pdna` | Always True — NDFI PD/NA numerators set to 0.0 (no CR-only mapping) |
| `_audit_ndfi_nco_unsupported` | Always True — NDFI NCO set to 0.0 (no valid direct CR field) |
| `_audit_ag_pd_fallback_to_zero` | True when both P3AG and P9AG are zero (fields absent for this bank) |
| `_audit_resi_balance_fallback_used` | True when RC-C components were zero and LNRERES fallback was used |
| `category_balance_zero_but_nco_nonzero_flag` | True when any category had zero balance but nonzero raw NCO (gated to 0) |

### Normalized Composite Minimum Coverage

Normalized composites (90004/90006) must have ≥50% non-NaN contributor share per critical metric, otherwise the composite metric is NaN'd out. This prevents misleading composites when only 2 of 8 banks have usable normalized data. The `Composite_Coverage_Audit` sheet documents per-group/metric contributor counts, coverage percentages, and NaN-out decisions.

**Critical normalized metrics for coverage checks:**
- `Norm_NCO_Rate`, `Norm_Nonaccrual_Rate`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Gross_Loans`

**Important**: Normalized NCO is NOT fully solved — many G-SIBs still produce `material_nan` severity because their excluded categories exceed totals by >5%. The composite coverage threshold mitigates misleading averages but does not fix the upstream data quality issue.

### Case-Shiller ZIP Enrichment

Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` env var (default `true`). When disabled, `build_case_shiller_zip_sheets()` returns audit with `SKIPPED` status. Uses county-level FIPS codes (S&P CoreLogic methodology) joined to HUD County-to-ZIP crosswalk (type=7) for exact geographic mapping. Maps 5-digit ZIP codes to 20 regional Case-Shiller metros via ~160 constituent counties.

### IDB Label Convention

Dictionary keys in `master_data_dictionary.py` must **never** use the `IDB_` prefix. All former `IDB_*` keys have been renamed (e.g., `IDB_CRE_Growth_TTM` → `CRE_Growth_TTM`). User-facing labels, CSS classes, and HTML headers must reference **MSPBNA**, never **IDB**.

### Curated Presentation Tabs

`Summary_Dashboard` and `Normalized_Comparison` use curated metric allowlists (`SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS` in `MSPBNA_CR_Normalized.py`). Only approved KPIs appear in these presentation-facing tabs. Raw MDRM fields and internal pipeline columns are excluded. The full dataset remains available in the `FDIC_Data` sheet.

### Display Label Policy

All presentation tabs use `_get_metric_short_name(code)` to resolve display names from `FDIC_Metric_Descriptions` (via `MasterDataDictionary`). Columns: `Metric Code` (technical field name) + `Metric Name` (display label). Falls back to the code itself when no display label exists. Debug logging is emitted for any fallback.

**Validation note**: Every metric in `SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS` must have a corresponding entry in `LOCAL_DERIVED_METRICS` (Tier 3) or resolve via MDRM/FDIC API (Tiers 1-2). Regression tests in `TestWorkbookLevelCuration` and `TestDisplayLabelCoverage` enforce this. Presentation-layer fixes are only considered complete once visible in the generated workbook — source-code-level intent alone is insufficient.

### Metric Role Classification

Metrics are classified as **evaluative** (risk/return/coverage — receives performance flags) or **descriptive** (size/balance/composition — no evaluative flags). `DESCRIPTIVE_METRICS` frozenset in `MSPBNA_CR_Normalized.py` lists all descriptive metrics. `_get_performance_flag()` returns blank for descriptive metrics to prevent misleading "Top Quartile" / "Bottom Quartile" flags on non-evaluative fields like ASSET or LNLS.

### Norm_Provision_Rate Treatment

`Norm_Provision_Rate` is intentionally set to NaN in the pipeline — provision expense (`ELNATR`) is not segment-specific in call reports, so a normalized rate would be semantically misleading. It is excluded from `NORMALIZED_COMPARISON_METRICS` (presentation tab), and dead-metric suppression in HTML tables catches any remaining instances. It should never be presented as a normal KPI or as a silent zero.

---

### Normalized Segment Taxonomy (Loan-Segmentation Alignment)

The following rules govern the normalized segment definitions. They were aligned
to Call Report schedule RC-C in the 2026-03-12 taxonomy cleanup.

| Segment | Balance Definition | Notes |
|---|---|---|
| **Wealth Resi** | RC-C components (1797+5367+5368) preferred; fallback: **LNRERES alone** (includes HELOC/open-end). LNRELOC is NOT added to avoid double-counting. | Numerators (NCO, PD, NA) still use split fields (NTRERES+NTRELOC, etc.) |
| **C&I Exclusion** | `Excl_CI_Balance = LNCI` (or best_of RCON1763/RCFD1763) directly. SBL is NOT subtracted — SBL lives under RC-C item 9, not item 4. | |
| **NDFI / Fund Finance** | J454 retained for balance exclusion. **J458/J459/J460 removed** — not Call Report-consistent for PD/NA. All NDFI PD/NA numerators set to 0.0. | LIMITATION: CR-only PD/NA mapping is unsupported for NDFI. |
| **Agricultural PD** | **RCON2746/RCFD2746 and RCON2747/RCFD2747 removed** (mis-mapped — those are "All other loans" PD). Replaced with P3AG/P9AG. | |
| **CRE Investment Pure** | `LNREMULT + LNRENROT` only. **LNREOTH removed** from the calculation path. | Owner-occupied CRE excluded separately. |
| **Tailored Lending** | **Unsupported** in Call Report-only mode. No proxy from fine art, aircraft, bespoke HNW unsecured, J451, or other CR proxies. Internal product tags would be required. | |

### Known Limitations (Segment Taxonomy)

- **NDFI credit quality**: Call Report does not publish NDFI-specific delinquency/nonaccrual.
  J458/J459/J460 were removed; PD/NA exclusion numerators are 0.0 until an internal data
  source is integrated.
- **Tailored Lending**: Cannot be inferred from Call Report data. Requires internal product tags.
  No proxy math is present in this codebase.
- **Ag PD fallback**: If P3AG/P9AG are absent from the FDIC dataset for a given bank, the
  exclusion PD defaults to 0.0 via `best_of(...).fillna(0)`. This is preferred over mis-mapping
  RCON2746/2747.

### Segment Support Boundaries

This table documents what the Call Report can and cannot identify for each
wealth-management loan segment. Presentation-facing labels must not overstate
the precision available.

| Segment | Balance | Risk Metrics (NCO/PD/NA) | Boundary Notes |
|---|---|---|---|
| **SBL** | Supported (RCFD1545/RCON1545, fallback LNOTHER) | **Proxy only** — no SBL-specific NCO, PD, or NA fields in Call Report. "All Other Loans" numerators would mix SBL with other uncategorised lending. Not shown in presentation. | SBL balance is clean; SBL risk rates are NOT computed. |
| **Wealth Resi** | Supported (1-4 family incl. HELOC via LNRERES) | Supported via split MDRM fields (NTRERES+NTRELOC, P3RERES+P3RELOC, etc.) | Jumbo is **not** separately identifiable in Call Report. The segment covers all 1-4 family residential including HELOCs and open-end lines. |
| **CRE Investment** | Supported (LNREMULT + LNRENROT) | Supported (NTREMULT+NTRENROT, P3REMULT+P3RENROT, P9REMULT+P9RENROT, NAREMULT+NARENROT) | Multifamily + non-owner-occupied nonfarm only. Excludes ADC (construction) and owner-occupied CRE, which are separate segments. |
| **Tailored Lending** | **Not segmented** in Call Report | **Not available** | Fine art, aircraft, bespoke HNW unsecured, etc. cannot be identified from any Call Report schedule. Requires internal product-level tags. J451 (total to nondepository institutions) is NOT a tailored-lending proxy. |
| **NDFI / Fund Finance** | Supported (J454 balance only) | **Not available** — all set to 0.0 with audit flags | CR-only mode has no NDFI-specific NCO/PD/NA fields. |

---

## 7. Changelog / Recent Fixes

### 2026-03-12 — Normalized KRI Bullet Chart Split (Rates vs Composition)

**Problem**: The single `kri_bullet_normalized` artifact mixed rate metrics (0.xx% scale) and composition metrics (xx% scale) on a shared x-axis, making the chart unreadable. Additionally, the comparator fallback collapsed the gray band to the subject value when both peer composites were NaN, which was visually misleading.

**3-part fix:**

1. **Artifact split**: Replaced `kri_bullet_normalized` with two new artifacts:
   - `kri_bullet_normalized_rates` — 5 rate metrics (Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage)
   - `kri_bullet_normalized_composition` — 5 composition metrics (Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share)
   - Each gets its own axes, avoiding the mixed-scale problem
   - Exact titles: "Key Risk Indicators — MSPBNA vs Peer Range (Normalized Rates)" and "(Normalized Composition)"

2. **Comparator fallback fix** in `generate_kri_bullet_chart()`:
   - Both comparators available → gray band between min and max (unchanged)
   - Single comparator available → thin vertical reference marker (new behavior)
   - Neither comparator available → metric row skipped entirely (was: collapsed to subject value)
   - Legend includes "Single Comparator Ref." when applicable

3. **Integration update**: `report_generator.py` Phase 8 now produces 3 bullet artifacts (1 standard + 2 normalized) instead of 2. Each normalized artifact passes its own `metrics` list and `title_override`.

**Changes:**

1. **rendering_mode.py** — Replaced `kri_bullet_normalized` registration with `kri_bullet_normalized_rates` and `kri_bullet_normalized_composition` (both FULL_LOCAL_ONLY).

2. **executive_charts.py** — Added `BULLET_METRICS_NORMALIZED_RATES` (5 metrics) and `BULLET_METRICS_NORMALIZED_COMPOSITION` (5 metrics). Added `title_override` parameter to `generate_kri_bullet_chart()`. Refactored comparator fallback: `ref_markers` dict tracks single-comparator rows; thin vertical line drawn instead of zero-width gray band. Neither-available rows skipped entirely via `continue`.

3. **report_generator.py** — Phase 8 bullet chart section rewritten: standard bullet produced as single artifact; normalized produced via loop over `_norm_bullet_specs` list with separate metric lists and titles. Imports `BULLET_METRICS_NORMALIZED_RATES` and `BULLET_METRICS_NORMALIZED_COMPOSITION` from `executive_charts`.

4. **test_regression.py** — Updated 8 existing tests to reference new artifact names. Added 8 new tests: `test_no_obsolete_kri_bullet_normalized_artifact`, `test_normalized_rates_metric_list`, `test_normalized_composition_metric_list`, `test_no_overlap_between_rates_and_composition`, `test_bullet_chart_has_title_override_param`, `test_comparator_fallback_neither_skips_row`, `test_comparator_fallback_single_uses_ref_marker`, `test_exact_normalized_rates_title`, `test_exact_normalized_composition_title`.

5. **CLAUDE.md** — Updated executive artifacts table (5→6 artifacts), bullet metric lists, output filenames, comparator fallback documentation. Added changelog.

**Files changed:** `rendering_mode.py`, `executive_charts.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Rendering Architecture Reconciliation (Final Verification)

**Objective**: Verify that `report_generator.py` is fully aligned with the canonical rendering architecture in `rendering_mode.py`, and that executive/macro chart integration matches the documented target state.

**Verification results** (all items confirmed clean — no code changes needed in report_generator.py, rendering_mode.py, or executive_charts.py):

1. **No merge conflict markers** in `report_generator.py` — confirmed zero instances.
2. **No duplicate local rendering abstractions** — `ReportMode`, `ArtifactStatus`, `ArtifactSpec`, `ManifestEntry`, `ArtifactManifest`, local `ARTIFACT_REGISTRY`, local `should_produce()`, `resolve_report_mode_for_generator()` are all absent. All imported from `rendering_mode.py`.
3. **Executive chart integration correct** — 5 artifacts produced with correct loop patterns, composite CERTs (90001/90003 standard, 90004/90006 normalized), sparkline norm_peer_cert=90006. Obsolete `kri_bullet_chart` artifact name absent.
4. **Macro chart integration correct** — 3 deterministic artifacts with exact FRED series IDs. No heuristic fallback. `plot_macro_overlay()` deleted. Obsolete `macro_overlay` artifact name absent.
5. **`rendering_mode.py` registry** contains all 5 executive + 3 macro artifacts with correct mode declarations.

**Changes:**

1. **CLAUDE.md** — Added "Canonical Rendering Abstraction Rule" to Section 3a. Added "Executive Chart Artifacts" subsection documenting all 5 artifacts. Added "Deterministic Macro Chart Artifacts" subsection documenting all 3 artifacts. Added "Remaining Risks" subsection. Updated changelog.

2. **test_regression.py** — Added `TestRenderingReconciliation` class (22 tests): no merge conflicts, no duplicate abstractions (8 checks), no obsolete artifact names (2), canonical executive artifacts present, deterministic macro artifacts present, no heuristic fallback, rendering_mode.py is canonical source (7 checks), report_generator imports from rendering_mode.

**Files changed:** `CLAUDE.md`, `test_regression.py`

### 2026-03-12 — HUD HTTP Failure Diagnostics & Request Hardening

**Problem**: All HUD county crosswalk HTTP requests were failing (`FAILED_HTTP`), preventing `CaseShiller_Zip_Coverage` and `CaseShiller_Zip_Summary` sheets from being produced. The root cause was twofold: (1) `fetch_hud_crosswalk()` returned empty DataFrames on HTTP failure without propagating diagnostics, and (2) the orchestrator's `http_errors` list stayed empty even when all requests failed because the function never raised exceptions on HTTP errors, causing misclassification as `FAILED_EMPTY_RESPONSE` instead of `FAILED_HTTP`.

**9-part fix:**

1. **HTTP failure diagnostics (Part 1)**: `fetch_hud_crosswalk()` now returns `(DataFrame, diagnostics_dict)` tuple. Diagnostics include: `query`, `status`, `failure_class`, `status_code`, `url`, `params`, `response_preview`, `exception_info`, `retry_count`.

2. **Request hardening (Part 2)**: Switched from bare `requests.get()` to `requests.Session()` with persistent `Accept: application/json` and `User-Agent` headers. Module-level session is lazy-initialized via `_get_hud_session()`.

3. **Request validation (Part 3)**: Added `build_hud_crosswalk_request()` helper that validates query, crosswalk_type, year, quarter, and token before sending. First-request debug log shows masked token prefix and full session headers.

4. **Status code classification (Part 4)**: Added `_classify_http_status()` mapping HTTP codes to query-level failure constants: `QUERY_FAILED_TOKEN_AUTH` (401/403), `QUERY_FAILED_HTTP_NOT_FOUND` (404), `QUERY_FAILED_HTTP_BAD_REQUEST` (400), `QUERY_FAILED_HTTP_RATE_LIMIT` (429), `QUERY_FAILED_HTTP_SERVER` (5xx), `QUERY_FAILED_HTTP_EXCEPTION` (connection/timeout errors).

5. **County-level failure summary (Part 5)**: Orchestrator logs breakdown at end: total, success, failed_auth, failed_bad_request, failed_not_found, failed_rate_limit, failed_server, failed_exception, failed_parse, failed_empty.

6. **Misclassification fix (Part 6)**: Orchestrator now tracks `county_diagnostics` list and checks `any_http_failure` + `all_failed_http` guards. When all county requests fail HTTP, final status is `FAILED_HTTP` (not `FAILED_EMPTY_RESPONSE` / `SUCCESS_NO_MATCHES` / `SUCCESS_NO_ZIPS`).

7. **Smoke test helper (Part 7)**: `run_hud_smoke_test(fips_code, token)` runs a single HUD request for local debugging. Returns structured result dict with success, status_code, failure_class, row_count, columns.

8. **Regression tests (Part 8)**: 9 new tests in `TestHUDHTTPDiagnosticsAndHardening`: tuple return, diagnostics keys, session headers, request validation, status classification, constants, misclassification guard, smoke test, failure summary logging.

9. **Documentation (Part 9)**: Updated CLAUDE.md with HTTP failure blocker documentation and new diagnostics.

**Query-level failure constants:**

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

**Example diagnostics dict:**
```python
{
    "query": "06037",
    "status": "error",
    "failure_class": "FAILED_HTTP_SERVER",
    "status_code": 500,
    "url": "https://www.huduser.gov/hudapi/public/usps",
    "params": {"type": 7, "query": "06037"},
    "response_preview": "Internal Server Error",
    "exception_info": None,
    "retry_count": 2
}
```

**Example county-level failure summary log:**
```
HUD county-ZIP fetch summary: total=160, success=0, failed=160,
breakdown={ auth=0, bad_request=0, not_found=0, rate_limit=0,
server=160, exception=0, parse=0, empty=0 }
```

**Files changed:** `case_shiller_zip_mapper.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Corp-Safe Overlay Workflow (4 Artifacts, Separate Module)

**Objective**: Build a dedicated corp-safe overlay layer as a separate module/workflow. Joins local Bank_Performance_Dashboard output with internal loan-level extracts to produce 4 corp-safe artifacts. NOT integrated into report_generator.py or MSPBNA_CR_Normalized.py.

**New modules:**

| Module | Role |
|---|---|
| `corp_overlay.py` | Schema contracts, loan-file ingestion, dashboard ingestion, join logic, optional enrichment hooks, 4 artifact generators, orchestrator |
| `corp_overlay_runner.py` | Standalone CLI entrypoint with argparse (separate from report_generator.py) |

**4 Artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `loan_balance_by_product` | `corp_overlay_YYYYMMDD_loan_balance_by_product.png` | FULL_LOCAL_ONLY | PNG chart |
| `top10_geography_by_balance` | `corp_overlay_YYYYMMDD_top10_geography_by_balance.png` | FULL_LOCAL_ONLY | PNG chart |
| `internal_credit_flags_summary` | `corp_overlay_YYYYMMDD_internal_credit_flags_summary.html` | BOTH | HTML table |
| `peer_vs_internal_mix_bridge` | `corp_overlay_YYYYMMDD_peer_vs_internal_mix_bridge.html` | BOTH | HTML table |

**Input contracts:**

- Input A: `Bank_Performance_Dashboard_YYYYMMDD.xlsx` (auto-discovered from `output/`)
- Input B: Internal loan-level CSV or Excel with required columns:
  - `loan_id` — unique loan identifier (required)
  - `current_balance` — outstanding balance in $ (required)
  - `product_type` — loan product classification (required)
  - At least one geo field: `msa` (preferred), `zip_code`, or `county` (required)
  - Optional: `risk_rating`, `delinquency_status`, `nonaccrual_flag`, `segment`, `portfolio`, `collateral_type`

**Contract validation:** `validate_loan_file()` raises `LoanFileContractError` if required columns or all geo fields are missing. Geo priority: MSA > zip_code > county. Optional columns degrade gracefully — reduced output when absent.

**Optional enrichment hooks:**
- Census (`CENSUS_API_KEY`): population/income by geography — hook point, not yet implemented
- BEA (`BEA_API_KEY` → `BEA_USER_ID` fallback): GDP/employment by MSA/county — hook point, not yet implemented
- Case-Shiller: `map_zip_to_metro()` from existing `case_shiller_zip_mapper.py` for ZIP → metro tagging
- All enrichment is optional — workflow runs fully offline without any API keys

**Reduced-mode behavior:**
- No `risk_rating` → credit flags summary shows portfolio summary only, no rating distribution
- No `delinquency_status` → no delinquency section
- No `nonaccrual_flag` → no nonaccrual section
- No dashboard found → bridge table shows "not available" for peer composition
- corp_safe mode → PNG charts skipped, HTML tables produced

**Changes:**

1. **corp_overlay.py** (NEW) — Full module: `REQUIRED_COLUMNS`, `GEO_COLUMNS`, `OPTIONAL_COLUMNS` contracts; `LoanFileContractError`; `validate_loan_file()`; `find_latest_dashboard()`; `load_dashboard_composition()`; `load_loan_file()`; `enrich_geography()` with Census/BEA/Case-Shiller hooks; `generate_loan_balance_by_product()`; `generate_top10_geography()`; `generate_credit_flags_summary()`; `generate_peer_vs_internal_bridge()`; `run_corp_overlay()` orchestrator.

2. **corp_overlay_runner.py** (NEW) — CLI entrypoint with argparse: `loan_file` (required), `--dashboard`, `--output-dir`, `--mode`. Validates loan file existence before importing corp_overlay.

3. **rendering_mode.py** — Added 4 artifact registrations: `loan_balance_by_product` (FULL_LOCAL_ONLY), `top10_geography_by_balance` (FULL_LOCAL_ONLY), `internal_credit_flags_summary` (BOTH), `peer_vs_internal_mix_bridge` (BOTH).

4. **test_regression.py** — Added 6 test classes (31 tests): `TestCorpOverlayContractValidation` (10): required/geo/optional column validation, case-insensitive, priority order. `TestCorpOverlayArtifactRegistration` (5): all 4 registered, mode availability correct. `TestCorpOverlayOfflineOperation` (4): enrichment without keys, key resolution fallback. `TestCorpOverlayReducedMode` (5): HTML output with/without optional columns, bridge with/without dashboard. `TestCorpOverlayNoMergeConflicts` (3): no conflict markers in report_generator.py, corp_overlay not referenced in existing scripts. `TestCorpOverlayCLAUDEMDAccuracy` (4): documentation completeness.

5. **CLAUDE.md** — Added Section 12 (Corp-Safe Overlay Architecture), script table entries, changelog.

**Files changed:** `corp_overlay.py` (NEW), `corp_overlay_runner.py` (NEW), `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Macro Chart Tranche (3 Artifacts, Deterministic Series Selection)

**Objective**: Implement macro-to-credit correlation and overlay charts using precise named FRED series. Replace the old heuristic "pick first available" macro overlay with deterministic, documented series selection.

**3 Artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `macro_corr_heatmap_lag1` | `{stem}_macro_corr_heatmap_lag1.html` | BOTH | HTML table |
| `macro_overlay_credit_stress` | `{stem}_macro_overlay_credit_stress.png` | FULL_LOCAL_ONLY | PNG chart |
| `macro_overlay_rates_housing` | `{stem}_macro_overlay_rates_housing.png` | FULL_LOCAL_ONLY | PNG chart |

**Required FRED Series (13):**

| Category | Series ID | Short Name | Frequency |
|---|---|---|---|
| Rates/Curve | FEDFUNDS | Fed Funds Rate | Monthly |
| Rates/Curve | T10Y2Y | 10Y-2Y Spread | Daily |
| Credit/Stress | BAMLH0A0HYM2 | HY OAS | Daily |
| Credit/Stress | VIXCLS | VIX | Daily |
| Credit/Stress | NFCI | Chicago Fed NFCI | Weekly |
| Credit/Stress | STLFSI2 | St. Louis FSI v2 | Weekly |
| Credit/Stress | DRTSCILM | C&I Standards (Large/Med) | Quarterly |
| Bank Health | DRALACBS | All Loans Delinquency | Quarterly |
| Bank Health | DRCRELEXFACBS | CRE Delinq (ex-farm) | Quarterly |
| Bank Health | DRSFRMACBS | 1-4 Resi Delinquency | Quarterly |
| Housing | MORTGAGE30US | 30Y Mortgage Rate | Weekly |
| Housing | HOUST | Housing Starts | Monthly |
| Housing | CSUSHPISA | Case-Shiller National (SA) | Monthly |

**Series availability:** All 13 series are now in `FRED_SERIES_TO_FETCH` (legacy ingestion path). STLFSI2 and CSUSHPISA were added in this prompt. Other 11 were already present.

**Artifact 1 — macro_corr_heatmap_lag1.html:**
- Internal metric rows (8): Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_NCO_Rate, RIC_CRE_ACL_Coverage
- Macro columns (13): All 13 FRED series above
- Pearson correlation, trailing 20Q window
- Macro series lagged +1 quarter vs internal metrics
- Insufficient overlap (< 4 quarters) → N/A cell
- Color scale: green (strong negative/counter-cyclical) → neutral → red (strong positive/comovement)
- All series quarterly-aligned via `_fred_to_quarterly()` (last-observation-per-quarter resampling)

**Artifact 2 — macro_overlay_credit_stress.png:**
- Left axis: MSPBNA Norm_NCO_Rate (gold, solid)
- Right axis: BAMLH0A0HYM2 (blue, dashed) + NFCI (red, dashed), both z-scored over plotted window
- Z-scoring: `(value - mean) / std` for readability when units differ
- Title: "Credit Stress Overlay: Norm NCO Rate vs HY OAS + NFCI"

**Artifact 3 — macro_overlay_rates_housing.png:**
- Left axis: RIC_Resi_Nonaccrual_Rate (preferred) or Norm_Nonaccrual_Rate (fallback)
- Right axis: FEDFUNDS (solid), MORTGAGE30US (dashed), CSUSHPISA YoY % (dash-dot)
- CSUSHPISA transformed: `pct_change(4) * 100` (quarterly YoY %)
- Title dynamically names the actual plotted series

**Critical change — heuristic fallback deleted:**
The old `plot_macro_overlay()` used: `target_names = ["Fed Funds", "Unemployment", "All Loans Delinquency Rate"]` with fallback to "any available series". This has been completely removed. Macro chart selection is now deterministic — each chart function specifies exact FRED series IDs. No fallback to random/first-available series. The old `macro_overlay` artifact is replaced by 3 deterministic artifacts.

**Changes:**

1. **MSPBNA_CR_Normalized.py** — Added STLFSI2 and CSUSHPISA to `FRED_SERIES_TO_FETCH` so both are fetched into the workbook FRED_Data sheet.

2. **rendering_mode.py** — Replaced `macro_overlay` (FULL_LOCAL_ONLY) with 3 new registrations: `macro_corr_heatmap_lag1` (BOTH), `macro_overlay_credit_stress` (FULL_LOCAL_ONLY), `macro_overlay_rates_housing` (FULL_LOCAL_ONLY).

3. **report_generator.py** — Deleted old `plot_macro_overlay()` with its heuristic fallback. Added constants `MACRO_CORR_INTERNAL_METRICS` (8 metrics), `MACRO_CORR_FRED_SERIES` (13 series), `_FRED_DISPLAY` (human-readable names). Added `_fred_to_quarterly()` helper for quarterly alignment. Added 3 new generators: `generate_macro_corr_heatmap()`, `plot_macro_overlay_credit_stress()`, `plot_macro_overlay_rates_housing()`. Updated `generate_reports()` Phase 6 to call all 3 new artifacts instead of the old single macro_overlay.

4. **test_regression.py** — Added `TestMacroChartTranche` class (18 tests): all 3 artifacts registered, mode declarations correct, old macro_overlay removed, no heuristic fallback in source, all 13 FRED series in fetch list, exact internal metric list, exact FRED column list, deterministic series in credit stress, deterministic series in rates/housing, left-axis preference order, chart titles include series names, empty data handling, new artifacts referenced in report_generator, _fred_to_quarterly exists, z-scoring verified, STLFSI2 in fetch, CSUSHPISA in fetch. Updated `test_registry_covers_known_artifacts` to include new artifacts.

5. **CLAUDE.md** — Added artifact table, series table, transformations, quarterly alignment, and changelog.

**Files changed:** `MSPBNA_CR_Normalized.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Executive Chart Tranche (5 Artifacts)

**Objective**: Complete and harden the first executive chart tranche using existing workbook data only. No new external dependencies.

**5 Artifacts:**

| Artifact | File | Mode | Type |
|---|---|---|---|
| `yoy_heatmap_standard` | `{stem}_yoy_heatmap_standard.html` | BOTH | HTML table |
| `yoy_heatmap_normalized` | `{stem}_yoy_heatmap_normalized.html` | BOTH | HTML table |
| `kri_bullet_standard` | `{stem}_kri_bullet_standard.png` | FULL_LOCAL_ONLY | PNG chart |
| `kri_bullet_normalized_rates` | `{stem}_kri_bullet_normalized_rates.png` | FULL_LOCAL_ONLY | PNG chart |
| `kri_bullet_normalized_composition` | `{stem}_kri_bullet_normalized_composition.png` | FULL_LOCAL_ONLY | PNG chart |
| `sparkline_summary` | `{stem}_sparkline_summary.html` | BOTH | HTML table |

**Exact metric lists:**

| Artifact | Metrics |
|---|---|
| Standard heatmap (12) | TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Past_Due_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, SBL_Composition, RIC_CRE_Loan_Share, RIC_Resi_Loan_Share, RIC_CRE_ACL_Coverage, RIC_CRE_Risk_Adj_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_NCO_Rate |
| Normalized heatmap (10) | Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage, Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share |
| Standard bullet (7) | TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Past_Due_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, RIC_CRE_ACL_Coverage, RIC_CRE_Risk_Adj_Coverage |
| Normalized bullet — rates (5) | Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_Delinquency_Rate, Norm_ACL_Coverage, Norm_Risk_Adj_Allowance_Coverage |
| Normalized bullet — composition (5) | Norm_SBL_Composition, Norm_Wealth_Resi_Composition, Norm_CRE_Investment_Composition, Norm_CRE_ACL_Share, Norm_Resi_ACL_Share |
| Sparkline (10) | TTM_NCO_Rate, Nonaccrual_to_Gross_Loans_Rate, Allowance_to_Gross_Loans_Rate, Risk_Adj_Allowance_Coverage, Past_Due_Rate, Norm_NCO_Rate, Norm_Nonaccrual_Rate, Norm_ACL_Coverage, RIC_CRE_Nonaccrual_Rate, RIC_CRE_ACL_Coverage |

**Comparator CERTs:**

| Artifact | Standard | Normalized |
|---|---|---|
| Heatmap | 90003 (All Peers) | 90006 (All Peers Norm) |
| Bullet | 90001 (Core PB) + 90003 (All Peers) | 90004 (Core PB Norm) + 90006 (All Peers Norm) |
| Sparkline | 90003 for standard rows | 90006 for Norm_ rows (per-metric selection) |

**Changes:**

1. **executive_charts.py** — Split `BULLET_METRICS` into `BULLET_METRICS_STANDARD` (7 metrics) and `BULLET_METRICS_NORMALIZED` (7 metrics). Added `is_normalized` parameter to `generate_kri_bullet_chart()` which selects correct metric list and chart title. Added `norm_peer_cert` parameter to `generate_sparkline_table()` — sparkline peer lookup uses 90006 for `Norm_*` metrics, 90003 for standard metrics. Backward-compatible `BULLET_METRICS` alias retained.

2. **rendering_mode.py** — Replaced single `kri_bullet_chart` registration with two: `kri_bullet_standard` (FULL_LOCAL_ONLY) and `kri_bullet_normalized` (FULL_LOCAL_ONLY). Heatmaps and sparkline remain BOTH.

3. **report_generator.py** — Phase 8 executive charts section now loops over `[False, True]` for bullet charts (same pattern as heatmaps), producing both `kri_bullet_standard` and `kri_bullet_normalized` with correct composite CERTs per variant. Sparkline call now passes `norm_peer_cert=ACTIVE_NORMALIZED_COMPOSITES["all_peers"]`.

4. **test_regression.py** — Updated 3 existing tests (`test_executive_chart_artifacts_registered`, `test_executive_charts_integrated_in_report_generator`, `test_corp_safe_skips_bullet_chart`) to reflect new artifact names. Updated `test_registry_covers_known_artifacts` to include both bullet variants. Added `TestExecutiveChartTranche` class (16 tests): all 5 artifacts registered, heatmap metric allowlists exact match, bullet metric lists exact match, sparkline metrics exact match, is_normalized parameter exists, norm_peer_cert parameter exists with default 90006, mode support declarations correct, missing metric resilience (heatmap and sparkline), ordered_metrics usage verified, both bullet variants in report_generator, sparkline norm peer logic verified, N/A cells explicit, old kri_bullet_chart removed, all metrics in semantic registry.

5. **CLAUDE.md** — Added artifact table, exact metric lists, comparator CERTs, and changelog.

**Verification:** Heatmaps already use `ordered_metrics()` (from `metric_semantics.py`) for GROUP_ORDER row sorting. All metrics in all lists are registered in `metric_semantics.py`. Missing metrics skip individually (not whole artifact). Missing values render as explicit "N/A". HTML artifacts are self-contained.

**Files changed:** `executive_charts.py`, `rendering_mode.py`, `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Architecture Reconciliation (Merge Conflict Resolution)

**Mandatory cleanup**: Resolved all merge conflicts and made `rendering_mode.py` the single canonical home for render abstractions.

**Changes:**

1. **report_generator.py** — Resolved 7 merge conflicts (scatter plots, segment/roadmap charts, FRED expansion charts, executive charts, tables). Removed ~230 lines of duplicate local abstractions (`ReportMode`, `ArtifactStatus`, `ArtifactSpec`, `ManifestEntry`, `ArtifactManifest`, `ReportContext`, `resolve_report_mode_for_generator()`, local `ARTIFACT_REGISTRY`, `_ARTIFACT_BY_NAME`, `get_artifacts_for_mode()`, local `should_produce()`). All render types now imported from `rendering_mode.py`. Added `_ReportContext` (lightweight internal dataclass) and `_produce_table()`/`_produce_chart()` DRY helpers. Fixed manifest API from stale `manifest.record()` to canonical `record_generated()`/`record_skipped()`/`record_failed()`. Fixed FRED expansion preflight to use `is_artifact_available()` (side-effect-free) instead of `should_produce()`.

2. **rendering_mode.py** — Already canonical. Added `is_artifact_available()` for side-effect-free availability checks, `ArtifactCapability.filename_suffix` field with auto-generation in `_reg()`, and backward-compatible `REPORT_RENDER_MODE` alias in `select_mode()`.

3. **test_regression.py** — Resolved 1 merge conflict. Replaced broken `TestDualModeArchitecture` (imported removed local types) with 3 new test classes (25 tests): `TestCanonicalRenderingArchitecture` (no merge conflicts, no duplicate abstractions, canonical imports, correct manifest API), `TestCanonicalModeResolution` (REPORT_MODE resolution, REPORT_RENDER_MODE alias, explicit override, invalid raises, active composites), `TestArtifactRegistryCanonical` (registry coverage, mode availability, manifest API, should_produce records skips, is_artifact_available no side effects).

4. **CLAUDE.md** — Updated env var table (added REPORT_MODE, REPORT_RENDER_MODE, BEA_API_KEY, BEA_USER_ID, CENSUS_API_KEY). Updated mode selection priority (4-level: explicit → REPORT_MODE → REPORT_RENDER_MODE → default). Documented alias resolution rules. Added changelog.

**Deferred work:**
- Executive charts (`_produce_chart` DRY conversion) — currently use inline `should_produce` + manual manifest calls. Works correctly but not as DRY as scatter/segment/roadmap charts.
- `_produce_chart` does not yet handle all chart function signatures uniformly (some take `proc_df_with_peers, subject_bank_cert`, others take `fred_expansion_df`).

**Files changed:** `report_generator.py`, `rendering_mode.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — Normalized Segment Taxonomy Alignment

Aligned the normalized segment taxonomy to Call Report research and removed
non-CR-consistent mappings. Changes:

1. **RESI denominator correction**: Removed `LNRERES + LNRELOC` fallback;
   denominator now uses `LNRERES` alone (already includes HELOC/open-end).
   Split numerators (NTRERES+NTRELOC, etc.) preserved.
2. **C&I exclusion**: Changed `Excl_CI_Balance` from `max(0, LNCI - SBL_Balance)`
   to `LNCI` directly. SBL is RC-C item 9, not item 4.
3. **NDFI (Fund Finance)**: Removed J458/J459/J460 from all credit-quality
   mappings (PD30, PD90, NA). J454 retained for balance. NDFI PD/NA set to 0.0
   with limitation flags.
4. **Ag past due**: Replaced RCON2746/RCFD2746 and RCON2747/RCFD2747 with
   P3AG/P9AG (actual agricultural past-due fields).
5. **CRE pure balance**: Tightened to `LNREMULT + LNRENROT` only; removed
   LNREOTH.
6. **Tailored lending**: Documented as unsupported in CR-only mode. No proxy
   math added.

Files changed: `MSPBNA_CR_Normalized.py`, `CLAUDE.md`

### 2026-03-12 — Hardened Normalized Exclusion Engine

Hardened the exclusion engine so balances, NCO, nonaccrual, PD30, and PD90 all
use internally consistent exclusion math with no unsupported numerator leakage.

1. **C&I NCO balance-gating**: `Excl_CI_NCO_YTD` now gated to 0.0 when
   `Excl_CI_Balance == 0`. Audit flag `_CI_NCO_Gated` added.
2. **Credit Card NCO balance-gating**: `Excl_CC_NCO_YTD` now gated to 0.0 when
   `Excl_CreditCard_Balance == 0`. Audit flag `_CC_NCO_Gated` added.
3. **NDFI NCO set to 0.0**: `Excl_NDFI_NCO_YTD` forced to 0.0 (no CR-only
   mapping exists). Audit flag `_audit_ndfi_nco_unsupported` added.
4. **Structured audit flags**: Added 4 boolean audit columns:
   - `_audit_unsupported_ndfi_pdna` — NDFI PD/NA unsupported in CR-only mode
   - `_audit_ndfi_nco_unsupported` — NDFI NCO unsupported in CR-only mode
   - `_audit_ag_pd_fallback_to_zero` — P3AG/P9AG both absent, Ag PD fell back to 0
   - `_audit_resi_balance_fallback_used` — RC-C components absent, LNRERES fallback used
5. **Metric registry**: Added `Norm_Delinquency_Rate` MetricSpec
   (`(Norm_PD30 + Norm_PD90) / Norm_Gross_Loans`). Added Rule F
   (`_check_unsupported_mappings`) to semantic validation.
6. **Exclusion audit sheet**: `excl_audit_cols` extended with NDFI balance/NCO,
   all gating flags, and all audit flags. `gate_cols` updated.
7. **CLAUDE.md**: Added Supported Exclusion Stack table, updated balance-gating
   documentation (6 categories), added Structured Audit Flags section.

Files changed: `MSPBNA_CR_Normalized.py`, `metric_registry.py`, `CLAUDE.md`

### 2026-03-12 — Dual-Mode Rendering Architecture (Preflight Refactor)

1. **New module `rendering_mode.py`**: Implements dual-mode architecture with `RenderMode` enum (`full_local`, `corp_safe`), `ArtifactCapability` declarations, `ArtifactManifest` outcome tracking, `ARTIFACT_REGISTRY` (31 artifacts), and `should_produce()` guard helper.

2. **Refactored `report_generator.py`**: `generate_reports()` now accepts `render_mode` parameter (default: `full_local`, preserves existing behaviour). Every artifact production block is guarded by `should_produce()`. Returns `ArtifactManifest` with per-artifact outcome tracking. CLI supports `python report_generator.py [full_local|corp_safe]`.

3. **Capability matrix**: All HTML tables declared `BOTH` (available in both modes). All matplotlib-based charts/scatters declared `FULL_LOCAL_ONLY`. In `corp_safe` mode, chart artifacts are skipped with clear `[SKIP]` log messages.

4. **Artifact manifest**: Every run produces a summary table printed to console: artifact name, mode, status (generated/skipped/failed), path or reason. Counts displayed at bottom.

5. **15 new regression tests** in `test_regression.py` covering: mode selection (default, explicit, env var, invalid), capability matrix (tables both, charts full_local only), skip reasons, manifest recording, `should_produce()` behaviour, preflight suppression integration, registry completeness, render_mode parameter presence, active composite preservation.

6. **Non-breaking**: Default execution path (`python report_generator.py` with no args) is identical to pre-refactor behaviour. No peers, segments, composite definitions, or chart semantics changed.

**New files**: `rendering_mode.py`
**Changed files**: `report_generator.py`, `test_regression.py`, `CLAUDE.md`

### 2026-03-12 — HUD Response Two-Pass Flattening Fix

**Problem**: Even after the initial parsing fix, runtime logs showed the combined HUD DataFrame still had only wrapper columns (`year`, `quarter`, `input`, `crosswalk_type`, `results`). The parser was stopping one layer too early.

**Root cause**: The HUD type=7 API returns `{"results": [wrapper, wrapper, ...]}` where each wrapper is `{"year":..., "quarter":..., "input":..., "crosswalk_type":..., "results": [actual_row, ...]}`. `extract_hud_result_rows()` correctly extracted the wrapper list (Shape B), but `pd.DataFrame(wrapper_rows)` produced a DataFrame with wrapper columns because each wrapper's `results` key contained the actual crosswalk rows as a nested list.

**Fix**: Added `flatten_hud_rows()` as a second-pass flattener that detects wrapper rows (keys ⊆ `_HUD_WRAPPER_KEYS` + nested `results` list) and explodes them into actual data rows, propagating parent metadata. The fetch pipeline is now: `extract_hud_result_rows()` → `flatten_hud_rows()` → `pd.json_normalize()` → `canonicalize_hud_columns()`.

**Changes:**
1. **case_shiller_zip_mapper.py**:
   - `flatten_hud_rows(rows)` — NEW: detects wrapper rows, explodes nested `results`, propagates parent metadata
   - `_HUD_WRAPPER_KEYS` — NEW: frozenset of known wrapper metadata keys
   - `fetch_hud_crosswalk()` — now calls `flatten_hud_rows()` between extraction and normalization; logs `rows_before_flatten` and `rows_after_flatten`
   - Wrapper-only detection uses `_HUD_WRAPPER_KEYS` constant instead of inline set
2. **test_regression.py** — 8 new tests:
   - `test_extract_hud_rows_wrapper_results_list`, `test_flatten_hud_rows_propagates_parent_metadata`, `test_flatten_hud_rows_already_flat`, `test_flatten_hud_rows_multiple_wrappers`, `test_fetch_hud_crosswalk_uses_flatten`, `test_wrapper_keys_constant_exists`, `test_failed_parse_not_misclassified_as_success_no_zips`, `test_end_to_end_nested_wrapper_to_canonical_columns`
3. **CLAUDE.md** — Updated "HUD Response Parsing & Flattening" to document two-pass architecture with example payload

**Example before/after parse shape:**
```
HUD API returns: {"results": [{"year":2025, "quarter":4, "input":"06037",
                               "crosswalk_type":"7",
                               "results": [{"zip":"90001","county":"06037",...}]}]}
Pass 1 (extract): [{"year":2025, "quarter":4, "input":"06037", "results":[...]}]  ← 1 wrapper row
Pass 2 (flatten): [{"year":2025, "quarter":4, "input":"06037", "zip":"90001", "county":"06037", ...}]  ← N data rows
```

**Test baseline**: 246 tests (previous 238 + 8 new).

### 2026-03-12 — HUD Response Parsing & Canonicalization Fix

**Problem**: HUD token was no longer the blocker. The mapper was reaching the HUD API and getting responses, but the returned payload was not being flattened correctly. The resulting DataFrame had only wrapper columns (`year`, `quarter`, `input`, `crosswalk_type`, `results`) and `build_case_shiller_zip_coverage()` failed because it could not find usable ZIP/FIPS columns.

**Root cause**: `fetch_hud_crosswalk()` extracted `data["results"]` but if that was still a wrapper dict (not a flat list of row dicts), `pd.DataFrame(records)` produced a single-row DataFrame with wrapper columns instead of the actual crosswalk rows.

**Changes by file:**

1. **case_shiller_zip_mapper.py** — 6 new/refactored functions:
   - `extract_hud_result_rows(payload)` — Robust row-list extractor handling 5 documented HUD response shapes (A: flat list, B: `results` list, C: `results.rows`, D: `results.data`, E: `data` key)
   - `canonicalize_hud_columns(df)` — Renames HUD column variants to canonical names (`zip`, `county_fips`, `res_ratio`, etc.) and zero-pads ZIP/FIPS to 5 chars
   - `_describe_payload(payload)` — Log-safe payload shape diagnostics (type, keys, nested types, sample keys)
   - `_HUD_COLUMN_MAP` — Canonical column name mapping dict (14 variant → 6 canonical)
   - `fetch_hud_crosswalk()` — Now uses `extract_hud_result_rows()` + `pd.json_normalize()` + `canonicalize_hud_columns()`. Detects wrapper-only columns and rejects them.
   - `build_case_shiller_zip_coverage()` — Added structured diagnostics (total HUD rows, distinct FIPS counts, matched/unmatched counts, FIPS samples on mismatch)
   - `build_case_shiller_zip_sheets()` — New `FAILED_PARSE` status when columns are wrapper-only. New `SUCCESS_NO_MATCHES` status when HUD rows exist but none match S&P FIPS. Downstream write validation logs `zip_coverage_rows`, `zip_summary_rows`, `metro_count` before returning.

2. **test_regression.py** — 14 new tests in `TestHUDResponseParsing`:
   - `test_extract_hud_rows_shape_a_list` through `test_extract_hud_rows_shape_e_data_key` (5 shape tests)
   - `test_extract_hud_rows_empty_payload`
   - `test_canonicalize_hud_columns_maps_zip_and_county`
   - `test_canonicalize_zeropad_zip`
   - `test_build_case_shiller_zip_coverage_missing_ratios`
   - `test_failed_parse_status_distinct_from_no_token`
   - `test_success_no_matches_status_exists`
   - `test_describe_payload_returns_shape_info`
   - `test_fetch_hud_crosswalk_uses_extract_and_canonicalize`
   - `test_metric_format_type_maintenance_comment`
   - Updated 3 existing tests for new status codes (`FAILED_PARSE`, `SUCCESS_NO_MATCHES`)

3. **CLAUDE.md** — Updated Section 11:
   - Added "HUD Response Parsing & Flattening" subsection with 5-shape table
   - Added "Canonical HUD Crosswalk Fields" subsection with variant mapping
   - Expanded enrichment status table from 8 to 10 codes
   - Noted that token is no longer the main blocker

**Enrichment status transitions:**
```
Token missing     → SKIPPED_NO_TOKEN
Token found + 401 → FAILED_TOKEN_AUTH
Token found + 5xx → FAILED_HTTP
Token found + 200 + wrapper-only columns → FAILED_PARSE
Token found + 200 + empty results        → FAILED_EMPTY_RESPONSE
Token found + 200 + rows but no FIPS match → SUCCESS_NO_MATCHES
Token found + 200 + rows + FIPS match + 0 ZIPs → SUCCESS_NO_ZIPS
Token found + 200 + rows + FIPS match + ZIPs   → SUCCESS_WITH_ZIPS
```

**Example normalized HUD columns after parsing:**
```
Before: year | quarter | input | crosswalk_type | results
After:  zip | county_fips | res_ratio | bus_ratio | oth_ratio | tot_ratio | year | quarter
```

**Test baseline**: 238 tests (previous 224 + 14 new).

### 2026-03-11 — Final Consistency Pass (HUD Token + ACL Semantics)

**Objective**: Verify code, tests, and CLAUDE.md all agree after the DashboardConfig HUD token fix and ACL coverage/share semantics fix.

**Gaps found and fixed:**
1. **`Norm_CRE_ACL_Coverage` missing from metric_registry.py** — Computed in MSPBNA_CR_Normalized.py (`RIC_CRE_ACL / CRE_Investment_Pure_Balance`) but had no `MetricSpec`. Added with correct exposure-base dependency.
2. **`RIC_Comm_Risk_Adj_Coverage` missing from `_METRIC_FORMAT_TYPE`** — C&I NPL coverage metric (ACL / Nonaccrual) was not registered for x-format. Added alongside the existing CRE and Resi NPL coverage entries.

**Verified clean (no changes needed):**
- No dict-style `config[...]` writes in MSPBNA_CR_Normalized.py
- No `self.config.get()` calls
- Single `resolve_hud_token()` call path (in `_validate_runtime_env()` only)
- All ratio-components labels match denominators (no "Coverage" on ACL-pool denominator)
- CLAUDE.md accurately documents `DashboardConfig.hud_user_token`, `_METRIC_FORMAT_TYPE`, coverage/share rule, enrichment status codes

**Changes:**
1. **metric_registry.py** — Added `Norm_CRE_ACL_Coverage` MetricSpec (deps: `RIC_CRE_ACL`, `CRE_Investment_Pure_Balance`)
2. **report_generator.py** — Added `RIC_Comm_Risk_Adj_Coverage: "x"` to `_METRIC_FORMAT_TYPE`
3. **test_regression.py** — Added `TestConsistencyPass` (10 tests): no dict config access, single token resolution path, all Risk_Adj_Coverage in x-format, Norm_CRE_ACL_Coverage in registry with exposure denominator, CLAUDE.md documents DashboardConfig token + _METRIC_FORMAT_TYPE + coverage/share rule, no stale dict references in docs, enrichment status codes match code and docs

**Test baseline**: 224 tests (previous 213 + 10 new + 1 pre-existing Norm_CRE_ACL_Coverage test now passes).

### 2026-03-11 — ACL Ratio Semantic Integrity & Metric Registry Completeness

**Problem**: Missing metric registry entries for `RIC_CRE_ACL_Share`, `RIC_Resi_ACL_Share`, and `Norm_CRE_ACL_Share`. The `_METRIC_FORMAT_TYPE` maintenance rule needed explicit exclusion examples.

**Coverage vs Share vs x-Multiple Rule (non-negotiable):**

| Denominator Type | Label Term | Format | Examples |
|---|---|---|---|
| Exposure/loan base | "Coverage" or "Ratio" | % | `RIC_CRE_ACL / RIC_CRE_Cost`, `Norm_ACL_Balance / Norm_Gross_Loans` |
| ACL pool | "Share" or "% of ACL" | % | `RIC_CRE_ACL / Total_ACL`, `RIC_CRE_ACL / Norm_ACL_Balance` |
| Nonaccrual/NPL base | "NPL Coverage" | x-multiple | `RIC_CRE_ACL / RIC_CRE_Nonaccrual` |

**`_METRIC_FORMAT_TYPE` Maintenance Rule**: Only NPL coverage metrics (denominator = nonaccrual/past-due) belong in the x-format registry. Any new NPL coverage metric MUST be added explicitly. Loan-coverage and share-of-ACL metrics must NEVER be added — they default to percent.

**Changes:**
1. **metric_registry.py** — Added 3 missing `MetricSpec` entries: `RIC_CRE_ACL_Share` (deps: `RIC_CRE_ACL`, `Total_ACL`), `RIC_Resi_ACL_Share` (deps: `RIC_Resi_ACL`, `Total_ACL`), `Norm_CRE_ACL_Share` (deps: `RIC_CRE_ACL`, `Norm_ACL_Balance`)
2. **report_generator.py** — Added explicit exclusion examples to `_METRIC_FORMAT_TYPE` comment listing coverage/share metrics that must NOT be added to x-format
3. **test_regression.py** — Added `TestACLRatioSemanticIntegrity` (9 tests): Total_ACL denominator never labeled Coverage, Norm_ACL_Balance denominator never labeled Coverage, exposure denominator labeled Coverage/Ratio, NPL metrics in x-format, coverage/share not in x-format, maintenance rule documented, registry has all share entries, share metrics depend on ACL denominators, Norm_CRE_ACL_Share depends on Norm_ACL_Balance

**Example corrected labels (all verified correct in current code):**
- `RIC_CRE_ACL / Total_ACL` → "CRE % of ACL" (Share) ✓
- `RIC_CRE_ACL / RIC_CRE_Cost` → "CRE ACL Coverage" (Coverage) ✓
- `RIC_CRE_ACL / Norm_ACL_Balance` → "Norm CRE % of ACL" (Share) ✓
- `RIC_Resi_ACL / Wealth_Resi_Balance` → "Norm Resi ACL Coverage" (Coverage) ✓
- `RIC_CRE_ACL / RIC_CRE_Nonaccrual` → "CRE NPL Coverage" (x-multiple) ✓

**Test baseline**: 213 tests (previous 204 + 9 new).

### 2026-03-11 — Fix DashboardConfig HUD Token TypeError

**Bug**: `main()` crashed with `TypeError: 'DashboardConfig' object does not support item assignment` at `config["_hud_user_token"] = _hud_token`. The `DashboardConfig` class is a plain Python class, not a dict, so bracket-style item assignment fails.

**Root cause**: Two dict-style accesses on `DashboardConfig`:
1. `config["_hud_user_token"] = _hud_token` in `main()` (item assignment)
2. `self.config.get("_hud_user_token")` in the pipeline (dict `.get()` call)

**Fix** (3 changes in `MSPBNA_CR_Normalized.py`):
1. Added `hud_user_token: Optional[str] = None` field to `DashboardConfig` class
2. Changed `config["_hud_user_token"] = _hud_token` → `config.hud_user_token = _hud_token` (attribute assignment)
3. Changed `self.config.get("_hud_user_token")` → `getattr(self.config, "hud_user_token", None)` (safe attribute access)

**Token flow (explicit)**: `_validate_runtime_env()` → returns resolved token → `main()` sets `config.hud_user_token` → pipeline reads via `getattr(self.config, "hud_user_token", None)` → passes `hud_user_token=_hud_tok` explicitly to `build_case_shiller_zip_sheets()`.

**Why the TypeError can no longer occur**: `DashboardConfig` now has a proper `hud_user_token` attribute. All access uses Python attribute syntax (dot notation), never dict bracket syntax. No code path attempts `config[...]` assignment.

**Tests added** (`TestHUDTokenDashboardConfigFix`, 8 tests):
- `test_dashboard_config_has_hud_user_token_field`
- `test_no_item_assignment_on_dashboard_config`
- `test_no_config_get_dict_style`
- `test_main_uses_attribute_assignment_for_token`
- `test_pipeline_uses_getattr_for_token`
- `test_hud_token_passed_explicitly_to_enrichment`
- `test_token_diagnostics_no_full_token_leak`
- `test_enrichment_status_codes_are_distinct`

**Test baseline**: 204 tests (previous 196 + 8 new).

### 2026-03-11 — Coverage/Share Semantics & HUD Token Discovery (Directive 7)

**8-part directive**: Fix coverage vs share label semantics, metric formatting, and HUD token discovery/diagnostics.

**Coverage vs Share Rule (non-negotiable):**
- **"Coverage"** = ACL or reserve divided by an **exposure base** (loans, cost basis, nonaccrual)
  - Examples: `Total_ACL / Gross_Loans`, `RIC_CRE_ACL / RIC_CRE_Cost`, `Norm_ACL_Balance / Norm_Gross_Loans`
- **"Share" or "% of ACL"** = segment reserve divided by **ACL pool**
  - Examples: `RIC_CRE_ACL / Total_ACL`, `RIC_CRE_ACL / Norm_ACL_Balance`
- If denominator is `Total_ACL` or `Norm_ACL_Balance`, the metric **must NOT** be labeled "Coverage"

**Metric Format Types:**
| Type | Display | Typical Range | Examples |
|---|---|---|---|
| Loan coverage | % | 0.5%-3% | ACL/Loans, Risk-Adj ACL, Norm ACL Coverage |
| NPL coverage | x-multiple | 0.5x-5x | CRE ACL/CRE NPL, Resi ACL/Resi NPL |
| Share/composition | % | 5%-50% | CRE % of ACL, Resi % of ACL |

Formatting is now driven by `_METRIC_FORMAT_TYPE` dict in `report_generator.py` (keyed by metric code), not fragile keyword matching. Only NPL coverage metrics (`RIC_CRE_Risk_Adj_Coverage`, `RIC_Resi_Risk_Adj_Coverage`) use x-format. Everything else defaults to percent.

**HUD Token Discovery:**

`resolve_hud_token(explicit_token, script_dir)` in `case_shiller_zip_mapper.py` is the single token resolver. Resolution order:
1. Explicit function argument (if provided)
2. `os.getenv("HUD_USER_TOKEN")`
3. `.env` in script directory
4. `.env` in current working directory

Returns `(token, diagnostics_dict)` where diagnostics includes: `token_found`, `source_used`, `token_length`, `token_prefix_masked` (first 6 chars only), `dotenv_available`, `paths_checked`, `current_working_directory`, `script_directory`, `process_executable`, `process_pid`. The full token is **never** logged or printed.

**Enrichment Status Codes:**
| Status | Meaning |
|---|---|
| `SKIPPED_DISABLED` | `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT=false` |
| `SKIPPED_NO_TOKEN` | Token not visible to current Python process |
| `SKIPPED_NO_REQUESTS` | `requests` library not installed |
| `FAILED_TOKEN_AUTH` | HTTP 401/403 from HUD API |
| `FAILED_HTTP` | Non-auth HTTP failures |
| `FAILED_EMPTY_RESPONSE` | API responded but all counties returned empty |
| `SUCCESS_NO_ZIPS` | Enrichment ran but produced zero ZIP rows |
| `SUCCESS_WITH_ZIPS` | Normal success |

**Changes:**
1. **MSPBNA_CR_Normalized.py** — `_validate_runtime_env()` uses `resolve_hud_token()` for multi-source discovery with full diagnostics; returns resolved token. `main()` passes token to pipeline via config. Enrichment call passes `hud_user_token=` explicitly. Fixed `Norm_CRE_ACL_Coverage` display label from "CRE Reserve % of Norm ACL" to "CRE ACL Coverage (% of CRE Loans)".
2. **case_shiller_zip_mapper.py** — Added `resolve_hud_token()` with 4-source resolution and diagnostics dict. Added 8 enrichment status constants. `build_case_shiller_zip_sheets()` now accepts `hud_user_token` parameter, returns `enrichment_status` and `token_diagnostics` keys, and distinguishes auth failure / HTTP failure / empty response / success.
3. **report_generator.py** — Added `_METRIC_FORMAT_TYPE` dict for semantic formatting. Replaced `_COVERAGE_KEYWORDS` substring matching with `_METRIC_FORMAT_TYPE.get(rat_col, "pct")` lookup. NPL coverage → x-multiples, all other metrics → percent.
4. **test_regression.py** — 16 new tests across 3 classes + 2 updated tests:
   - `TestCoverageVsShareSemantics` (6): share rows not labeled coverage, exposure coverage has correct denominator, Norm_ACL_Coverage uses Norm_Gross_Loans, NPL format is x, loan coverage format is %, display label corrected
   - `TestHUDTokenDiscovery` (6): resolver exists, explicit overrides, diagnostics keys, token never logged in full, env var resolution, missing token diagnostics
   - `TestEnrichmentStatusCodes` (4): status codes defined, build returns status/diagnostics, hud_user_token param, token passed explicitly from main
   - Updated `TestCoverageMetricFormatting` (2): _METRIC_FORMAT_TYPE replaces _COVERAGE_KEYWORDS

**Test baseline**: 196 tests — 184 passing, 0 failures, 10 pre-existing errors (missing matplotlib/aiohttp), 2 skipped.

### 2026-03-11 — Output Quality Fixes (Directive 6)

**8-part directive** covering FDIC history, chart quality, formatting, logging, and label accuracy.

**Changes:**

1. **FDIC history extended to 48 quarters (MSPBNA_CR_Normalized.py)**: `quarters_back` changed from 30 to 48 (12 years). Removed hardcoded `[:30]` slice in FFIEC healing so the full date range is retained.

2. **Scatter outlier labels use tickers (report_generator.py)**: `plot_scatter_dynamic()` outlier annotations now use `resolve_display_label(cert, name)` instead of raw bank names. This produces ticker-style labels (GS, UBS, JPM) consistent with table output.

3. **Comparative migration ladder (report_generator.py)**: `plot_migration_ladder()` rewritten as a comparative chart plotting MSPBNA (solid), Wealth Peers (dashed), and All Peers (dotted). Uses `ACTIVE_STANDARD_COMPOSITES` for peer CERTs. Title updated to "Early-Warning Migration Ladder — MSPBNA vs Peers".

4. **Low-coverage bar series suppression (report_generator.py)**: `create_credit_deterioration_chart_ppt()` now checks `vals.isna().all()` for each bar entity. All-NaN series are excluded from the chart and tracked in `suppressed_series`. A footnote ("Suppressed (low coverage): ...") is added when series are suppressed.

5. **Coverage metric x-format (report_generator.py)**: `generate_ratio_components_table()` now uses `_METRIC_FORMAT_TYPE` dict for semantic formatting — NPL coverage metrics (ACL/NPL) display as x-multiples (0.62x), loan coverage and share metrics display as percentages. Superseded the fragile `_COVERAGE_KEYWORDS` substring approach.

6. **Normalized CRE label fix (report_generator.py)**: "Norm CRE ACL Coverage" renamed to "Norm CRE % of ACL" — the metric is `Norm_CRE_ACL_Share` (share of ACL allocated to CRE), not a coverage ratio.

7. **CSV log severity classification (logging_utils.py)**: `TeeToLogger._classify_level()` rewritten with `_PROGRESS_PATTERNS` regex. Progress bars and tqdm output on stderr are now classified as INFO (not ERROR). Explicit error keywords → ERROR. Other stderr → WARNING.

8. **Duplicate-date artifact paths confirmed fixed**: Verified no `{stamp}` variable usage in `report_generator.py` — all artifact paths use the dashboard stem which already contains the date.

**Tests (test_regression.py)**: 15 new tests across 8 classes:
- `TestFDICHistoryHorizon` (2): quarters_back=48, no hardcoded [:30]
- `TestScatterTickerLabels` (2): resolve_display_label in scatter annotations
- `TestMigrationLadderComparative` (2): ACTIVE_STANDARD_COMPOSITES usage, title mentions Peers
- `TestLowCoverageChartSuppression` (2): suppressed_series tracking, footnote annotation
- `TestCoverageMetricFormatting` (3): _fmt_multiple usage, _METRIC_FORMAT_TYPE registry, explicit NPL policy
- `TestNormalizedRatioLabels` (1): CRE ACL share not labeled "Coverage"
- `TestCsvLogSeverityClassification` (3): _classify_level heuristics, no blind ERROR, progress pattern detection
- `TestNoDoubleDateArtifactPaths` (1): no {stamp} variable in report_generator.py

**Test baseline**: 180 tests — 168 passing, 0 failures, 10 pre-existing errors (missing matplotlib/aiohttp), 2 skipped.

### 2026-03-11 — FRED Frequency Inference Bugfix

**Bug**: `_infer_freq_from_index()` crashed with `TypeError: 'int' object is not subscriptable` during the last-resort fallback branch. The old code built a `pd.Series` of `(year, month)` tuples, then did `.groupby(lambda x: x[0])` — but `groupby` passes the **Series index label** (an integer) into the lambda, not the tuple value. So `x[0]` tried to subscript an `int`.

**Fix**: Replaced the `Series.groupby(lambda)` pattern with a `DataFrame.groupby("year")["month"].nunique()` approach that groups by actual column values, not index labels. The function now:
- Normalizes the index: coerce to `DatetimeIndex`, drop `NaT`, sort, deduplicate
- Tries `pd.infer_freq()` first (most reliable)
- Falls back to median-observations-per-year heuristic
- Last resort uses `DataFrame.groupby` on year/month columns (the fixed path)
- Wraps everything in `try/except` — returns `("quarterly", "Q")` on any unexpected failure

**Note**: Frequency inference is still heuristic when FRED metadata is missing or marked "Unknown". The helper is a best-effort fallback, not a guarantee. Series with highly irregular observation patterns may be classified conservatively as quarterly.

**Changes:**
1. **MSPBNA_CR_Normalized.py** — Extracted `infer_freq_from_index()` to module level for testability. Nested version now delegates to it. Added `NaT` handling, `try/except` safety wrapper.
2. **test_regression.py** — Added `TestInferFreqFromIndex` (9 tests): monthly, quarterly, daily, irregular, duplicates, NaT, empty, exact reproduction of old bug, module-level function existence.

**Test baseline**: 165 tests — 153 passing, 0 failures, 10 pre-existing errors (missing `matplotlib`/`aiohttp`), 2 skipped.

### 2026-03-11 — Final Cleanup & Testability

**Changes:**
1. **Deprecated `generate_normalized_comparison_table()`** — Function body replaced with `raise NotImplementedError`. Retained as a compatibility stub with deprecation docstring pointing to the separate standard/normalized table generators. Was already removed from `generate_reports()` in prior directive; this formalizes the deprecation.

2. **Restored `validate_composite_cert_regime()` as a proper function** — The implementation was orphaned as dead code after `resolve_display_label()`'s `return` statement. Extracted into a standalone top-level function with proper signature (`proc_df: pd.DataFrame`) and docstring. Returns structured dict with `valid`, `active_present`, `active_missing`, `legacy_present`, `warnings`, `errors`. Fixes the pre-existing `test_validate_composite_cert_regime_exists` test failure.

3. **Removed import-time side effects from `MSPBNA_CR_Normalized.py`** — Importing the module no longer requires environment variables, alters `cwd`, prints to stdout, or opens log files. Specifically:
   - `os.chdir(script_dir)` moved from module level into `main()`
   - HUD token print statements moved into `_validate_runtime_env()`
   - `ValueError` raise on missing MSPBNA_CERT/MSBNA_CERT moved into `_validate_runtime_env()`
   - `MSPBNA_CERT` and `MSBNA_CERT` now use `os.getenv()` with defaults (`34221`/`32992`) for import safety
   - `logger = setup_logging()` moved from module level into `main()`
   - `main()` now calls `os.chdir(script_dir)`, `_validate_runtime_env()`, `setup_logging()` at start

4. **Added 15 new regression tests** across 3 test classes:
   - `TestDeprecatedNormalizedComparison` (3 tests): stub exists, raises NotImplementedError, not called in generate_reports
   - `TestValidateCompositeCertRegime` (5 tests): function exists, top-level (not dead code), returns required keys, checks active/legacy composites
   - `TestImportSafety` (7 tests): no import-time ValueError, print, setup_logging, os.chdir; env vars have defaults; `_validate_runtime_env` exists; main calls runtime bootstrap

5. **Fixed `TestNoMixedRegimeArtifact` false positive** — Test regex was matching the `NotImplementedError` message string, not an actual function call. Updated to skip string literals.

**Test baseline**: 156 tests — 144 passing, 0 failures, 10 pre-existing errors (missing `matplotlib`/`aiohttp`), 2 skipped.

### 2026-03-11 — Presentation & Output Cleanup

**Fixes:**
1. **Removed mixed standard-vs-normalized comparison HTML artifact** — `generate_normalized_comparison_table()` produced a side-by-side table mixing standard and normalized columns. This violates the single-regime-per-artifact rule. Removed from `generate_reports()`. The function definition is retained but no longer called.
2. **Fixed double-date artifact filenames** — `{stem}` already contains the date from the Excel file (`Bank_Performance_Dashboard_YYYYMMDD`), so appending `_{stamp}` produced `..._20260311_..._20260311.html`. Removed the redundant `_{stamp}` suffix from all 28+ artifact paths.
3. **Fixed "Norm NCO Rate (TTM) (%)" label** — Normalized NCO is not TTM; changed to "Norm NCO Rate (%)" in `generate_core_pb_peer_table`, `generate_detailed_peer_table`, and `generate_segment_focus_table`.
4. **Updated CLAUDE.md** — Corrected artifact filename examples (no double date), updated table column examples to use tickers (GS, UBS not Goldman Sachs), removed stale normalized_comparison_table reference from naming docs.
5. **Added 5 regression tests** — no mixed-regime artifacts, no double-date filenames, correct normalized NCO label, CLAUDE.md ticker convention, CLAUDE.md no stale full-name output examples.

### 2026-03-11 — CSV Logger Lifecycle Safety Fix

**Problem**: `csv_log.close()` inside `dash.run()` closed the CSV file while `sys.stdout`/`sys.stderr` were still wrapped by `TeeToLogger`. Subsequent `print()` calls in `main()` raised `ValueError: I/O operation on closed file`. Same risk in `report_generator.py`.

**Root cause**: `CsvLogger.close()` did not restore streams, `TeeToLogger.write()` did not check if the logger was closed, and the close was called too early in the pipeline (inside `run()` before `main()` finished printing).

**Solution**: Made the entire logging lifecycle crash-proof.

**Changes by file:**

1. **logging_utils.py** — Safe lifecycle:
   - `CsvLogger._closed` flag; `is_closed` property
   - `CsvLogger.log()` — no-op after close, wrapped in try/except (never raises)
   - `CsvLogger.log_exception()` — wrapped in try/except (never raises)
   - `CsvLogger.restore_streams()` — puts `sys.stdout`/`sys.stderr` back to originals
   - `CsvLogger.close()` — idempotent: restores streams → flushes → closes file → sets `_closed = True`
   - `CsvLogger.shutdown()` — logs final CONFIG message, then calls `close()`; suppresses secondary exceptions
   - `TeeToLogger.write()` — checks `is_closed` before CSV logging; try/except around CSV call
   - `setup_csv_logging()` — unwraps existing `TeeToLogger` to prevent nested stacking
   - Docstring corrected: 15-column schema (was incorrectly labeled 16)

2. **MSPBNA_CR_Normalized.py** — Removed early close:
   - Removed `csv_log.close()` from `run()` method (was line ~6406)
   - Added `csv_log.shutdown()` in `main()`'s `finally` block — after ALL prints, verification, Power BI, and FRED series check

3. **report_generator.py** — Safe shutdown:
   - Replaced `csv_log.info("shutdown") + csv_log.close()` with `csv_log.shutdown()`
   - Wrapped `csv_log.log_exception()` in try/except to prevent logging errors masking real exceptions

4. **test_regression.py** — 7 new tests in `TestLoggerSafeLifecycle`:
   - `test_print_after_logger_close_does_not_crash`
   - `test_stderr_after_logger_close_does_not_crash`
   - `test_close_restores_streams`
   - `test_double_close_is_safe`
   - `test_setup_csv_logging_does_not_stack_nested_tees`
   - `test_mspbna_has_single_terminal_csv_close_path`
   - `test_report_generator_safe_shutdown`
   - Fixed docstring: "16 columns" → "15 columns"

5. **CLAUDE.md** — Added safe logging lifecycle documentation, corrected schema count, added changelog entry

### 2026-03-11 — Output Naming + CSV Logging Overhaul

**Problem**: Artifact filenames used HHMMSS timestamps causing unnecessary file proliferation. Logging used unstructured text files not suitable for LLM debugging. No stdout/stderr capture.

**Solution**: Date-only artifact naming (YYYYMMDD), per-script CSV structured logs with 15-column schema, stdout/stderr tee capture, centralized utilities.

**Changes by file:**

1. **logging_utils.py** (NEW) — Centralized logging & naming module:
   - `get_run_date_str()` — returns YYYYMMDD date string
   - `build_artifact_filename()` — date-stamped filenames
   - `CsvLogger` — 15-column CSV log writer (reset each run)
   - `TeeToLogger` — stdout/stderr stream wrapper
   - `setup_csv_logging()` — one-call setup
   - Event types: CONFIG, FILE_DISCOVERED, FILE_WRITTEN, DATAFRAME_SHAPE, VALIDATION_WARNING, VALIDATION_ERROR, EXCEPTION, STDOUT, STDERR, CHART_SKIPPED, TABLE_SKIPPED, METRIC_SUPPRESSED, PRECHECK_FAIL, PRECHECK_WARN

2. **MSPBNA_CR_Normalized.py** — Integrated CSV logging:
   - Dashboard filename: `build_artifact_filename()` (date-only, no HHMMSS)
   - `setup_logging()` now creates CSV log via `setup_csv_logging("MSPBNA_CR_Normalized")`
   - Structured log events at: startup (CONFIG), FDIC fetch (DATAFRAME_SHAPE), peer composite (DATAFRAME_SHAPE), 8Q averages (DATAFRAME_SHAPE), workbook write (FILE_WRITTEN), shutdown (CONFIG)
   - stdout/stderr tee'd into CSV log

3. **report_generator.py** — Integrated CSV logging:
   - Date stamp: `get_run_date_str()` (centralized, no inline datetime)
   - CSV log via `setup_csv_logging("report_generator")` at start of `generate_reports()`
   - Structured log events at: workbook discovery (FILE_DISCOVERED), sheet loads (DATAFRAME_SHAPE), preflight warnings (PRECHECK_WARN), preflight errors (PRECHECK_FAIL), chart/scatter/table writes (FILE_WRITTEN), exception (EXCEPTION), shutdown (CONFIG)
   - stdout/stderr tee'd into CSV log

4. **test_regression.py** — 19 new tests across 6 test classes:
   - `TestDateOnlyNaming` (6 tests): date string format, no HHMMSS, suffix/no-suffix, output dir, source checks
   - `TestCsvLogging` (5 tests): column count, required columns, script name in filename, reset per run, schema headers
   - `TestStdoutStderrCapture` (2 tests): stdout/stderr captured as STDOUT/STDERR events
   - `TestFileWriteEvents` (2 tests): FILE_WRITTEN logged in both scripts
   - `TestPreflightEvents` (2 tests): PRECHECK_WARN and PRECHECK_FAIL events
   - `TestClaudeMDLogging` (2 tests): CLAUDE.md documents CSV logging and YYYYMMDD naming

5. **CLAUDE.md** — Updated: date-only naming documentation, CSV log schema, event types, per-script log files, stdout/stderr mirroring, `logging_utils.py` in script table

### 2026-03-11 — County-Level Case-Shiller Geographic Mapping Refactor

**Problem**: Case-Shiller ZIP mapping used CBSA/CBSA-Division approximations (HUD type 8/9) which do not match the exact county-level definitions in the S&P CoreLogic methodology.

**Solution**: Replaced `CASE_SHILLER_METRO_MAP` (20-entry CBSA-based) with `CASE_SHILLER_COUNTY_MAP` (~160-entry FIPS-based) sourced from the S&P Case-Shiller "Index Geography" table. Switched HUD API from type 8/9 (CBSA/CBSADIV-to-ZIP) to type 7 (County-to-ZIP).

**Changes by file:**

1. **case_shiller_zip_mapper.py** — Complete refactor:
   - Removed `CASE_SHILLER_METRO_MAP` (CBSA-based), replaced with `CASE_SHILLER_COUNTY_MAP` (FIPS-based, ~160 entries across 20 metros)
   - Changed HUD API from type 8/9 to type 7 (`_HUD_TYPE_COUNTY_ZIP = 7`)
   - Removed `build_case_shiller_metro_map()`, added `build_case_shiller_county_map()`
   - Rewrote `build_case_shiller_zip_coverage()` — now takes single `county_xwalk` DataFrame, joins on 5-digit FIPS
   - Rewrote `summarize_case_shiller_zip_coverage()` — aggregates by region with unique county counts
   - Updated `validate_zip_coverage()` — validates FIPS code format instead of CBSA duplicates
   - Renamed output sheet `CaseShiller_Metro_Map_Audit` → `CaseShiller_County_Map_Audit`
   - Updated `build_case_shiller_zip_sheets()` orchestrator — fetches per-county HUD data
   - Retained: retry/backoff logic, env toggle, HUD token auth, ratio extraction, `_normalize_zip`, backward-compatible `CASE_SHILLER_METROS`/`map_zip_to_metro`

2. **MSPBNA_CR_Normalized.py** — Updated comment referencing `CaseShiller_County_Map_Audit`

3. **report_generator.py** — Fixed MSBNA label: `resolve_display_label` returns `"MSBNA"` (not `"MS"`)

4. **test_regression.py** — Updated `TestCaseShillerZIP`:
   - Changed audit sheet key assertions from `CaseShiller_Metro_Map_Audit` to `CaseShiller_County_Map_Audit`
   - Added 6 new tests: `test_county_map_has_20_regions`, `test_county_map_fips_are_5_digit`, `test_county_map_has_required_fields`, `test_hud_type_is_county_zip`, `test_no_cbsa_references_in_coverage_builder`
   - Fixed `test_msbna_label_is_msbna` (was `test_msbna_label_is_ms`)

5. **CLAUDE.md** — Rewrote Section 11 for county-level mapping. Updated sheet layout table. Added changelog.

### 2026-03-11 — Centralized Label Resolver (Ticker-Style Display Labels)

**Problem**: Bank/comparator labels were hardcoded as full names ("Goldman Sachs", "Core PB Avg") across 6+ table/chart builder functions, producing inconsistent output.

**Solution**: Created `resolve_display_label(cert, name)` — a single centralized resolver in `report_generator.py`.

**Changes by file:**

1. **report_generator.py** — Added `_TICKER_MAP`, `_COMPOSITE_LABELS`, `_SUBJECT_CERT`, `_MSBNA_CERT` constants and `resolve_display_label()` function. Applied resolver to:
   - `generate_credit_metrics_email_table()` — dynamic peer identification via resolver
   - `generate_core_pb_peer_table()` — col_mapping uses resolver
   - `generate_detailed_peer_table()` — col_mapping uses resolver; `_MSBNA_CERT` constant
   - `generate_segment_focus_table()` — col_certs built via resolver
   - `_build_dynamic_peer_html()` — uses `_MSBNA_CERT` constant
   - `create_credit_deterioration_chart_v3()` — entity_names via resolver
   - `create_credit_deterioration_chart_ppt()` — names dict via resolver
   - `generate_html_email_table_dynamic()` — CSS class check uses `"MS"` instead of `"MSBNA"`
   - Years-of-reserves scatter label uses resolver

2. **test_regression.py** — Added `TestLabelResolver` class (11 tests): resolver exists, ticker map entries, composite labels, no hardcoded "Goldman Sachs" in 6 builder functions, chart v3/PPT use resolver, detailed/segment tables use resolver, subject→MSPBNA, MSBNA→MS, _MSBNA_CERT constant usage. Updated 3 existing TestDirectiveC tests for resolver checks.

3. **CLAUDE.md** — Added ENTITY DISPLAY LABEL POLICY section with full label map. Updated table column examples from full names to tickers. Added naming convention documentation.

### 2026-03-11 — Wealth-Focused Tables, Data Integrity, Stock vs Flow

**Report Design — Wealth-Focused Executive Summary & Segment Tables:**

1. **Executive summary rewrite (report_generator.py)**: `generate_credit_metrics_email_table` now accepts `is_normalized` parameter. Columns changed from MSPBNA/MSBNA/Core PB/All Peers to **MSPBNA | GS | UBS | Wealth Peers | Delta MSPBNA vs Wealth Peers**. Wealth Peers = Core PB composite (90001 standard, 90004 normalized). Both standard and normalized versions generated as separate artifacts.

2. **Segment focus tables (report_generator.py)**: `generate_segment_focus_table` rewritten with same wealth-focused columns (MSPBNA | GS | UBS | Wealth Peers | Delta). Dropped MSBNA and All Peers. Uses Core PB composite as Wealth Peers. GS and UBS identified dynamically from bank NAME.

3. **Core PB peer table (report_generator.py)**: `generate_core_pb_peer_table` repurposed — dropped MSBNA column, composite labeled "Wealth Peers" instead of "Core PB Avg". Title changed to "Wealth Peer Analysis".

4. **Ratio components fix (report_generator.py)**: `_synth()` now synthesizes `Norm_Risk_Adj_Gross_Loans = Norm_Gross_Loans - SBL_Balance` for normalized mode. Fixes N/A denominator on Norm_Risk_Adj_Allowance_Coverage row.

5. **Detailed peer table unchanged**: `generate_detailed_peer_table` remains the only broad all-peer table using 90003/90006.

**Data Integrity — Stock vs Flow Math:**

1. **PLLL added to FDIC_FIELDS_TO_FETCH**: Provision for Loan & Lease Losses now fetched.

2. **Module-level `ytd_to_discrete()` (MSPBNA_CR_Normalized.py)**: New helper converts YTD cumulative series to discrete quarterly flows. Inner `compute_quarterly_from_ytd` now delegates to this. Groups by CERT, handles Q1 (flow = YTD), Q2-Q4 (flow = diff).

3. **Module-level `annualize_ytd()` (MSPBNA_CR_Normalized.py)**: New helper annualizes YTD flow variables: `YTD_value * (4.0 / quarter)`. Standard banking convention for income-statement ratios.

4. **Provision and Yield annualized**: `Provision_to_Loans_Rate` and `Loan_Yield_Proxy` (and `Norm_Loan_Yield`) now use `annualize_ytd()` instead of TTM rolling sums. Gives current-period-only view without stale prior-year quarters.

5. **NCO TTM verified correct**: All NCO calculations already use `ytd_to_discrete` → `rolling(4).sum()` path. No change needed.

6. **`TTM_Past_Due_Rate` → `Past_Due_Rate`**: Renamed globally across MSPBNA_CR_Normalized.py, report_generator.py, master_data_dictionary.py. Delinquency is a point-in-time stock variable — the TTM prefix was incorrect. Display label updated to "Past Due Rate (%)" without TTM.

**Tests (test_regression.py)**: Added `TestDirectiveC` class (15 tests): executive summary wealth columns, core_pb composite usage, is_normalized parameter, segment table wealth peers, core PB table no MSBNA, detailed table all_peers, ratio components norm denominator, Past_Due_Rate rename, PLLL fetch, ytd_to_discrete/annualize_ytd existence, provision/yield annualization, quarterly delegation, both executive versions, display label.

**CLAUDE.md**: Added Stock vs Flow Math convention, Wealth-Focused vs Detailed Table distinction, executive/segment column requirements.

### 2026-03-10 — Balance-Gating, Composite Coverage, Preflight Scoping

**Root causes from workbook review:**
- Excluded NCO over-exclusion: MSPBNA 2024-12-31 showed Total_NCO_TTM=27K but Excluded_NCO_TTM=54K. Root cause: Auto/Ag NCO MDRM fields (RIADK205/K206, RIAD4635/4645) were nonzero despite zero excluded balances for those categories.
- Normalized composites misleading: Only 2 of 8 banks had non-NaN normalized NCO, yet composites showed usable averages.
- Preflight over-blocking: Historical material_nan rows caused blocking even when the latest plotted period was clean.
- Stale load_config defaults were 19977; corrected to production CERTs 34221/32992.

**Fixes applied:**
1. **Balance-gating for excluded NCO (MSPBNA_CR_Normalized.py)**: Added balance-gating for 4 excluded NCO categories (Auto, Ag, ADC, OO CRE). If excluded balance is zero, force excluded NCO to zero and set `_*_NCO_Gated` flag. Added `Exclusion_Component_Audit` sheet with per-bank/quarter gating decisions, dominant exclusion categories, and zero-balance/nonzero-NCO flags.

2. **Normalized composite minimum coverage (MSPBNA_CR_Normalized.py)**: Added 50% minimum contributor coverage threshold for normalized composites (90004/90006). Metrics below threshold are NaN'd out. Added `Composite_Coverage_Audit` sheet documenting per-group/metric contributor counts, coverage %, and NaN-out decisions. Critical metrics: `Norm_NCO_Rate`, `Norm_Nonaccrual_Rate`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Gross_Loans`.

3. **Preflight period-scoping (report_generator.py)**: Severity checks now determine `latest_repdte` and only block on material failures at the latest plotted period. Historical material_nan failures become informational warnings.

4. **load_config defaults fixed (report_generator.py)**: Changed defaults from 19977 to `MSPBNA_CERT=34221`, `MSBNA_CERT=32992`.

5. **Regression tests (test_regression.py)**: Added 7 function-based tests + 7 unittest classes covering balance-gating (Ag, Auto, ADC, OO CRE), composite coverage (below/above/at threshold), preflight period-scoping (blocks at latest, ignores historical), load_config defaults, and audit sheet inclusion.

6. **Case-Shiller ZIP sheet persistence (MSPBNA_CR_Normalized.py)**: Wrapped `build_case_shiller_zip_sheets()` call in try/except so HUD API failures do not crash the pipeline. Added logging for which ZIP sheets are written. The `**cs_kwargs` unpack in `write_excel_output()` persists non-empty sheets (CaseShiller_Zip_Coverage, CaseShiller_Zip_Summary, CaseShiller_Metro_Map_Audit). When enrichment is disabled or HUD token is missing, only the audit sheet (always non-empty) is written. Added regression tests for resilience and audit sheet presence.

**New Excel sheets:** `Exclusion_Component_Audit`, `Composite_Coverage_Audit`, `Metric_Validation_Audit`

### 2026-03-11 — Composite CERT Regime Cleanup (report_generator.py)

**Root cause**: Upstream peer-group construction was correct (4 groups, 7 peers, active composites 90001/90003/90004/90006 in workbook). But downstream `report_generator.py` was still littered with stale references to legacy composite CERTs from the old scheme (99998, 99999, 90002, 90005). This mismatch prevented many charts and tables from constructing correctly.

**Active composite regime (current):**

| Role | Standard | Normalized |
|---|---|---|
| All Peers | 90003 | 90006 |
| Core PB | 90001 | 90004 |

**Legacy/inactive (must never drive artifact selection):**
- 90002 (former MSPBNA+Wealth standard)
- 90005 (former MSPBNA+Wealth normalized)
- 99998 (former "Peers Ex. F&V")
- 99999 (former "All Peers" alias)

Legacy CERTs may still appear in `ALL_COMPOSITE_CERTS` for scatter-dot exclusion, but they are never used as peer-average selectors for charts or tables.

**Fixes applied:**

1. **Canonical constants (report_generator.py)**: Added `ACTIVE_STANDARD_COMPOSITES`, `ACTIVE_NORMALIZED_COMPOSITES`, `INACTIVE_LEGACY_COMPOSITES` at module top. `ALL_COMPOSITE_CERTS` is now derived from these.

2. **create_credit_deterioration_chart_v3**: Replaced 99999→90003 (All Peers), 99998→90001 (Core PB).

3. **create_credit_deterioration_chart_ppt**: Default entities changed from `[subject, 99999, 99998]` to `[subject, 90003, 90001]`. Names/colors dict updated for all 4 active composites.

4. **plot_scatter_dynamic**: Default args changed from `peer_avg_cert_primary=99999, peer_avg_cert_alt=99998` to `90003, 90001`.

5. **generate_detailed_peer_table**: Removed 99999 fallback — now uses `ACTIVE_NORMALIZED_COMPOSITES["all_peers"]` or `ACTIVE_STANDARD_COMPOSITES["all_peers"]`.

6. **generate_segment_focus_table**: Same — removed 99999 fallback.

7. **generate_ratio_components_table**: Same — uses canonical constants.

8. **generate_credit_metrics_email_table**: Changed from 90002 (removed "Wealth" composite) to `ACTIVE_STANDARD_COMPOSITES["all_peers"]` (90003). Column header changed from "MS+Wealth" to "All Peers".

9. **generate_flexible_html_table**: Default peer_certs changed from `[90001, 90002, 90003]` to `[90001, 90003]`.

10. **Scatter exclusion sets**: All 3 chart helpers (`build_growth_vs_deterioration_chart`, `build_risk_adjusted_return_chart`, `build_concentration_vs_capital_chart`) now use `ALL_COMPOSITE_CERTS` instead of inline sets.

11. **validate_composite_cert_regime helper**: New function confirms active composites present and flags legacy composites.

12. **Regression tests (test_regression.py)**: Added `TestCompositeRegimeCleanup` (11 tests): canonical constant definitions, no legacy CERTs in selection paths, correct standard/normalized peer avg CERTs, chart v3 uses current composites, scatter defaults current regime, no legacy fallback in table generators, validate helper exists.

### 2026-03-11 — End-to-End Workbook Output Fix, Display Labels, Matplotlib Suppression

**Root cause of workbook-level failure**: The presentation-layer code changes (curated allowlists, metric roles, display labels) were structurally correct in source, but the workbook output still appeared as a raw dump because:
- `LOCAL_DERIVED_METRICS` in `master_data_dictionary.py` was missing display labels for ~30 metrics used in the curated tabs (`Norm_*`, `RIC_*`, profitability, liquidity). `_get_metric_short_name()` fell back to raw codes, making `Metric Name` identical to `Metric Code`.
- `create_normalized_comparison` did not compute `Performance_Flag`, so the Normalized_Comparison tab lacked evaluative context.
- `generate_ratio_components_table` always selected 99999/90003 as the peer composite regardless of `is_normalized` mode, so normalized HTML tables used the wrong peer.
- No workbook-level regression tests existed to catch these end-to-end failures.

**Lesson**: Presentation-layer fixes are only considered complete once visible in the generated workbook. Source-code-level tests alone are insufficient — workbook-level validation is required.

**Fixes applied:**

1. **Display label coverage (master_data_dictionary.py)**: Added ~30 entries to `LOCAL_DERIVED_METRICS` covering all metrics in `SUMMARY_DASHBOARD_METRICS` and `NORMALIZED_COMPARISON_METRICS`: `Norm_Gross_Loans`, `Norm_ACL_Coverage`, `Norm_Risk_Adj_Allowance_Coverage`, `Norm_Nonaccrual_Rate`, `Norm_NCO_Rate`, `Norm_Delinquency_Rate`, `Norm_SBL_Composition`, `Norm_Fund_Finance_Composition`, `Norm_Wealth_Resi_Composition`, `Norm_CRE_Investment_Composition`, `Norm_Exclusion_Pct`, `Norm_Loan_Yield`, `Norm_Loss_Adj_Yield`, `Norm_Risk_Adj_Return`, `Norm_Provision_Rate`, `Norm_Total_NCO`, `Norm_Total_Nonaccrual`, `Norm_CRE_ACL_Share`, `Norm_Resi_ACL_Share`, `RIC_CRE_Loan_Share`, `RIC_Resi_Loan_Share`, `RIC_CRE_ACL_Share`, `RIC_CRE_ACL_Coverage`, `RIC_CRE_Risk_Adj_Coverage`, `RIC_CRE_NCO_Rate`, `Provision_to_Loans_Rate`, `Liquidity_Ratio`, `HQLA_Ratio`, `Loans_to_Deposits`. Debug logging added for any remaining fallback cases.

2. **Normalized comparison Performance_Flag (MSPBNA_CR_Normalized.py)**: `create_normalized_comparison` now computes `Performance_Flag` using Core PB Norm (90004) percentile, routed through `_get_performance_flag()` which returns blank for descriptive metrics.

3. **Diagnostic logging before workbook write (MSPBNA_CR_Normalized.py)**: Added explicit logging of row count and first 5 `Metric Code` values for both `Summary_Dashboard` and `Normalized_Comparison` immediately before `write_excel_output()`. Also adds a hard error log if `Norm_Provision_Rate` leaks into the normalized tab.

4. **Ratio components table is_normalized-aware peer (report_generator.py)**: `generate_ratio_components_table` now selects `peer_cert=90006` when `is_normalized=True` (previously always used 99999/90003). Re-pick after synthetic columns uses safe `peer_slice.empty` check instead of bare `.iloc[0]`.

5. **Matplotlib warning suppressed (report_generator.py)**: Added `import warnings` and `warnings.filterwarnings("ignore", message="This figure includes Axes that are not compatible with tight_layout", category=UserWarning)` at module top.

6. **Workbook-level regression tests (test_regression.py)**: Added 4 new test classes (14 test methods total):
   - `TestWorkbookLevelCuration` — validates curated allowlist sizes, excludes raw MDRM fields, excludes internal pipeline columns, excludes Norm_Provision_Rate, verifies display label coverage for all curated metrics
   - `TestDisplayLabelCoverage` — validates LOCAL_DERIVED_METRICS has entries for all Norm_, RIC_, and profitability/liquidity metrics
   - `TestHTMLTableResilience` — validates is_normalized-aware peer selection (90006), safe .empty fallback, matplotlib warning filter
   - `TestNormalizedComparisonFlags` — validates Performance_Flag column and _get_performance_flag usage in create_normalized_comparison

### 2026-03-11 — Presentation Curation, Metric Roles, Chart Resilience

1. **Curated presentation tabs (MSPBNA_CR_Normalized.py)**: `Summary_Dashboard` now uses `SUMMARY_DASHBOARD_METRICS` allowlist (22 curated KPIs) instead of dumping all numeric columns. `Normalized_Comparison` uses `NORMALIZED_COMPARISON_METRICS` (14 curated normalized KPIs). Raw MDRM fields and internal pipeline columns are excluded from presentation tabs.

2. **Metric-role classification (MSPBNA_CR_Normalized.py)**: Added `DESCRIPTIVE_METRICS` frozenset containing size/balance/composition metrics (ASSET, LNLS, Gross_Loans, SBL_Composition, etc.). `_get_performance_flag()` returns blank for descriptive metrics — prevents misleading "Top Quartile" / "Bottom Quartile" flags on non-evaluative fields.

3. **Norm_Provision_Rate suppressed from presentation**: Excluded from `NORMALIZED_COMPARISON_METRICS` since it is intentionally NaN (provision expense is not segment-specific). Dead-metric suppression in HTML tables also catches it.

4. **Display labels**: Both `create_peer_comparison` and `create_normalized_comparison` use `_get_metric_short_name(metric)` which resolves display names from `FDIC_Metric_Descriptions` (via `MasterDataDictionary.lookup_metric`). Falls back to the metric code itself when no display label exists.

5. **Chart resilience (report_generator.py)**: `create_credit_deterioration_chart_ppt` filters out CERTs missing from data before plotting. Uses `colors.get(c, fallback)` instead of `colors[c]`. Skips all-NaN peer lines with warning. Removed `fig.tight_layout()` call that conflicted with `twinx()` + `GridSpec`.

6. **Ratio components table resilience (report_generator.py)**: `generate_ratio_components_table` no longer aborts when peer composite is missing. Subject bank is required; peer defaults to `pd.Series()` (renders as N/A).

7. **Regression tests (test_regression.py)**: Added 6 function-based tests + 8 unittest methods covering curated allowlists, metric-role policy, display labels, preflight logic, tight_layout removal, and ratio components safe lookup.

### 2026-03-10 — Consistency Pass: TTM Fix, Validation Wiring, Doc Cleanup

1. **Norm_Loan_Yield / Norm_Loss_Adj_Yield TTM fix (MSPBNA_CR_Normalized.py)**: Root cause was a column-name mismatch in the TTM rolling map. `col.replace('_YTD', '_Q')` produces `Int_Inc_Loans_Q` but the TTM map key was `Int_Inc_Loans_YTD_Q`. Same bug for `Provision_Exp_Q` and `Total_Int_Exp_Q`. Corrected all three TTM map keys. Also fixed `prov_q` reference and changed fallback from 0 to NaN so missing TTM columns produce NaN instead of silent zeros.

2. **Norm_Provision_Rate intentionally NaN**: Confirmed by-design — provision expense is not segment-specific in call reports. Documented in Section 8.

3. **Validation wiring (MSPBNA_CR_Normalized.py)**: Wired `run_upstream_validation_suite(proc_df_with_peers)` from `metric_registry.py` into Step 1, just before Excel output. Results go to `Metric_Validation_Audit` sheet. Wrapped in try/except for resilience.

4. **CLAUDE.md cleanup**: Removed stale `19977` default examples (replaced with `34221`/`32992`). Removed false claim that validation suite was already wired. Updated Section 8 (known issues) to reflect actual metric status. Updated Section 9 (validation engine) to accurately describe wiring.

5. **Regression tests**: Added TTM column name tests, validation wiring source check, no-stale-19977-defaults test.

### 2026-03-10 — Data Mapping, Math, and Formatting Bug Fixes

1. **Duplicate Peer Composites (MSPBNA_CR_Normalized.py)**: Fixed `_create_peer_composite()` so normalized composites (90004/90005/90006) are mathematically distinct from standard composites (90001/90002/90003). Standard composites now NaN-out `Norm_*` columns; normalized composites NaN-out standard rate columns. Previously, both sets produced identical averages because they shared the same cert lists and averaged all numeric columns indiscriminately.

2. **Diff Math — Percentage-Point Deltas (report_generator.py)**: Created `_fmt_percent_diff(diff, ref_value)` to fix inflated diff values (e.g., ROE 14.69% vs 13.94% showing +75.00% instead of +0.75%). The bug was in `_fmt_percent()` being used for both display and diffs — its `abs(v) < 1.0` heuristic incorrectly multiplied small ppt diffs by 100 when source values were in pct-point scale (>=1.0). Fixed in `generate_credit_metrics_email_table`, `generate_detailed_peer_table`, and `generate_segment_focus_table`.

3. **Ratio Components Column Mapping (report_generator.py)**: Fixed ~12 phantom column names in `generate_ratio_components_table` that didn't match upstream DataFrame columns. Key fixes: `Nonaccrual_Total` → `Total_Nonaccrual`, `Risk_Adj_Gross_Loans` → computed inline, `Total_Past_Due` → computed from `TopHouse_PD30 + TopHouse_PD90`, `RIC_Resi_Balance` → `RIC_Resi_Cost`, `RIC_CRE_NCO` → `RIC_CRE_NCO_TTM`, `RIC_CRE_Delinq`/`RIC_Resi_Delinq` → computed from PD30+PD90, `RIC_Fund_Finance_Loan_Share` → `Fund_Finance_Composition`.

4. **Unit Formatting ($B display) (report_generator.py)**: Fixed `_fmt_money_billions()` and inline `fmt_val()` functions. FDIC stores dollar amounts in $K (thousands), so dividing by `1e6` gives billions. The inline formatters were incorrectly dividing by `1e9` (producing values like `$0.3B` instead of `$254.7B`). Also fixed label mismatches (`$254.7M` → `$254.7B`).

5. **Dead Metric Suppression (report_generator.py)**: Added filtering in all 4 HTML table generators (`generate_credit_metrics_email_table`, `generate_ratio_components_table`, `generate_segment_focus_table`, `generate_detailed_peer_table`) to skip metric rows where all displayed entity values are 0 or NaN. Prevents misleading `0.00%` rows for metrics like `Norm_Loan_Yield` and `Norm_Provision_Rate`.

### 2026-03-10 — FRED Expansion Layer (Registry-Driven Macro/Market Context)

1. **New module `fred_series_registry.py`**: Registry-driven design with `FREDSeriesSpec` dataclass. 80+ series across 4 modules: SBL proxy (14), Residential/Jumbo (19), CRE (23), Case-Shiller seed (24+). Each spec carries category, priority, transforms, chart routing, and sheet assignment.

2. **New module `fred_case_shiller_discovery.py`**: Async release-table discovery for standard HPI (release 199159), tiered HPI (release 345173), and sales-pair counts. Classifies each discovered series by tier, metro, SA/NSA. Merges with static seed, deduplicating by series_id.

3. **New module `fred_transforms.py`**: Full transformation pipeline: pct_chg (MoM/QoQ/YoY), z-scores (5Y/10Y), rolling averages (3M/12M), 4 named spreads (jumbo-conforming, CRE demand-standards, resi growth-delinquency, high-tier vs national HPI), and 4 regime flags.

4. **New module `fred_ingestion_engine.py`**: Async fetcher (`FREDExpansionFetcher`) compatible with upstream pattern, validation engine (duplicates, discontinued, stale, missing metadata), sheet-level output routing, and Excel writer.

5. **First-wave charts in `report_generator.py`**: 5 new chart functions: `plot_sbl_backdrop`, `plot_jumbo_conditions`, `plot_resi_credit_cycle`, `plot_cre_cycle`, `plot_cs_collateral_panel`. Integrated into `generate_reports()` with graceful fallback when expansion sheets are absent.

6. **CLAUDE.md Section 9**: Full documentation of FRED expansion architecture, registry design, transforms, spreads, regime flags, output sheets, and validation checks.

### 2026-03-10 — HTML Table Overhaul (Standard + Normalized Split)

1. **Detailed Peer Table refactored**: `generate_detailed_peer_table` now accepts `is_normalized` parameter. Standard version uses headline metrics (TTM_NCO_Rate, etc.); Normalized version uses Norm_ metrics. Column ordering changed: MSPBNA and MSBNA placed on the extreme left.

2. **New Core PB Table**: Added `generate_core_pb_peer_table` that dynamically identifies Goldman/UBS CERTs and uses Core PB Avg (90001 standard, 90004 normalized).

3. **Shared HTML builder**: Added `_build_dynamic_peer_html` helper to eliminate code duplication between detailed peer and core PB tables.

4. **Segment Focus Table refactored**: `generate_segment_focus_table` now accepts `is_normalized` parameter. Normalized variant uses Norm_ common metrics and Norm_ segment-specific ACL share/coverage columns. Both variants maintain full 15-metric lists.

5. **generate_reports() unified loop**: All detailed peer, core PB, ratio components, CRE segment, and Resi segment tables now generate both Standard and Normalized versions via `for is_norm in [False, True]` loop.

6. **CLAUDE.md conventions added**: Table Column Ordering, Table Completeness, and Table Duplication rules added to Section 4.

### 2026-03-10 — 7-Part Defect Fix (Normalized Metrics, Scatter, Validation)

**Root Causes:**
- Scatter contamination: `build_plot_df_with_alias` appended alias rows instead of replacing, causing composite CERTs (90004, 90006) to appear as blue peer dots in normalized scatter plots.
- Silent NCO zeroing: `.clip(lower=0)` at 6 locations masked over-exclusion errors where Excluded_* exceeded Total_*, producing misleading zero values instead of NaN.
- Duplicate peer groups: 90001/90004, 90002/90005, 90003/90006 share identical cert lists — distinction is only `use_normalized` flag.

**Fixes applied:**
1. **Scatter integrity (report_generator.py)**: Removed all `build_plot_df_with_alias` calls and later deleted the function definition entirely. `plot_scatter_dynamic()` now has `composite_certs` parameter (default: all 9 synthetic CERTs) that excludes composites from peer dots. Standard/normalized scatters pass true composite CERTs (`90003`/`90006`) directly via `peer_avg_cert_primary`.

2. **Diagnostics-first normalization (MSPBNA_CR_Normalized.py)**: Replaced `.clip(lower=0)` with tolerance-aware logic: minor negatives (<5% of total) clip to 0, material negatives become NaN. Added diagnostic columns: `_Norm_NCO_Residual`, `_Norm_NA_Residual`, `_Norm_Loans_Residual`, `_Flag_*_OverExclusion`, `_*_OverExclusion_Pct`, `_Norm_*_Severity` (ok/minor_clip/material_nan).

3. **Peer group validation (MSPBNA_CR_Normalized.py)**: Added `validate_peer_group_uniqueness()` — hard validation that no two groups within the same `use_normalized` mode share identical cert lists. Added `build_peer_group_definitions_df()` for Excel output.

4. **Semantic validation rules (metric_registry.py)**: Added `ValidationRule` dataclass and 5 rules: (A) over-exclusion check, (B) flatline anomaly detection, (C) duplicate composite detection, (D) output contamination check, (E) consumer linkage audit. `run_semantic_validation()` and `run_full_validation_suite()` entry points.

5. **Preflight validation (report_generator.py)**: Added `validate_output_inputs()` — runs before report generation, checks for missing subject bank, all-NaN/all-zero normalized metrics, material over-exclusion severity, missing real peers, and semantic validation. Aborts on blocking errors, prints warnings.

6. **Regression tests (test_regression.py)**: 11 executable assertions covering scatter integrity, peer group uniqueness, over-exclusion detection, workbook sanity, consumer trace, and semantic validation.

**New Excel sheets:** `Normalization_Diagnostics`, `Peer_Group_Definitions`

### 2026-03-10 — Normalized Residential QA (Part 4B)

**Defects found:**
1. **Naming collision**: `Norm_Resi_Composition` referenced in detailed peer table and segment focus table but never computed upstream. Canonical name is `Norm_Wealth_Resi_Composition`.
2. **Casing inconsistency**: Upstream created `Norm_RESI_ACL_Coverage`, report_generator expected `Norm_Resi_ACL_Coverage`.
3. **Mislabeled metadata**: `Norm_RESI_ACL_Coverage` described as "Resi Reserve % of Norm ACL" — actually `RIC_Resi_ACL / Wealth_Resi_Balance` (coverage, not share). Same for `Norm_CRE_ACL_Coverage` and `Norm_Comm_ACL_Coverage`.
4. **Phantom column refs**: `RIC_Resi_Balance` and `RIC_CRE_Balance` in segment focus tables — these don't exist (should be `RIC_Resi_Cost` / `RIC_CRE_Cost`).

**Fixes applied:**
1. All `Norm_Resi_Composition` refs → `Norm_Wealth_Resi_Composition` in report_generator.py
2. `Norm_RESI_ACL_Coverage` → `Norm_Resi_ACL_Coverage` in upstream
3. Metadata display labels corrected to reflect actual numerator/denominator
4. `RIC_Resi_Balance` → `RIC_Resi_Cost`, `RIC_CRE_Balance` → `RIC_CRE_Cost`
5. Added `Norm_Wealth_Resi_Composition`, `Norm_Resi_ACL_Share`, `Norm_Resi_ACL_Coverage` to metric registry
6. Added `Resi_Normalized_Audit` Excel sheet
7. Added 4 regression tests for resi naming, coverage/share, and canonical references

**New Excel sheet:** `Resi_Normalized_Audit`

### 2026-03-10 — HUD USPS ZIP Crosswalk Enrichment for Case-Shiller Metros

1. **New module `case_shiller_zip_mapper.py`**: Maps 20 regional Case-Shiller metros to ZIP codes via HUD USPS Crosswalk API. Contains 20-entry `CASE_SHILLER_METRO_MAP` with CBSA/CBSA-Div codes, HUD API client with retry logic, coverage builder, summary aggregator, and 7-check validation.

2. **Metro mapping judgment calls**: 10 metros use CBSA Division-level mapping (type=9) where S&P CoreLogic tracks a subdivision: New York (Div 35614), Los Angeles (31084), Chicago (16974), Miami (33124), Washington (47894), Detroit (19804), Seattle (42644), Boston (14454), Dallas (19124), San Francisco (41884). Remaining 10 metros use CBSA-level (type=8).

3. **Integration into `fred_ingestion_engine.py`**: ZIP enrichment runs automatically after FRED data fetch in `run_expansion_pipeline_async()`. Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` env var (default: `true`). Graceful fallback if HUD token is missing or API fails.

4. **3 new Excel sheets**: `CaseShiller_Zip_Coverage` (one row per region/ZIP), `CaseShiller_Zip_Summary` (aggregate per metro), `CaseShiller_Metro_Map_Audit` (mapping table with judgment call notes).

5. **Excluded indexes**: U.S. National, Composite-10, Composite-20 are explicitly excluded (no geographic footprint).

### 2026-03-10 — Resi Series Expansion + Top-Down Normalization Fix

**Root cause**: A "OVERRIDE: Normalized Performance (Wealth Segments Only)" block was reconstructing `Norm_Total_NCO`, `Norm_Total_Nonaccrual`, `Norm_PD30`, and `Norm_PD90` bottom-up from `wealth_resi_nco_pure + sum_cols(...)`. This accidentally excluded SBL and any unmapped RESI lines from normalized totals.

**Fixes applied:**
1. **Expanded residential balance definitions**: `compute_wealth_resi_bal()` and `resi_sum` (for `RIC_Resi_Cost`) now include First Liens (1797) + Junior Liens (5367) + HELOC/Open-End (5368, 1799) with `best_of()` fallback. Both definitions are now consistent.
2. **Deleted bottom-up override**: Removed the 4 bottom-up assignments (`Norm_Total_NCO`, `Norm_Total_Nonaccrual`, `Norm_PD30`, `Norm_PD90`) from the override block.
3. **Restored top-down math**: `Norm_Total_NCO` and `Norm_Total_Nonaccrual` remain computed top-down via diagnostics-first logic. Added top-down `Norm_PD30 = TopHouse_PD30 - Excluded_PD30` and `Norm_PD90 = TopHouse_PD90 - Excluded_PD90`. Persisted `Excluded_PD30`/`Excluded_PD90` columns for this purpose.
4. **Retained segment metrics**: `Wealth_Resi_TTM_NCO_Rate`, `Wealth_Resi_NA_Rate`, `Wealth_Resi_Delinquency_Rate` remain as segment-level rates for HTML tables.
5. **CLAUDE.md**: Added "Normalization Methodology" convention mandating top-down-only approach.

### 2026-03-10 — Production-Safety Fix Pass

1. **Metadata sheet contract**: Renamed `FDIC_Metadata` kwarg to `FDIC_Metric_Descriptions` in `write_excel_output()` so workbook sheet name matches what `report_generator.py` reads.
2. **IDB label cleanup**: Removed `IDB_` prefix from all 17 keys in `master_data_dictionary.py` (`LOCAL_DERIVED_METRICS`) and updated 2 references in `MSPBNA_CR_Normalized.py`.
3. **Peer group docs**: Updated CLAUDE.md to reflect 4 groups (not 6); documented removal of MSPBNA+Wealth duplicates.
4. **Normalization conventions**: Added Section 6 documenting `_Norm_Total_Past_Due`, `Norm_ACL_Balance`, `Norm_Risk_Adj_Gross_Loans`, IDB label ban, and Case-Shiller toggle.

### 2026-03-10 — FRED Series Fix + metric_registry + Dead Code Cleanup

1. **FRED series typos fixed (MSPBNA_CR_Normalized.py)**: `CORCCLACBS` → `CORCCACBS`, `RCVRUSQ156N` → `RHVRUSQ156N`.
2. **Discontinued FRED series removed**: `GOLDAMGBD228NLBM` (discontinued), `DEPALL` (redundant with `DPSACBW027SBOG`).
3. **metric_registry.py — Norm_ACL_Coverage fix**: Changed dependencies from `Total_ACL` to `Norm_ACL_Balance`. Added `Norm_Risk_Adj_Allowance_Coverage` spec (`Norm_ACL_Balance / (Norm_Gross_Loans - SBL_Balance)`).
4. **Removed `build_plot_df_with_alias()` (report_generator.py)**: Function was dead code — defined but never called. Removed to eliminate confusion with SCATTER & CHART COMPOSITE HANDLING convention.
5. **CLAUDE.md**: Added FRED Series Validation convention, updated scatter handling docs, documented all changes.

**Remaining risks:**
- `Norm_Provision_Rate` is intentionally NaN — provision expense is not segment-specific in call reports, so a normalized rate would be semantically misleading.
- `run_upstream_validation_suite()` is now wired into MSPBNA_CR_Normalized.py (Step 1) and writes `Metric_Validation_Audit` sheet.

### 2026-03-10 — Targeted Cleanup Pass (ZIP Toggle, Metro Map, Tests, Preflight)

1. **case_shiller_zip_mapper.py — consolidated dual implementations**: Removed the second concatenated simple-version implementation (lines 767-897). Kept the comprehensive HUD API version as the single authoritative implementation. Moved `is_zip_enrichment_enabled()` to module-level. Added env toggle check at top of `build_case_shiller_zip_sheets()` — when disabled, returns empty Coverage/Summary DataFrames with audit noting `SKIPPED: disabled by env flag`. Preserved `CASE_SHILLER_METROS` dict and `map_zip_to_metro()` as backward-compatible helpers.

2. **fred_case_shiller_discovery.py — METRO_MAP fixes**: Fixed duplicate key `"CH"` (was mapped to both Chicago and Charlotte — Charlotte's correct FRED prefix is `"CR"`). Fixed `"Washington DC"` → `"Washington"` to match case_shiller_zip_mapper.py. Added `validate_metro_map()` assertion helper that checks entry count, duplicate values, and Washington naming.

3. **test_regression.py — stale peer group test rewrite**: Rewrote `test_peer_group_no_intra_mode_duplicates()` to import real `PEER_GROUPS` from `MSPBNA_CR_Normalized.py` (4 groups, not former 6). Added assertion for exactly 4 groups. Updated `test_zip_toggle_disables_output` to validate HUD version output (empty Coverage/Summary, SKIPPED in audit). Replaced old simple-version tests (`test_zip_codes_are_5_char_strings`, `test_summary_zip_count_matches_detail`) with unit tests for `_normalize_zip()` and `map_zip_to_metro()`. Added `test_discovery_metro_map_no_duplicates` and `test_discovery_washington_no_dc_suffix`.

4. **report_generator.py — preflight blocks on material normalization**: Updated `validate_output_inputs()` so material normalization severity (`material_nan`) on the **subject bank** is now a **blocking error** (added to `errors` list, suppresses affected normalized charts). Peer-only material failures remain warnings.

5. **Verified**: `IDB_` prefix absent from `master_data_dictionary.py`. `FDIC_Metric_Descriptions` used consistently across all files.

### 2026-03-10 — Preflight Hardening, Peer-Average Blocking, Test Expansion

1. **report_generator.py — preflight now blocks on plotted peer-average failures**: `validate_output_inputs()` now checks material normalization severity (`material_nan`) on plotted peer-average composites (90004, 90006) in addition to the subject bank. Any material failure on these CERTs is a blocking error that suppresses normalized charts/scatter.

2. **report_generator.py — composite existence validation**: Preflight now validates that all required composite CERTs exist in data before plotting. Standard flow requires 90003 + 90001; normalized flow requires 90006 + 90004. Missing composites produce blocking errors.

3. **report_generator.py — readable artifact names**: `suppressed_charts` now uses human-readable names (`normalized_credit_chart`, `normalized_scatter_nco_vs_nonaccrual`, `standard_scatter_nco_vs_npl`, `standard_scatter_pd_vs_npl`) instead of severity-column-derived names.

4. **test_regression.py — 7 new tests added**:
   - `test_no_idb_keys_in_data_dictionary` — asserts `LOCAL_DERIVED_METRICS` has no `IDB_` keys
   - `test_preflight_blocks_peer_avg_material_nan` — 90006 with `material_nan` blocks
   - `test_preflight_blocks_missing_normalized_composite` — missing 90004/90006 blocks
   - `test_claude_md_no_conflict_markers` — no git merge conflict markers in CLAUDE.md
   - `TestIDBCleanup.test_no_idb_keys_in_local_derived_metrics` (unittest)
   - `TestPreflightValidation` class (3 tests: peer-avg blocking, missing composite, healthy pass)
   - `TestClaudeMDIntegrity.test_no_merge_conflict_markers` (unittest)

5. **CLAUDE.md — preflight docs updated**: Section 9 (Preflight Validation) now accurately documents peer-average blocking, composite existence checks, and readable artifact names.

6. **Verified**: No merge conflict markers in CLAUDE.md. No `IDB_` keys in `master_data_dictionary.py`.

---

## 8. To-Do / Known Issues

### Normalized Profitability Metrics (MSPBNA_CR_Normalized.py)

| Metric | Status | Notes |
|---|---|---|
| `Norm_Loan_Yield` | **Fixed** | `Int_Inc_Loans_TTM / Norm_Gross_Loans`. Root cause was a column-name mismatch in the TTM map: `col.replace('_YTD', '_Q')` produces `Int_Inc_Loans_Q` but the TTM map key was `Int_Inc_Loans_YTD_Q`. Corrected. Source: `ILNDOM + ILNFOR` (both fetched). |
| `Norm_Provision_Rate` | **Intentionally NaN** | Provision expense (`ELNATR`) is not segment-specific in call reports. A normalized rate denominated by `Norm_Gross_Loans` would be semantically misleading since provision flow includes C&I/Consumer. Set to NaN by design. |
| `Norm_Loss_Adj_Yield` | **Fixed** (cascading) | `Norm_Loan_Yield - Norm_NCO_Rate`. Auto-resolves now that `Norm_Loan_Yield` is populated. |

**TTM pipeline for income metrics**: `ILNDOM + ILNFOR → Int_Inc_Loans_YTD → replace('_YTD','_Q') → Int_Inc_Loans_Q → rolling(4).sum() → Int_Inc_Loans_TTM`. Same pattern for `Provision_Exp_YTD → Provision_Exp_Q → Provision_Exp_TTM`.

---

## 9. Metric Registry & Validation Architecture

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

### Semantic Validation Rules (A–E)

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

## 10. FRED Expansion Layer — Registry-Driven Macro / Market Context

### Architecture

The FRED expansion is implemented as four new modules:

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

---

## 11. HUD USPS ZIP Crosswalk — Case-Shiller County-Level Enrichment

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

**Token discovery** uses `resolve_hud_token()` from `case_shiller_zip_mapper.py` with multi-source resolution: explicit argument → `os.getenv` → `.env` in script dir → `.env` in cwd. The resolver returns `(token, diagnostics)` — diagnostics include `source_used`, masked prefix, paths checked, cwd, executable, and PID. The full token is never logged. If the token exists in the shell but is not visible to the Python process, diagnostics clearly distinguish "token not visible to current process" from "token missing from machine".

### Integration

ZIP enrichment runs automatically as part of the MSPBNA_CR_Normalized pipeline. Token is resolved once at startup via `_validate_runtime_env()`, stored on `DashboardConfig.hud_user_token` (attribute assignment, never dict-style), and passed explicitly to `build_case_shiller_zip_sheets(hud_user_token=config.hud_user_token)`. The pipeline reads the token via `getattr(self.config, "hud_user_token", None)`. Controlled by `ENABLE_CASE_SHILLER_ZIP_ENRICHMENT` (default: `true`).

**HUD token is no longer the main blocker.** The token resolution chain works correctly. The primary remaining failure mode is HUD response parsing — the API may return nested wrapper objects that must be flattened before DataFrame creation.

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

**Pass 2 — Wrapper row flattening** (`flatten_hud_rows()`):

The HUD county-to-ZIP API (type=7) returns Shape B where each "row" is actually a **wrapper object** with metadata fields and a nested `results` list containing the actual crosswalk rows:
```
{"results": [
  {"year": 2025, "quarter": 4, "input": "06037", "crosswalk_type": "7",
   "results": [
     {"zip": "90001", "county": "06037", "res_ratio": 0.85, ...},
     {"zip": "90002", "county": "06037", "res_ratio": 0.72, ...}
   ]},
  ...
]}
```
`flatten_hud_rows()` processes **each row independently** (not just a first-row heuristic). Any row with a list-valued `results` key is a wrapper — it is exploded into its child rows with parent metadata propagated (child keys take precedence). Wrapper rows with extra metadata keys beyond `_HUD_WRAPPER_KEYS` are still flattened correctly. The function iterates until no row contains a list-valued `results`, handling multi-level nesting. Mixed payloads (some wrapper, some already flat) are handled safely in the same pass.

**Without this second pass**, `pd.DataFrame(wrapper_rows)` produces a DataFrame with only wrapper columns (`year`, `quarter`, `input`, `crosswalk_type`, `results`) and `build_case_shiller_zip_coverage()` fails silently because no ZIP/FIPS columns exist.

After both passes, rows are normalized via `pd.json_normalize()` and canonicalized via `canonicalize_hud_columns()`. Canonicalization also handles **dotted column variants** from `json_normalize` (e.g. `results.zip` → `zip`, `results.county` → `county_fips`). When both canonical and dotted columns exist, the canonical value is preferred where non-null; gaps are filled from the dotted version.

A wrapper-only column check (`_HUD_WRAPPER_KEYS`) detects payloads that were not properly flattened and returns `FAILED_PARSE`.

**Query-level outcome tracking**: `fetch_hud_crosswalk()` returns a structured dict `{"dataframe", "status", "diagnostics"}` with per-query status (`SUCCESS_ROWS`, `FAILED_PARSE`, `FAILED_EMPTY_RESPONSE`, `FAILED_HTTP`, `FAILED_TOKEN_AUTH`). The orchestrator aggregates these: if all counties returned empty and at least one had `FAILED_PARSE`, the final status is `FAILED_PARSE` (not `FAILED_EMPTY_RESPONSE`). `FAILED_EMPTY_RESPONSE` is only used when the API returned no usable rows without any parse failures.

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

The HUD USPS Crosswalk API is called with `type=7` (county-to-ZIP) for each unique FIPS code in `CASE_SHILLER_COUNTY_MAP`. The API returns all ZIP codes that overlap each county along with allocation ratios (`tot_ratio`, `res_ratio`, `bus_ratio`, `oth_ratio`). Results are joined strictly on 5-digit FIPS code. After fetching, the combined frame is validated for usable columns before proceeding to the coverage join.

**Request hardening**: All HUD requests use a `requests.Session()` with `Accept: application/json` and `User-Agent` headers. The session is lazy-initialized at module level. `fetch_hud_crosswalk()` returns `(DataFrame, diagnostics_dict)` — the diagnostics dict contains: query, status, failure_class, status_code, url, params, response_preview, exception_info, retry_count. HTTP status codes are classified via `_classify_http_status()` into query-level failure constants (QUERY_FAILED_TOKEN_AUTH, QUERY_FAILED_HTTP_NOT_FOUND, etc.).

**Misclassification prevention**: The orchestrator tracks `county_diagnostics` for every FIPS fetch and checks `any_http_failure` from those diagnostics. When all county requests fail HTTP (failure_class is non-None for every query AND success count is 0), the enrichment status is `FAILED_HTTP` — it will NOT drift to `FAILED_EMPTY_RESPONSE`, `SUCCESS_NO_MATCHES`, or `SUCCESS_NO_ZIPS`.

**Smoke test**: `run_hud_smoke_test(fips_code, token)` runs a single HUD request for local debugging. Use it to verify token validity and API connectivity before running the full pipeline.

### Output Sheets

| Sheet | Description |
|---|---|
| `CaseShiller_Zip_Coverage` | One row per (region, ZIP, county_fips) with county name, state, HUD ratios, crosswalk vintage |
| `CaseShiller_Zip_Summary` | One row per region: ZIP count, unique county count, county/state lists, vintage |
| `CaseShiller_County_Map_Audit` | Full county-level FIPS mapping with `included_in_zip_output` flag |

### Validation (7 Checks)

1. No non-metro regions leak into coverage
2. No blank ZIP codes
3. All ZIPs are 5-character zero-padded strings
4. Summary ZIP counts reconcile with detail rows
5. All FIPS codes in county map are valid 5-digit strings
6. All 20 metros have at least one ZIP row
7. HUD ratio columns are not entirely null

---

## 12. Corp-Safe Overlay Architecture

### Overview

`corp_overlay.py` is a **standalone module** that joins the local `Bank_Performance_Dashboard_YYYYMMDD.xlsx` output with an internal loan-level extract to produce corp-safe artifacts. It runs via its own CLI entrypoint (`corp_overlay_runner.py`) and is **NOT** integrated into `report_generator.py` or `MSPBNA_CR_Normalized.py`.

### Data Flow

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

### Input Contract (Loan File)

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

### Optional Enrichment Hooks

| Source | Env Var | Resolution | Status |
|---|---|---|---|
| Census | `CENSUS_API_KEY` | Direct env lookup → None | Hook point (not yet implemented) |
| BEA | `BEA_API_KEY` → `BEA_USER_ID` | Canonical → alias → None | Hook point (not yet implemented) |
| Case-Shiller | (uses existing mapper) | `map_zip_to_metro()` from `case_shiller_zip_mapper.py` | Implemented (ZIP → metro tagging) |

All enrichment is **optional**. The workflow runs fully offline without any API keys or internet access.

### CLI Usage

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

### Artifact Details

| Artifact | Description | Mode |
|---|---|---|
| `loan_balance_by_product.png` | Descending horizontal bar chart of `current_balance` aggregated by `product_type` | FULL_LOCAL_ONLY |
| `top10_geography_by_balance.png` | Top 10 geographies (by resolved geo field) ranked by aggregate balance | FULL_LOCAL_ONLY |
| `internal_credit_flags_summary.html` | Distribution tables for risk_rating, delinquency_status, nonaccrual_flag; reduced to portfolio summary if optional columns absent | BOTH |
| `peer_vs_internal_mix_bridge.html` | Side-by-side: MSPBNA peer-report composition (SBL/CRE/Resi shares from dashboard) vs internal loan product/geography mix | BOTH |

### Known Limitations

- Census and BEA enrichment hooks are stub implementations — they return no-op results. When API integrations are needed, implement the actual API calls inside `enrich_geography()`.
- The bridge table extracts composition from `Summary_Dashboard` sheet only. If the dashboard uses a different sheet layout, `load_dashboard_composition()` falls back to `FDIC_Data`.
- Dashboard auto-discovery uses the same `Bank_Performance_Dashboard_*.xlsx` glob pattern as `report_generator.py`.
- Loan file must be CSV, TSV, or Excel (.xlsx/.xls). Other formats are rejected.
