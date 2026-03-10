# CR_PEERS_JP — Credit Risk Performance Reporting Engine

## 1. Project Overview

This repository is an **automated Credit Risk Performance reporting engine** for MSPBNA (Morgan Stanley Private Bank, National Association). The pipeline:

1. **Fetches** raw call-report data from the FDIC API and macroeconomic time-series from the FRED API.
2. **Processes** the data into standard and normalized credit-quality metrics, computes peer-group composites, and builds rolling 8-quarter averages.
3. **Outputs** a consolidated Excel dashboard (`Bank_Performance_Dashboard_*.xlsx`) containing multiple sheets (FDIC_Data, Averages_8Q, FRED_Data, FRED_Descriptions, FDIC_Metric_Descriptions).
4. **Generates reports**: PNG credit-deterioration charts, PNG scatter plots, and HTML comparison tables — all routed to structured subdirectories under `output/Peers/`.

The two core scripts are:

| Script | Role |
|---|---|
| `MSPBNA_CR_Normalized.py` | Data fetch, processing, normalization, and Excel dashboard creation |
| `report_generator.py` | Reads the dashboard Excel and produces charts, scatters, and HTML tables |

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
| `MSPBNA_CERT` | Subject bank CERT number (dynamic, no hardcoding) | `19977` |
| `MSBNA_CERT` | Secondary bank CERT number | `19977` |
| `SUBJECT_BANK_CERT` | Used by `MSPBNA_CR_Normalized.py` | `34221` |
| `MS_COMBINED_CERT` | MS Combined Entity CERT (default `88888`) | `88888` |
| `REPORT_VIEW` | Controls table filtering logic | `ALL_BANKS` or `MSPBNA_WEALTH_NORM` |

These can be set in a `.env` file in the project root or exported in the shell.

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
output/Bank_Performance_Dashboard_YYYYMMDD_HHMMSS.xlsx
        │
        ▼
report_generator.py
        │
        ├──► output/Peers/charts/   (Standard + Normalized deterioration charts,
        │                           Portfolio Mix, Segment Attribution,
        │                           Reserve Allocation, Migration Ladder PNGs)
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

### Output File Naming

All output files include the source Excel stem and a datestamp:
- `{stem}_standard_credit_chart_YYYYMMDD.png`
- `{stem}_normalized_credit_chart_YYYYMMDD.png`
- `{stem}_portfolio_mix_YYYYMMDD.png`
- `{stem}_problem_asset_attribution_YYYYMMDD.png`
- `{stem}_reserve_risk_allocation_YYYYMMDD.png`
- `{stem}_migration_ladder_YYYYMMDD.png`
- `{stem}_scatter_nco_vs_npl_YYYYMMDD.png`
- `{stem}_scatter_pd_vs_npl_YYYYMMDD.png`
- `{stem}_scatter_norm_nco_vs_nonaccrual_YYYYMMDD.png`
- `{stem}_standard_table_YYYYMMDD.html`
- `{stem}_normalized_table_YYYYMMDD.html`
- `{stem}_fred_table_YYYYMMDD.html`

---

## 4. Strict Coding Conventions & Rules

These are **non-negotiable** for any agent editing this codebase:

### NO HARDCODING

- Subject bank CERTs **must** be loaded dynamically via `int(os.getenv("MSPBNA_CERT", "19977"))`.
- Never hardcode `19977` or any CERT number directly. Always use the config/env pattern.
- The FRED API key must come from `os.getenv('FRED_API_KEY')` or `.env`.

### ALIASING

- When plotting, use the `build_plot_df_with_alias(df, alias_map)` helper to rename composite CERTs (e.g., `{99999: 90006, 99998: 90004}`) rather than modifying the core dataframe permanently.
- The alias map creates temporary copies — the original dataframe must remain unmodified.

### PEER GROUPINGS

HTML tables must always reflect **3 peer columns** per table type:

| Table Type | Peer 1 | Peer 2 | Peer 3 |
|---|---|---|---|
| **Standard** | 90001 — Core PB | 90002 — MSPBNA+Wealth | 90003 — All Peers |
| **Normalized** | 90004 — Core PB Norm | 90005 — MSPBNA+Wealth Norm | 90006 — All Peers Norm |

### MS COMBINED ENTITY

- CERT `88888` (MS Combined Entity) must be **filtered out** of HTML table listings when `REPORT_VIEW == "MSPBNA_WEALTH_NORM"`.
- Load via `MS_COMBINED_CERT = int(os.getenv("MS_COMBINED_CERT", "88888"))`.

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

### CHART METRICS

**Standard chart:** Bar = `TTM_NCO_Rate`, Line = `NPL_to_Gross_Loans_Rate`
**Normalized chart:** Bar = `Norm_NCO_Rate`, Line = `Norm_Nonaccrual_Rate` (fallback: `Norm_NPL_to_Gross_Loans_Rate`)

Both standard and normalized credit-deterioration charts are generated.

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

## 6. Future Roadmap / Unfinished Charts

The following visuals are prioritized for future development. When implementing, add the plotting logic to `report_generator.py` and route output to `output/Peers/charts/`.

- **Years-of-reserves by segment:** (Line/Lollipop) Series: `RIC_*_Years_of_Reserves`. Intuitive framing for senior management — "coverage in years" lands better than raw reserve ratios.

- **Growth vs deterioration quadrant:** (Peer Scatter) Series: `*_Growth_TTM` vs `TTM_NCO_Rate` / `NPL_to_Gross_Loans_Rate`. Answers whether portfolio growth is being bought at the cost of future credit losses.

- **Risk-adjusted return frontier:** (Bubble Scatter) Series: `Norm_Risk_Adj_Return`, `Norm_Loss_Adj_Yield`, `Norm_NCO_Rate`. Makes the business-unit tradeoff visible.

- **Concentration vs capital sensitivity:** (Quadrant) Series: `CRE_Concentration_Capital_Risk`, `CI_to_Capital_Risk`. Frames loan risk in capital language for escalation.

- **Liquidity / draw-risk overlay:** (Combo Chart) Series: `Loans_to_Deposits`, `Liquidity_Ratio`, `HQLA_Ratio`. Stress often arrives through utilization and liquidity pressure before credit losses show up.

- **Macro overlay on credit trend:** (Dual-axis/Small multiples) Series: FRED macro data vs `Norm_NCO_Rate`. Useful for explaining whether current deterioration is idiosyncratic or macro-linked.
