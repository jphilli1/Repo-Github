# 02 — Build, Run & Configuration

## Build & Execution Commands

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

# Or run both steps with the unified pipeline runner:
python run_pipeline.py                    # Both steps, full_local mode
python run_pipeline.py --mode corp_safe   # Both steps, corp_safe mode
python run_pipeline.py --step 2           # Step 2 only (assumes Step 1 already ran)
python run_pipeline.py --force            # Continue Step 2 even if Step 1 fails
```

## Required Environment Variables

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

## Key Dependencies

`aiohttp`, `matplotlib`, `numpy`, `openpyxl`, `pandas`, `python-dotenv`, `requests`, `scipy`, `seaborn`, `tqdm`

---

## Import Safety

`MSPBNA_CR_Normalized.py` must be importable **without** setting environment variables, altering `cwd`, printing to stdout, or opening log files. All side effects live in `main()` (via `_validate_runtime_env()`, `setup_logging()`, `os.chdir()`).

- Module-level `MSPBNA_CERT` and `MSBNA_CERT` use `os.getenv()` with defaults for import safety. The hard validation (ValueError on missing vars) happens at runtime in `_validate_runtime_env()`.
- `csv_log` and `logger` are initialized to `None` at module level and set by `setup_logging()` inside `main()`.

## No Hardcoding

- Subject bank CERTs **must** be loaded dynamically via `int(os.getenv("MSPBNA_CERT", "34221"))`.
- Never hardcode CERT numbers directly. Always use the config/env pattern. Production defaults: `MSPBNA_CERT=34221`, `MSBNA_CERT=32992`.
- The FRED API key must come from `os.getenv('FRED_API_KEY')` or `.env`.
