# 09 — Troubleshooting & Known Issues

## Common Errors

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

## Remaining Risks

1. **Executive charts import guard**: `_HAS_EXECUTIVE_CHARTS` flag means if `executive_charts.py` fails to import (e.g., `metric_semantics.py` missing), all executive artifacts silently skip. No manifest entry is recorded for the import-level skip.
2. **FRED data dependency**: Macro chart artifacts depend on FRED_Data sheet being present in the workbook. If `MSPBNA_CR_Normalized.py` was run without FRED_API_KEY, macro charts silently return None.
3. **matplotlib tight_layout warning**: The `warnings.filterwarnings` suppression in `report_generator.py` masks a real twinx() incompatibility in macro overlay charts. The charts render correctly but may have suboptimal spacing.
4. **Normalized metric coverage**: Macro correlation heatmap rows use normalized metrics that may be NaN for some banks due to over-exclusion. N/A cells are shown correctly but reduce information density.
5. **fred_case_shiller_discovery import dependency**: Tests referencing `fred_case_shiller_discovery` module error when `aiohttp` is not installed. These are pre-existing and do not affect report generation.
6. **HUD crosswalk fetch_hud_crosswalk return contract**: One pre-existing test asserts a `"dataframe"` key in the source that may use a different return format. This is a test-vs-source contract mismatch in `case_shiller_zip_mapper.py`, not in report_generator or rendering_mode.

---

## Known Issues / To-Do

- **Norm profitability metrics**: `Norm_Provision_Rate` is intentionally NaN — provision expense (ELNATR) is not segment-specific in call reports. No resolution planned in CR-only mode.
