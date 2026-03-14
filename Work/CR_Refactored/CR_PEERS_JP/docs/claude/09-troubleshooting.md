# 09 — Troubleshooting

## DNS / Proxy Error: `[Errno 11001] getaddrinfo failed`

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

## Missing FRED_API_KEY

**Symptom:** `ValueError: FRED_API_KEY not found`

**Fix:** Create a `.env` file in the project root:

```
FRED_API_KEY=your_key_here
SUBJECT_BANK_CERT=34221
```

Or export it directly: `export FRED_API_KEY='your_key_here'`

## No Excel File Found

**Symptom:** `report_generator.py` prints `ERROR: No Excel files found in output/`

**Fix:** Run Step 1 first (`python MSPBNA_CR_Normalized.py`). The report generator reads the latest `Bank_Performance_Dashboard_*.xlsx` from the `output/` directory.

## Missing Sheet in Excel

**Symptom:** `FileNotFoundError: 8Q average sheet not found`

**Fix:** Ensure `MSPBNA_CR_Normalized.py` completed successfully and the output Excel contains the `Averages_8Q*` sheet. Re-run Step 1 if needed.

---

## To-Do / Known Issues

### Normalized Profitability Metrics (MSPBNA_CR_Normalized.py)

| Metric | Status | Notes |
|---|---|---|
| `Norm_Loan_Yield` | **Fixed** | `Int_Inc_Loans_TTM / Norm_Gross_Loans`. Root cause was a column-name mismatch in the TTM map: `col.replace('_YTD', '_Q')` produces `Int_Inc_Loans_Q` but the TTM map key was `Int_Inc_Loans_YTD_Q`. Corrected. Source: `ILNDOM + ILNFOR` (both fetched). |
| `Norm_Provision_Rate` | **Intentionally NaN** | Provision expense (`ELNATR`) is not segment-specific in call reports. A normalized rate denominated by `Norm_Gross_Loans` would be semantically misleading since provision flow includes C&I/Consumer. Set to NaN by design. |
| `Norm_Loss_Adj_Yield` | **Fixed** (cascading) | `Norm_Loan_Yield - Norm_NCO_Rate`. Auto-resolves now that `Norm_Loan_Yield` is populated. |

**TTM pipeline for income metrics**: `ILNDOM + ILNFOR → Int_Inc_Loans_YTD → replace('_YTD','_Q') → Int_Inc_Loans_Q → rolling(4).sum() → Int_Inc_Loans_TTM`. Same pattern for `Provision_Exp_YTD → Provision_Exp_Q → Provision_Exp_TTM`.
