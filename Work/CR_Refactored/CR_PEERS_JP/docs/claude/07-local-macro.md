# 07 — Local Macro Pipeline (`local_macro.py`)

## Overview

`local_macro.py` is a **dedicated module** that builds a canonical geography spine and fetches MSA-level macroeconomic data from BEA, BLS, and Census APIs. It is called by `MSPBNA_CR_Normalized.py` (Step 1) and produces six Excel sheets in the dashboard output (plus an optional `Local_Macro_Skip_Audit` when APIs are unavailable). It is **separate** from the Case-Shiller ZIP mapper (`case_shiller_zip_mapper.py`) and from the corp overlay (`corp_overlay.py`).

## Geography Spine

The spine is keyed by CBSA code and resolves geographies through a strict 4-tier hierarchy:

```
Input geography
    │
    ├─ Tier 1: Direct CBSA match (TOP_MSAS lookup)      → quality: high
    │
    ├─ Tier 2: ZIP → CBSA (HUD USPS Crosswalk type=4)   → quality: medium
    │
    ├─ Tier 3: County FIPS → CBSA (_COUNTY_TO_CBSA)     → quality: low
    │
    ├─ Tier 4: State fallback (state-level aggregation)  → quality: low
    │
    └─ Unmatched (no resolution possible)                → quality: unmatched
```

**Spine columns:** `cbsa_code`, `msa_name`, `state_fips`, `state_abbrev`, `county_fips`, `zip_code`, `mapping_method`, `quality`

**Mapping methods:** `direct_cbsa`, `zip_to_cbsa`, `county_to_cbsa`, `state_fallback`, `unmatched`

**Important:** The Case-Shiller ZIP mapper uses county-level FIPS codes per S&P CoreLogic methodology (HUD type=7). The local macro spine uses ZIP-level crosswalks (HUD type=4) and a separate internal county-to-CBSA table. These are intentionally different systems and must NOT be merged.

## Data Sources

| Source | API | Series/Table | Frequency | Env Var |
|---|---|---|---|---|
| BEA Regional GDP | `apps.bea.gov/api/data` | CAGDP2 (metro GDP) | Annual | `BEA_API_KEY` |
| BLS LAUS | `api.bls.gov/publicAPI/v2/timeseries/data` | LAUMT{sfips}{cbsa}00000003 | Monthly | (none — public) |
| Census Population | `api.census.gov/data/{year}/pep/population` | Metro population estimates | Annual | `CENSUS_API_KEY` |
| HUD Crosswalk | `hudgis.hud.gov/hudapi/public/usps` | type=4 (ZIP→CBSA) | Quarterly | `HUD_USER_TOKEN` |

All API calls are **optional**. If keys are missing or APIs are unavailable, the pipeline returns empty DataFrames and logs warnings. It never crashes.

## Output Sheets

| Sheet | Contents | Key Columns |
|---|---|---|
| `Local_Macro_Raw` | Raw API responses with source provenance metadata | `cbsa_code`, `date`, `value`, `metric_name`, `source_dataset`, `source_series_id`, `source_frequency`, `data_vintage`, `load_timestamp` |
| `Local_Macro_Derived` | Per-capita, YoY, quarterly transforms | `cbsa_code`, `date`, `metric_name`, `value`, `units`, `transform_type`, `aggregation_rule` |
| `Local_Macro_Mapped` | Raw + derived data joined to geography spine | All spine columns + all raw/derived columns |
| `Local_Macro_Latest` | One row per CBSA, latest-period values, board-ready | All `BOARD_COLUMNS` (30 columns — see below) |
| `MSA_Board_Panel` | Compact board-presentation panel sorted by GDP | Key subset of Latest columns for executive consumption |
| `MSA_Crosswalk_Audit` | Full audit trail of every geography resolution | `source_geo_type`, `source_geo_value`, `target_cbsa_code`, `mapping_method`, `quality_flag` |
| `Local_Macro_Skip_Audit` | Written when pipeline has no data — explains skip reason | `sheet_name`, `skip_reason`, `context`, `timestamp` |

**The `MSA_Crosswalk_Audit` sheet is always produced**, even if all API calls fail. It documents every resolution attempt including unmatched geographies.

**The `Local_Macro_Skip_Audit` sheet** is written when the pipeline runs but produces no data (e.g., no API keys configured). This ensures downstream consumers see an explicit reason rather than silent omission.

## Board/Risk Column Specification (`BOARD_COLUMNS`)

The `Local_Macro_Latest` sheet uses a fixed 30-column schema for board/risk consumption:

| Column | Description |
|---|---|
| `as_of_date` | Date the pipeline ran |
| `geo_level` | Classification: `msa`, `county`, `state`, or `unknown` |
| `msa_name` | MSA name from spine |
| `cbsa_code` | CBSA code |
| `state_abbrev`, `state_fips` | State identifiers |
| `county_fips`, `zip_code` | Sub-state geography (if available) |
| `mapping_method` | How the geography was resolved (direct_cbsa, zip_to_cbsa, etc.) |
| `mapping_weight`, `coverage_pct` | Quality metrics from crosswalk |
| `source_dataset`, `source_series_id`, `source_frequency` | Data provenance |
| `real_gdp_level` | Latest GDP level (dollars) |
| `real_gdp_yoy_pct` | GDP YoY % (from levels) |
| `population` | Latest population count |
| `population_yoy_pct` | Population YoY % |
| `real_gdp_per_capita` | GDP / population (dollars per person) |
| `real_gdp_per_100k` | GDP per 100k population |
| `real_gdp_per_100k_yoy_pct` | Per-100k YoY % (from normalized level) |
| `unemployment_rate` | Latest quarterly mean unemployment rate (%) |
| `unemployment_yoy_pp` | Unemployment change YoY in pp |
| `unemployment_qoq_pp` | Unemployment change QoQ in pp |
| `hpi_yoy_pct`, `hpi_qoq_pct` | House price index changes (when available) |
| `portfolio_balance`, `portfolio_share` | Loan portfolio context (when available) |
| `macro_stress_flag` | `OK`, `WATCH`, or `STRESS` based on unemployment + GDP signals |
| `data_vintage`, `load_timestamp` | Data currency metadata |

## Source Metadata Policy

Every row in `Local_Macro_Raw` carries provenance fields:

| Field | Description | Example |
|---|---|---|
| `source_dataset` | API/dataset identifier | `BEA_CAGDP2`, `BLS_LAUS`, `CENSUS_PEP` |
| `source_series_id` | Specific series within the dataset | `LAUMT2435620000003` |
| `source_frequency` | Reporting frequency | `annual`, `monthly` |
| `data_vintage` | Vintage/release date of the data | `2026-03-13` |
| `load_timestamp` | When the data was fetched | `2026-03-13T10:30:00` |

## Integration Point

`MSPBNA_CR_Normalized.py` calls `run_local_macro_pipeline()` after Case-Shiller enrichment:

```python
from local_macro import run_local_macro_pipeline
lm_sheets = run_local_macro_pipeline(
    hud_token=getattr(config, "hud_user_token", None),
    bea_api_key=getattr(config, "bea_api_key", None),
    census_api_key=getattr(config, "census_api_key", None),
)
# Sheets passed to write_excel_output() via **local_macro_kwargs
```

**Fallback rules:**
- `ImportError` → pipeline skipped silently (module not available)
- Any other exception → logged as warning, non-fatal, dashboard produced without local macro sheets
- Missing API keys → empty DataFrames returned, audit sheet still produced

## Transformation Policy Registry

Each local macro series has a declared `TransformPolicy` controlling quarterly aggregation and valid derived metrics.

| Series Family | `transform_type` | `aggregation_rule` | `units` | `per_capita_eligible` |
|---|---|---|---|---|
| `gdp` | level | sum | dollars | Yes |
| `unemployment` | rate | mean | pct | No |
| `population` | level | point_in_time | persons | No |
| `hpi` | index | last | index | No |

**Aggregation rules (for `aggregate_to_quarter()`):**
- `mean` — Average of observations within the quarter (correct for rates like unemployment)
- `last` — Last observation of the quarter (correct for indices like HPI)
- `sum` — Sum of observations within the quarter (correct for flows like GDP)
- `point_in_time` — Same as `last`; use the latest available estimate (correct for stock variables like population)

## Per-Capita Normalization Formulas

**Hard rules:**
1. **Never divide a growth rate by population.** Normalize levels first, then compute growth.
2. GDP growth stays in **%**.
3. Unemployment changes are in **percentage points (pp)**, not %. A move from 4.0% to 4.5% is +0.5 pp, NOT +12.5%.
4. Zero or negative population → hard-fail (`ValueError`). Missing population → `NaN` (flagged, not zero-filled).

**Formulas:**
```
real_gdp_per_capita      = real_gdp_level / population
real_gdp_per_100k        = real_gdp_level / population × 100,000
real_gdp_per_100k_yoy_pct = real_gdp_per_100k(t) / real_gdp_per_100k(t-4) - 1
                            (lag 4 quarters for quarterly, lag 1 for annual)

unemployment_rate_quarterly = mean(monthly rates in quarter)
unemployment_change_pp      = unemployment_rate(t) - unemployment_rate(t-4)
```

**Identity checks (used in tests):**
- Constant population ⇒ GDP YoY == GDP-per-capita YoY
- Flat GDP + rising population ⇒ GDP-per-capita YoY < 0
- `real_gdp_per_100k == real_gdp_per_capita × 100,000` (exact)

## Helper Functions

| Function | Purpose | Key Constraint |
|---|---|---|
| `compute_real_gdp_per_capita(gdp, pop)` | GDP level / population | Validates population > 0 |
| `compute_real_gdp_per_100k(gdp, pop)` | Per-capita × 100,000 | Same validation |
| `compute_yoy_from_level(series, lag)` | `series / lag(series) - 1` | Apply to LEVELS only, never rates |
| `compute_unemployment_change_pp(rate, lag)` | `rate - lag(rate)` | Arithmetic diff (pp), not pct change |
| `aggregate_to_quarter(series, rule)` | Resample to QE per declared rule | Requires DatetimeIndex |
| `validate_population(pop)` | Rejects ≤0; preserves NaN | Raises ValueError on bad data |
| `build_derived_metrics(gdp, unemp, pop)` | Orchestrates all derived computations | Empty inputs → empty output |
