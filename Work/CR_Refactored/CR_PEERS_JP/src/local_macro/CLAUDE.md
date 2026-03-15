# Local Macro Subsystem Rules

Scoped rules for `local_macro.py`.

> `local_macro.py` lives in this directory.

## Geography Spine

- 4-tier hierarchy: Direct CBSA → ZIP→CBSA → County→CBSA → State fallback
- `_resolve_cbsa()` accepts any valid 5-digit CBSA, not just TOP_MSAS
- Case-Shiller ZIP mapper (HUD type=7) and local macro spine (HUD type=4) are **intentionally separate** — never merge them.
- MSA_Crosswalk_Audit is **always** produced, even when all APIs fail.

## GDP / Population Math

- **Never divide a growth rate by population.** Normalize levels first, then compute growth.
- GDP growth stays in **%**.
- Unemployment changes are in **percentage points (pp)**, not percent.
- Zero/negative population → hard-fail (`ValueError`). Missing population → `NaN`.
- `real_gdp_per_100k == real_gdp_per_capita * 100,000` (exact identity).

## Transformation Policy

- Each series family has a declared aggregation rule: gdp=sum, unemployment=mean, population=point_in_time, hpi=last.
- `aggregate_to_quarter()` dispatches per declared rule — no one-size-fits-all.

## Completeness Rules

- `macro_data_completeness`: `complete` (all 3 sources), `partial` (some), `none`
- `missing_sources`: comma-separated list when not complete
- `macro_stress_flag`: `OK`, `WATCH`, `STRESS` based on unemployment + GDP signals

## Integration

- Called by `MSPBNA_CR_Normalized.py` (Step 1) — produces 6 Excel sheets
- `report_generator.py` reads workbook sheets only — never imports `local_macro`
- Pipeline never crashes if APIs are unavailable — returns empty DataFrames + audit sheet
