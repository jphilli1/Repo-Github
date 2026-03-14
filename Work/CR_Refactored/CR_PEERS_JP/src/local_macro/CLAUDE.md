# src/local_macro — Local Macro Pipeline Subtree

This directory contains the local macroeconomic data pipeline for geography-aware context.

## Key Rules

- `local_macro.py` is the **canonical** module for BEA/BLS/Census macro data and the geography spine.
- It is called by `MSPBNA_CR_Normalized.py` (Step 1), NOT by `report_generator.py`.
- `report_generator.py` consumes workbook sheets only (`Local_Macro_Latest`, `MSA_Crosswalk_Audit`) — it never imports `local_macro.py`.
- The Case-Shiller ZIP mapper (`case_shiller_zip_mapper.py`) uses a **separate** geography system (HUD type=7 county-to-ZIP). Do NOT merge the two.
- All API calls are **optional** — pipeline never crashes if APIs are unavailable.
- Per-capita math: normalize levels first, then compute growth. Never divide growth rates by population.
- Unemployment changes are in **percentage points (pp)**, not percent.

## Output: 6 Excel Sheets

`Local_Macro_Raw`, `Local_Macro_Derived`, `Local_Macro_Mapped`, `Local_Macro_Latest`, `MSA_Board_Panel`, `MSA_Crosswalk_Audit`

Plus `Local_Macro_Skip_Audit` when the pipeline has no data.

## Reference

@docs/claude/07-local-macro.md
