# 01 — Project Overview

This repository is an **automated Credit Risk Performance reporting engine** for MSPBNA (Morgan Stanley Private Bank, National Association). The pipeline:

1. **Fetches** raw call-report data from the FDIC API and macroeconomic time-series from the FRED API.
2. **Processes** the data into standard and normalized credit-quality metrics, computes peer-group composites, and builds rolling 8-quarter averages.
3. **Outputs** a consolidated Excel dashboard (`Bank_Performance_Dashboard_*.xlsx`) containing multiple sheets (Summary_Dashboard, Normalized_Comparison, Latest_Peer_Snapshot, Averages_8Q_All_Metrics, FDIC_Metric_Descriptions, Macro_Analysis, FDIC_Data, FRED_Data, FRED_Metadata, FRED_Descriptions, Data_Validation_Report, Normalization_Diagnostics, Peer_Group_Definitions, Exclusion_Component_Audit, Composite_Coverage_Audit, Metric_Validation_Audit, Normalization_Reconciliation_Sample, optional Case-Shiller ZIP sheets, and optional local macro sheets: Local_Macro_Raw, Local_Macro_Derived, Local_Macro_Mapped, Local_Macro_Latest, MSA_Board_Panel, MSA_Crosswalk_Audit).
4. **Generates reports**: PNG credit-deterioration charts, PNG scatter plots, and HTML comparison tables — all routed to structured subdirectories under `output/Peers/`.

## Core Scripts

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
| `local_macro.py` | Canonical geography spine, BEA/BLS/Census macro fetchers, MSA crosswalk audit |
| `run_pipeline.py` | Unified pipeline runner — runs Step 1 + Step 2 sequentially via subprocess |
