# 04 — Rendering Architecture

## Dual-Mode Rendering

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

## Capability Matrix

Each artifact declares its availability:

- **`BOTH`** — available in `full_local` and `corp_safe`. All HTML table artifacts use this.
- **`FULL_LOCAL_ONLY`** — requires matplotlib/seaborn. All PNG chart/scatter artifacts use this.

When an artifact is not available in the current mode, `should_produce()`:
1. Records a skip in the `ArtifactManifest` with a human-readable reason
2. Prints `[SKIP] <reason>` to the console
3. Returns `False` so the caller skips the block

## Artifact Skip Semantics

Artifacts can be skipped for two independent reasons:
1. **Mode-based skip**: The artifact's `ArtifactAvailability` does not include the current `RenderMode`.
2. **Preflight suppression**: The `validate_output_inputs()` preflight adds artifact names to `suppressed_charts` (e.g., when a composite CERT has material over-exclusion).

Both reasons are logged in the manifest. Skipping is always intentional and never silent.

## Artifact Manifest

Every `generate_reports()` run produces an `ArtifactManifest` object (also returned to callers). The manifest tracks:
- **artifact name** — canonical identifier from `ARTIFACT_REGISTRY`
- **mode** — the render mode used
- **status** — `generated`, `skipped`, or `failed`
- **path** — file path for generated artifacts
- **skip_reason** — human-readable reason for skipped artifacts
- **error** — error message for failed artifacts

The manifest summary table is printed at the end of every run.

## CLI Usage

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

## Adding New Artifacts

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

## Canonical Rendering Abstraction Rule

**`rendering_mode.py` is the single canonical source** for all rendering abstractions. `report_generator.py` must NOT define its own copies of:
- `RenderMode` / `ReportMode`
- `ArtifactStatus`, `ArtifactSpec`, `ArtifactCapability`
- `ManifestEntry`, `ArtifactManifest`
- `ARTIFACT_REGISTRY`
- `should_produce()`
- `resolve_report_mode_for_generator()`

All of these must be imported from `rendering_mode.py`. The `_ReportContext` dataclass in `report_generator.py` is a lightweight internal carrier and is NOT a duplicate of any rendering-mode type.

## Remaining Risks

1. **Executive charts import guard**: `_HAS_EXECUTIVE_CHARTS` flag means if `executive_charts.py` fails to import (e.g., `metric_semantics.py` missing), all 6 executive artifacts silently skip. No manifest entry is recorded for the import-level skip.
2. **FRED data dependency**: Macro chart artifacts depend on FRED_Data sheet being present in the workbook. If `MSPBNA_CR_Normalized.py` was run without FRED_API_KEY, macro charts silently return None.
3. **matplotlib tight_layout warning**: The `warnings.filterwarnings` suppression in `report_generator.py` masks a real twinx() incompatibility in macro overlay charts. The charts render correctly but may have suboptimal spacing.
4. **Normalized metric coverage**: Macro correlation heatmap rows use normalized metrics that may be NaN for some banks due to over-exclusion. N/A cells are shown correctly but reduce information density.
5. **fred_case_shiller_discovery import dependency**: Tests referencing `fred_case_shiller_discovery` module error when `aiohttp` is not installed. These are pre-existing and do not affect report generation.
6. **HUD crosswalk fetch_hud_crosswalk return contract**: One pre-existing test (`test_fetch_hud_crosswalk_returns_failed_parse_status_for_wrapper_only_output`) asserts a `"dataframe"` key in the source that may use a different return format. This is a test-vs-source contract mismatch in `case_shiller_zip_mapper.py`, not in report_generator or rendering_mode.
