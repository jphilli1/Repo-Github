#!/usr/bin/env python3
"""
Rendering Mode & Capability Matrix
====================================

Dual-mode architecture for report_generator.py:

  * **full_local** — default mode, uses matplotlib/seaborn for all charts.
    Equivalent to today's local-machine behavior.
  * **corp_safe** — restricted mode for corporate environments.
    Skips artifacts that require rich libraries not commonly available
    on locked-down machines.  HTML tables (pure-Python string generation)
    are always available; matplotlib-based PNGs may be skipped.

Key concepts:
  * **ArtifactCapability** — declares whether an artifact is available in
    both modes, full_local only, or skipped in corp_safe.
  * **ArtifactManifest** — collects per-run artifact outcomes: name, mode,
    status (generated / skipped / failed), path, skip reason.
  * **RenderMode** — enum for the two modes.
  * **select_mode()** — resolves mode from env var / argument.
"""

from __future__ import annotations

import os
import enum
import dataclasses
from datetime import datetime
from typing import List, Optional


# =====================================================================
# Render Mode Enum
# =====================================================================

class RenderMode(enum.Enum):
    """Execution modes for the report generator."""
    FULL_LOCAL = "full_local"
    CORP_SAFE = "corp_safe"


def select_mode(explicit: Optional[str] = None) -> RenderMode:
    """Resolve the rendering mode.

    Priority:
      1. ``explicit`` argument (from CLI or caller).
      2. ``REPORT_MODE`` environment variable (canonical).
      3. ``REPORT_RENDER_MODE`` environment variable (backward-compatible alias).
      4. Default → ``full_local`` (preserves today's behaviour).

    Raises ``ValueError`` for unrecognised mode strings.
    """
    raw = (explicit
           or os.getenv("REPORT_MODE")
           or os.getenv("REPORT_RENDER_MODE")
           or "")
    raw = raw.strip().lower()
    if not raw or raw == "full_local":
        return RenderMode.FULL_LOCAL
    if raw == "corp_safe":
        return RenderMode.CORP_SAFE
    raise ValueError(
        f"Unknown render mode '{raw}'. "
        f"Valid values: full_local, corp_safe"
    )


# =====================================================================
# Artifact Capability
# =====================================================================

class ArtifactAvailability(enum.Enum):
    """Where an artifact can be produced."""
    BOTH = "both"              # Available in full_local AND corp_safe
    FULL_LOCAL_ONLY = "full_local_only"  # Requires rich libs (matplotlib, seaborn)


@dataclasses.dataclass(frozen=True)
class ArtifactCapability:
    """Declares an artifact and its availability across render modes."""
    name: str
    availability: ArtifactAvailability
    category: str              # "chart", "scatter", "table", "fred_chart"
    description: str = ""
    filename_suffix: str = ""  # e.g. "_scatter_nco_vs_npl.png"

    def is_available(self, mode: RenderMode) -> bool:
        """Return True if this artifact can be produced in *mode*."""
        if self.availability == ArtifactAvailability.BOTH:
            return True
        # FULL_LOCAL_ONLY → only available in full_local
        return mode == RenderMode.FULL_LOCAL

    def skip_reason(self, mode: RenderMode) -> Optional[str]:
        """Return a human-readable skip reason, or None if available."""
        if self.is_available(mode):
            return None
        return (
            f"{self.name}: skipped in {mode.value} mode "
            f"(requires {self.availability.value})"
        )


# =====================================================================
# Artifact Registry — canonical list of all report artifacts
# =====================================================================
# Maintenance: add new artifacts here.  The generate_reports() function
# checks this registry before attempting to produce each artifact.

ARTIFACT_REGISTRY: dict[str, ArtifactCapability] = {}


def _reg(name: str, avail: ArtifactAvailability, category: str,
         description: str = "", suffix: str = "") -> ArtifactCapability:
    if not suffix:
        ext = ".html" if category == "table" else ".png"
        suffix = f"_{name}{ext}"
    cap = ArtifactCapability(name=name, availability=avail,
                             category=category, description=description,
                             filename_suffix=suffix)
    ARTIFACT_REGISTRY[name] = cap
    return cap


# --- HTML Tables (pure Python string generation → both modes) ---
_reg("executive_summary_standard", ArtifactAvailability.BOTH, "table",
     "Wealth-focused executive summary (standard)")
_reg("executive_summary_normalized", ArtifactAvailability.BOTH, "table",
     "Wealth-focused executive summary (normalized)")
_reg("detailed_peer_table_standard", ArtifactAvailability.BOTH, "table",
     "Detailed peer analysis (standard)")
_reg("detailed_peer_table_normalized", ArtifactAvailability.BOTH, "table",
     "Detailed peer analysis (normalized)")
_reg("core_pb_peer_table_standard", ArtifactAvailability.BOTH, "table",
     "Core PB peer table (standard)")
_reg("core_pb_peer_table_normalized", ArtifactAvailability.BOTH, "table",
     "Core PB peer table (normalized)")
_reg("ratio_components_standard", ArtifactAvailability.BOTH, "table",
     "Ratio components analysis (standard)")
_reg("ratio_components_normalized", ArtifactAvailability.BOTH, "table",
     "Ratio components analysis (normalized)")
_reg("cre_segment_standard", ArtifactAvailability.BOTH, "table",
     "CRE segment analysis (standard)")
_reg("cre_segment_normalized", ArtifactAvailability.BOTH, "table",
     "CRE segment analysis (normalized)")
_reg("resi_segment_standard", ArtifactAvailability.BOTH, "table",
     "Resi segment analysis (standard)")
_reg("resi_segment_normalized", ArtifactAvailability.BOTH, "table",
     "Resi segment analysis (normalized)")
_reg("fred_table", ArtifactAvailability.BOTH, "table",
     "FRED macro indicators table")

# --- Credit Deterioration Charts (matplotlib → full_local only) ---
_reg("standard_credit_chart", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Standard credit deterioration bar+line chart")
_reg("normalized_credit_chart", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Normalized credit deterioration bar+line chart")

# --- Scatter Plots (matplotlib → full_local only) ---
_reg("scatter_nco_vs_npl", ArtifactAvailability.FULL_LOCAL_ONLY, "scatter",
     "Standard scatter: NCO vs NPL (8Q avg)")
_reg("scatter_pd_vs_npl", ArtifactAvailability.FULL_LOCAL_ONLY, "scatter",
     "Standard scatter: PD vs NPL (8Q avg)")
_reg("scatter_norm_nco_vs_nonaccrual", ArtifactAvailability.FULL_LOCAL_ONLY, "scatter",
     "Normalized scatter: NCO vs Nonaccrual (8Q avg)")
_reg("scatter_cre_nco_vs_nonaccrual", ArtifactAvailability.FULL_LOCAL_ONLY, "scatter",
     "CRE scatter: NCO Rate vs Nonaccrual Rate (8Q avg)")
_reg("scatter_norm_acl_vs_delinquency", ArtifactAvailability.FULL_LOCAL_ONLY, "scatter",
     "Normalized scatter: ACL Coverage vs Delinquency Rate (8Q avg)")

# --- Segment-Level Charts (matplotlib → full_local only) ---
_reg("portfolio_mix", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Portfolio mix stacked area chart")
_reg("problem_asset_attribution", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Problem asset attribution stacked bar chart")
_reg("reserve_risk_allocation", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Reserve allocation vs risk exposure grouped bar chart")
_reg("migration_ladder", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Early-warning migration ladder line chart")

# --- Roadmap Charts (matplotlib → full_local only) ---
_reg("years_of_reserves", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Years-of-reserves lollipop chart")
_reg("growth_vs_deterioration", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Growth vs deterioration quadrant scatter (CRE)")
_reg("growth_vs_deterioration_bookwide", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Bookwide growth vs deterioration quadrant scatter (total gross loans)")
_reg("risk_adjusted_return", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Risk-adjusted return frontier bubble scatter")
_reg("concentration_vs_capital", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Concentration vs capital sensitivity quadrant scatter")
_reg("liquidity_overlay", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Liquidity / draw-risk overlay combo chart")
_reg("macro_corr_heatmap_lag1", ArtifactAvailability.BOTH, "table",
     "Lagged Pearson correlation heatmap (internal metrics vs FRED macro, +1Q lag)")
_reg("macro_overlay_credit_stress", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Macro overlay: Norm NCO Rate vs HY OAS + NFCI (z-scored)")
_reg("macro_overlay_rates_housing", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Macro overlay: Resi credit vs FEDFUNDS + MORTGAGE30US + CSUSHPISA YoY")

# --- FRED Expansion Charts (matplotlib → full_local only) ---
_reg("sbl_backdrop", ArtifactAvailability.FULL_LOCAL_ONLY, "fred_chart",
     "SBL market backdrop dual-axis chart")
_reg("jumbo_conditions", ArtifactAvailability.FULL_LOCAL_ONLY, "fred_chart",
     "Jumbo mortgage conditions multi-panel chart")
_reg("resi_credit_cycle", ArtifactAvailability.FULL_LOCAL_ONLY, "fred_chart",
     "Residential credit cycle dual-axis chart")
_reg("cre_cycle", ArtifactAvailability.FULL_LOCAL_ONLY, "fred_chart",
     "CRE cycle multi-panel chart")
_reg("cs_collateral_panel", ArtifactAvailability.FULL_LOCAL_ONLY, "fred_chart",
     "Case-Shiller collateral panel chart")

# --- Executive Charts (Prompt 2) ---
# YoY Heatmaps — 4 variants: Standard/Normalized × Wealth/All Peers
_reg("yoy_heatmap_standard_wealth", ArtifactAvailability.BOTH, "table",
     "YoY directional heatmap (standard metrics, Wealth Peers)")
_reg("yoy_heatmap_standard_allpeers", ArtifactAvailability.BOTH, "table",
     "YoY directional heatmap (standard metrics, All Peers)")
_reg("yoy_heatmap_normalized_wealth", ArtifactAvailability.BOTH, "table",
     "YoY directional heatmap (normalized metrics, Wealth Peers)")
_reg("yoy_heatmap_normalized_allpeers", ArtifactAvailability.BOTH, "table",
     "YoY directional heatmap (normalized metrics, All Peers)")
# KRI Bullet Charts (unchanged)
_reg("kri_bullet_standard", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "KRI football-field chart — standard % rate metrics (MSPBNA vs nested peer range)")
_reg("kri_bullet_standard_coverage", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "KRI football-field chart — standard x-multiple coverage metrics (MSPBNA vs nested peer range)")
_reg("kri_bullet_normalized_rates", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "KRI football-field chart — normalized rate metrics (MSPBNA vs norm peer range)")
_reg("kri_bullet_normalized_composition", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "KRI football-field chart — normalized composition metrics (MSPBNA vs norm peer range)")
# Sparklines — 4 variants: Standard/Normalized × Wealth/All Peers
_reg("sparkline_standard_wealth", ArtifactAvailability.BOTH, "table",
     "Sparkline summary (standard metrics, Wealth Peers)")
_reg("sparkline_standard_allpeers", ArtifactAvailability.BOTH, "table",
     "Sparkline summary (standard metrics, All Peers)")
_reg("sparkline_normalized_wealth", ArtifactAvailability.BOTH, "table",
     "Sparkline summary (normalized metrics, Wealth Peers)")
_reg("sparkline_normalized_allpeers", ArtifactAvailability.BOTH, "table",
     "Sparkline summary (normalized metrics, All Peers)")
# Cumulative Growth: Target Loans vs CRE ACL
_reg("cumul_growth_loans_vs_acl_wealth", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Cumulative growth: Target Loans vs CRE ACL (MSPBNA vs Wealth Peers)")
_reg("cumul_growth_loans_vs_acl_allpeers", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Cumulative growth: Target Loans vs CRE ACL (MSPBNA vs All Peers)")

# --- Corp Overlay Artifacts (separate workflow: corp_overlay_runner.py) ---
_reg("loan_balance_by_product", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Descending bar chart of current_balance by product_type")
_reg("top10_geography_by_balance", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "Top 10 geographies by aggregate loan balance")
_reg("internal_credit_flags_summary", ArtifactAvailability.BOTH, "table",
     "Internal delinquency/nonaccrual/risk-rating distribution summary")
_reg("peer_vs_internal_mix_bridge", ArtifactAvailability.BOTH, "table",
     "Peer-report composition vs internal loan product/geography mix")
_reg("msa_macro_panel", ArtifactAvailability.FULL_LOCAL_ONLY, "chart",
     "MSA-level macro backdrop panel (Case-Shiller, GDP, unemployment)")


# =====================================================================
# Artifact Manifest — per-run outcome tracking
# =====================================================================

class ArtifactStatus(enum.Enum):
    GENERATED = "generated"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclasses.dataclass
class ArtifactOutcome:
    """Outcome of a single artifact within a run."""
    name: str
    mode: str
    status: ArtifactStatus
    path: Optional[str] = None
    skip_reason: Optional[str] = None
    error: Optional[str] = None


class ArtifactManifest:
    """Collects outcomes for all artifacts in a single run."""

    def __init__(self, mode: RenderMode):
        self.mode = mode
        self._outcomes: List[ArtifactOutcome] = []

    def record_generated(self, name: str, path: str) -> None:
        self._outcomes.append(ArtifactOutcome(
            name=name, mode=self.mode.value,
            status=ArtifactStatus.GENERATED, path=path,
        ))

    def record_skipped(self, name: str, reason: str) -> None:
        self._outcomes.append(ArtifactOutcome(
            name=name, mode=self.mode.value,
            status=ArtifactStatus.SKIPPED, skip_reason=reason,
        ))

    def record_failed(self, name: str, error: str,
                      path: Optional[str] = None) -> None:
        self._outcomes.append(ArtifactOutcome(
            name=name, mode=self.mode.value,
            status=ArtifactStatus.FAILED, path=path, error=error,
        ))

    @property
    def outcomes(self) -> List[ArtifactOutcome]:
        return list(self._outcomes)

    def summary_table(self) -> str:
        """Return a human-readable summary table."""
        lines = [
            f"{'Artifact':<45} {'Mode':<12} {'Status':<10} {'Path / Reason'}",
            "-" * 110,
        ]
        for o in self._outcomes:
            detail = o.path or o.skip_reason or o.error or ""
            lines.append(
                f"{o.name:<45} {o.mode:<12} {o.status.value:<10} {detail}"
            )
        # Counts
        gen = sum(1 for o in self._outcomes if o.status == ArtifactStatus.GENERATED)
        skip = sum(1 for o in self._outcomes if o.status == ArtifactStatus.SKIPPED)
        fail = sum(1 for o in self._outcomes if o.status == ArtifactStatus.FAILED)
        lines.append("-" * 110)
        lines.append(f"Total: {len(self._outcomes)}  |  "
                     f"Generated: {gen}  |  Skipped: {skip}  |  Failed: {fail}")
        return "\n".join(lines)

    def counts(self) -> dict[str, int]:
        gen = sum(1 for o in self._outcomes if o.status == ArtifactStatus.GENERATED)
        skip = sum(1 for o in self._outcomes if o.status == ArtifactStatus.SKIPPED)
        fail = sum(1 for o in self._outcomes if o.status == ArtifactStatus.FAILED)
        return {"generated": gen, "skipped": skip, "failed": fail,
                "total": len(self._outcomes)}


# =====================================================================
# Convenience: check-and-skip helper for generate_reports()
# =====================================================================

def is_artifact_available(artifact_name: str, mode: RenderMode,
                          suppressed_charts: Optional[set] = None) -> bool:
    """Side-effect-free check: can this artifact be produced?

    Unlike ``should_produce``, this does NOT record anything in the manifest.
    Use it for preflight availability sweeps (e.g. "should I load FRED data?").
    """
    cap = ARTIFACT_REGISTRY.get(artifact_name)
    if cap is None:
        return True
    if not cap.is_available(mode):
        return False
    if suppressed_charts and artifact_name in suppressed_charts:
        return False
    return True


def should_produce(artifact_name: str, mode: RenderMode,
                   manifest: ArtifactManifest,
                   suppressed_charts: Optional[set] = None) -> bool:
    """Return True if the artifact should be produced in the current run.

    If the artifact is skipped (by mode or preflight suppression), logs
    the skip reason to the manifest and prints a message.  The caller
    should guard each artifact block with::

        if should_produce("scatter_nco_vs_npl", mode, manifest, suppressed):
            ...  # produce the artifact
    """
    cap = ARTIFACT_REGISTRY.get(artifact_name)
    if cap is None:
        # Unknown artifact — produce it (backwards compatibility)
        return True

    # Mode-based skip
    reason = cap.skip_reason(mode)
    if reason:
        manifest.record_skipped(artifact_name, reason)
        print(f"  [SKIP] {reason}")
        return False

    # Preflight suppression (from validate_output_inputs)
    if suppressed_charts and artifact_name in suppressed_charts:
        reason = f"{artifact_name}: suppressed by preflight validation"
        manifest.record_skipped(artifact_name, reason)
        print(f"  [SKIP] {reason}")
        return False

    return True
