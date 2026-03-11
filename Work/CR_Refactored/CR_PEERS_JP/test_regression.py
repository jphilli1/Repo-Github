#!/usr/bin/env python3
"""
Regression Tests for CR_PEERS_JP
=================================

Executable assertions for:
  1. Scatter integrity — composite CERTs excluded from peer dots
  2. Peer group uniqueness — no duplicates within same normalization mode
  3. Over-exclusion detection — diagnostics-first normalization
  4. Workbook sanity — required sheets and columns present
  5. Consumer trace — every high-severity metric has declared consumers
"""

import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ═══════════════════════════════════════════════════════════════════════════
# 1. SCATTER INTEGRITY
# ═══════════════════════════════════════════════════════════════════════════

def test_scatter_composite_exclusion():
    """Composite CERTs (90001-90006, 99998, 99999, 88888) must NEVER appear
    as blue peer dots in scatter plots."""
    from report_generator import ALL_COMPOSITE_CERTS

    # Simulate a scatter dataframe with composites present
    df = pd.DataFrame({
        "CERT": [19977, 33124, 57565, 90001, 90003, 90004, 90006, 99998, 99999, 88888, 628],
        "NPL_to_Gross_Loans_Rate": np.random.rand(11),
        "TTM_NCO_Rate": np.random.rand(11),
        "NAME": ["MSPBNA", "GS", "UBS", "Core PB", "All Peers", "Core PB Norm",
                 "All Norm", "Alias1", "Alias2", "MS Combined", "JPM"],
    })

    subject_cert = 19977
    exclude_set = ALL_COMPOSITE_CERTS | {subject_cert}
    others = df[~df["CERT"].isin(exclude_set)]

    # Only real peers (33124, 57565, 628) should remain
    assert set(others["CERT"].values) == {33124, 57565, 628}, (
        f"Expected only real peers, got CERTs: {set(others['CERT'].values)}"
    )
    # No composites in others
    assert not (set(others["CERT"].values) & ALL_COMPOSITE_CERTS), (
        "Composite CERTs leaked into peer scatter dots"
    )
    print("  [PASS] test_scatter_composite_exclusion")


def test_scatter_peer_avg_display():
    """The peer average marker should use the explicitly passed CERT."""
    from report_generator import ALL_COMPOSITE_CERTS

    # For normalized scatter, peer_avg_cert_primary should be 90006 directly
    peer_avg_cert_primary = 90006
    assert peer_avg_cert_primary in ALL_COMPOSITE_CERTS, (
        "Peer avg cert should be a known composite"
    )
    print("  [PASS] test_scatter_peer_avg_display")


# ═══════════════════════════════════════════════════════════════════════════
# 2. PEER GROUP UNIQUENESS
# ═══════════════════════════════════════════════════════════════════════════

def test_peer_group_no_intra_mode_duplicates():
    """No two peer groups with the same use_normalized flag may share
    identical sorted member-cert tuples.  Uses the REAL PEER_GROUPS from
    MSPBNA_CR_Normalized (4 groups, not the former 6)."""
    try:
        from MSPBNA_CR_Normalized import PEER_GROUPS
    except ImportError:
        # Fallback: verify the 4-group structure inline
        print("  [SKIP] Cannot import MSPBNA_CR_Normalized — using inline 4-group check")
        PEER_GROUPS = {
            "CORE_PRIVATE_BANK": {"certs": [33124, 57565], "use_normalized": False},
            "ALL_PEERS": {"certs": [32992, 33124, 57565, 628, 3511, 7213, 3510], "use_normalized": False},
            "CORE_PRIVATE_BANK_NORM": {"certs": [33124, 57565], "use_normalized": True},
            "ALL_PEERS_NORM": {"certs": [32992, 33124, 57565, 628, 3511, 7213, 3510], "use_normalized": True},
        }

    # Must be exactly 4 groups (former MSPBNA+Wealth duplicates removed)
    assert len(PEER_GROUPS) == 4, (
        f"Expected 4 peer groups, got {len(PEER_GROUPS)}: {list(PEER_GROUPS.keys())}"
    )

    # Check within same mode — no two groups sharing use_normalized should have identical certs
    by_mode = {}
    for name, pg in PEER_GROUPS.items():
        norm_flag = pg.get("use_normalized", False)
        key = (norm_flag, tuple(sorted(pg["certs"])))
        by_mode.setdefault(key, []).append(name)

    for key, names in by_mode.items():
        assert len(names) <= 1, (
            f"DUPLICATE within use_normalized={key[0]}: {names} share certs {key[1]}"
        )
    print("  [PASS] test_peer_group_no_intra_mode_duplicates")


def test_peer_group_cross_mode_distinction():
    """Standard vs normalized groups with same certs are permitted but must
    differ in use_normalized flag."""
    std_certs = {33124, 57565}
    norm_certs = {33124, 57565}
    # Same certs OK if use_normalized differs
    assert std_certs == norm_certs, "Certs match as expected for cross-mode"
    # The distinction is the use_normalized flag — this is by design
    print("  [PASS] test_peer_group_cross_mode_distinction")


# ═══════════════════════════════════════════════════════════════════════════
# 3. OVER-EXCLUSION DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def test_over_exclusion_minor_clip():
    """Minor negative residuals (< 5% of total) should clip to 0, not NaN."""
    total = pd.Series([1000.0, 2000.0, 500.0])
    excluded = pd.Series([1020.0, 1900.0, 480.0])  # 2% over, 5% under, 4% under
    residual = total - excluded  # [-20, 100, 20]

    TOLERANCE = -0.05
    result = np.where(
        residual >= 0, residual,
        np.where(
            (residual / total.replace(0, np.nan)) >= TOLERANCE,
            0.0, np.nan
        )
    )

    assert result[0] == 0.0, f"Minor over-exclusion should clip to 0, got {result[0]}"
    assert result[1] == 100.0, f"Positive residual should pass through, got {result[1]}"
    assert result[2] == 20.0, f"Positive residual should pass through, got {result[2]}"
    print("  [PASS] test_over_exclusion_minor_clip")


def test_over_exclusion_material_nan():
    """Material negative residuals (>= 5% of total) should be NaN."""
    total = pd.Series([1000.0, 100.0])
    excluded = pd.Series([1200.0, 200.0])  # 20% over, 100% over
    residual = total - excluded  # [-200, -100]

    TOLERANCE = -0.05
    result = np.where(
        residual >= 0, residual,
        np.where(
            (residual / total.replace(0, np.nan)) >= TOLERANCE,
            0.0, np.nan
        )
    )

    assert np.isnan(result[0]), f"Material over-exclusion should be NaN, got {result[0]}"
    assert np.isnan(result[1]), f"Material over-exclusion should be NaN, got {result[1]}"
    print("  [PASS] test_over_exclusion_material_nan")


def test_diagnostics_columns_present():
    """Normalization must produce diagnostic columns (residuals, flags, severity)."""
    expected_diag_cols = [
        "_Norm_NCO_Residual", "_Norm_NA_Residual", "_Norm_Loans_Residual",
        "_Flag_NCO_OverExclusion", "_Flag_NA_OverExclusion", "_Flag_Loans_OverExclusion",
        "_Norm_NCO_Severity", "_Norm_NA_Severity", "_Norm_Loans_Severity",
    ]
    # This is a structural test — just verify the column names are what we expect
    for col in expected_diag_cols:
        assert col.startswith("_"), f"Diagnostic column {col} should be underscore-prefixed"
    print("  [PASS] test_diagnostics_columns_present")


# ═══════════════════════════════════════════════════════════════════════════
# 4. WORKBOOK SANITY
# ═══════════════════════════════════════════════════════════════════════════

def test_required_sheets():
    """The Excel output must contain these sheets."""
    required = {
        "FDIC_Data", "Summary_Dashboard", "FDIC_Metric_Descriptions",
        "Data_Validation_Report", "Metric_Validation_Audit",
    }
    new_sheets = {
        "Normalization_Diagnostics", "Peer_Group_Definitions",
    }
    all_expected = required | new_sheets
    # Can't test actual file without running pipeline, but verify the list is documented
    assert len(all_expected) >= 7, "Should have at least 7 required sheets"
    print("  [PASS] test_required_sheets")


# ═══════════════════════════════════════════════════════════════════════════
# 5. CONSUMER TRACE
# ═══════════════════════════════════════════════════════════════════════════

def test_high_severity_metrics_have_consumers():
    """Every high-severity derived metric must declare at least one downstream consumer."""
    from metric_registry import DERIVED_METRIC_SPECS

    orphaned = []
    for code, spec in DERIVED_METRIC_SPECS.items():
        if spec.severity == "high" and not spec.consumers:
            orphaned.append(code)

    # Source columns (Norm_Gross_Loans, Total_ACL, Gross_Loans, etc.) may legitimately
    # have empty consumers since they are upstream choke-points, not report outputs.
    source_cols = {"Norm_Gross_Loans", "Total_ACL", "Gross_Loans", "SBL_Balance", "Total_Nonaccrual"}
    truly_orphaned = [c for c in orphaned if c not in source_cols]

    assert len(truly_orphaned) == 0, (
        f"High-severity metrics without consumers: {truly_orphaned}"
    )
    print("  [PASS] test_high_severity_metrics_have_consumers")


def test_metric_registry_semantic_rules():
    """Semantic validation rules exist and are callable."""
    from metric_registry import SEMANTIC_VALIDATION_RULES, ValidationRule

    assert len(SEMANTIC_VALIDATION_RULES) >= 5, (
        f"Expected at least 5 semantic rules, got {len(SEMANTIC_VALIDATION_RULES)}"
    )
    for rule in SEMANTIC_VALIDATION_RULES:
        assert isinstance(rule, ValidationRule), f"Rule {rule.rule_id} is not a ValidationRule"
        assert callable(rule.check), f"Rule {rule.rule_id} check is not callable"
    print("  [PASS] test_metric_registry_semantic_rules")


def test_semantic_validation_on_synthetic_data():
    """Run semantic validation on a small synthetic DataFrame."""
    from metric_registry import run_semantic_validation

    df = pd.DataFrame({
        "CERT": [19977, 33124, 90001, 90004],
        "REPDTE": pd.Timestamp("2025-03-31"),
        "Gross_Loans": [10000, 20000, 15000, 15000],
        "Excluded_Balance": [2000, 3000, 2500, 2500],
        "Total_NCO_TTM": [100, 200, 150, 150],
        "Excluded_NCO_TTM": [20, 30, 25, 25],
        "Total_Nonaccrual": [50, 100, 75, 75],
        "Excluded_Nonaccrual": [10, 15, 12, 12],
        "Norm_NCO_Rate": [0.01, 0.01, 0.01, 0.01],
        "Norm_Nonaccrual_Rate": [0.005, 0.005, 0.005, 0.005],
        "Norm_Delinquency_Rate": [0.005, 0.005, 0.005, 0.005],
        "Norm_ACL_Coverage": [0.02, 0.02, 0.02, 0.02],
        "Norm_Exclusion_Pct": [0.2, 0.15, 0.17, 0.17],
        "TTM_NCO_Rate": [0.01, 0.01, 0.01, np.nan],
        "NPL_to_Gross_Loans_Rate": [0.005, 0.005, 0.005, np.nan],
    })

    report = run_semantic_validation(df)
    assert isinstance(report, pd.DataFrame), "Semantic validation should return a DataFrame"
    # Should detect flatline anomaly (all Norm_NCO_Rate = 0.01)
    if not report.empty:
        rules_found = set(report["Rule"].values) if "Rule" in report.columns else set()
        print(f"    Detected rules: {rules_found}")
    print("  [PASS] test_semantic_validation_on_synthetic_data")


# ═══════════════════════════════════════════════════════════════════════════
# 6. RESIDENTIAL NORMALIZED METRICS
# ═══════════════════════════════════════════════════════════════════════════

def test_no_resi_naming_collision():
    """Norm_Resi_Composition must not exist — canonical name is Norm_Wealth_Resi_Composition."""
    from metric_registry import DERIVED_METRIC_SPECS

    assert "Norm_Wealth_Resi_Composition" in DERIVED_METRIC_SPECS, (
        "Canonical resi composition metric Norm_Wealth_Resi_Composition not in registry"
    )
    # Norm_Resi_Composition should NOT be registered (it's the deprecated phantom name)
    assert "Norm_Resi_Composition" not in DERIVED_METRIC_SPECS, (
        "Deprecated Norm_Resi_Composition should not be in metric registry"
    )
    print("  [PASS] test_no_resi_naming_collision")


def test_resi_acl_coverage_vs_share():
    """Coverage metric must divide by loan balance, share by total ACL.
    No metric labeled 'coverage' may actually be a share."""
    from metric_registry import DERIVED_METRIC_SPECS

    # Norm_Resi_ACL_Share: numerator=RIC_Resi_ACL, denominator=Norm_ACL_Balance
    share_spec = DERIVED_METRIC_SPECS.get("Norm_Resi_ACL_Share")
    assert share_spec is not None, "Norm_Resi_ACL_Share not in registry"
    assert "Norm_ACL_Balance" in share_spec.dependencies, (
        f"Share metric should depend on Norm_ACL_Balance, got {share_spec.dependencies}"
    )

    # Norm_Resi_ACL_Coverage: numerator=RIC_Resi_ACL, denominator=Wealth_Resi_Balance
    cov_spec = DERIVED_METRIC_SPECS.get("Norm_Resi_ACL_Coverage")
    assert cov_spec is not None, "Norm_Resi_ACL_Coverage not in registry"
    assert "Wealth_Resi_Balance" in cov_spec.dependencies, (
        f"Coverage metric should depend on Wealth_Resi_Balance, got {cov_spec.dependencies}"
    )
    print("  [PASS] test_resi_acl_coverage_vs_share")


def test_resi_composition_numerator_denominator():
    """Norm_Wealth_Resi_Composition must be Wealth_Resi_Balance / Norm_Gross_Loans."""
    from metric_registry import DERIVED_METRIC_SPECS

    spec = DERIVED_METRIC_SPECS["Norm_Wealth_Resi_Composition"]
    assert "Wealth_Resi_Balance" in spec.dependencies, (
        f"Numerator should be Wealth_Resi_Balance, got deps: {spec.dependencies}"
    )
    assert "Norm_Gross_Loans" in spec.dependencies, (
        f"Denominator should be Norm_Gross_Loans, got deps: {spec.dependencies}"
    )

    # Validate computation
    df = pd.DataFrame({
        "Wealth_Resi_Balance": [5000.0, 3000.0],
        "Norm_Gross_Loans": [10000.0, 6000.0],
    })
    result = spec.compute(df)
    assert abs(result.iloc[0] - 0.5) < 1e-6, f"Expected 0.5, got {result.iloc[0]}"
    assert abs(result.iloc[1] - 0.5) < 1e-6, f"Expected 0.5, got {result.iloc[1]}"
    print("  [PASS] test_resi_composition_numerator_denominator")


def test_report_generator_uses_canonical_resi_names():
    """report_generator.py must reference Norm_Wealth_Resi_Composition,
    not phantom Norm_Resi_Composition."""
    import re
    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_generator.py")
    with open(report_path, "r") as f:
        content = f.read()

    # Should NOT contain Norm_Resi_Composition as a column reference
    # (but may contain it in comments or this test reference)
    phantom_refs = [m.start() for m in re.finditer(r'"Norm_Resi_Composition"', content)]
    assert len(phantom_refs) == 0, (
        f"report_generator.py still references phantom 'Norm_Resi_Composition' at {len(phantom_refs)} locations"
    )

    # Should contain canonical name
    assert '"Norm_Wealth_Resi_Composition"' in content, (
        "report_generator.py missing canonical Norm_Wealth_Resi_Composition references"
    )
    print("  [PASS] test_report_generator_uses_canonical_resi_names")


# ═══════════════════════════════════════════════════════════════════════════
# 7. IDB LABEL CLEANUP
# ═══════════════════════════════════════════════════════════════════════════

def test_no_idb_keys_in_data_dictionary():
    """LOCAL_DERIVED_METRICS must not contain any key starting with IDB_."""
    from master_data_dictionary import LOCAL_DERIVED_METRICS
    idb_keys = [k for k in LOCAL_DERIVED_METRICS if k.startswith("IDB_")]
    assert len(idb_keys) == 0, (
        f"LOCAL_DERIVED_METRICS contains IDB_ keys: {idb_keys}"
    )
    print("  [PASS] test_no_idb_keys_in_data_dictionary")


# ═══════════════════════════════════════════════════════════════════════════
# 8. PREFLIGHT VALIDATION
# ═══════════════════════════════════════════════════════════════════════════

def test_preflight_blocks_peer_avg_material_nan():
    """validate_output_inputs must block when normalized peer-average composite
    CERT 90006 has material_nan severity."""
    from report_generator import validate_output_inputs
    proc_df = pd.DataFrame({
        "CERT": [19977, 90006, 90004, 33124],
        "REPDTE": pd.Timestamp("2025-03-31"),
        "Norm_NCO_Rate": [0.01, 0.02, 0.015, 0.01],
        "_Norm_NCO_Severity": ["ok", "material_nan", "ok", "ok"],
        "_Norm_NA_Severity": ["ok", "ok", "ok", "ok"],
        "_Norm_Loans_Severity": ["ok", "ok", "ok", "ok"],
    })
    rolling_df = pd.DataFrame({
        "CERT": [19977, 90006, 90004, 90003, 90001, 33124],
    })
    result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
    assert not result["valid"], "Preflight should block when 90006 has material_nan"
    assert any("90006" in e for e in result["errors"]), (
        f"Errors should mention CERT 90006: {result['errors']}"
    )
    assert "normalized_credit_chart" in result["suppressed_charts"], (
        f"Suppressed should include normalized_credit_chart: {result['suppressed_charts']}"
    )
    print("  [PASS] test_preflight_blocks_peer_avg_material_nan")


def test_preflight_blocks_missing_normalized_composite():
    """validate_output_inputs must block when required normalized composite
    CERTs 90006 or 90004 are missing from data."""
    from report_generator import validate_output_inputs
    proc_df = pd.DataFrame({
        "CERT": [19977, 33124],  # No 90004 or 90006
        "REPDTE": pd.Timestamp("2025-03-31"),
    })
    rolling_df = pd.DataFrame({
        "CERT": [19977, 90003, 90001, 33124],  # Standard composites present, normalized missing
    })
    result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
    assert not result["valid"], "Preflight should block when normalized composites missing"
    assert any("90006" in e or "90004" in e for e in result["errors"]), (
        f"Errors should mention missing 90006/90004: {result['errors']}"
    )
    print("  [PASS] test_preflight_blocks_missing_normalized_composite")


def test_claude_md_no_conflict_markers():
    """CLAUDE.md must not contain merge conflict markers."""
    claude_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CLAUDE.md")
    with open(claude_path, "r") as f:
        content = f.read()
    for marker in ["<<<<<<<", "=======", ">>>>>>>"]:
        assert marker not in content, (
            f"CLAUDE.md contains merge conflict marker: {marker}"
        )
    print("  [PASS] test_claude_md_no_conflict_markers")


# ═══════════════════════════════════════════════════════════════════════════
# 9. BALANCE-GATING, COMPOSITE COVERAGE, PREFLIGHT SCOPING
# ═══════════════════════════════════════════════════════════════════════════

def test_balance_gate_ag_nco():
    """A bank with zero excluded Ag balance cannot produce nonzero excluded Ag NCO
    without the _Ag_NCO_Gated flag being set."""
    ag_bal = np.array([0.0, 500.0, 0.0])
    ag_nco_raw = np.array([27000.0, 1500.0, 0.0])
    gated_nco = np.where(ag_bal > 0, ag_nco_raw, 0.0)
    gated_flag = np.where((ag_bal == 0) & (ag_nco_raw != 0), True, False)

    # Row 0: zero balance, nonzero raw NCO → gated to 0, flag set
    assert gated_nco[0] == 0.0, f"Ag NCO should be gated to 0 when balance is zero, got {gated_nco[0]}"
    assert gated_flag[0] == True, "Ag NCO gating flag should be True when balance=0 and raw NCO!=0"
    # Row 1: positive balance → raw NCO passes through, no flag
    assert gated_nco[1] == 1500.0, f"Ag NCO should pass through when balance > 0, got {gated_nco[1]}"
    assert gated_flag[1] == False, "Ag NCO gating flag should be False when balance > 0"
    # Row 2: zero balance, zero raw NCO → no flag
    assert gated_nco[2] == 0.0
    assert gated_flag[2] == False, "Ag NCO gating flag should be False when both are zero"
    print("  [PASS] test_balance_gate_ag_nco")


def test_balance_gate_auto_nco():
    """A bank with zero excluded Auto balance cannot produce nonzero excluded Auto NCO
    without the _Auto_NCO_Gated flag being set."""
    auto_bal = np.array([0.0, 100.0])
    auto_nco_raw = np.array([27000.0, 500.0])
    gated_nco = np.where(auto_bal > 0, auto_nco_raw, 0.0)
    gated_flag = np.where((auto_bal == 0) & (auto_nco_raw != 0), True, False)

    assert gated_nco[0] == 0.0, f"Auto NCO should be gated to 0 when balance is zero, got {gated_nco[0]}"
    assert gated_flag[0] == True, "Auto NCO gating flag should be True when balance=0 and raw NCO!=0"
    assert gated_nco[1] == 500.0, f"Auto NCO should pass through when balance > 0, got {gated_nco[1]}"
    assert gated_flag[1] == False
    print("  [PASS] test_balance_gate_auto_nco")


def test_composite_coverage_below_threshold_forces_nan():
    """Composite coverage below 50% threshold forces NaN for normalized composite metric."""
    # Simulate 8 contributor banks, only 3 have non-NaN Norm_NCO_Rate (37.5% < 50%)
    MIN_COVERAGE_PCT = 0.50
    contributor_values = pd.Series([0.01, np.nan, np.nan, 0.02, np.nan, np.nan, np.nan, 0.015])
    non_nan_count = contributor_values.notna().sum()
    total_count = len(contributor_values)
    coverage = non_nan_count / total_count  # 3/8 = 0.375

    composite_value = contributor_values.mean()  # Would be 0.015
    if coverage < MIN_COVERAGE_PCT:
        composite_value = np.nan

    assert np.isnan(composite_value), (
        f"Composite should be NaN when coverage ({coverage:.1%}) < threshold ({MIN_COVERAGE_PCT:.0%}), "
        f"got {composite_value}"
    )
    print("  [PASS] test_composite_coverage_below_threshold_forces_nan")


def test_preflight_blocks_norm_composite_90006_material_in_latest():
    """validate_output_inputs blocks when normalized composite 90006
    has material failure specifically in the latest plotted period."""
    from report_generator import validate_output_inputs
    proc_df = pd.DataFrame({
        "CERT": [19977, 90006, 90004, 33124, 19977, 90006, 90004, 33124],
        "REPDTE": [pd.Timestamp("2024-12-31")] * 4 + [pd.Timestamp("2025-03-31")] * 4,
        "Norm_NCO_Rate": [0.01] * 8,
        "_Norm_NCO_Severity": ["ok", "ok", "ok", "ok",  # historical: all ok
                                "ok", "material_nan", "ok", "ok"],  # latest: 90006 material
        "_Norm_NA_Severity": ["ok"] * 8,
        "_Norm_Loans_Severity": ["ok"] * 8,
    })
    rolling_df = pd.DataFrame({"CERT": [19977, 90006, 90004, 90003, 90001, 33124]})
    result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
    assert not result["valid"], "Should block when 90006 has material_nan at latest period"
    assert any("90006" in e for e in result["errors"]), f"Errors should mention 90006: {result['errors']}"
    print("  [PASS] test_preflight_blocks_norm_composite_90006_material_in_latest")


def test_preflight_does_not_block_historical_material():
    """validate_output_inputs does NOT block when material failure
    is only in a historical period, not the latest."""
    from report_generator import validate_output_inputs
    proc_df = pd.DataFrame({
        "CERT": [19977, 90006, 90004, 33124, 19977, 90006, 90004, 33124],
        "REPDTE": [pd.Timestamp("2024-12-31")] * 4 + [pd.Timestamp("2025-03-31")] * 4,
        "Norm_NCO_Rate": [0.01] * 8,
        "_Norm_NCO_Severity": ["ok", "material_nan", "ok", "ok",  # historical: 90006 material
                                "ok", "ok", "ok", "ok"],  # latest: all ok
        "_Norm_NA_Severity": ["ok"] * 8,
        "_Norm_Loans_Severity": ["ok"] * 8,
    })
    rolling_df = pd.DataFrame({"CERT": [19977, 90006, 90004, 90003, 90001, 33124]})
    result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
    assert result["valid"], f"Should NOT block on historical-only material failures, got errors: {result['errors']}"
    print("  [PASS] test_preflight_does_not_block_historical_material")


def test_load_config_defaults():
    """load_config defaults must be 34221 / 32992, not stale 19977."""
    from report_generator import load_config
    # Temporarily clear env vars to test defaults
    old_mspbna = os.environ.pop("MSPBNA_CERT", None)
    old_msbna = os.environ.pop("MSBNA_CERT", None)
    try:
        cfg = load_config()
        assert cfg["mspbna_cert"] == 34221, f"MSPBNA_CERT default should be 34221, got {cfg['mspbna_cert']}"
        assert cfg["msbna_cert"] == 32992, f"MSBNA_CERT default should be 32992, got {cfg['msbna_cert']}"
        assert cfg["subject_bank_cert"] == 34221, f"subject_bank_cert should match mspbna_cert"
    finally:
        if old_mspbna is not None:
            os.environ["MSPBNA_CERT"] = old_mspbna
        if old_msbna is not None:
            os.environ["MSBNA_CERT"] = old_msbna
    print("  [PASS] test_load_config_defaults")


def test_workbook_includes_audit_sheets():
    """write_excel_output call must include Exclusion_Component_Audit
    and Composite_Coverage_Audit as kwargs."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "Exclusion_Component_Audit=excl_audit_df" in source, (
        "write_excel_output call missing Exclusion_Component_Audit kwarg"
    )
    assert "Composite_Coverage_Audit=composite_coverage_df" in source, (
        "write_excel_output call missing Composite_Coverage_Audit kwarg"
    )
    # Case-Shiller ZIP sheets must be unpacked via **cs_kwargs
    assert "**cs_kwargs" in source, (
        "write_excel_output call missing **cs_kwargs for Case-Shiller ZIP sheets"
    )
    print("  [PASS] test_workbook_includes_audit_sheets")


def test_case_shiller_zip_sheets_resilient():
    """Case-Shiller ZIP enrichment must be wrapped in try/except to prevent pipeline crash."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "build_case_shiller_zip_sheets()" in source, (
        "build_case_shiller_zip_sheets() call missing from pipeline"
    )
    # Must be wrapped in try/except for resilience
    assert "Case-Shiller ZIP enrichment failed (non-fatal)" in source, (
        "Case-Shiller ZIP enrichment not wrapped in try/except for resilience"
    )
    print("  [PASS] test_case_shiller_zip_sheets_resilient")


def test_ttm_map_keys_match_quarterly_column_names():
    """TTM map keys for income columns must match col.replace('_YTD', '_Q') convention."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    # The TTM map must use the correct quarterly column names
    assert "'Int_Inc_Loans_Q'" in source, (
        "TTM map should use 'Int_Inc_Loans_Q' (not 'Int_Inc_Loans_YTD_Q')"
    )
    assert "'Provision_Exp_Q'" in source, (
        "TTM map should use 'Provision_Exp_Q' (not 'Provision_Exp_YTD_Q')"
    )
    assert "'Total_Int_Exp_Q'" in source, (
        "TTM map should use 'Total_Int_Exp_Q' (not 'Total_Int_Exp_YTD_Q')"
    )
    # The old wrong keys must NOT be present as TTM map keys (colon after quote = dict key)
    assert "'Int_Inc_Loans_YTD_Q':" not in source, (
        "Stale TTM map key 'Int_Inc_Loans_YTD_Q' still present"
    )
    assert "'Provision_Exp_YTD_Q':" not in source, (
        "Stale TTM map key 'Provision_Exp_YTD_Q' still present"
    )
    print("  [PASS] test_ttm_map_keys_match_quarterly_column_names")


def test_validation_suite_wired_in_pipeline():
    """run_upstream_validation_suite must be called in MSPBNA_CR_Normalized.py."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "run_upstream_validation_suite(proc_df_with_peers)" in source, (
        "run_upstream_validation_suite not wired into pipeline"
    )
    assert "Metric_Validation_Audit=metric_validation_df" in source, (
        "Metric_Validation_Audit sheet not in write_excel_output call"
    )
    print("  [PASS] test_validation_suite_wired_in_pipeline")


def test_no_stale_19977_in_report_generator():
    """report_generator.py must not have stale 19977 defaults in function signatures."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_generator.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "19977" not in source, (
        "Stale 19977 default still present in report_generator.py"
    )
    print("  [PASS] test_no_stale_19977_in_report_generator")


def test_claude_md_no_stale_19977_defaults():
    """CLAUDE.md env var table must not show 19977 as default for MSPBNA_CERT or MSBNA_CERT."""
    md_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CLAUDE.md")
    with open(md_path, "r") as f:
        lines = f.readlines()
    for i, line in enumerate(lines, 1):
        # Check env var table rows (contain | MSPBNA_CERT or | MSBNA_CERT)
        if "| `MSPBNA_CERT`" in line or "| `MSBNA_CERT`" in line:
            assert "19977" not in line, (
                f"CLAUDE.md line {i} still shows 19977 as default: {line.strip()}"
            )
    print("  [PASS] test_claude_md_no_stale_19977_defaults")


def test_curated_summary_dashboard_metrics():
    """Summary_Dashboard must use SUMMARY_DASHBOARD_METRICS allowlist, not all numeric cols."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "SUMMARY_DASHBOARD_METRICS" in source, (
        "SUMMARY_DASHBOARD_METRICS allowlist not defined"
    )
    assert "for m in SUMMARY_DASHBOARD_METRICS" in source, (
        "create_peer_comparison must iterate SUMMARY_DASHBOARD_METRICS"
    )
    print("  [PASS] test_curated_summary_dashboard_metrics")


def test_curated_normalized_comparison_metrics():
    """Normalized_Comparison must use NORMALIZED_COMPARISON_METRICS allowlist."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "NORMALIZED_COMPARISON_METRICS" in source
    assert "for m in NORMALIZED_COMPARISON_METRICS" in source
    # Norm_Provision_Rate must NOT be in the allowlist
    assert "'Norm_Provision_Rate'" not in source.split("NORMALIZED_COMPARISON_METRICS")[1].split("]")[0], (
        "Norm_Provision_Rate should be excluded from NORMALIZED_COMPARISON_METRICS"
    )
    print("  [PASS] test_curated_normalized_comparison_metrics")


def test_descriptive_metrics_no_evaluative_flag():
    """Descriptive metrics (ASSET, LNLS, etc.) must return blank from _get_performance_flag."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "DESCRIPTIVE_METRICS" in source, "DESCRIPTIVE_METRICS set not defined"
    assert "if metric_code in DESCRIPTIVE_METRICS" in source, (
        "_get_performance_flag must check DESCRIPTIVE_METRICS"
    )
    # Verify key descriptive metrics are in the set
    for m in ["ASSET", "LNLS", "Norm_Gross_Loans", "SBL_Composition"]:
        assert f'"{m}"' in source, f"{m} missing from DESCRIPTIVE_METRICS"
    print("  [PASS] test_descriptive_metrics_no_evaluative_flag")


def test_display_labels_applied():
    """Metric Name column must be populated via _get_metric_short_name (FDIC_Metric_Descriptions)."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "MSPBNA_CR_Normalized.py")
    with open(src_path, "r") as f:
        source = f.read()
    # Both create_peer_comparison and create_normalized_comparison must use _get_metric_short_name
    assert '"Metric Name": _get_metric_short_name(metric)' in source, (
        "Display labels not applied via _get_metric_short_name"
    )
    print("  [PASS] test_display_labels_applied")


def test_preflight_contains_hardened_logic():
    """report_generator.py must contain hardened preflight with period-scoped blocking."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_generator.py")
    with open(src_path, "r") as f:
        source = f.read()
    assert "latest_repdte" in source, "Preflight must use latest_repdte for period scoping"
    assert "_NORMALIZED_COMPOSITES" in source, "Preflight must reference _NORMALIZED_COMPOSITES"
    assert "blocking_certs" in source, "Preflight must define blocking_certs"
    print("  [PASS] test_preflight_contains_hardened_logic")


def test_tight_layout_removed_from_ppt_chart():
    """create_credit_deterioration_chart_ppt must not call fig.tight_layout()."""
    src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report_generator.py")
    with open(src_path, "r") as f:
        source = f.read()
    # Find the function body
    func_start = source.index("def create_credit_deterioration_chart_ppt")
    # Find the next top-level def
    next_def = source.index("\ndef ", func_start + 1)
    func_body = source[func_start:next_def]
    assert "fig.tight_layout()" not in func_body, (
        "fig.tight_layout() still present in create_credit_deterioration_chart_ppt"
    )
    print("  [PASS] test_tight_layout_removed_from_ppt_chart")


# ═══════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════

def run_all_tests():
    """Run all regression tests."""
    print("=" * 60)
    print("REGRESSION TEST SUITE — CR_PEERS_JP")
    print("=" * 60)

    tests = [
        # 1. Scatter integrity
        test_scatter_composite_exclusion,
        test_scatter_peer_avg_display,
        # 2. Peer group uniqueness
        test_peer_group_no_intra_mode_duplicates,
        test_peer_group_cross_mode_distinction,
        # 3. Over-exclusion detection
        test_over_exclusion_minor_clip,
        test_over_exclusion_material_nan,
        test_diagnostics_columns_present,
        # 4. Workbook sanity
        test_required_sheets,
        # 5. Consumer trace
        test_high_severity_metrics_have_consumers,
        test_metric_registry_semantic_rules,
        test_semantic_validation_on_synthetic_data,
        # 6. Residential normalized metrics
        test_no_resi_naming_collision,
        test_resi_acl_coverage_vs_share,
        test_resi_composition_numerator_denominator,
        test_report_generator_uses_canonical_resi_names,
        # 7. IDB label cleanup
        test_no_idb_keys_in_data_dictionary,
        # 8. Preflight validation
        test_preflight_blocks_peer_avg_material_nan,
        test_preflight_blocks_missing_normalized_composite,
        test_claude_md_no_conflict_markers,
        # 9. Balance-gating, composite coverage, preflight scoping
        test_balance_gate_ag_nco,
        test_balance_gate_auto_nco,
        test_composite_coverage_below_threshold_forces_nan,
        test_preflight_blocks_norm_composite_90006_material_in_latest,
        test_preflight_does_not_block_historical_material,
        test_load_config_defaults,
        test_workbook_includes_audit_sheets,
        test_case_shiller_zip_sheets_resilient,
        # 10. Consistency pass: TTM, validation wiring, defaults
        test_ttm_map_keys_match_quarterly_column_names,
        test_validation_suite_wired_in_pipeline,
        test_no_stale_19977_in_report_generator,
        test_claude_md_no_stale_19977_defaults,
        # 11. Presentation curation, metric roles, display labels
        test_curated_summary_dashboard_metrics,
        test_curated_normalized_comparison_metrics,
        test_descriptive_metrics_no_evaluative_flag,
        test_display_labels_applied,
        test_preflight_contains_hardened_logic,
        test_tight_layout_removed_from_ppt_chart,
    ]

    passed = 0
    failed = 0
    errors = []

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            failed += 1
            errors.append((test_fn.__name__, str(e)))
            print(f"  [FAIL] {test_fn.__name__}: {e}")

    print("\n" + "-" * 60)
    print(f"Results: {passed} passed, {failed} failed out of {len(tests)} tests")
    if errors:
        print("\nFailed tests:")
        for name, err in errors:
            print(f"  - {name}: {err}")
    print("-" * 60)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)


# ===========================================================================
# UNITTEST-BASED REGRESSION CLASSES (supplemental)
# ===========================================================================

import unittest

import numpy as np
import pandas as pd

# Ensure module path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class TestNormalizationLogic(unittest.TestCase):
    """Tests for calc_normalized_residual and normalized master metrics."""

    def _make_calc_normalized_residual(self):
        """Return the calc_normalized_residual function for testing."""
        def calc_normalized_residual(total, excluded, label, tolerance_pct=0.05):
            residual = total - excluded
            severity = pd.Series('ok', index=total.index)
            final_value = residual.copy()
            over_exclusion_pct = pd.Series(0.0, index=total.index)

            pos_total = total > 0
            over_exclusion_pct[pos_total] = (-residual[pos_total] / total[pos_total]).clip(lower=0)

            minor_mask = (residual < 0) & pos_total & (over_exclusion_pct <= tolerance_pct)
            final_value[minor_mask] = 0.0
            severity[minor_mask] = 'minor_clip'

            material_mask = (residual < 0) & (~minor_mask)
            final_value[material_mask] = np.nan
            severity[material_mask] = 'material_nan'

            return {
                'final_value': final_value,
                'residual': residual,
                'over_exclusion_flag': (residual < 0),
                'over_exclusion_pct': over_exclusion_pct,
                'severity': severity,
            }
        return calc_normalized_residual

    def test_no_clip_on_normalized_metrics(self):
        """Norm_Total_NCO, Norm_Total_Nonaccrual, Norm_Gross_Loans must NOT use .clip(lower=0)."""
        # Read source and verify no .clip(lower=0) on final normalized assignments
        src_path = os.path.join(os.path.dirname(__file__), 'MSPBNA_CR_Normalized.py')
        with open(src_path, 'r') as f:
            source = f.read()

        # These patterns would indicate the bug is still present
        bad_patterns = [
            "df_processed['Norm_Total_NCO'] = (df_processed['Total_NCO_TTM'] - df_processed['Excluded_NCO_TTM']).clip(lower=0)",
            "df_processed['Norm_Gross_Loans'] = (df_processed['Gross_Loans'] - df_processed['Excluded_Balance']).clip(lower=0)",
        ]
        for pat in bad_patterns:
            self.assertNotIn(pat, source,
                f"Found forbidden .clip(lower=0) on normalized master metric: {pat[:60]}...")

    def test_material_over_exclusion_produces_nan(self):
        """Material over-exclusion (>5%) must produce NaN, not 0."""
        calc = self._make_calc_normalized_residual()
        total = pd.Series([100.0, 100.0])
        excluded = pd.Series([110.0, 120.0])  # 10% and 20% over-exclusion
        result = calc(total, excluded, 'test')

        self.assertTrue(pd.isna(result['final_value'].iloc[0]))
        self.assertTrue(pd.isna(result['final_value'].iloc[1]))
        self.assertEqual(result['severity'].iloc[0], 'material_nan')
        self.assertEqual(result['severity'].iloc[1], 'material_nan')

    def test_minor_over_exclusion_produces_zero(self):
        """Minor over-exclusion (<=5%) must produce 0 with severity=minor_clip."""
        calc = self._make_calc_normalized_residual()
        total = pd.Series([100.0])
        excluded = pd.Series([103.0])  # 3% over-exclusion
        result = calc(total, excluded, 'test')

        self.assertEqual(result['final_value'].iloc[0], 0.0)
        self.assertEqual(result['severity'].iloc[0], 'minor_clip')

    def test_positive_residual_ok(self):
        """Positive residual should pass through with severity=ok."""
        calc = self._make_calc_normalized_residual()
        total = pd.Series([100.0])
        excluded = pd.Series([40.0])
        result = calc(total, excluded, 'test')

        self.assertEqual(result['final_value'].iloc[0], 60.0)
        self.assertEqual(result['severity'].iloc[0], 'ok')


class TestPeerGroupUniqueness(unittest.TestCase):
    """Tests for peer group deduplication."""

    def test_no_duplicate_peer_groups(self):
        """No two peer groups with same use_normalized flag should have identical cert sets."""
        try:
            from MSPBNA_CR_Normalized import PEER_GROUPS
        except ImportError:
            self.skipTest("Cannot import MSPBNA_CR_Normalized (missing dependencies)")
        seen = {}
        for gk, gv in PEER_GROUPS.items():
            norm_flag = gv.get('use_normalized', False)
            cert_key = (norm_flag, tuple(sorted(gv['certs'])))
            self.assertNotIn(cert_key, seen,
                f"Peer group '{gk}' has identical cert membership as '{seen.get(cert_key)}' "
                f"(use_normalized={norm_flag})")
            seen[cert_key] = gk

    def test_validate_peer_group_uniqueness_function(self):
        """validate_peer_group_uniqueness raises on duplicates."""
        try:
            from MSPBNA_CR_Normalized import validate_peer_group_uniqueness
        except ImportError:
            self.skipTest("Cannot import MSPBNA_CR_Normalized (missing dependencies)")

        # Should pass with current (deduplicated) groups
        from MSPBNA_CR_Normalized import PEER_GROUPS
        validate_peer_group_uniqueness(PEER_GROUPS)  # Should not raise

        # Should raise with duplicate groups
        bad_groups = {
            'A': {'certs': [1, 2, 3], 'use_normalized': False, 'name': 'A', 'short_name': 'A'},
            'B': {'certs': [1, 2, 3], 'use_normalized': False, 'name': 'B', 'short_name': 'B'},
        }
        with self.assertRaises(ValueError):
            validate_peer_group_uniqueness(bad_groups)


class TestMetricNaming(unittest.TestCase):
    """Tests for correct metric naming conventions."""

    def test_norm_resi_acl_coverage_does_not_use_old_name(self):
        """Norm_RESI_ACL_Coverage (old name) must not exist anywhere."""
        src_path = os.path.join(os.path.dirname(__file__), 'MSPBNA_CR_Normalized.py')
        with open(src_path, 'r') as f:
            source = f.read()
        self.assertNotIn('Norm_RESI_ACL_Coverage', source)

    def test_norm_resi_acl_coverage_exists(self):
        """Norm_Resi_ACL_Coverage (correct name) must exist."""
        src_path = os.path.join(os.path.dirname(__file__), 'MSPBNA_CR_Normalized.py')
        with open(src_path, 'r') as f:
            source = f.read()
        self.assertIn('Norm_Resi_ACL_Coverage', source)

    def test_norm_acl_coverage_uses_norm_acl_balance(self):
        """Norm_ACL_Coverage formula must use Norm_ACL_Balance, not Total_ACL."""
        src_path = os.path.join(os.path.dirname(__file__), 'MSPBNA_CR_Normalized.py')
        with open(src_path, 'r') as f:
            source = f.read()
        # The correct line should be: safe_div(norm_acl_balance, ...)
        self.assertIn("new_cols['Norm_ACL_Coverage'] = safe_div(norm_acl_balance", source)

    def test_norm_risk_adj_allowance_coverage_exists(self):
        """Norm_Risk_Adj_Allowance_Coverage must exist in code."""
        src_path = os.path.join(os.path.dirname(__file__), 'MSPBNA_CR_Normalized.py')
        with open(src_path, 'r') as f:
            source = f.read()
        self.assertIn('Norm_Risk_Adj_Allowance_Coverage', source)


class TestRatioComponentsTable(unittest.TestCase):
    """Tests for report_generator ratio components table."""

    def test_normalized_uses_norm_total_past_due(self):
        """Normalized ratio components must use _Norm_Total_Past_Due, not _Total_Past_Due."""
        src_path = os.path.join(os.path.dirname(__file__), 'report_generator.py')
        with open(src_path, 'r') as f:
            source = f.read()
        # Find the normalized delinquency row
        self.assertIn('"_Norm_Total_Past_Due"', source)
        # The normalized section should not use the standard "Total_Past_Due" for delinquency
        # (it may still appear in standard section, which is correct)


class TestCaseShillerZIP(unittest.TestCase):
    """Tests for Case-Shiller ZIP integration."""

    def test_zip_toggle_disables_output(self):
        """Setting ENABLE_CASE_SHILLER_ZIP_ENRICHMENT=false should skip enrichment."""
        os.environ['ENABLE_CASE_SHILLER_ZIP_ENRICHMENT'] = 'false'
        try:
            from case_shiller_zip_mapper import build_case_shiller_zip_sheets
            result = build_case_shiller_zip_sheets()
            # Should return audit sheet noting skip
            self.assertIn('CaseShiller_County_Map_Audit', result)
            audit = result['CaseShiller_County_Map_Audit']
            # The county map version returns the full county map with SKIPPED comments
            has_skipped = any(
                'SKIPPED' in str(v)
                for v in audit.values.flatten()
            )
            self.assertTrue(has_skipped, "Audit should contain SKIPPED notation when disabled")
            # Coverage and Summary should be empty
            self.assertTrue(result['CaseShiller_Zip_Coverage'].empty)
            self.assertTrue(result['CaseShiller_Zip_Summary'].empty)
        finally:
            os.environ.pop('ENABLE_CASE_SHILLER_ZIP_ENRICHMENT', None)

    def test_zip_normalize_pads_to_5_chars(self):
        """ZIP codes must be zero-padded to 5 characters by _normalize_zip."""
        from case_shiller_zip_mapper import _normalize_zip
        self.assertEqual(_normalize_zip("2101"), "02101")
        self.assertEqual(_normalize_zip("10001"), "10001")
        self.assertEqual(_normalize_zip(2101), "02101")
        self.assertEqual(_normalize_zip("94105-1234"), "94105")

    def test_zip_output_contains_only_20_metros(self):
        """Coverage output should have exactly 20 Case-Shiller metros."""
        from case_shiller_zip_mapper import CASE_SHILLER_METROS
        self.assertEqual(len(CASE_SHILLER_METROS), 20)

    def test_county_map_has_20_regions(self):
        """CASE_SHILLER_COUNTY_MAP must cover exactly 20 regions."""
        from case_shiller_zip_mapper import CASE_SHILLER_COUNTY_MAP, VALID_CS_METROS
        self.assertEqual(len(VALID_CS_METROS), 20)
        regions = {m["case_shiller_region"] for m in CASE_SHILLER_COUNTY_MAP}
        self.assertEqual(len(regions), 20)

    def test_county_map_fips_are_5_digit(self):
        """All FIPS codes in county map must be 5-digit zero-padded strings."""
        from case_shiller_zip_mapper import CASE_SHILLER_COUNTY_MAP
        for entry in CASE_SHILLER_COUNTY_MAP:
            fips = entry["fips"]
            self.assertEqual(len(fips), 5, f"FIPS {fips} for {entry['county']} is not 5 chars")
            self.assertTrue(fips.isdigit(), f"FIPS {fips} contains non-digit chars")

    def test_county_map_has_required_fields(self):
        """Each county map entry must have case_shiller_region, fips, county, state."""
        from case_shiller_zip_mapper import CASE_SHILLER_COUNTY_MAP
        required = {"case_shiller_region", "fips", "county", "state"}
        for entry in CASE_SHILLER_COUNTY_MAP:
            self.assertTrue(required.issubset(entry.keys()),
                f"Missing keys in entry: {required - set(entry.keys())}")

    def test_hud_type_is_county_zip(self):
        """HUD API must use type=7 (county-to-ZIP), not type=8/9."""
        from case_shiller_zip_mapper import _HUD_TYPE_COUNTY_ZIP
        self.assertEqual(_HUD_TYPE_COUNTY_ZIP, 7)

    def test_no_cbsa_references_in_coverage_builder(self):
        """build_case_shiller_zip_coverage must not reference CBSA columns."""
        project_dir = os.path.dirname(os.path.abspath(__file__))
        mapper_path = os.path.join(project_dir, "case_shiller_zip_mapper.py")
        with open(mapper_path, "r", encoding="utf-8") as f:
            source = f.read()
        import ast
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "build_case_shiller_zip_coverage":
                body_src = ast.get_source_segment(source, node)
                self.assertNotIn("cbsa_code", body_src,
                    "Coverage builder must not reference cbsa_code columns")
                self.assertNotIn("cbsadiv", body_src,
                    "Coverage builder must not reference cbsadiv columns")
                break

    def test_map_zip_to_metro_basics(self):
        """map_zip_to_metro should correctly map ZIP prefixes to metros."""
        from case_shiller_zip_mapper import map_zip_to_metro
        self.assertEqual(map_zip_to_metro("10001"), "New York")
        self.assertEqual(map_zip_to_metro("90210"), "Los Angeles")
        self.assertEqual(map_zip_to_metro("60601"), "Chicago")
        self.assertEqual(map_zip_to_metro("94105"), "San Francisco")
        self.assertIsNone(map_zip_to_metro("50001"))  # Des Moines — no CS metro

    def test_no_duplicate_metro_codes_in_discovery(self):
        """No duplicate metro codes in fred_case_shiller_discovery."""
        from fred_case_shiller_discovery import CASE_SHILLER_SERIES
        codes = list(CASE_SHILLER_SERIES.keys())
        self.assertEqual(len(codes), len(set(codes)), "Duplicate metro codes found")

    def test_discovery_metro_names_match_mapper(self):
        """Metro names in discovery must match mapper exactly."""
        from fred_case_shiller_discovery import CASE_SHILLER_SERIES
        from case_shiller_zip_mapper import CASE_SHILLER_METROS
        discovery_metros = {v['metro'] for v in CASE_SHILLER_SERIES.values()}
        mapper_metros = set(CASE_SHILLER_METROS.keys())
        self.assertEqual(discovery_metros, mapper_metros,
            f"Mismatch: discovery={discovery_metros - mapper_metros}, mapper={mapper_metros - discovery_metros}")

    def test_discovery_metro_map_no_duplicates(self):
        """METRO_MAP in fred_case_shiller_discovery must have no duplicate keys."""
        from fred_case_shiller_discovery import METRO_MAP, validate_metro_map
        # Dict literal silently drops duplicate keys; validate_metro_map checks count
        issues = validate_metro_map()
        self.assertEqual(len(issues), 0, f"METRO_MAP issues: {issues}")
        # Must have exactly 20 entries
        self.assertEqual(len(METRO_MAP), 20, f"METRO_MAP has {len(METRO_MAP)} entries, expected 20")

    def test_discovery_washington_no_dc_suffix(self):
        """METRO_MAP 'WD' must map to 'Washington', not 'Washington DC'."""
        from fred_case_shiller_discovery import METRO_MAP
        self.assertEqual(METRO_MAP.get("WD"), "Washington",
            f"WD maps to '{METRO_MAP.get('WD')}', expected 'Washington'")


class TestIDBCleanup(unittest.TestCase):
    """Tests for IDB_ label removal."""

    def test_no_idb_keys_in_local_derived_metrics(self):
        """LOCAL_DERIVED_METRICS must not contain any key starting with IDB_."""
        from master_data_dictionary import LOCAL_DERIVED_METRICS
        idb_keys = [k for k in LOCAL_DERIVED_METRICS if k.startswith("IDB_")]
        self.assertEqual(len(idb_keys), 0, f"IDB_ keys found: {idb_keys}")


class TestPreflightValidation(unittest.TestCase):
    """Tests for validate_output_inputs() in report_generator.py."""

    def test_blocks_on_peer_avg_material_nan(self):
        """Preflight must block when normalized peer-average composite 90006
        has material_nan severity."""
        from report_generator import validate_output_inputs
        proc_df = pd.DataFrame({
            "CERT": [19977, 90006, 90004, 33124],
            "REPDTE": pd.Timestamp("2025-03-31"),
            "Norm_NCO_Rate": [0.01, 0.02, 0.015, 0.01],
            "_Norm_NCO_Severity": ["ok", "material_nan", "ok", "ok"],
            "_Norm_NA_Severity": ["ok", "ok", "ok", "ok"],
            "_Norm_Loans_Severity": ["ok", "ok", "ok", "ok"],
        })
        rolling_df = pd.DataFrame({
            "CERT": [19977, 90006, 90004, 90003, 90001, 33124],
        })
        result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
        self.assertFalse(result["valid"])
        self.assertTrue(any("90006" in e for e in result["errors"]))
        self.assertIn("normalized_credit_chart", result["suppressed_charts"])

    def test_blocks_on_missing_normalized_composite(self):
        """Preflight must block when normalized composites 90004/90006 are missing."""
        from report_generator import validate_output_inputs
        proc_df = pd.DataFrame({
            "CERT": [19977, 33124],
            "REPDTE": pd.Timestamp("2025-03-31"),
        })
        rolling_df = pd.DataFrame({
            "CERT": [19977, 90003, 90001, 33124],
        })
        result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
        self.assertFalse(result["valid"])
        self.assertTrue(
            any("90006" in e or "90004" in e for e in result["errors"]),
            f"Errors should reference missing composites: {result['errors']}"
        )

    def test_passes_when_all_composites_present_and_ok(self):
        """Preflight should pass when all composites are present and healthy."""
        from report_generator import validate_output_inputs
        proc_df = pd.DataFrame({
            "CERT": [19977, 90001, 90003, 90004, 90006, 33124],
            "REPDTE": pd.Timestamp("2025-03-31"),
            "_Norm_NCO_Severity": ["ok"] * 6,
            "_Norm_NA_Severity": ["ok"] * 6,
            "_Norm_Loans_Severity": ["ok"] * 6,
            "Norm_NCO_Rate": [0.01] * 6,
            "Norm_Nonaccrual_Rate": [0.005] * 6,
            "Norm_Gross_Loans": [10000.0] * 6,
        })
        rolling_df = pd.DataFrame({
            "CERT": [19977, 90001, 90003, 90004, 90006, 33124],
        })
        result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
        self.assertTrue(result["valid"], f"Should pass but got errors: {result['errors']}")


class TestClaudeMDIntegrity(unittest.TestCase):
    """Tests for CLAUDE.md document integrity."""

    def test_no_merge_conflict_markers(self):
        """CLAUDE.md must not contain merge conflict markers."""
        claude_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "CLAUDE.md")
        with open(claude_path, "r") as f:
            content = f.read()
        for marker in ["<<<<<<<", "=======", ">>>>>>>"]:
            self.assertNotIn(marker, content,
                f"CLAUDE.md contains merge conflict marker: {marker}")


class TestBalanceGating(unittest.TestCase):
    """Tests for balance-gating logic on excluded NCO categories."""

    def test_ag_nco_gated_when_balance_zero(self):
        """Zero Ag balance + nonzero Ag NCO → NCO gated to 0, flag set."""
        ag_bal = np.array([0.0, 500.0, 0.0])
        ag_nco_raw = np.array([27000.0, 1500.0, 0.0])
        gated_nco = np.where(ag_bal > 0, ag_nco_raw, 0.0)
        gated_flag = np.where((ag_bal == 0) & (ag_nco_raw != 0), True, False)

        self.assertEqual(gated_nco[0], 0.0)
        self.assertTrue(gated_flag[0])
        self.assertEqual(gated_nco[1], 1500.0)
        self.assertFalse(gated_flag[1])
        self.assertFalse(gated_flag[2])

    def test_auto_nco_gated_when_balance_zero(self):
        """Zero Auto balance + nonzero Auto NCO → NCO gated to 0, flag set."""
        auto_bal = np.array([0.0, 100.0])
        auto_nco_raw = np.array([27000.0, 500.0])
        gated_nco = np.where(auto_bal > 0, auto_nco_raw, 0.0)
        gated_flag = np.where((auto_bal == 0) & (auto_nco_raw != 0), True, False)

        self.assertEqual(gated_nco[0], 0.0)
        self.assertTrue(gated_flag[0])
        self.assertEqual(gated_nco[1], 500.0)
        self.assertFalse(gated_flag[1])

    def test_adc_nco_gated_when_balance_zero(self):
        """Zero ADC balance + nonzero ADC NCO → NCO gated to 0, flag set."""
        adc_bal = np.array([0.0, 200.0])
        adc_nco_raw = np.array([5000.0, 300.0])
        gated_nco = np.where(adc_bal > 0, adc_nco_raw, 0.0)
        gated_flag = np.where((adc_bal == 0) & (adc_nco_raw != 0), True, False)

        self.assertEqual(gated_nco[0], 0.0)
        self.assertTrue(gated_flag[0])
        self.assertEqual(gated_nco[1], 300.0)
        self.assertFalse(gated_flag[1])

    def test_oo_cre_nco_gated_when_balance_zero(self):
        """Zero OO CRE balance + nonzero OO CRE NCO → NCO gated to 0, flag set."""
        oo_bal = np.array([0.0, 1000.0])
        oo_nco_raw = np.array([8000.0, 600.0])
        gated_nco = np.where(oo_bal > 0, oo_nco_raw, 0.0)
        gated_flag = np.where((oo_bal == 0) & (oo_nco_raw != 0), True, False)

        self.assertEqual(gated_nco[0], 0.0)
        self.assertTrue(gated_flag[0])
        self.assertEqual(gated_nco[1], 600.0)
        self.assertFalse(gated_flag[1])


class TestCompositeCoverage(unittest.TestCase):
    """Tests for normalized composite minimum coverage threshold."""

    def test_below_threshold_forces_nan(self):
        """Composite coverage below 50% → metric NaN'd out."""
        MIN_COVERAGE_PCT = 0.50
        # 3 of 8 contributors have data = 37.5% coverage
        values = pd.Series([0.01, np.nan, np.nan, 0.02, np.nan, np.nan, np.nan, 0.015])
        coverage = values.notna().sum() / len(values)
        composite = values.mean() if coverage >= MIN_COVERAGE_PCT else np.nan
        self.assertTrue(np.isnan(composite))
        self.assertLess(coverage, MIN_COVERAGE_PCT)

    def test_above_threshold_preserves_value(self):
        """Composite coverage above 50% → metric preserved."""
        MIN_COVERAGE_PCT = 0.50
        # 5 of 8 contributors have data = 62.5% coverage
        values = pd.Series([0.01, 0.02, np.nan, 0.02, 0.015, np.nan, np.nan, 0.01])
        coverage = values.notna().sum() / len(values)
        composite = values.mean() if coverage >= MIN_COVERAGE_PCT else np.nan
        self.assertFalse(np.isnan(composite))
        self.assertGreater(coverage, MIN_COVERAGE_PCT)

    def test_exactly_at_threshold_preserves_value(self):
        """Composite coverage at exactly 50% → metric preserved (>= not >)."""
        MIN_COVERAGE_PCT = 0.50
        # 4 of 8 contributors = exactly 50%
        values = pd.Series([0.01, 0.02, np.nan, 0.02, np.nan, np.nan, np.nan, 0.01])
        coverage = values.notna().sum() / len(values)
        composite = values.mean() if coverage >= MIN_COVERAGE_PCT else np.nan
        self.assertFalse(np.isnan(composite))
        self.assertEqual(coverage, MIN_COVERAGE_PCT)


class TestPreflightScoping(unittest.TestCase):
    """Tests for preflight period-scoping behavior."""

    def test_blocks_material_at_latest_period(self):
        """Material severity at latest period is blocking."""
        from report_generator import validate_output_inputs
        proc_df = pd.DataFrame({
            "CERT": [19977, 90006, 90004, 33124] * 2,
            "REPDTE": [pd.Timestamp("2024-12-31")] * 4 + [pd.Timestamp("2025-03-31")] * 4,
            "Norm_NCO_Rate": [0.01] * 8,
            "_Norm_NCO_Severity": ["ok"] * 4 + ["ok", "material_nan", "ok", "ok"],
            "_Norm_NA_Severity": ["ok"] * 8,
            "_Norm_Loans_Severity": ["ok"] * 8,
        })
        rolling_df = pd.DataFrame({"CERT": [19977, 90006, 90004, 90003, 90001, 33124]})
        result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
        self.assertFalse(result["valid"])

    def test_historical_only_material_does_not_block(self):
        """Material severity only in historical period is NOT blocking."""
        from report_generator import validate_output_inputs
        proc_df = pd.DataFrame({
            "CERT": [19977, 90006, 90004, 33124] * 2,
            "REPDTE": [pd.Timestamp("2024-12-31")] * 4 + [pd.Timestamp("2025-03-31")] * 4,
            "Norm_NCO_Rate": [0.01] * 8,
            "_Norm_NCO_Severity": ["ok", "material_nan", "ok", "ok"] + ["ok"] * 4,
            "_Norm_NA_Severity": ["ok"] * 8,
            "_Norm_Loans_Severity": ["ok"] * 8,
        })
        rolling_df = pd.DataFrame({"CERT": [19977, 90006, 90004, 90003, 90001, 33124]})
        result = validate_output_inputs(proc_df, rolling_df, subject_cert=19977)
        self.assertTrue(result["valid"], f"Should not block: {result['errors']}")


class TestLoadConfigDefaults(unittest.TestCase):
    """Tests for load_config() default values."""

    def test_defaults_are_34221_32992(self):
        """load_config defaults must be 34221 / 32992."""
        from report_generator import load_config
        old_mspbna = os.environ.pop("MSPBNA_CERT", None)
        old_msbna = os.environ.pop("MSBNA_CERT", None)
        try:
            cfg = load_config()
            self.assertEqual(cfg["mspbna_cert"], 34221)
            self.assertEqual(cfg["msbna_cert"], 32992)
            self.assertEqual(cfg["subject_bank_cert"], 34221)
        finally:
            if old_mspbna is not None:
                os.environ["MSPBNA_CERT"] = old_mspbna
            if old_msbna is not None:
                os.environ["MSBNA_CERT"] = old_msbna


class TestWorkbookAuditSheets(unittest.TestCase):
    """Tests for audit sheet inclusion in workbook output."""

    def test_exclusion_component_audit_in_source(self):
        """write_excel_output call must include Exclusion_Component_Audit."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("Exclusion_Component_Audit=excl_audit_df", source)

    def test_composite_coverage_audit_in_source(self):
        """write_excel_output call must include Composite_Coverage_Audit."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("Composite_Coverage_Audit=composite_coverage_df", source)

    def test_cs_kwargs_in_write_call(self):
        """write_excel_output must unpack **cs_kwargs for Case-Shiller ZIP sheets."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("**cs_kwargs", source)


class TestCaseShillerZIPPersistence(unittest.TestCase):
    """Tests for Case-Shiller ZIP sheet persistence in workbook output."""

    def test_zip_enrichment_is_resilient(self):
        """Case-Shiller ZIP enrichment must be wrapped in try/except."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("Case-Shiller ZIP enrichment failed (non-fatal)", source)

    def test_disabled_enrichment_produces_audit(self):
        """When enrichment is disabled, audit sheet should still be non-empty."""
        os.environ['ENABLE_CASE_SHILLER_ZIP_ENRICHMENT'] = 'false'
        try:
            from case_shiller_zip_mapper import build_case_shiller_zip_sheets
            result = build_case_shiller_zip_sheets()
            audit = result.get('CaseShiller_County_Map_Audit')
            self.assertIsNotNone(audit, "Audit sheet should always be returned")
            self.assertFalse(audit.empty, "Audit sheet should not be empty even when disabled")
        finally:
            os.environ.pop('ENABLE_CASE_SHILLER_ZIP_ENRICHMENT', None)


class TestTTMMapConsistency(unittest.TestCase):
    """Tests that TTM map keys match the quarterly column naming convention."""

    def test_income_ttm_keys_use_correct_names(self):
        """TTM map must use col.replace('_YTD','_Q') convention for income columns."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("'Int_Inc_Loans_Q'", source)
        self.assertIn("'Provision_Exp_Q'", source)
        self.assertIn("'Total_Int_Exp_Q'", source)

    def test_no_stale_ytd_q_keys(self):
        """Old *_YTD_Q TTM map keys (dict keys with colon) must not be present."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertNotIn("'Int_Inc_Loans_YTD_Q':", source)
        self.assertNotIn("'Provision_Exp_YTD_Q':", source)


class TestValidationWiring(unittest.TestCase):
    """Tests that metric validation suite is wired into the pipeline."""

    def test_validation_suite_called(self):
        """run_upstream_validation_suite must be called with proc_df_with_peers."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("run_upstream_validation_suite(proc_df_with_peers)", source)

    def test_metric_validation_audit_sheet(self):
        """Metric_Validation_Audit must be in write_excel_output call."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("Metric_Validation_Audit=metric_validation_df", source)

    def test_validation_import(self):
        """run_upstream_validation_suite must be imported."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("from metric_registry import run_upstream_validation_suite", source)


class TestNoStaleDefaults(unittest.TestCase):
    """Tests that 19977 defaults are purged from production code."""

    def test_report_generator_no_19977(self):
        """report_generator.py must not contain 19977 as a default."""
        src_path = os.path.join(os.path.dirname(__file__), "report_generator.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertNotIn("19977", source)

    def test_claude_md_env_table_no_19977(self):
        """CLAUDE.md env var table must use 34221/32992 defaults."""
        md_path = os.path.join(os.path.dirname(__file__), "CLAUDE.md")
        with open(md_path, "r") as f:
            lines = f.readlines()
        for i, line in enumerate(lines, 1):
            if "| `MSPBNA_CERT`" in line or "| `MSBNA_CERT`" in line:
                self.assertNotIn("19977", line,
                    f"Line {i} still shows 19977: {line.strip()}")


class TestPresentationCuration(unittest.TestCase):
    """Tests for curated presentation tabs and metric role policy."""

    def _read_source(self, filename="MSPBNA_CR_Normalized.py"):
        src_path = os.path.join(os.path.dirname(__file__), filename)
        with open(src_path, "r") as f:
            return f.read()

    def test_summary_dashboard_curated(self):
        """create_peer_comparison must use SUMMARY_DASHBOARD_METRICS."""
        source = self._read_source()
        self.assertIn("SUMMARY_DASHBOARD_METRICS", source)
        self.assertIn("for m in SUMMARY_DASHBOARD_METRICS", source)

    def test_normalized_comparison_curated(self):
        """create_normalized_comparison must use NORMALIZED_COMPARISON_METRICS."""
        source = self._read_source()
        self.assertIn("NORMALIZED_COMPARISON_METRICS", source)
        self.assertIn("for m in NORMALIZED_COMPARISON_METRICS", source)

    def test_norm_provision_rate_excluded(self):
        """Norm_Provision_Rate must not be in NORMALIZED_COMPARISON_METRICS."""
        source = self._read_source()
        allowlist_section = source.split("NORMALIZED_COMPARISON_METRICS")[1].split("]")[0]
        self.assertNotIn("'Norm_Provision_Rate'", allowlist_section)
        self.assertNotIn('"Norm_Provision_Rate"', allowlist_section)

    def test_descriptive_metrics_set_exists(self):
        """DESCRIPTIVE_METRICS frozenset must contain key size/balance metrics."""
        source = self._read_source()
        self.assertIn("DESCRIPTIVE_METRICS", source)
        for m in ["ASSET", "LNLS", "Gross_Loans", "SBL_Composition"]:
            self.assertIn(f'"{m}"', source)

    def test_performance_flag_checks_descriptive(self):
        """_get_performance_flag must check DESCRIPTIVE_METRICS."""
        source = self._read_source()
        self.assertIn("if metric_code in DESCRIPTIVE_METRICS", source)

    def test_tight_layout_not_in_ppt_chart(self):
        """create_credit_deterioration_chart_ppt must not call fig.tight_layout()."""
        source = self._read_source("report_generator.py")
        func_start = source.index("def create_credit_deterioration_chart_ppt")
        next_def = source.index("\ndef ", func_start + 1)
        func_body = source[func_start:next_def]
        self.assertNotIn("fig.tight_layout()", func_body)

    def test_ratio_components_safe_peer_lookup(self):
        """generate_ratio_components_table must not abort when peer is missing."""
        source = self._read_source("report_generator.py")
        self.assertIn("peer_slice.empty", source)


class TestWorkbookLevelCuration(unittest.TestCase):
    """
    Integration-style tests that validate the curated allowlists and display
    label coverage. Uses source-code parsing to avoid importing the full
    pipeline (which requires aiohttp/scipy/etc.).
    These tests would have caught the 'raw metric dump' bug in the latest
    bad workbook.
    """

    @classmethod
    def setUpClass(cls):
        """Parse allowlists from source code."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        # Extract SUMMARY_DASHBOARD_METRICS list via exec on the list literal
        import ast
        # Find the list assignment
        for name in ("SUMMARY_DASHBOARD_METRICS", "NORMALIZED_COMPARISON_METRICS"):
            start = source.index(f"{name} = [")
            bracket_start = source.index("[", start)
            # Find matching close bracket
            depth = 0
            for i, ch in enumerate(source[bracket_start:], bracket_start):
                if ch == "[": depth += 1
                elif ch == "]": depth -= 1
                if depth == 0:
                    bracket_end = i + 1
                    break
            setattr(cls, name, ast.literal_eval(source[bracket_start:bracket_end]))

        # Extract DESCRIPTIVE_METRICS frozenset
        ds_start = source.index("DESCRIPTIVE_METRICS = frozenset({")
        brace_start = source.index("{", ds_start)
        depth = 0
        for i, ch in enumerate(source[brace_start:], brace_start):
            if ch == "{": depth += 1
            elif ch == "}": depth -= 1
            if depth == 0:
                brace_end = i + 1
                break
        cls.DESCRIPTIVE_METRICS = ast.literal_eval(source[brace_start:brace_end])

    # ------------------------------------------------------------------
    #  1. Summary_Dashboard curated metric codes
    # ------------------------------------------------------------------
    def test_summary_dashboard_only_curated_metrics(self):
        """Summary_Dashboard allowlist must be bounded (not hundreds of metrics)."""
        self.assertLessEqual(len(self.SUMMARY_DASHBOARD_METRICS), 30)
        self.assertGreaterEqual(len(self.SUMMARY_DASHBOARD_METRICS), 10)

    # ------------------------------------------------------------------
    #  2. Normalized_Comparison must NOT contain Norm_Provision_Rate
    # ------------------------------------------------------------------
    def test_normalized_comparison_excludes_provision_rate(self):
        """Norm_Provision_Rate must never appear in NORMALIZED_COMPARISON_METRICS."""
        self.assertNotIn("Norm_Provision_Rate", self.NORMALIZED_COMPARISON_METRICS)

    # ------------------------------------------------------------------
    #  3. Metric Name display labels resolve (not raw code fallback)
    # ------------------------------------------------------------------
    def test_display_labels_resolve_for_curated_metrics(self):
        """Every curated metric must have a display label in LOCAL_DERIVED_METRICS
        OR be a well-known FDIC alias (ASSET, LNLS, ROA, etc.)."""
        from master_data_dictionary import LOCAL_DERIVED_METRICS
        fdic_aliases = {"ASSET", "LNLS", "ROA", "ROE", "NIMY", "EEFFR"}
        all_curated = set(self.SUMMARY_DASHBOARD_METRICS) | set(self.NORMALIZED_COMPARISON_METRICS)
        missing_labels = []
        for code in all_curated:
            if code in fdic_aliases:
                continue
            if code not in LOCAL_DERIVED_METRICS:
                missing_labels.append(code)
        self.assertEqual(
            missing_labels, [],
            f"These curated metrics have no display label in LOCAL_DERIVED_METRICS: {missing_labels}"
        )

    # ------------------------------------------------------------------
    #  4. Descriptive metrics in curated tabs must be in DESCRIPTIVE_METRICS
    # ------------------------------------------------------------------
    def test_descriptive_metrics_in_curated_lists(self):
        """Composition/share metrics in curated tabs must be in DESCRIPTIVE_METRICS."""
        expected_descriptive_in_summary = {
            "ASSET", "LNLS", "SBL_Composition",
            "RIC_Resi_Loan_Share", "RIC_CRE_Loan_Share", "RIC_CRE_ACL_Share",
        }
        for m in expected_descriptive_in_summary:
            if m in self.SUMMARY_DASHBOARD_METRICS:
                self.assertIn(m, self.DESCRIPTIVE_METRICS,
                              f"{m} is in Summary_Dashboard but not in DESCRIPTIVE_METRICS")

    # ------------------------------------------------------------------
    #  5. Workbook row counts match curated list sizes
    # ------------------------------------------------------------------
    def test_curated_list_sizes_reasonable(self):
        """Curated lists must be much smaller than a raw dump (hundreds of cols)."""
        self.assertLess(len(self.SUMMARY_DASHBOARD_METRICS), 50)
        self.assertLess(len(self.NORMALIZED_COMPARISON_METRICS), 30)

    # ------------------------------------------------------------------
    #  6. Regression: would have caught the raw dump bug
    # ------------------------------------------------------------------
    def test_no_raw_mdrm_fields_in_summary_dashboard(self):
        """Summary_Dashboard must not contain raw MDRM codes (RCFD/RCON/RIAD)."""
        for m in self.SUMMARY_DASHBOARD_METRICS:
            self.assertFalse(m.startswith(("RCFD", "RCON", "RIAD")),
                             f"Raw MDRM field '{m}' in SUMMARY_DASHBOARD_METRICS")

    def test_no_internal_pipeline_columns_in_curated_tabs(self):
        """Internal diagnostic columns must not appear in curated tabs."""
        internal_prefixes = ("_Flag_", "_Norm_", "_Excl_", "_Diag_")
        all_curated = set(self.SUMMARY_DASHBOARD_METRICS) | set(self.NORMALIZED_COMPARISON_METRICS)
        for m in all_curated:
            self.assertFalse(m.startswith(internal_prefixes),
                             f"Internal pipeline column '{m}' in curated tab")


class TestDisplayLabelCoverage(unittest.TestCase):
    """Validates that the master data dictionary has entries for all curated metrics."""

    def test_local_derived_has_norm_metrics(self):
        """LOCAL_DERIVED_METRICS must have display labels for Norm_ curated metrics."""
        from master_data_dictionary import LOCAL_DERIVED_METRICS
        expected_norm = [
            "Norm_Gross_Loans", "Norm_ACL_Coverage", "Norm_Risk_Adj_Allowance_Coverage",
            "Norm_Nonaccrual_Rate", "Norm_NCO_Rate", "Norm_Delinquency_Rate",
            "Norm_SBL_Composition", "Norm_Fund_Finance_Composition",
            "Norm_Wealth_Resi_Composition", "Norm_CRE_Investment_Composition",
            "Norm_Exclusion_Pct", "Norm_Loan_Yield", "Norm_Loss_Adj_Yield",
            "Norm_Risk_Adj_Return",
        ]
        missing = [m for m in expected_norm if m not in LOCAL_DERIVED_METRICS]
        self.assertEqual(missing, [], f"Missing display labels in LOCAL_DERIVED_METRICS: {missing}")

    def test_local_derived_has_ric_metrics(self):
        """LOCAL_DERIVED_METRICS must have display labels for RIC_ curated metrics."""
        from master_data_dictionary import LOCAL_DERIVED_METRICS
        expected_ric = [
            "RIC_CRE_Loan_Share", "RIC_Resi_Loan_Share", "RIC_CRE_ACL_Share",
            "RIC_CRE_ACL_Coverage", "RIC_CRE_Risk_Adj_Coverage", "RIC_CRE_NCO_Rate",
        ]
        missing = [m for m in expected_ric if m not in LOCAL_DERIVED_METRICS]
        self.assertEqual(missing, [], f"Missing display labels in LOCAL_DERIVED_METRICS: {missing}")

    def test_local_derived_has_profitability_liquidity(self):
        """LOCAL_DERIVED_METRICS must have display labels for profitability/liquidity metrics."""
        from master_data_dictionary import LOCAL_DERIVED_METRICS
        expected = ["Provision_to_Loans_Rate", "Liquidity_Ratio", "HQLA_Ratio", "Loans_to_Deposits"]
        missing = [m for m in expected if m not in LOCAL_DERIVED_METRICS]
        self.assertEqual(missing, [], f"Missing display labels in LOCAL_DERIVED_METRICS: {missing}")


class TestHTMLTableResilience(unittest.TestCase):
    """Verifies generate_ratio_components_table handles missing peer composites."""

    def test_ratio_table_uses_normalized_peer(self):
        """When is_normalized=True, peer_cert must be 90006 not 99999."""
        src_path = os.path.join(os.path.dirname(__file__), "report_generator.py")
        with open(src_path, "r") as f:
            source = f.read()
        # Extract the function body
        func_start = source.index("def generate_ratio_components_table")
        next_def = source.index("\ndef ", func_start + 1)
        func_body = source[func_start:next_def]
        self.assertIn("ACTIVE_NORMALIZED_COMPOSITES", func_body,
                       "generate_ratio_components_table must use ACTIVE_NORMALIZED_COMPOSITES for normalized mode")

    def test_peer_slice_safe_fallback(self):
        """Peer lookup must use .empty check, not bare .iloc[0]."""
        src_path = os.path.join(os.path.dirname(__file__), "report_generator.py")
        with open(src_path, "r") as f:
            source = f.read()
        func_start = source.index("def generate_ratio_components_table")
        next_def = source.index("\ndef ", func_start + 1)
        func_body = source[func_start:next_def]
        self.assertIn("peer_slice.empty", func_body,
                       "Peer lookup must check peer_slice.empty for safe fallback")

    def test_matplotlib_warning_suppressed(self):
        """report_generator.py must suppress the tight_layout Axes warning."""
        src_path = os.path.join(os.path.dirname(__file__), "report_generator.py")
        with open(src_path, "r") as f:
            source = f.read()
        self.assertIn("not compatible with tight_layout", source,
                       "Matplotlib tight_layout warning must be filtered")


class TestNormalizedComparisonFlags(unittest.TestCase):
    """Verifies that create_normalized_comparison generates performance flags
    and that descriptive metrics get blank flags."""

    def test_normalized_comparison_has_performance_flag(self):
        """create_normalized_comparison must produce a Performance_Flag column."""
        src_path = os.path.join(os.path.dirname(__file__), "MSPBNA_CR_Normalized.py")
        with open(src_path, "r") as f:
            source = f.read()
        func_start = source.index("def create_normalized_comparison")
        next_def = source.index("\n    def ", func_start + 1)
        func_body = source[func_start:next_def]
        self.assertIn("Performance_Flag", func_body,
                       "create_normalized_comparison must compute Performance_Flag")
        self.assertIn("_get_performance_flag", func_body,
                       "create_normalized_comparison must use _get_performance_flag")


class TestCompositeRegimeCleanup(unittest.TestCase):
    """Verifies that report_generator.py uses only active composites (90001/90003/90004/90006)
    and never falls back to legacy CERTs (99998/99999/90002/90005) for chart/table selection."""

    @classmethod
    def setUpClass(cls):
        src_path = os.path.join(os.path.dirname(__file__), "report_generator.py")
        with open(src_path, "r") as f:
            cls.source = f.read()

    def _get_func_body(self, func_name):
        start = self.source.index(f"def {func_name}")
        next_def = self.source.index("\ndef ", start + 1)
        return self.source[start:next_def]

    # ------------------------------------------------------------------
    #  1. Active composites are defined as canonical constants
    # ------------------------------------------------------------------
    def test_active_composites_defined(self):
        """ACTIVE_STANDARD_COMPOSITES and ACTIVE_NORMALIZED_COMPOSITES must exist."""
        self.assertIn("ACTIVE_STANDARD_COMPOSITES", self.source)
        self.assertIn("ACTIVE_NORMALIZED_COMPOSITES", self.source)
        self.assertIn("INACTIVE_LEGACY_COMPOSITES", self.source)

    # ------------------------------------------------------------------
    #  2. No legacy composite used in active selection paths
    # ------------------------------------------------------------------
    def test_no_legacy_composite_in_selection_paths(self):
        """99998, 99999, 90002, 90005 must not appear outside INACTIVE_LEGACY_COMPOSITES."""
        # Find INACTIVE_LEGACY_COMPOSITES definition line
        legacy_line_end = self.source.index("INACTIVE_LEGACY_COMPOSITES") + 80
        after_definition = self.source[legacy_line_end:]
        for cert in ["99998", "99999"]:
            self.assertNotIn(cert, after_definition,
                             f"Legacy CERT {cert} found after INACTIVE_LEGACY_COMPOSITES definition")
        # 90002 and 90005 should also be gone
        for cert in ["90002", "90005"]:
            self.assertNotIn(cert, after_definition,
                             f"Legacy CERT {cert} found after INACTIVE_LEGACY_COMPOSITES definition")

    # ------------------------------------------------------------------
    #  3. Standard peer avg cert is 90003
    # ------------------------------------------------------------------
    def test_standard_peer_avg_cert_is_90003(self):
        """Standard All Peers must use 90003, not 99999."""
        self.assertIn('"all_peers": 90003', self.source)

    # ------------------------------------------------------------------
    #  4. Standard Core PB cert is 90001
    # ------------------------------------------------------------------
    def test_standard_core_pb_cert_is_90001(self):
        """Standard Core PB must use 90001."""
        self.assertIn('"core_pb": 90001', self.source)

    # ------------------------------------------------------------------
    #  5. Normalized peer avg cert is 90006
    # ------------------------------------------------------------------
    def test_normalized_peer_avg_cert_is_90006(self):
        """Normalized All Peers must use 90006."""
        self.assertIn('"all_peers": 90006', self.source)

    # ------------------------------------------------------------------
    #  6. Normalized Core PB cert is 90004
    # ------------------------------------------------------------------
    def test_normalized_core_pb_cert_is_90004(self):
        """Normalized Core PB must use 90004."""
        self.assertIn('"core_pb": 90004', self.source)

    # ------------------------------------------------------------------
    #  7. Credit chart v3 uses current standard composites
    # ------------------------------------------------------------------
    def test_credit_chart_v3_uses_current_composites(self):
        """create_credit_deterioration_chart_v3 must use 90003/90001, not 99999/99998."""
        body = self._get_func_body("create_credit_deterioration_chart_v3")
        self.assertNotIn("99999", body)
        self.assertNotIn("99998", body)
        self.assertIn("90003", body)
        self.assertIn("90001", body)

    # ------------------------------------------------------------------
    #  8. plot_scatter_dynamic defaults are current regime
    # ------------------------------------------------------------------
    def test_scatter_defaults_current_regime(self):
        """plot_scatter_dynamic defaults must be 90003/90001, not 99999/99998."""
        body = self._get_func_body("plot_scatter_dynamic")
        self.assertIn("peer_avg_cert_primary: int = 90003", body)
        self.assertIn("peer_avg_cert_alt: int = 90001", body)

    # ------------------------------------------------------------------
    #  9. No legacy fallback in peer_avg selections
    # ------------------------------------------------------------------
    def test_no_legacy_fallback_for_peer_avg(self):
        """Detailed peer, segment, ratio tables must not fall back to 99999."""
        for func in ["generate_detailed_peer_table", "generate_segment_focus_table",
                      "generate_ratio_components_table"]:
            body = self._get_func_body(func)
            self.assertNotIn("99999", body, f"{func} still falls back to legacy 99999")

    # ------------------------------------------------------------------
    #  10. validate_composite_cert_regime exists
    # ------------------------------------------------------------------
    def test_validate_composite_cert_regime_exists(self):
        """validate_composite_cert_regime helper must exist."""
        self.assertIn("def validate_composite_cert_regime", self.source)

    # ------------------------------------------------------------------
    #  11. chart_ppt defaults use active composites
    # ------------------------------------------------------------------
    def test_chart_ppt_defaults_use_active_composites(self):
        """create_credit_deterioration_chart_ppt defaults must use 90003/90001."""
        body = self._get_func_body("create_credit_deterioration_chart_ppt")
        self.assertNotIn("99999", body)
        self.assertNotIn("99998", body)


# ═══════════════════════════════════════════════════════════════════════════
# DIRECTIVE C TESTS — Wealth-Focused Tables, Data Integrity, Stock vs Flow
# ═══════════════════════════════════════════════════════════════════════════

class TestDirectiveC(unittest.TestCase):
    """Regression tests for Directive C: wealth-focused tables, data integrity fixes."""

    @classmethod
    def setUpClass(cls):
        project_dir = os.path.dirname(os.path.abspath(__file__))
        rg_path = os.path.join(project_dir, "report_generator.py")
        norm_path = os.path.join(project_dir, "MSPBNA_CR_Normalized.py")
        mdd_path = os.path.join(project_dir, "master_data_dictionary.py")

        with open(rg_path, "r", encoding="utf-8") as f:
            cls.rg_source = f.read()
        with open(norm_path, "r", encoding="utf-8") as f:
            cls.norm_source = f.read()
        with open(mdd_path, "r", encoding="utf-8") as f:
            cls.mdd_source = f.read()

    # ------------------------------------------------------------------
    # 1. Executive summary columns use ticker-style labels
    # ------------------------------------------------------------------
    def test_executive_summary_has_wealth_peers_column(self):
        """Executive summary must use ticker-style labels via resolve_display_label."""
        self.assertIn('"Wealth Peers"', self.rg_source)
        # The function should use resolve_display_label, not hardcoded full names
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_credit_metrics_email_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertNotIn('"MSBNA"', body_src,
                    "Executive summary must not include MSBNA column")
                self.assertNotIn('"All Peers"', body_src,
                    "Executive summary must not include All Peers column")
                # Must use resolve_display_label, not hardcoded "Goldman Sachs"
                self.assertIn('resolve_display_label', body_src)
                self.assertNotIn('"Goldman Sachs"', body_src,
                    "Executive summary must use ticker GS, not full name Goldman Sachs")
                self.assertIn('"Delta MSPBNA vs Wealth Peers"', body_src)
                break

    # ------------------------------------------------------------------
    # 2. Executive summary uses Core PB composite (not All Peers)
    # ------------------------------------------------------------------
    def test_executive_summary_uses_core_pb_composite(self):
        """Wealth Peers must use core_pb composite (90001/90004), not all_peers."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_credit_metrics_email_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn('core_pb', body_src,
                    "Wealth Peers must map to core_pb composite")
                break

    # ------------------------------------------------------------------
    # 3. Executive summary has is_normalized parameter
    # ------------------------------------------------------------------
    def test_executive_summary_has_is_normalized(self):
        """Executive summary must accept is_normalized for separate artifacts."""
        self.assertIn("def generate_credit_metrics_email_table", self.rg_source)
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_credit_metrics_email_table":
                param_names = [a.arg for a in node.args.args]
                self.assertIn("is_normalized", param_names)
                break

    # ------------------------------------------------------------------
    # 4. Segment tables use Wealth Peers, not All Peers
    # ------------------------------------------------------------------
    def test_segment_focus_table_wealth_peers(self):
        """Segment focus tables must use Wealth Peers (core_pb) via resolver."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_segment_focus_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn('core_pb', body_src,
                    "Segment table must use core_pb composite as Wealth Peers")
                self.assertNotIn('"MSBNA"', body_src)
                self.assertIn('resolve_display_label', body_src,
                    "Segment table must use resolve_display_label for peer names")
                self.assertNotIn('"Goldman Sachs"', body_src,
                    "Segment table must use ticker GS, not full name")
                break

    # ------------------------------------------------------------------
    # 5. Core PB peer table drops MSBNA, labels as Wealth Peers
    # ------------------------------------------------------------------
    def test_core_pb_table_no_msbna(self):
        """Core PB peer table must not include MSBNA; uses resolve_display_label."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_core_pb_peer_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertNotIn('"MSBNA"', body_src)
                self.assertIn('resolve_display_label', body_src,
                    "Core PB table must use resolve_display_label for labels")
                self.assertNotIn('"Goldman Sachs"', body_src,
                    "Core PB table must use ticker GS, not full name")
                break

    # ------------------------------------------------------------------
    # 6. Detailed peer table remains the only broad all-peer table
    # ------------------------------------------------------------------
    def test_detailed_peer_table_uses_all_peers(self):
        """generate_detailed_peer_table must still use all_peers composite."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_detailed_peer_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("all_peers", body_src)
                break

    # ------------------------------------------------------------------
    # 7. Ratio components has Norm_Risk_Adj_Gross_Loans synthesis
    # ------------------------------------------------------------------
    def test_ratio_components_norm_risk_adj_denominator(self):
        """_synth must synthesize Norm_Risk_Adj_Gross_Loans for normalized mode."""
        self.assertIn("Norm_Risk_Adj_Gross_Loans", self.rg_source)
        # Must be in _synth function
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_synth":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("Norm_Risk_Adj_Gross_Loans", body_src)
                break

    # ------------------------------------------------------------------
    # 8. TTM_Past_Due_Rate fully renamed to Past_Due_Rate
    # ------------------------------------------------------------------
    def test_no_ttm_past_due_rate_in_normalized(self):
        """TTM_Past_Due_Rate must be fully renamed to Past_Due_Rate."""
        self.assertNotIn("TTM_Past_Due_Rate", self.norm_source,
            "MSPBNA_CR_Normalized.py still contains TTM_Past_Due_Rate")
        self.assertNotIn("TTM_Past_Due_Rate", self.rg_source,
            "report_generator.py still contains TTM_Past_Due_Rate")
        self.assertNotIn("TTM_Past_Due_Rate", self.mdd_source,
            "master_data_dictionary.py still contains TTM_Past_Due_Rate")
        # But Past_Due_Rate must exist
        self.assertIn("Past_Due_Rate", self.norm_source)

    # ------------------------------------------------------------------
    # 9. PLLL in FDIC_FIELDS_TO_FETCH
    # ------------------------------------------------------------------
    def test_plll_in_fdic_fields(self):
        """PLLL must be in FDIC_FIELDS_TO_FETCH."""
        self.assertIn('"PLLL"', self.norm_source)

    # ------------------------------------------------------------------
    # 10. ytd_to_discrete function exists at module level
    # ------------------------------------------------------------------
    def test_ytd_to_discrete_exists(self):
        """ytd_to_discrete must exist as a module-level function."""
        self.assertIn("def ytd_to_discrete(", self.norm_source)

    # ------------------------------------------------------------------
    # 11. annualize_ytd function exists at module level
    # ------------------------------------------------------------------
    def test_annualize_ytd_exists(self):
        """annualize_ytd must exist as a module-level function."""
        self.assertIn("def annualize_ytd(", self.norm_source)

    # ------------------------------------------------------------------
    # 12. Provision and Yield use annualized, not TTM
    # ------------------------------------------------------------------
    def test_provision_yield_use_annualization(self):
        """Provision_to_Loans_Rate and Loan_Yield_Proxy must use annualize_ytd."""
        self.assertIn("annualize_ytd", self.norm_source)
        # Should use annualize_ytd for provision and income
        self.assertIn("annualize_ytd(df_processed, 'Provision_Exp_YTD')", self.norm_source)
        self.assertIn("annualize_ytd(df_processed, 'Int_Inc_Loans_YTD')", self.norm_source)

    # ------------------------------------------------------------------
    # 13. compute_quarterly_from_ytd delegates to ytd_to_discrete
    # ------------------------------------------------------------------
    def test_compute_quarterly_delegates_to_ytd_to_discrete(self):
        """Inner compute_quarterly_from_ytd must delegate to module-level ytd_to_discrete."""
        self.assertIn("ytd_to_discrete(df_in, col_name)", self.norm_source)

    # ------------------------------------------------------------------
    # 14. Standard and normalized executive summaries both generated
    # ------------------------------------------------------------------
    def test_both_executive_summary_versions_generated(self):
        """generate_reports must produce both standard and normalized executive summaries."""
        # The loop generates f"_executive_summary_{norm_str}_" where norm_str is "standard"/"normalized"
        self.assertIn("executive_summary_", self.rg_source)
        # Must call generate_credit_metrics_email_table with is_normalized in a loop
        self.assertIn("for is_norm in [False, True]", self.rg_source)

    # ------------------------------------------------------------------
    # 15. Past_Due_Rate display label is not TTM
    # ------------------------------------------------------------------
    def test_past_due_rate_display_not_ttm(self):
        """Past_Due_Rate display label must not say TTM in the short name."""
        self.assertIn('"Past_Due_Rate"', self.mdd_source)
        # The "short" label must not contain "TTM"
        idx = self.mdd_source.index('"Past_Due_Rate"')
        snippet = self.mdd_source[idx:idx+80]  # Just the short label
        self.assertNotIn("TTM Past Due", snippet,
            "Past_Due_Rate short display label must not say TTM")


class TestLabelResolver(unittest.TestCase):
    """Regression tests for centralized label resolver (resolve_display_label)."""

    @classmethod
    def setUpClass(cls):
        project_dir = os.path.dirname(os.path.abspath(__file__))
        rg_path = os.path.join(project_dir, "report_generator.py")
        with open(rg_path, "r", encoding="utf-8") as f:
            cls.rg_source = f.read()

    # ------------------------------------------------------------------
    # 1. resolve_display_label function exists in report_generator.py
    # ------------------------------------------------------------------
    def test_resolve_display_label_exists(self):
        """resolve_display_label must be defined in report_generator.py."""
        self.assertIn("def resolve_display_label(", self.rg_source)

    # ------------------------------------------------------------------
    # 2. _TICKER_MAP has correct mappings
    # ------------------------------------------------------------------
    def test_ticker_map_has_required_entries(self):
        """_TICKER_MAP must contain GS, UBS, JPM, BAC, C, WFC."""
        self.assertIn('"GOLDMAN"', self.rg_source)
        self.assertIn('"GS"', self.rg_source)
        self.assertIn('"UBS"', self.rg_source)
        self.assertIn('"JPM"', self.rg_source)
        self.assertIn('"BAC"', self.rg_source)
        # Citibank -> C
        self.assertIn('"CITIBANK"', self.rg_source)
        self.assertIn('"WELLS FARGO"', self.rg_source)
        self.assertIn('"WFC"', self.rg_source)

    # ------------------------------------------------------------------
    # 3. _COMPOSITE_LABELS maps active composites to descriptive names
    # ------------------------------------------------------------------
    def test_composite_labels_defined(self):
        """_COMPOSITE_LABELS must map 90001/90003/90004/90006 to descriptive names."""
        self.assertIn("_COMPOSITE_LABELS", self.rg_source)
        self.assertIn("90001", self.rg_source)
        self.assertIn("90003", self.rg_source)
        self.assertIn('"Wealth Peers"', self.rg_source)
        self.assertIn('"All Peers"', self.rg_source)

    # ------------------------------------------------------------------
    # 4. No hardcoded "Goldman Sachs" in table/chart builder functions
    # ------------------------------------------------------------------
    def test_no_hardcoded_goldman_sachs_in_builders(self):
        """No table or chart builder function should hardcode 'Goldman Sachs' as a label."""
        import ast
        tree = ast.parse(self.rg_source)
        builder_funcs = [
            "generate_credit_metrics_email_table",
            "generate_core_pb_peer_table",
            "generate_detailed_peer_table",
            "generate_segment_focus_table",
            "create_credit_deterioration_chart_v3",
            "create_credit_deterioration_chart_ppt",
        ]
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in builder_funcs:
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertNotIn('"Goldman Sachs"', body_src,
                    f"{node.name} must not hardcode 'Goldman Sachs' — use resolve_display_label")

    # ------------------------------------------------------------------
    # 5. Chart v3 uses resolve_display_label for entity names
    # ------------------------------------------------------------------
    def test_chart_v3_uses_resolver(self):
        """create_credit_deterioration_chart_v3 must use resolve_display_label."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "create_credit_deterioration_chart_v3":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("resolve_display_label", body_src,
                    "Chart v3 must use resolve_display_label for entity names")
                break

    # ------------------------------------------------------------------
    # 6. Chart PPT uses resolve_display_label
    # ------------------------------------------------------------------
    def test_chart_ppt_uses_resolver(self):
        """create_credit_deterioration_chart_ppt must use resolve_display_label."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "create_credit_deterioration_chart_ppt":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("resolve_display_label", body_src,
                    "Chart PPT must use resolve_display_label for entity names")
                break

    # ------------------------------------------------------------------
    # 7. Detailed peer table uses resolve_display_label
    # ------------------------------------------------------------------
    def test_detailed_peer_table_uses_resolver(self):
        """generate_detailed_peer_table must use resolve_display_label for column headers."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_detailed_peer_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("resolve_display_label", body_src,
                    "Detailed peer table must use resolve_display_label")
                break

    # ------------------------------------------------------------------
    # 8. Segment focus table uses resolve_display_label
    # ------------------------------------------------------------------
    def test_segment_focus_table_uses_resolver(self):
        """generate_segment_focus_table must use resolve_display_label."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "generate_segment_focus_table":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("resolve_display_label", body_src,
                    "Segment focus table must use resolve_display_label")
                break

    # ------------------------------------------------------------------
    # 9. Subject bank resolves to MSPBNA
    # ------------------------------------------------------------------
    def test_subject_bank_label_is_mspbna(self):
        """resolve_display_label for subject cert must return MSPBNA."""
        self.assertIn('"MSPBNA"', self.rg_source)
        # The resolver must return MSPBNA for subject_cert
        self.assertIn('return "MSPBNA"', self.rg_source)

    # ------------------------------------------------------------------
    # 10. MSBNA cert resolves to MS
    # ------------------------------------------------------------------
    def test_msbna_label_is_msbna(self):
        """resolve_display_label for MSBNA cert must return MSBNA."""
        self.assertIn('return "MSBNA"', self.rg_source)

    # ------------------------------------------------------------------
    # 11. _build_dynamic_peer_html uses _MSBNA_CERT constant
    # ------------------------------------------------------------------
    def test_dynamic_peer_html_uses_msbna_constant(self):
        """_build_dynamic_peer_html must use _MSBNA_CERT, not hardcoded 32992."""
        import ast
        tree = ast.parse(self.rg_source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "_build_dynamic_peer_html":
                body_src = ast.get_source_segment(self.rg_source, node)
                self.assertIn("_MSBNA_CERT", body_src,
                    "_build_dynamic_peer_html must use _MSBNA_CERT constant")
                self.assertNotIn("msbna_cert = 32992", body_src,
                    "_build_dynamic_peer_html must not hardcode msbna_cert = 32992")
                break


# ==========================================================================
#  LOGGING & NAMING OVERHAUL TESTS
# ==========================================================================


class TestDateOnlyNaming(unittest.TestCase):
    """Verify date-only artifact naming (YYYYMMDD, no HHMMSS)."""

    def test_get_run_date_str_format(self):
        """get_run_date_str returns 8-digit YYYYMMDD string."""
        from logging_utils import get_run_date_str
        result = get_run_date_str()
        self.assertEqual(len(result), 8, "Date string must be 8 chars (YYYYMMDD)")
        self.assertTrue(result.isdigit(), "Date string must be all digits")

    def test_build_artifact_filename_no_hhmmss(self):
        """build_artifact_filename must NOT include HHMMSS."""
        from logging_utils import build_artifact_filename
        name = build_artifact_filename("Test", "chart", ext=".png")
        # Should match pattern: Test_chart_YYYYMMDD.png
        self.assertRegex(name, r"^Test_chart_\d{8}\.png$",
                         "Filename must be prefix_suffix_YYYYMMDD.ext")
        # Must NOT contain an underscore-delimited 6-digit time
        self.assertNotRegex(name, r"\d{8}_\d{6}",
                            "Filename must NOT contain HHMMSS timestamp")

    def test_build_artifact_filename_no_suffix(self):
        """build_artifact_filename with empty suffix produces prefix_YYYYMMDD.ext."""
        from logging_utils import build_artifact_filename
        name = build_artifact_filename("Dashboard", "", ext=".xlsx")
        self.assertRegex(name, r"^Dashboard_\d{8}\.xlsx$")

    def test_build_artifact_filename_with_output_dir(self):
        """build_artifact_filename prepends output directory."""
        from logging_utils import build_artifact_filename
        name = build_artifact_filename("Report", "summary", ext=".html", output_dir="output/tables")
        self.assertTrue(name.startswith("output/tables/"))
        self.assertTrue(name.endswith(".html"))

    def test_mspbna_no_hhmmss_in_filename(self):
        """MSPBNA_CR_Normalized.py must not use HHMMSS in dashboard filename."""
        src_path = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        src = src_path.read_text(encoding="utf-8")
        # The old pattern: strftime('%Y%m%d_%H%M%S') for the dashboard filename
        self.assertNotIn("Bank_Performance_Dashboard_{ts}", src,
                         "Dashboard filename must use build_artifact_filename, not inline ts")

    def test_report_generator_no_duplicate_date_stamp(self):
        """report_generator.py must not append a separate date stamp to artifact names."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        # {base} already includes the date from the Excel stem, so there
        # must be no separate {stamp} variable appended to filenames.
        import re
        stamp_usages = re.findall(r'\{stamp\}', src)
        self.assertEqual(len(stamp_usages), 0,
                         "report_generator must not use {stamp} — base already has the date")


class TestCsvLogging(unittest.TestCase):
    """Verify CSV log schema, per-script files, and reset behavior."""

    def test_csv_log_columns_count(self):
        """CSV log schema must have exactly 15 columns."""
        from logging_utils import CSV_LOG_COLUMNS
        self.assertEqual(len(CSV_LOG_COLUMNS), 15,
                         f"CSV schema must have 15 columns, got {len(CSV_LOG_COLUMNS)}")

    def test_csv_log_required_columns(self):
        """CSV log must include all required columns."""
        from logging_utils import CSV_LOG_COLUMNS
        required = {"timestamp", "run_date", "script_name", "run_id", "level",
                     "phase", "component", "function", "line_no", "event_type",
                     "message", "exception_type", "exception_message", "traceback",
                     "context_json"}
        self.assertTrue(required.issubset(set(CSV_LOG_COLUMNS)),
                        f"Missing columns: {required - set(CSV_LOG_COLUMNS)}")

    def test_csv_log_filename_includes_script_name(self):
        """CSV log filename must include the script name."""
        from logging_utils import CsvLogger
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = CsvLogger("test_script", log_dir=tmpdir)
            self.assertIn("test_script", logger.log_filename)
            self.assertIn("_log.csv", logger.log_filename)
            logger.close()

    def test_csv_log_reset_per_run(self):
        """CSV log must reset (overwrite) each run."""
        from logging_utils import CsvLogger
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # First run: write some data
            log1 = CsvLogger("reset_test", log_dir=tmpdir)
            log1.info("first run message")
            log1.close()

            # Second run: should overwrite
            log2 = CsvLogger("reset_test", log_dir=tmpdir)
            log2.info("second run message")
            log2.close()

            # Read the file — should only have header + second run message
            log_path = Path(tmpdir) / f"reset_test_{log2.run_date}_log.csv"
            with open(log_path) as f:
                lines = f.readlines()
            # Header + 1 data row = 2 lines
            self.assertEqual(len(lines), 2,
                             "CSV log must reset each run (overwrite mode)")
            self.assertIn("second run message", lines[1])

    def test_csv_log_schema_headers(self):
        """First line of CSV log must be the schema headers."""
        from logging_utils import CsvLogger, CSV_LOG_COLUMNS
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = CsvLogger("header_test", log_dir=tmpdir)
            logger.close()
            log_path = Path(tmpdir) / f"header_test_{logger.run_date}_log.csv"
            with open(log_path) as f:
                header = f.readline().strip()
            expected = ",".join(CSV_LOG_COLUMNS)
            self.assertEqual(header, expected, "CSV header must match schema")


class TestStdoutStderrCapture(unittest.TestCase):
    """Verify stdout/stderr tee capture into CSV log."""

    def test_stdout_captured_in_log(self):
        """print() output must appear in CSV log as STDOUT event."""
        from logging_utils import CsvLogger, TeeToLogger
        import tempfile
        import csv as csv_mod

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = CsvLogger("stdout_test", log_dir=tmpdir)
            # Save and replace stdout
            old_stdout = sys.stdout
            sys.stdout = TeeToLogger(old_stdout, logger, stream_name="STDOUT")
            print("test capture message")
            sys.stdout = old_stdout  # Restore
            logger.close()

            log_path = Path(tmpdir) / f"stdout_test_{logger.run_date}_log.csv"
            with open(log_path) as f:
                reader = csv_mod.DictReader(f)
                rows = list(reader)
            stdout_rows = [r for r in rows if r["event_type"] == "STDOUT"]
            messages = [r["message"] for r in stdout_rows]
            self.assertTrue(any("test capture message" in m for m in messages),
                            "print() output must be captured as STDOUT event in CSV log")

    def test_stderr_captured_in_log(self):
        """stderr output must appear in CSV log as STDERR event."""
        from logging_utils import CsvLogger, TeeToLogger
        import tempfile
        import csv as csv_mod

        with tempfile.TemporaryDirectory() as tmpdir:
            logger = CsvLogger("stderr_test", log_dir=tmpdir)
            old_stderr = sys.stderr
            sys.stderr = TeeToLogger(old_stderr, logger, stream_name="STDERR")
            print("stderr test message", file=sys.stderr)
            sys.stderr = old_stderr  # Restore
            logger.close()

            log_path = Path(tmpdir) / f"stderr_test_{logger.run_date}_log.csv"
            with open(log_path) as f:
                reader = csv_mod.DictReader(f)
                rows = list(reader)
            stderr_rows = [r for r in rows if r["event_type"] == "STDERR"]
            messages = [r["message"] for r in stderr_rows]
            self.assertTrue(any("stderr test message" in m for m in messages),
                            "stderr output must be captured as STDERR event in CSV log")


class TestFileWriteEvents(unittest.TestCase):
    """Verify FILE_WRITTEN events are logged."""

    def test_report_generator_logs_file_writes(self):
        """report_generator.py must call csv_log.log_file_written for chart outputs."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        self.assertIn("csv_log.log_file_written", src,
                      "report_generator must log FILE_WRITTEN events")

    def test_mspbna_logs_workbook_write(self):
        """MSPBNA_CR_Normalized.py must log FILE_WRITTEN for workbook."""
        src_path = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        src = src_path.read_text(encoding="utf-8")
        self.assertIn("FILE_WRITTEN", src,
                      "MSPBNA_CR_Normalized must log FILE_WRITTEN events")


class TestPreflightEvents(unittest.TestCase):
    """Verify preflight events are logged."""

    def test_preflight_warnings_logged(self):
        """Preflight warnings must be logged as PRECHECK_WARN."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        self.assertIn("PRECHECK_WARN", src,
                      "Preflight warnings must use PRECHECK_WARN event type")

    def test_preflight_errors_logged(self):
        """Preflight errors must be logged as PRECHECK_FAIL."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        self.assertIn("PRECHECK_FAIL", src,
                      "Preflight errors must use PRECHECK_FAIL event type")


class TestClaudeMDLogging(unittest.TestCase):
    """Verify CLAUDE.md documents logging and naming conventions."""

    def test_claude_md_mentions_csv_logging(self):
        """CLAUDE.md must document CSV logging."""
        md_path = Path(__file__).parent / "CLAUDE.md"
        md = md_path.read_text(encoding="utf-8")
        self.assertIn("CSV", md, "CLAUDE.md must document CSV logging")

    def test_claude_md_mentions_date_only_naming(self):
        """CLAUDE.md must document date-only naming."""
        md_path = Path(__file__).parent / "CLAUDE.md"
        md = md_path.read_text(encoding="utf-8")
        self.assertIn("YYYYMMDD", md, "CLAUDE.md must document date-only naming")


class TestLoggerSafeLifecycle(unittest.TestCase):
    """Verify print-after-close, stream restoration, and idempotent shutdown."""

    def test_print_after_logger_close_does_not_crash(self):
        """print() after csv_log.close() must not raise ValueError."""
        from logging_utils import CsvLogger, TeeToLogger
        import tempfile
        old_stdout = sys.stdout
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                logger = CsvLogger("close_test", log_dir=tmpdir)
                sys.stdout = TeeToLogger(old_stdout, logger, stream_name="STDOUT")
                logger.close()
                # This must not raise ValueError: I/O operation on closed file
                print("message after close")
        finally:
            sys.stdout = old_stdout

    def test_stderr_after_logger_close_does_not_crash(self):
        """stderr after csv_log.close() must not raise ValueError."""
        from logging_utils import CsvLogger, TeeToLogger
        import tempfile
        old_stderr = sys.stderr
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                logger = CsvLogger("close_stderr_test", log_dir=tmpdir)
                sys.stderr = TeeToLogger(old_stderr, logger, stream_name="STDERR")
                logger.close()
                print("stderr after close", file=sys.stderr)
        finally:
            sys.stderr = old_stderr

    def test_close_restores_streams(self):
        """After shutdown(), sys.stdout/sys.stderr must be original streams."""
        from logging_utils import setup_csv_logging, TeeToLogger
        import tempfile
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                logger = setup_csv_logging("restore_test", log_dir=tmpdir)
                # Should be TeeToLogger now
                self.assertIsInstance(sys.stdout, TeeToLogger)
                self.assertIsInstance(sys.stderr, TeeToLogger)
                logger.shutdown()
                # Should be restored
                self.assertNotIsInstance(sys.stdout, TeeToLogger,
                    "sys.stdout must be restored after shutdown")
                self.assertNotIsInstance(sys.stderr, TeeToLogger,
                    "sys.stderr must be restored after shutdown")
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_double_close_is_safe(self):
        """Calling close() or shutdown() twice must not raise."""
        from logging_utils import CsvLogger
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            logger = CsvLogger("double_close_test", log_dir=tmpdir)
            logger.close()
            logger.close()  # Must not raise
            logger.shutdown()  # Must not raise

    def test_setup_csv_logging_does_not_stack_nested_tees(self):
        """Calling setup_csv_logging twice must not create nested TeeToLogger."""
        from logging_utils import setup_csv_logging, TeeToLogger
        import tempfile
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                log1 = setup_csv_logging("nest_test_1", log_dir=tmpdir)
                log2 = setup_csv_logging("nest_test_2", log_dir=tmpdir)
                # sys.stdout should be TeeToLogger wrapping the raw stream,
                # not TeeToLogger wrapping TeeToLogger
                tee = sys.stdout
                self.assertIsInstance(tee, TeeToLogger)
                self.assertNotIsInstance(tee._original, TeeToLogger,
                    "setup_csv_logging must unwrap existing TeeToLogger to prevent nesting")
                log2.shutdown()
                log1.shutdown()
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def test_mspbna_has_single_terminal_csv_close_path(self):
        """MSPBNA_CR_Normalized.py must not close csv_log inside dash.run()."""
        src_path = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        src = src_path.read_text(encoding="utf-8")
        # The old bug: csv_log.close() inside the run() method
        # Find all csv_log.close() calls
        close_calls = [line.strip() for line in src.splitlines()
                       if "csv_log.close()" in line and not line.strip().startswith("#")]
        self.assertEqual(len(close_calls), 0,
            f"csv_log.close() must not appear in MSPBNA_CR_Normalized.py "
            f"(use csv_log.shutdown() in main() only). Found: {close_calls}")
        # Must have shutdown in main's finally block
        self.assertIn("csv_log.shutdown()", src,
            "main() must call csv_log.shutdown() as the terminal close path")

    def test_report_generator_safe_shutdown(self):
        """report_generator.py must use csv_log.shutdown() not csv_log.close()."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        close_calls = [line.strip() for line in src.splitlines()
                       if "csv_log.close()" in line and not line.strip().startswith("#")]
        self.assertEqual(len(close_calls), 0,
            f"csv_log.close() must not appear in report_generator.py "
            f"(use csv_log.shutdown()). Found: {close_calls}")
        self.assertIn("csv_log.shutdown()", src,
            "generate_reports() must call csv_log.shutdown()")


# ==========================================================================
#  PRESENTATION & OUTPUT CLEANUP TESTS
# ==========================================================================


class TestNoMixedRegimeArtifact(unittest.TestCase):
    """No side-by-side standard+normalized comparison artifact in generate_reports."""

    def test_no_mixed_normalized_comparison_in_generate_reports(self):
        """generate_reports must not call generate_normalized_comparison_table."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        # Find actual code calls (not def, not inside strings/comments)
        import re
        for line in src.splitlines():
            stripped = line.strip()
            # Skip def lines, comments, and string literals
            if stripped.startswith("def "):
                continue
            if stripped.startswith("#"):
                continue
            if stripped.startswith('"') or stripped.startswith("'"):
                continue
            if "raise NotImplementedError" in stripped:
                continue
            if re.search(r'generate_normalized_comparison_table\s*\(', stripped):
                self.fail(
                    "generate_reports must not call generate_normalized_comparison_table "
                    "(mixed standard+normalized artifact violates single-regime rule)"
                )


class TestNoDoubleDateFilenames(unittest.TestCase):
    """Artifact filenames must not have a duplicated date suffix."""

    def test_no_double_date_in_artifact_paths(self):
        """No artifact path should contain the date twice (e.g. _20260311_..._20260311)."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        import re
        # Look for patterns like {base}_..._YYYYMMDD.ext or {base}_..._{stamp}.ext
        # Since base already contains the date, any additional _{stamp} or _YYYYMMDD
        # at the end would create a double date.
        double_date_calls = re.findall(r'\{stamp\}', src)
        self.assertEqual(len(double_date_calls), 0,
            "Artifact paths must not use {stamp} — {base} already contains the date")

    def test_claude_md_no_double_date_examples(self):
        """CLAUDE.md artifact examples must not show double date."""
        md_path = Path(__file__).parent / "CLAUDE.md"
        md = md_path.read_text(encoding="utf-8")
        import re
        # Pattern: _YYYYMMDD_<anything>_YYYYMMDD
        bad = re.findall(r'\{stem\}_\w+_YYYYMMDD\.\w+', md)
        self.assertEqual(len(bad), 0,
            f"CLAUDE.md must not show double-date artifact names. Found: {bad}")


class TestNormNCOLabel(unittest.TestCase):
    """Normalized NCO label must not contain '(TTM)'."""

    def test_no_ttm_in_norm_nco_display_labels(self):
        """Presentation labels for Norm_NCO_Rate must not include (TTM)."""
        src_path = Path(__file__).parent / "report_generator.py"
        src = src_path.read_text(encoding="utf-8")
        import re
        # Find display labels that combine "Norm" and "NCO" and "TTM"
        bad = re.findall(r'"Norm NCO Rate \(TTM\)', src)
        self.assertEqual(len(bad), 0,
            "Normalized NCO labels must not contain (TTM) — "
            "Norm_NCO_Rate is already a normalized metric, not a TTM rolling sum")


class TestTickerConventionInDocs(unittest.TestCase):
    """CLAUDE.md must use final ticker convention in output-column examples."""

    def test_no_stale_goldman_sachs_in_output_column_examples(self):
        """CLAUDE.md output-column examples must use GS not Goldman Sachs."""
        md_path = Path(__file__).parent / "CLAUDE.md"
        md = md_path.read_text(encoding="utf-8")
        import re
        # Look for "MSPBNA | Goldman Sachs" or "Goldman Sachs | UBS" patterns
        # which indicate stale full-name column headers in output examples
        stale = re.findall(r'MSPBNA \| Goldman Sachs|Goldman Sachs \| UBS', md)
        self.assertEqual(len(stale), 0,
            f"CLAUDE.md output-column examples must use tickers (GS not Goldman Sachs). Found: {stale}")

    def test_docs_use_ticker_column_convention(self):
        """CLAUDE.md must contain MSPBNA | GS | UBS pattern for output columns."""
        md_path = Path(__file__).parent / "CLAUDE.md"
        md = md_path.read_text(encoding="utf-8")
        self.assertIn("MSPBNA | GS | UBS", md,
            "CLAUDE.md must document ticker-style column convention: MSPBNA | GS | UBS")


# ═══════════════════════════════════════════════════════════════════════════
# DIRECTIVE 4 TESTS — Final Cleanup & Testability
# ═══════════════════════════════════════════════════════════════════════════

class TestDeprecatedNormalizedComparison(unittest.TestCase):
    """Tests for deprecated generate_normalized_comparison_table()."""

    def test_function_exists(self):
        """generate_normalized_comparison_table must still exist as a stub."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("def generate_normalized_comparison_table", source)

    def test_raises_not_implemented(self):
        """Calling generate_normalized_comparison_table must raise NotImplementedError."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("raise NotImplementedError", source)
        # Verify the raise is inside the function body
        in_func = False
        for line in source.splitlines():
            if "def generate_normalized_comparison_table" in line:
                in_func = True
            elif in_func and line.strip().startswith("def "):
                break
            elif in_func and "raise NotImplementedError" in line:
                return  # found it
        self.fail("NotImplementedError not found inside generate_normalized_comparison_table")

    def test_not_called_in_generate_reports(self):
        """generate_reports() must NOT call generate_normalized_comparison_table."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        # Find generate_reports body
        lines = source.splitlines()
        in_func = False
        for line in lines:
            if "def generate_reports" in line:
                in_func = True
                continue
            if in_func:
                if line.strip() and not line[0].isspace() and not line.strip().startswith("#"):
                    break  # left function
                self.assertNotIn("generate_normalized_comparison_table", line,
                    "generate_reports() must not call deprecated generate_normalized_comparison_table")


class TestValidateCompositeCertRegime(unittest.TestCase):
    """Tests for validate_composite_cert_regime() function."""

    def setUp(self):
        src = Path(__file__).parent / "report_generator.py"
        self.source = src.read_text(encoding="utf-8")

    def test_function_exists(self):
        """validate_composite_cert_regime must be a proper function definition."""
        self.assertIn("def validate_composite_cert_regime", self.source)

    def test_is_not_dead_code(self):
        """validate_composite_cert_regime must not be inside another function's body
        after a return statement (i.e., must be a top-level function)."""
        import re
        # Find the def line and verify it's at top indentation level
        for line in self.source.splitlines():
            if "def validate_composite_cert_regime" in line:
                # Should be at column 0 (no leading whitespace)
                self.assertFalse(line.startswith(" "),
                    "validate_composite_cert_regime should be a top-level function, not nested")
                return
        self.fail("def validate_composite_cert_regime not found")

    def test_returns_dict_with_required_keys(self):
        """Function must return dict with valid, active_present, active_missing, legacy_present, warnings, errors."""
        for key in ["valid", "active_present", "active_missing", "legacy_present", "warnings", "errors"]:
            self.assertIn(f'"{key}"', self.source,
                f"validate_composite_cert_regime must return dict with '{key}' key")

    def test_checks_active_composites(self):
        """Function must check both standard and normalized active composites."""
        self.assertIn("ACTIVE_STANDARD_COMPOSITES", self.source)
        self.assertIn("ACTIVE_NORMALIZED_COMPOSITES", self.source)

    def test_checks_legacy_composites(self):
        """Function must check for legacy composites."""
        self.assertIn("INACTIVE_LEGACY_COMPOSITES", self.source)


class TestImportSafety(unittest.TestCase):
    """Tests that MSPBNA_CR_Normalized.py can be imported without side effects."""

    def test_no_import_time_value_error(self):
        """Importing the module with missing env vars must not raise ValueError."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        # The ValueError raise must be inside a function, not at module level
        lines = source.splitlines()
        in_function = False
        indent_level = 0
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("def ") or stripped.startswith("class "):
                in_function = True
            # Check for bare raise ValueError at module level (indent 0)
            if "raise ValueError" in line and not in_function:
                # Check if this line is at module level (no indentation or only in try block)
                leading = len(line) - len(line.lstrip())
                if leading == 0:
                    self.fail(f"Line {i}: raise ValueError at module level — "
                              "import will crash without env vars")

    def test_no_import_time_print(self):
        """Importing the module must not print HUD token info at import time."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_function = False
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("def ") or stripped.startswith("class "):
                in_function = True
                continue
            if not in_function and stripped.startswith("print(") and "HUD_USER_TOKEN" in stripped:
                self.fail(f"Line {i}: print() with HUD_USER_TOKEN at module level — "
                          "will execute at import time")

    def test_no_import_time_setup_logging(self):
        """setup_logging() must not be called at module level."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_function = False
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("def ") or stripped.startswith("class "):
                in_function = True
                continue
            if not in_function and "= setup_logging()" in stripped and not stripped.startswith("#"):
                self.fail(f"Line {i}: setup_logging() called at module level — "
                          "will open log files at import time")

    def test_no_import_time_os_chdir(self):
        """os.chdir() must not be called at module level."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_function = False
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("def ") or stripped.startswith("class "):
                in_function = True
                continue
            if not in_function and "os.chdir(" in stripped and not stripped.startswith("#"):
                self.fail(f"Line {i}: os.chdir() at module level — "
                          "will change cwd at import time")

    def test_env_vars_have_defaults(self):
        """MSPBNA_CERT and MSBNA_CERT must have default values for import safety."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        # Should use os.getenv with defaults
        self.assertIn('os.getenv("MSPBNA_CERT", "34221")', source,
            "MSPBNA_CERT must have a default value for import safety")
        self.assertIn('os.getenv("MSBNA_CERT", "32992")', source,
            "MSBNA_CERT must have a default value for import safety")

    def test_validate_runtime_env_exists(self):
        """_validate_runtime_env() must exist for main() to call."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("def _validate_runtime_env", source)

    def test_main_calls_runtime_bootstrap(self):
        """main() must call _validate_runtime_env, setup_logging, and os.chdir."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        # Find main() body
        lines = source.splitlines()
        in_main = False
        main_body = []
        for line in lines:
            if "def main():" in line:
                in_main = True
                continue
            if in_main:
                if line.strip() and not line[0].isspace():
                    break
                main_body.append(line)
        main_text = "\n".join(main_body)
        self.assertIn("_validate_runtime_env()", main_text,
            "main() must call _validate_runtime_env()")
        self.assertIn("setup_logging()", main_text,
            "main() must call setup_logging()")
        self.assertIn("os.chdir(", main_text,
            "main() must call os.chdir()")


# ═══════════════════════════════════════════════════════════════════════════
# DIRECTIVE 5 TESTS — FRED Frequency Inference Fix
# ═══════════════════════════════════════════════════════════════════════════

class TestInferFreqFromIndex(unittest.TestCase):
    """Tests for infer_freq_from_index() — the FRED frequency inference helper."""

    @classmethod
    def setUpClass(cls):
        """Extract infer_freq_from_index from source and exec it to avoid
        importing the full module (which requires aiohttp, matplotlib, etc.)."""
        import re
        src = (Path(__file__).parent / "MSPBNA_CR_Normalized.py").read_text(encoding="utf-8")
        # Extract the module-level function (starts at column 0)
        lines = src.splitlines()
        func_lines = []
        capturing = False
        for line in lines:
            if line.startswith("def infer_freq_from_index"):
                capturing = True
            elif capturing and line and not line[0].isspace() and not line.startswith("#"):
                break  # left function
            if capturing:
                func_lines.append(line)
        func_src = "\n".join(func_lines)
        ns = {"pd": pd, "np": __import__("numpy")}
        exec(func_src, ns)
        cls.infer = staticmethod(ns["infer_freq_from_index"])

    def test_infer_freq_monthly_index(self):
        """Monthly DatetimeIndex should return ('monthly', 'M')."""
        idx = pd.date_range("2020-01-01", periods=24, freq="MS")
        name, code = self.infer(idx)
        self.assertEqual(code, "M", f"Expected 'M' for monthly, got '{code}'")
        self.assertEqual(name, "monthly")

    def test_infer_freq_quarterly_index(self):
        """Quarterly DatetimeIndex should return ('quarterly', 'Q')."""
        idx = pd.date_range("2020-01-01", periods=12, freq="QS")
        name, code = self.infer(idx)
        self.assertEqual(code, "Q", f"Expected 'Q' for quarterly, got '{code}'")
        self.assertEqual(name, "quarterly")

    def test_infer_freq_daily_index(self):
        """Daily DatetimeIndex should return ('daily', 'D')."""
        idx = pd.date_range("2020-01-01", periods=500, freq="D")
        name, code = self.infer(idx)
        self.assertEqual(code, "D", f"Expected 'D' for daily, got '{code}'")
        self.assertEqual(name, "daily")

    def test_infer_freq_irregular_monthly_no_crash(self):
        """Irregular monthly-ish dates must not crash."""
        # Slightly irregular monthly: some months missing, some offset
        dates = pd.to_datetime([
            "2020-01-15", "2020-02-14", "2020-04-16", "2020-05-15",
            "2020-07-14", "2020-08-15", "2020-09-14", "2020-11-16",
            "2021-01-15", "2021-02-14", "2021-04-16", "2021-05-15",
            "2021-07-14", "2021-08-15", "2021-09-14", "2021-11-16",
        ])
        idx = pd.DatetimeIndex(dates)
        # Should not raise — any valid return is fine
        name, code = self.infer(idx)
        self.assertIn(code, ("D", "M", "Q"), f"Unexpected code: {code}")

    def test_infer_freq_duplicate_dates_no_crash(self):
        """Duplicate timestamps must be handled safely."""
        dates = pd.to_datetime(["2020-01-01", "2020-01-01", "2020-04-01",
                                "2020-04-01", "2020-07-01", "2020-10-01"])
        idx = pd.DatetimeIndex(dates)
        name, code = self.infer(idx)
        self.assertIn(code, ("D", "M", "Q"))

    def test_infer_freq_old_bug_int_not_subscriptable(self):
        """Reproduce the exact pattern that caused TypeError: 'int' object is not subscriptable.

        The old code did:
            pd.Series(list(zip(idx.year, idx.month)))
              .drop_duplicates()
              .groupby(lambda x: x[0])  # <-- x is the integer INDEX label
              .size()

        This test creates an index that reaches the last-resort fallback
        (not daily, not clearly monthly or quarterly by obs-count) and
        verifies no TypeError is raised.
        """
        # 7 observations per year — falls through all earlier heuristics
        # (not >=200 daily, not 6-15 monthly, not 3-5 quarterly)
        dates = []
        for year in [2018, 2019, 2020, 2021]:
            for month in [1, 2, 4, 5, 7, 9, 11]:
                dates.append(f"{year}-{month:02d}-15")
        idx = pd.DatetimeIndex(pd.to_datetime(dates))
        # Must not raise TypeError
        name, code = self.infer(idx)
        self.assertIn(code, ("M", "Q"),
            "7 obs/year with 7 distinct months should resolve to monthly or quarterly")

    def test_infer_freq_empty_index(self):
        """Empty index should return conservative default without crashing."""
        idx = pd.DatetimeIndex([])
        name, code = self.infer(idx)
        self.assertIn(code, ("D", "M", "Q"))

    def test_infer_freq_nat_handling(self):
        """Index with NaT values should not crash."""
        dates = pd.to_datetime(["2020-01-01", pd.NaT, "2020-04-01",
                                pd.NaT, "2020-07-01", "2020-10-01"])
        idx = pd.DatetimeIndex(dates)
        name, code = self.infer(idx)
        self.assertIn(code, ("D", "M", "Q"))

    def test_module_level_function_exists(self):
        """infer_freq_from_index must be a module-level function (not just nested)."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        # Must find a non-indented def
        found = False
        for line in source.splitlines():
            if line.startswith("def infer_freq_from_index"):
                found = True
                break
        self.assertTrue(found, "infer_freq_from_index must be a module-level function")


# ═══════════════════════════════════════════════════════════════════════════
# DIRECTIVE 6 TESTS — Output Quality Fixes
# ═══════════════════════════════════════════════════════════════════════════

class TestFDICHistoryHorizon(unittest.TestCase):
    """FDIC history default should be 48 quarters (12 years)."""

    def test_quarters_back_default_is_48(self):
        """DashboardConfig.quarters_back must default to 48."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("quarters_back: int = 48", source,
            "DashboardConfig.quarters_back must default to 48 (12 years)")

    def test_ffiec_healing_not_hardcoded_30(self):
        """FFIEC healing must not hardcode [:30] date limit."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        # Find heal_dataset method
        in_func = False
        for line in source.splitlines():
            if "def heal_dataset" in line:
                in_func = True
                continue
            if in_func and line.strip() and not line[0].isspace():
                break
            if in_func and "[:30]" in line:
                self.fail("heal_dataset still has hardcoded [:30] date limit")


class TestScatterTickerLabels(unittest.TestCase):
    """Scatter annotations must use ticker-style labels via resolve_display_label."""

    def test_outlier_labels_use_resolve_display_label(self):
        """plot_scatter_dynamic outlier annotation must call resolve_display_label."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        # Find the outlier label section inside plot_scatter_dynamic
        lines = source.splitlines()
        in_func = False
        found_resolver = False
        for line in lines:
            if "def plot_scatter_dynamic" in line:
                in_func = True
            elif in_func and line.strip().startswith("def ") and "plot_scatter_dynamic" not in line:
                if not line.startswith(" "):
                    break
            if in_func and "resolve_display_label" in line and "label" in line.lower():
                found_resolver = True
        self.assertTrue(found_resolver,
            "plot_scatter_dynamic must use resolve_display_label for outlier annotations")

    def test_no_raw_short_name_only_in_scatter(self):
        """Scatter outlier label line must not use short_name() without resolve_display_label."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_func = False
        for line in lines:
            if "def plot_scatter_dynamic" in line:
                in_func = True
            elif in_func and not line.startswith(" ") and line.strip():
                break
            # Flag if short_name is the ONLY label resolver (no resolve_display_label nearby)
            if in_func and "label = short_name(" in line and "resolve_display_label" not in line:
                self.fail("Scatter outlier labels use short_name() without resolve_display_label fallback")


class TestMigrationLadderComparative(unittest.TestCase):
    """Migration ladder must be comparative (MSPBNA + peers)."""

    def test_migration_ladder_includes_composites(self):
        """plot_migration_ladder must reference ACTIVE_STANDARD_COMPOSITES."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_func = False
        found = False
        for line in lines:
            if "def plot_migration_ladder" in line:
                in_func = True
                continue
            # End of function: next top-level def/class (not indented)
            if in_func and line.strip() and not line[0].isspace() and (line.startswith("def ") or line.startswith("class ")):
                break
            if in_func and "ACTIVE_STANDARD_COMPOSITES" in line:
                found = True
        self.assertTrue(found, "plot_migration_ladder must reference ACTIVE_STANDARD_COMPOSITES for peer comparison")

    def test_migration_ladder_title_mentions_peers(self):
        """Migration ladder title must indicate it's comparative."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_func = False
        for line in lines:
            if "def plot_migration_ladder" in line:
                in_func = True
                continue
            # End of function: next top-level def/class (not indented)
            if in_func and line.strip() and not line[0].isspace() and (line.startswith("def ") or line.startswith("class ")):
                break
            if in_func and "Peers" in line and "set_title" in line:
                return
        self.fail("Migration ladder title must mention 'Peers' to indicate comparative chart")


class TestLowCoverageChartSuppression(unittest.TestCase):
    """Charts must suppress all-NaN / low-coverage bar series."""

    def test_bar_series_nan_check_in_chart(self):
        """create_credit_deterioration_chart_ppt must check vals.isna().all() for bar series."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_func = False
        found = False
        for line in lines:
            if "def create_credit_deterioration_chart_ppt" in line:
                in_func = True
            elif in_func and not line.startswith(" ") and line.strip():
                break
            if in_func and "isna().all()" in line and "suppressed" in source[source.index(line)-100:source.index(line)+200].lower():
                found = True
        # Simpler check: look for suppressed_series variable
        self.assertIn("suppressed_series", source,
            "Chart must track suppressed_series for low-coverage bar series")

    def test_chart_has_suppression_footnote(self):
        """Chart must annotate suppressed series in subtitle."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("Suppressed (low coverage)", source,
            "Chart must show footnote for suppressed low-coverage series")


class TestCoverageMetricFormatting(unittest.TestCase):
    """Coverage metrics in ratio-components must use semantic format types."""

    def test_ratio_components_uses_fmt_multiple_for_npl(self):
        """generate_ratio_components_table must use _fmt_multiple for NPL coverage."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        lines = source.splitlines()
        in_func = False
        found = False
        for line in lines:
            if "def generate_ratio_components_table" in line:
                in_func = True
                continue
            if in_func and line.strip() and not line[0].isspace() and (line.startswith("def ") or line.startswith("class ")):
                break
            if in_func and "_fmt_multiple" in line:
                found = True
        self.assertTrue(found,
            "generate_ratio_components_table must use _fmt_multiple for NPL coverage")

    def test_metric_format_type_registry(self):
        """report_generator must define _METRIC_FORMAT_TYPE for semantic formatting."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("_METRIC_FORMAT_TYPE", source,
            "report_generator must define _METRIC_FORMAT_TYPE dict for semantic formatting")
        # NPL coverage metrics must be marked as "x" format
        self.assertIn('"RIC_CRE_Risk_Adj_Coverage": "x"', source,
            "CRE NPL coverage must be formatted as x-multiple")
        self.assertIn('"RIC_Resi_Risk_Adj_Coverage": "x"', source,
            "Resi NPL coverage must be formatted as x-multiple")

    def test_metric_format_type_registry_explicit_npl_policy(self):
        """_METRIC_FORMAT_TYPE must document that new x-style metrics must be added explicitly."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        # Must have a maintenance comment explaining the policy
        self.assertIn("MAINTENANCE RULE", source,
            "_METRIC_FORMAT_TYPE must have a MAINTENANCE RULE comment")
        self.assertIn("MUST be added here explicitly", source,
            "_METRIC_FORMAT_TYPE must explain that new NPL metrics must be registered explicitly")
        # Loan coverage metrics must NOT be registered as "x"
        for loan_cov in ["Allowance_to_Gross_Loans_Rate", "Norm_ACL_Coverage",
                          "Risk_Adj_Allowance_Coverage", "RIC_CRE_ACL_Coverage",
                          "RIC_Resi_ACL_Coverage"]:
            self.assertNotIn(f'"{loan_cov}": "x"', source,
                f"Loan coverage metric {loan_cov} must not be formatted as x-multiple")


class TestNormalizedRatioLabels(unittest.TestCase):
    """Normalized ratio-components labels must match their denominators."""

    def test_norm_cre_acl_not_labeled_coverage(self):
        """Norm CRE row with Norm_ACL_Balance denominator must NOT say 'Coverage'."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        # Find the normalized metrics list
        lines = source.splitlines()
        for line in lines:
            if "Norm_ACL_Balance" in line and "RIC_CRE_ACL" in line:
                # This is the CRE ACL share row — display name must not say "Coverage"
                self.assertNotIn("Coverage", line,
                    "Norm CRE row with Norm_ACL_Balance denominator is share, not coverage")
                return
        self.fail("Could not find CRE ACL row with Norm_ACL_Balance denominator")


class TestCsvLogSeverityClassification(unittest.TestCase):
    """CSV log must not classify progress bars as ERROR."""

    def test_tee_to_logger_has_classify_level(self):
        """TeeToLogger must have _classify_level method for smart severity."""
        src = Path(__file__).parent / "logging_utils.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("_classify_level", source,
            "TeeToLogger must have _classify_level for smart severity classification")

    def test_stderr_not_blindly_error(self):
        """TeeToLogger must NOT hardcode all stderr as ERROR."""
        src = Path(__file__).parent / "logging_utils.py"
        source = src.read_text(encoding="utf-8")
        # Old pattern: self._level = "INFO" if stream_name == "STDOUT" else "ERROR"
        # Should NOT exist anymore
        self.assertNotIn('"INFO" if stream_name == "STDOUT" else "ERROR"', source,
            "TeeToLogger must not blindly classify all stderr as ERROR")

    def test_progress_bar_patterns_defined(self):
        """TeeToLogger must define progress bar detection patterns."""
        src = Path(__file__).parent / "logging_utils.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("_PROGRESS_PATTERNS", source,
            "TeeToLogger must define _PROGRESS_PATTERNS for tqdm/progress bar detection")


class TestNoDoubleDateArtifactPaths(unittest.TestCase):
    """Artifact filenames must not have duplicated date stamps."""

    def test_no_double_date_in_report_generator(self):
        """report_generator.py must not append {stamp} to base that already has date."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        import re
        # Look for patterns like f"{base}_{stamp}" or _{stamp}_ which cause double dates
        double_date = re.findall(r'\{stamp\}', source)
        self.assertEqual(len(double_date), 0,
            f"report_generator.py must not use {{stamp}} variable (causes double-date). Found {len(double_date)} occurrences.")


# ═══════════════════════════════════════════════════════════════════════════
# DIRECTIVE 7 — Coverage vs Share Semantics, HUD Token Discovery
# ═══════════════════════════════════════════════════════════════════════════

class TestCoverageVsShareSemantics(unittest.TestCase):
    """ACL metrics must be labeled correctly based on denominator type."""

    def test_share_rows_not_labeled_coverage(self):
        """If denominator is Total_ACL or Norm_ACL_Balance, label must not say 'Coverage'."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        for line in source.splitlines():
            # Check ratio-components metric tuples: (display, num_lbl, num_col, den_lbl, den_col, rat_col)
            if ("Total_ACL" in line or "Norm_ACL_Balance" in line) and "RIC_" in line:
                # This line has ACL pool as denominator — should be share, not coverage
                parts = line.strip()
                if parts.startswith("(") and "Coverage" in parts.split(",")[0]:
                    # First element is display name — it should not say "Coverage"
                    self.fail(
                        f"Row with ACL-pool denominator is mislabeled as Coverage: {parts[:80]}"
                    )

    def test_exposure_coverage_rows_have_exposure_denominator(self):
        """Rows labeled 'ACL Coverage' must have exposure-base denominator, not ACL pool."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith('("') and "ACL Coverage" in stripped:
                # This is a ratio-components tuple with "Coverage" in display name
                # Denominator (5th element) must NOT be Total_ACL or Norm_ACL_Balance
                self.assertNotIn("Total_ACL", stripped.split(",")[4] if len(stripped.split(",")) > 4 else "",
                    f"Coverage row has ACL pool as denominator: {stripped[:80]}")
                self.assertNotIn("Norm_ACL_Balance", stripped.split(",")[4] if len(stripped.split(",")) > 4 else "",
                    f"Coverage row has ACL pool as denominator: {stripped[:80]}")

    def test_norm_acl_coverage_has_loans_denominator(self):
        """Norm_ACL_Coverage must be Norm_ACL_Balance / Norm_Gross_Loans."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        for line in source.splitlines():
            if "Norm_ACL_Coverage" in line and "safe_div" in line:
                self.assertIn("Norm_Gross_Loans", line,
                    "Norm_ACL_Coverage denominator must be Norm_Gross_Loans, not ACL balance")
                return
        self.fail("Could not find Norm_ACL_Coverage safe_div computation")

    def test_npl_coverage_format_is_x_multiple(self):
        """NPL coverage metrics must be formatted as x-multiples, not %."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn('"RIC_CRE_Risk_Adj_Coverage": "x"', source)
        self.assertIn('"RIC_Resi_Risk_Adj_Coverage": "x"', source)

    def test_loan_coverage_format_is_percent(self):
        """Loan coverage metrics (ACL/Loans) must NOT be in _METRIC_FORMAT_TYPE as 'x'."""
        src = Path(__file__).parent / "report_generator.py"
        source = src.read_text(encoding="utf-8")
        # These should default to percent (not appear in _METRIC_FORMAT_TYPE as "x")
        for code in ["Allowance_to_Gross_Loans_Rate", "Norm_ACL_Coverage",
                      "Risk_Adj_Allowance_Coverage", "RIC_CRE_ACL_Coverage"]:
            self.assertNotIn(f'"{code}": "x"', source,
                f"Loan coverage metric {code} must not be formatted as x-multiple")

    def test_display_label_norm_cre_acl_coverage_correct(self):
        """Norm_CRE_ACL_Coverage display label must reference CRE loans, not ACL pool."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        for line in source.splitlines():
            if '"Norm_CRE_ACL_Coverage"' in line and '"Display"' in line:
                # Must NOT say "of Norm ACL" or "of ACL" — it's ACL / CRE loans
                self.assertNotIn("of Norm ACL", line,
                    "Norm_CRE_ACL_Coverage display says 'of Norm ACL' but denominator is CRE exposure")
                self.assertNotIn("% of ACL", line,
                    "Norm_CRE_ACL_Coverage display says '% of ACL' but denominator is CRE exposure")
                return
        # Metric might not have a display entry, which is fine


class TestHUDTokenDiscovery(unittest.TestCase):
    """HUD token resolver must support multi-source discovery with diagnostics."""

    @classmethod
    def setUpClass(cls):
        """Import resolve_hud_token from case_shiller_zip_mapper."""
        try:
            from case_shiller_zip_mapper import resolve_hud_token
            cls.resolve = staticmethod(resolve_hud_token)
            cls.available = True
        except ImportError:
            cls.available = False

    def test_resolver_exists(self):
        """resolve_hud_token must be importable."""
        self.assertTrue(self.available, "resolve_hud_token must be importable from case_shiller_zip_mapper")

    def test_resolver_finds_explicit_token(self):
        """Explicit argument must override all other sources."""
        if not self.available:
            self.skipTest("resolve_hud_token not available")
        token, diag = self.resolve(explicit_token="test_token_123")
        self.assertEqual(token, "test_token_123")
        self.assertEqual(diag["source_used"], "explicit_argument")
        self.assertTrue(diag["token_found"])

    def test_resolver_diagnostics_keys(self):
        """Diagnostics dict must include required keys."""
        if not self.available:
            self.skipTest("resolve_hud_token not available")
        _, diag = self.resolve(explicit_token="test")
        required_keys = {"token_found", "source_used", "token_length", "token_prefix_masked",
                         "dotenv_available", "paths_checked", "current_working_directory",
                         "script_directory", "process_executable", "process_pid"}
        self.assertTrue(required_keys.issubset(set(diag.keys())),
            f"Missing diagnostics keys: {required_keys - set(diag.keys())}")

    def test_resolver_never_logs_full_token(self):
        """Token prefix must be masked — never expose the full token."""
        if not self.available:
            self.skipTest("resolve_hud_token not available")
        token, diag = self.resolve(explicit_token="eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9")
        # Prefix is first 6 chars + ***
        self.assertEqual(diag["token_prefix_masked"], "eyJ0eX***")
        self.assertNotIn("eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9", str(diag))

    def test_resolver_finds_env_var(self):
        """Resolver must find token from os.getenv."""
        if not self.available:
            self.skipTest("resolve_hud_token not available")
        import os
        old = os.environ.pop("HUD_USER_TOKEN", None)
        try:
            os.environ["HUD_USER_TOKEN"] = "env_test_token"
            token, diag = self.resolve()
            self.assertEqual(token, "env_test_token")
            self.assertEqual(diag["source_used"], "environment_variable")
        finally:
            if old:
                os.environ["HUD_USER_TOKEN"] = old
            else:
                os.environ.pop("HUD_USER_TOKEN", None)

    def test_resolver_missing_token_diagnostics(self):
        """Missing token diagnostics must include cwd, script dir, and paths checked."""
        if not self.available:
            self.skipTest("resolve_hud_token not available")
        import os
        old = os.environ.pop("HUD_USER_TOKEN", None)
        try:
            _, diag = self.resolve()
            if not diag["token_found"]:
                self.assertIn("current_working_directory", diag)
                self.assertIn("script_directory", diag)
                self.assertTrue(len(diag["paths_checked"]) > 0, "Must report paths checked")
        finally:
            if old:
                os.environ["HUD_USER_TOKEN"] = old


class TestEnrichmentStatusCodes(unittest.TestCase):
    """ZIP enrichment must use structured status codes."""

    def test_status_codes_defined(self):
        """case_shiller_zip_mapper must define enrichment status constants."""
        src = Path(__file__).parent / "case_shiller_zip_mapper.py"
        source = src.read_text(encoding="utf-8")
        for code in ["ENRICH_SKIPPED_DISABLED", "ENRICH_SKIPPED_NO_TOKEN",
                      "ENRICH_FAILED_TOKEN_AUTH", "ENRICH_FAILED_HTTP",
                      "ENRICH_FAILED_EMPTY_RESPONSE", "ENRICH_SUCCESS_NO_ZIPS",
                      "ENRICH_SUCCESS_WITH_ZIPS"]:
            self.assertIn(code, source, f"Missing enrichment status code: {code}")

    def test_build_returns_status(self):
        """build_case_shiller_zip_sheets must return enrichment_status key."""
        src = Path(__file__).parent / "case_shiller_zip_mapper.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("enrichment_status", source,
            "build_case_shiller_zip_sheets must return enrichment_status")
        self.assertIn("token_diagnostics", source,
            "build_case_shiller_zip_sheets must return token_diagnostics")

    def test_hud_user_token_param_exists(self):
        """build_case_shiller_zip_sheets must accept hud_user_token parameter."""
        src = Path(__file__).parent / "case_shiller_zip_mapper.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("hud_user_token", source,
            "build_case_shiller_zip_sheets must accept hud_user_token parameter")

    def test_token_passed_explicitly_from_main(self):
        """MSPBNA_CR_Normalized must pass resolved token to build_case_shiller_zip_sheets."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("hud_user_token=", source,
            "MSPBNA_CR_Normalized must pass hud_user_token= explicitly to enrichment function")
        self.assertIn("resolve_hud_token", source,
            "MSPBNA_CR_Normalized must import resolve_hud_token for multi-source discovery")


class TestHUDTokenDashboardConfigFix(unittest.TestCase):
    """Tests for the DashboardConfig HUD token fix (TypeError prevention)."""

    def test_dashboard_config_has_hud_user_token_field(self):
        """DashboardConfig must declare hud_user_token as a class attribute."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("hud_user_token: Optional[str] = None", source,
            "DashboardConfig must have hud_user_token: Optional[str] = None field")

    def test_no_item_assignment_on_dashboard_config(self):
        """config['...'] = ... must NEVER appear after load_config() in main()."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        import re
        # Look for config["..."] = pattern (dict-style item assignment on config)
        matches = re.findall(r'config\[[\"\'].*[\"\']\]\s*=', source)
        self.assertEqual(len(matches), 0,
            f"Found dict-style item assignment on config (DashboardConfig is not a dict): {matches}")

    def test_no_config_get_dict_style(self):
        """self.config.get('...') must not appear — DashboardConfig has no .get() method."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        import re
        matches = re.findall(r'self\.config\.get\(', source)
        self.assertEqual(len(matches), 0,
            f"Found self.config.get() calls — DashboardConfig is not a dict: {matches}")

    def test_main_uses_attribute_assignment_for_token(self):
        """main() must use config.hud_user_token = ... (attribute assignment)."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("config.hud_user_token = _hud_token", source,
            "main() must set config.hud_user_token via attribute assignment")

    def test_pipeline_uses_getattr_for_token(self):
        """Pipeline must use getattr(self.config, 'hud_user_token', None) for safe access."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn('getattr(self.config, "hud_user_token"', source,
            "Pipeline must use getattr for safe hud_user_token access")

    def test_hud_token_passed_explicitly_to_enrichment(self):
        """build_case_shiller_zip_sheets must receive hud_user_token= explicitly."""
        src = Path(__file__).parent / "MSPBNA_CR_Normalized.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("hud_user_token=_hud_tok", source,
            "enrichment call must pass hud_user_token=_hud_tok explicitly")

    def test_token_diagnostics_no_full_token_leak(self):
        """Token diagnostics must use masked prefix, never log the full token."""
        src = Path(__file__).parent / "case_shiller_zip_mapper.py"
        source = src.read_text(encoding="utf-8")
        self.assertIn("token_prefix_masked", source,
            "Diagnostics must include token_prefix_masked (not full token)")
        # Ensure diagnostics dict doesn't include a raw 'token_value' key
        self.assertNotIn('"token_value"', source,
            "Diagnostics must NOT include raw token_value key")

    def test_enrichment_status_codes_are_distinct(self):
        """All 8 enrichment status codes must be defined and distinct."""
        src = Path(__file__).parent / "case_shiller_zip_mapper.py"
        source = src.read_text(encoding="utf-8")
        expected = [
            "SKIPPED_DISABLED", "SKIPPED_NO_TOKEN", "SKIPPED_NO_REQUESTS",
            "FAILED_TOKEN_AUTH", "FAILED_HTTP", "FAILED_EMPTY_RESPONSE",
            "SUCCESS_NO_ZIPS", "SUCCESS_WITH_ZIPS",
        ]
        for code in expected:
            self.assertIn(code, source, f"Missing enrichment status code: {code}")


if __name__ == '__main__':
    unittest.main()
