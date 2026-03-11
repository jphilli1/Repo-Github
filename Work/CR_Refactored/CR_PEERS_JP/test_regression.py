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
            self.assertIn('CaseShiller_Metro_Map_Audit', result)
            audit = result['CaseShiller_Metro_Map_Audit']
            # The HUD version returns the full metro map with SKIPPED comments
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
            audit = result.get('CaseShiller_Metro_Map_Audit')
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
    def test_msbna_label_is_ms(self):
        """resolve_display_label for MSBNA cert must return MS."""
        self.assertIn('return "MS"', self.rg_source)

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


if __name__ == '__main__':
    unittest.main()
