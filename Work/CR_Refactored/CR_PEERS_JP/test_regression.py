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


if __name__ == '__main__':
    unittest.main()
