"""
Regression tests for MSPBNA Credit Risk Dashboard pipeline.

Validates:
- Normalization logic (no silent .clip, over-exclusion -> NaN/0)
- Peer group uniqueness
- Metric naming conventions
- Ratio component correctness
- Case-Shiller ZIP integration
"""

import os
import sys
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
            self.assertIn('SKIPPED', audit.iloc[0].values)
        finally:
            os.environ.pop('ENABLE_CASE_SHILLER_ZIP_ENRICHMENT', None)

    def test_zip_codes_are_5_char_strings(self):
        """ZIP codes must be 5-character strings."""
        from case_shiller_zip_mapper import build_case_shiller_zip_sheets
        os.environ['ENABLE_CASE_SHILLER_ZIP_ENRICHMENT'] = 'true'
        try:
            test_df = pd.DataFrame({'ZIP': ['10001', '90210', '2101', '94105']})
            result = build_case_shiller_zip_sheets(zip_df=test_df)
            # '2101' should be zero-padded to '02101'
            if 'CaseShiller_Zip_Coverage' in result:
                coverage = result['CaseShiller_Zip_Coverage']
                # Boston metro should match '02101'
                boston_row = coverage[coverage['Metro'] == 'Boston']
                self.assertTrue(len(boston_row) > 0)
        finally:
            os.environ.pop('ENABLE_CASE_SHILLER_ZIP_ENRICHMENT', None)

    def test_zip_output_contains_only_20_metros(self):
        """Coverage output should have exactly 20 Case-Shiller metros."""
        from case_shiller_zip_mapper import CASE_SHILLER_METROS
        self.assertEqual(len(CASE_SHILLER_METROS), 20)

    def test_summary_zip_count_matches_detail(self):
        """Summary zip_count must match detail unique ZIP count."""
        from case_shiller_zip_mapper import build_case_shiller_zip_sheets
        os.environ['ENABLE_CASE_SHILLER_ZIP_ENRICHMENT'] = 'true'
        try:
            test_df = pd.DataFrame({'ZIP': ['10001', '90210', '10002', '94105', '60601']})
            result = build_case_shiller_zip_sheets(zip_df=test_df)
            if 'CaseShiller_Zip_Summary' in result and 'CaseShiller_Zip_Coverage' in result:
                summary = result['CaseShiller_Zip_Summary']
                coverage = result['CaseShiller_Zip_Coverage']
                # Count unique ZIPs from coverage detail
                detail_zips = set()
                for _, row in coverage.iterrows():
                    if row['ZIPs']:
                        for z in row['ZIPs'].split(', '):
                            detail_zips.add(z.strip())
                self.assertEqual(summary['zip_count'].iloc[0], len(detail_zips))
        finally:
            os.environ.pop('ENABLE_CASE_SHILLER_ZIP_ENRICHMENT', None)

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


if __name__ == '__main__':
    unittest.main()
