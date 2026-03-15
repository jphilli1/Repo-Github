#!/usr/bin/env python3
"""
Unit Tests for Flow-Variable Math: ytd_to_discrete() and annualize_ytd()
=========================================================================

Covers:
  - Standard YTD de-accumulation (Q1 passthrough, Q2-Q4 differencing)
  - Missing prior quarters (fallback behavior)
  - NaN passthrough
  - Q4 annualization idempotency
  - Multi-bank grouping (CERT isolation)
  - Empty / missing column edge cases

These tests import the functions by reading the source file to avoid pulling
in the full MSPBNA_CR_Normalized import chain (which has heavyweight deps).
"""

import os
import sys
import unittest

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
#  Import ytd_to_discrete and annualize_ytd from the extracted flow_math module
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_REPO_ROOT, "src", "data_processing"))

from flow_math import ytd_to_discrete, annualize_ytd


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _make_df(cert, dates, ytd_values, col="NCO_YTD"):
    """Build a minimal DataFrame for a single bank."""
    return pd.DataFrame({
        "CERT": cert,
        "REPDTE": pd.to_datetime(dates),
        col: ytd_values,
    })


def _four_quarters(year=2024, cert=1):
    """Return a standard 4-quarter DataFrame with known YTD NCO values.

    YTD values: Q1=100, Q2=250, Q3=420, Q4=600
    Expected discrete: Q1=100, Q2=150, Q3=170, Q4=180
    """
    dates = [f"{year}-03-31", f"{year}-06-30", f"{year}-09-30", f"{year}-12-31"]
    ytd = [100.0, 250.0, 420.0, 600.0]
    return _make_df(cert, dates, ytd)


# ===========================================================================
#  ytd_to_discrete tests
# ===========================================================================

class TestYtdToDiscrete(unittest.TestCase):
    """Tests for ytd_to_discrete() -- YTD cumulative to discrete quarterly."""

    # --- Standard de-accumulation ---

    def test_standard_four_quarter_deaccumulation(self):
        """Q1 passthrough, Q2-Q4 differenced from prior quarter."""
        df = _four_quarters()
        result = ytd_to_discrete(df, "NCO_YTD")
        expected = [100.0, 150.0, 170.0, 180.0]
        np.testing.assert_array_almost_equal(result.values, expected)

    def test_q1_equals_ytd_value(self):
        """Q1 discrete flow must equal the raw YTD value (fresh accumulation)."""
        df = _four_quarters()
        result = ytd_to_discrete(df, "NCO_YTD")
        self.assertAlmostEqual(result.iloc[0], 100.0)

    def test_q4_discrete_is_ytd_q4_minus_ytd_q3(self):
        """Q4 discrete = YTD_Q4 - YTD_Q3."""
        df = _four_quarters()
        result = ytd_to_discrete(df, "NCO_YTD")
        self.assertAlmostEqual(result.iloc[3], 600.0 - 420.0)

    def test_sum_of_discrete_equals_ytd_q4(self):
        """Sum of all discrete quarters must equal the full-year YTD (Q4)."""
        df = _four_quarters()
        result = ytd_to_discrete(df, "NCO_YTD")
        self.assertAlmostEqual(result.sum(), 600.0)

    # --- Multi-bank isolation ---

    def test_multi_cert_isolation(self):
        """Each CERT is differenced independently -- no cross-bank leakage."""
        df1 = _four_quarters(cert=1)
        df2 = _make_df(2,
                        ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"],
                        [50.0, 80.0, 110.0, 200.0])
        df = pd.concat([df1, df2], ignore_index=True)
        result = ytd_to_discrete(df, "NCO_YTD")

        # Bank 1: 100, 150, 170, 180
        np.testing.assert_array_almost_equal(result.iloc[:4].values,
                                             [100.0, 150.0, 170.0, 180.0])
        # Bank 2: 50, 30, 30, 90
        np.testing.assert_array_almost_equal(result.iloc[4:].values,
                                             [50.0, 30.0, 30.0, 90.0])

    # --- Cross-year boundary ---

    def test_cross_year_q1_resets(self):
        """Q1 of a new year must NOT difference from prior year Q4."""
        dates = ["2023-12-31", "2024-03-31", "2024-06-30"]
        ytd = [500.0, 120.0, 300.0]
        df = _make_df(1, dates, ytd)
        result = ytd_to_discrete(df, "NCO_YTD")
        # Q1-2024 = 120 (fresh YTD, not 120 - 500)
        self.assertAlmostEqual(result.iloc[1], 120.0)
        # Q2-2024 = 300 - 120 = 180
        self.assertAlmostEqual(result.iloc[2], 180.0)

    # --- Missing prior quarter (fallback) ---

    def test_missing_prior_quarter_fallback(self):
        """When a bank has only Q3 and Q4, Q3 falls back to its YTD value."""
        dates = ["2024-09-30", "2024-12-31"]
        ytd = [420.0, 600.0]
        df = _make_df(1, dates, ytd)
        result = ytd_to_discrete(df, "NCO_YTD")
        # Q3: first record for this CERT -> fallback to YTD value (420)
        # Q4: 600 - 420 = 180
        self.assertAlmostEqual(result.iloc[0], 420.0)
        self.assertAlmostEqual(result.iloc[1], 180.0)

    # --- NaN passthrough ---

    def test_nan_in_ytd_fills_to_zero(self):
        """NaN in YTD column: final .fillna(0) converts residual NaNs to 0."""
        dates = ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"]
        ytd = [100.0, np.nan, 420.0, 600.0]
        df = _make_df(1, dates, ytd)
        result = ytd_to_discrete(df, "NCO_YTD")
        # Q1 = 100 (is_q1 passthrough)
        self.assertAlmostEqual(result.iloc[0], 100.0)
        # Result should have no NaNs (final fillna(0))
        self.assertFalse(result.isna().any())

    # --- Edge cases ---

    def test_missing_column_returns_zeros(self):
        """If the requested column doesn't exist, return all zeros."""
        df = _four_quarters()
        result = ytd_to_discrete(df, "NONEXISTENT_COL")
        self.assertTrue((result == 0.0).all())
        self.assertEqual(len(result), len(df))

    def test_empty_dataframe(self):
        """Empty DataFrame returns empty zero series."""
        df = pd.DataFrame(columns=["CERT", "REPDTE", "NCO_YTD"])
        result = ytd_to_discrete(df, "NCO_YTD")
        self.assertEqual(len(result), 0)

    def test_single_quarter_returns_ytd_value(self):
        """A single-quarter bank returns the YTD value as the discrete flow."""
        df = _make_df(1, ["2024-03-31"], [100.0])
        result = ytd_to_discrete(df, "NCO_YTD")
        self.assertAlmostEqual(result.iloc[0], 100.0)

    def test_zero_ytd_values(self):
        """All-zero YTD produces all-zero discrete."""
        dates = ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"]
        df = _make_df(1, dates, [0.0, 0.0, 0.0, 0.0])
        result = ytd_to_discrete(df, "NCO_YTD")
        np.testing.assert_array_almost_equal(result.values, [0.0, 0.0, 0.0, 0.0])

    def test_monotonically_increasing_ytd(self):
        """Strictly increasing YTD always produces positive discrete flows."""
        dates = ["2024-03-31", "2024-06-30", "2024-09-30", "2024-12-31"]
        df = _make_df(1, dates, [10.0, 30.0, 60.0, 100.0])
        result = ytd_to_discrete(df, "NCO_YTD")
        self.assertTrue((result > 0).all())


# ===========================================================================
#  annualize_ytd tests
# ===========================================================================

class TestAnnualizeYtd(unittest.TestCase):
    """Tests for annualize_ytd() -- YTD to annualized projection."""

    def _make_annual_df(self, cert=1, year=2024, ytd_values=None):
        """Build a 4-quarter DataFrame for annualization tests."""
        dates = [f"{year}-03-31", f"{year}-06-30", f"{year}-09-30", f"{year}-12-31"]
        if ytd_values is None:
            ytd_values = [100.0, 250.0, 420.0, 600.0]
        return _make_df(cert, dates, ytd_values, col="INCOME_YTD")

    # --- Standard annualization ---

    def test_q1_annualized_times_4(self):
        """Q1: YTD * 4 (project 1 quarter to full year)."""
        df = self._make_annual_df()
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[0], 100.0 * 4.0)

    def test_q2_annualized_times_2(self):
        """Q2: YTD * 2 (project 2 quarters to full year)."""
        df = self._make_annual_df()
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[1], 250.0 * 2.0)

    def test_q3_annualized_times_4_over_3(self):
        """Q3: YTD * 4/3."""
        df = self._make_annual_df()
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[2], 420.0 * (4.0 / 3.0))

    def test_q4_annualization_idempotent(self):
        """Q4: YTD * 1 -- full year, no adjustment (idempotent)."""
        df = self._make_annual_df()
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[3], 600.0 * 1.0)

    def test_q4_equals_raw_ytd(self):
        """Q4 annualized value must equal the raw YTD value exactly."""
        df = self._make_annual_df(ytd_values=[100.0, 200.0, 300.0, 400.0])
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[3], 400.0)

    # --- Uniform earnings -> constant annualization ---

    def test_uniform_earnings_constant_annualization(self):
        """If earnings are perfectly uniform (100/q), all quarters annualize to 400."""
        df = self._make_annual_df(ytd_values=[100.0, 200.0, 300.0, 400.0])
        result = annualize_ytd(df, "INCOME_YTD")
        for i in range(4):
            self.assertAlmostEqual(result.iloc[i], 400.0,
                                   msg=f"Q{i+1} should annualize to 400")

    # --- NaN passthrough ---

    def test_nan_ytd_produces_nan(self):
        """NaN in the YTD column should produce NaN annualized."""
        df = self._make_annual_df(ytd_values=[100.0, np.nan, 420.0, 600.0])
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[0], 400.0)
        self.assertTrue(np.isnan(result.iloc[1]))
        self.assertAlmostEqual(result.iloc[2], 420.0 * (4.0 / 3.0))

    # --- Edge cases ---

    def test_missing_column_returns_nan(self):
        """If column doesn't exist, return all NaN."""
        df = self._make_annual_df()
        result = annualize_ytd(df, "NONEXISTENT_COL")
        self.assertTrue(result.isna().all())

    def test_zero_ytd_stays_zero(self):
        """Zero YTD * any multiplier = 0."""
        df = self._make_annual_df(ytd_values=[0.0, 0.0, 0.0, 0.0])
        result = annualize_ytd(df, "INCOME_YTD")
        np.testing.assert_array_almost_equal(result.values, [0.0, 0.0, 0.0, 0.0])

    def test_negative_ytd_annualized(self):
        """Negative YTD values (net losses) should annualize correctly."""
        df = self._make_annual_df(ytd_values=[-50.0, -100.0, -150.0, -200.0])
        result = annualize_ytd(df, "INCOME_YTD")
        self.assertAlmostEqual(result.iloc[0], -200.0)  # -50 * 4
        self.assertAlmostEqual(result.iloc[3], -200.0)  # -200 * 1


if __name__ == "__main__":
    unittest.main()
