#!/usr/bin/env python3
"""
Unit Tests for calc_normalized_residual() — Normalization Engine
=================================================================

Covers:
  - Standard exclusion (total > excluded → positive residual)
  - Over-exclusion clamping (minor: within tolerance → 0, material: beyond → NaN)
  - Severity classification (ok / minor_clip / material_nan)
  - Boundary zero values
  - NaN propagation
  - Multi-segment exclusion chains (sequential application)

The function is defined as a nested function inside CRProcessor.process(), so
we extract it from source to test in isolation.
"""

import os
import sys
import types
import textwrap
import unittest

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
#  Extract calc_normalized_residual from source without full import
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(_REPO_ROOT, "src", "data_processing", "MSPBNA_CR_Normalized.py")


def _extract_calc_normalized_residual():
    """Extract the nested calc_normalized_residual function.

    The function lives inside a method body, so we find the 'return {'
    block that ends it and stop there.
    """
    with open(_SRC, "r", encoding="utf-8", errors="replace") as f:
        source = f.read()

    # Find the function definition
    marker = "def calc_normalized_residual(total, excluded, label, tolerance_pct=0.05):"
    start = source.index(marker)

    # Extract lines belonging to the function body by tracking indentation.
    # The function ends after the closing brace of the return dict.
    lines = source[start:].split("\n")
    def_indent = len(lines[0]) - len(lines[0].lstrip())
    func_lines = [lines[0]]
    found_return = False
    brace_depth = 0
    for line in lines[1:]:
        stripped = line.strip()
        # Non-blank line at or below def indent → function is over
        if stripped and (len(line) - len(line.lstrip())) <= def_indent:
            break
        func_lines.append(line)
        # Track return { ... } to know we captured the full function
        if "return {" in stripped:
            found_return = True
            brace_depth += stripped.count("{") - stripped.count("}")
        elif found_return:
            brace_depth += stripped.count("{") - stripped.count("}")
            if brace_depth <= 0:
                break

    func_source = textwrap.dedent("\n".join(func_lines))

    mod = types.ModuleType("_norm_math")
    mod.__dict__["pd"] = pd
    mod.__dict__["np"] = np
    exec(func_source, mod.__dict__)
    return mod.calc_normalized_residual


calc_normalized_residual = _extract_calc_normalized_residual()


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _series(*values):
    """Create a pd.Series from values."""
    return pd.Series(values, dtype=float)


# ===========================================================================
#  Tests
# ===========================================================================

class TestCalcNormalizedResidual(unittest.TestCase):
    """Tests for calc_normalized_residual()."""

    # --- Standard exclusion (positive residual) ---

    def test_standard_exclusion_positive_residual(self):
        """total > excluded → residual is positive, severity = ok."""
        total = _series(1000.0, 2000.0, 3000.0)
        excluded = _series(200.0, 400.0, 600.0)
        result = calc_normalized_residual(total, excluded, "Loans")
        np.testing.assert_array_almost_equal(
            result["final_value"].values, [800.0, 1600.0, 2400.0]
        )
        self.assertTrue((result["severity"] == "ok").all())
        self.assertTrue((~result["over_exclusion_flag"]).all())

    def test_residual_equals_total_minus_excluded(self):
        """The raw residual must always equal total - excluded."""
        total = _series(500.0, 0.0, 100.0)
        excluded = _series(100.0, 50.0, 200.0)
        result = calc_normalized_residual(total, excluded, "Test")
        np.testing.assert_array_almost_equal(
            result["residual"].values, [400.0, -50.0, -100.0]
        )

    # --- Zero exclusion ---

    def test_zero_exclusion_passes_through(self):
        """excluded = 0 → final_value = total, severity = ok."""
        total = _series(1000.0)
        excluded = _series(0.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertAlmostEqual(result["final_value"].iloc[0], 1000.0)
        self.assertEqual(result["severity"].iloc[0], "ok")

    # --- Over-exclusion: minor clip ---

    def test_minor_over_exclusion_clamped_to_zero(self):
        """Negative residual within tolerance → clamped to 0, severity = minor_clip."""
        total = _series(1000.0)
        # 3% over-exclusion (within 5% default tolerance)
        excluded = _series(1030.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertAlmostEqual(result["final_value"].iloc[0], 0.0)
        self.assertEqual(result["severity"].iloc[0], "minor_clip")
        self.assertTrue(result["over_exclusion_flag"].iloc[0])

    def test_minor_clip_at_tolerance_boundary(self):
        """Exactly at tolerance boundary (5%) → minor_clip."""
        total = _series(1000.0)
        excluded = _series(1050.0)  # exactly 5% over
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertAlmostEqual(result["final_value"].iloc[0], 0.0)
        self.assertEqual(result["severity"].iloc[0], "minor_clip")

    # --- Over-exclusion: material NaN ---

    def test_material_over_exclusion_set_to_nan(self):
        """Negative residual beyond tolerance → NaN, severity = material_nan."""
        total = _series(1000.0)
        # 20% over-exclusion (well beyond 5% tolerance)
        excluded = _series(1200.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertTrue(np.isnan(result["final_value"].iloc[0]))
        self.assertEqual(result["severity"].iloc[0], "material_nan")
        self.assertTrue(result["over_exclusion_flag"].iloc[0])

    def test_material_just_beyond_tolerance(self):
        """Slightly beyond tolerance (5.1%) → material_nan."""
        total = _series(1000.0)
        excluded = _series(1051.0)  # 5.1% over
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertTrue(np.isnan(result["final_value"].iloc[0]))
        self.assertEqual(result["severity"].iloc[0], "material_nan")

    # --- Custom tolerance ---

    def test_custom_tolerance_10pct(self):
        """With tolerance_pct=0.10, 8% over-exclusion is minor_clip."""
        total = _series(1000.0)
        excluded = _series(1080.0)  # 8% over
        result = calc_normalized_residual(total, excluded, "NCO", tolerance_pct=0.10)
        self.assertAlmostEqual(result["final_value"].iloc[0], 0.0)
        self.assertEqual(result["severity"].iloc[0], "minor_clip")

    # --- Zero total ---

    def test_zero_total_zero_excluded(self):
        """Both total and excluded are zero → residual = 0, severity = ok."""
        total = _series(0.0)
        excluded = _series(0.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertAlmostEqual(result["final_value"].iloc[0], 0.0)
        self.assertEqual(result["severity"].iloc[0], "ok")

    def test_zero_total_nonzero_excluded(self):
        """total=0 with excluded > 0 → residual negative, material_nan."""
        total = _series(0.0)
        excluded = _series(100.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        # total is not > 0, so over_exclusion_pct stays 0
        # residual < 0 and not minor_mask → material_nan
        self.assertTrue(np.isnan(result["final_value"].iloc[0]))
        self.assertEqual(result["severity"].iloc[0], "material_nan")

    # --- NaN propagation ---

    def test_nan_total_propagates(self):
        """NaN in total → residual is NaN, severity = material_nan."""
        total = _series(np.nan)
        excluded = _series(100.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertTrue(np.isnan(result["final_value"].iloc[0]))

    def test_nan_excluded_propagates(self):
        """NaN in excluded → residual is NaN."""
        total = _series(1000.0)
        excluded = _series(np.nan)
        result = calc_normalized_residual(total, excluded, "NCO")
        self.assertTrue(np.isnan(result["residual"].iloc[0]))

    # --- Multi-element vector (mixed severities) ---

    def test_mixed_severity_vector(self):
        """Vector with ok, minor_clip, and material_nan entries."""
        total = _series(1000.0, 1000.0, 1000.0)
        excluded = _series(500.0, 1030.0, 1200.0)
        result = calc_normalized_residual(total, excluded, "NCO")

        # Element 0: ok (residual = 500)
        self.assertAlmostEqual(result["final_value"].iloc[0], 500.0)
        self.assertEqual(result["severity"].iloc[0], "ok")

        # Element 1: minor_clip (3% over → clamped to 0)
        self.assertAlmostEqual(result["final_value"].iloc[1], 0.0)
        self.assertEqual(result["severity"].iloc[1], "minor_clip")

        # Element 2: material_nan (20% over → NaN)
        self.assertTrue(np.isnan(result["final_value"].iloc[2]))
        self.assertEqual(result["severity"].iloc[2], "material_nan")

    # --- Over-exclusion percentage computation ---

    def test_over_exclusion_pct_correct(self):
        """Over-exclusion percentage = -residual / total, clipped at 0."""
        total = _series(1000.0, 1000.0)
        excluded = _series(500.0, 1100.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        # Element 0: positive residual → pct = 0 (clipped)
        self.assertAlmostEqual(result["over_exclusion_pct"].iloc[0], 0.0)
        # Element 1: 10% over-exclusion
        self.assertAlmostEqual(result["over_exclusion_pct"].iloc[1], 0.10)

    # --- Output dict contract ---

    def test_output_keys(self):
        """Result dict must contain exactly the 5 expected keys."""
        total = _series(100.0)
        excluded = _series(10.0)
        result = calc_normalized_residual(total, excluded, "Test")
        expected_keys = {"final_value", "residual", "over_exclusion_flag",
                         "over_exclusion_pct", "severity"}
        self.assertEqual(set(result.keys()), expected_keys)

    def test_output_lengths_match_input(self):
        """All output Series must have the same length as input."""
        total = _series(100.0, 200.0, 300.0)
        excluded = _series(10.0, 20.0, 30.0)
        result = calc_normalized_residual(total, excluded, "Test")
        for key, val in result.items():
            self.assertEqual(len(val), 3, msg=f"{key} length mismatch")

    # --- Final value is never negative ---

    def test_final_value_never_negative(self):
        """final_value must be >= 0 or NaN — never negative."""
        total = _series(100.0, 100.0, 100.0, 100.0)
        excluded = _series(50.0, 100.0, 103.0, 200.0)
        result = calc_normalized_residual(total, excluded, "NCO")
        for val in result["final_value"]:
            self.assertTrue(val >= 0 or np.isnan(val),
                            msg=f"final_value {val} is negative")


class TestNormalizationChains(unittest.TestCase):
    """Test sequential application of calc_normalized_residual (multi-segment)."""

    def test_sequential_exclusion_ncco_then_loans(self):
        """Applying normalization to NCO and Loans independently produces
        consistent results — NCO residual uses NCO total, Loans uses Loans total."""
        nco_total = _series(500.0)
        nco_excluded = _series(200.0)
        loans_total = _series(10000.0)
        loans_excluded = _series(3000.0)

        nco_result = calc_normalized_residual(nco_total, nco_excluded, "NCO")
        loans_result = calc_normalized_residual(loans_total, loans_excluded, "Loans")

        self.assertAlmostEqual(nco_result["final_value"].iloc[0], 300.0)
        self.assertAlmostEqual(loans_result["final_value"].iloc[0], 7000.0)

    def test_rate_from_normalized_components(self):
        """Normalized rate = Norm_NCO / Norm_Loans. Both must be from
        calc_normalized_residual, not raw totals."""
        nco_total = _series(100.0)
        nco_excluded = _series(40.0)
        loans_total = _series(5000.0)
        loans_excluded = _series(2000.0)

        nco_norm = calc_normalized_residual(nco_total, nco_excluded, "NCO")["final_value"]
        loans_norm = calc_normalized_residual(loans_total, loans_excluded, "Loans")["final_value"]

        rate = nco_norm / loans_norm
        expected = 60.0 / 3000.0  # 0.02
        self.assertAlmostEqual(rate.iloc[0], expected)


if __name__ == "__main__":
    unittest.main()
