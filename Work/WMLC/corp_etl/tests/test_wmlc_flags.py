"""Unit tests for WMLC flag evaluation logic.

Tests use known-answer cases to verify each flag fires correctly.
"""

import os
import sys
import unittest
import pandas as pd

# Ensure imports work
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from corp_etl.taggers.wmlc_tagger import evaluate_flags


def _make_row(**kwargs):
    """Build a minimal row Series with defaults for all required fields."""
    defaults = {
        "product_bucket": "",
        "credit_lii": 0,
        "is_ntc": False,
        "is_office": False,
        "focus_list": "",
        "NEW_CAMP_YN": "N",
        "txt_mstr_facil_collateral_desc": "",
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


class TestFlag1_NTC(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(product_bucket="TL CRE", is_ntc=True, credit_lii=60_000_000)
        flags = evaluate_flags(row)
        self.assertIn("NTC > $50MM", flags)

    def test_below_threshold(self):
        row = _make_row(product_bucket="TL CRE", is_ntc=True, credit_lii=40_000_000)
        flags = evaluate_flags(row)
        self.assertNotIn("NTC > $50MM", flags)

    def test_not_ntc(self):
        row = _make_row(product_bucket="TL CRE", is_ntc=False, credit_lii=60_000_000)
        flags = evaluate_flags(row)
        self.assertNotIn("NTC > $50MM", flags)

    def test_resi_excluded(self):
        row = _make_row(product_bucket="RESI", is_ntc=True, credit_lii=60_000_000)
        flags = evaluate_flags(row)
        self.assertNotIn("NTC > $50MM", flags)


class TestFlag2_NonPass(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(focus_list="Non-Pass", NEW_CAMP_YN="Y")
        flags = evaluate_flags(row)
        self.assertIn("Non-Pass Originations >$0MM", flags)

    def test_not_new(self):
        row = _make_row(focus_list="Non-Pass", NEW_CAMP_YN="N")
        flags = evaluate_flags(row)
        self.assertNotIn("Non-Pass Originations >$0MM", flags)

    def test_not_non_pass(self):
        row = _make_row(focus_list="", NEW_CAMP_YN="Y")
        flags = evaluate_flags(row)
        self.assertNotIn("Non-Pass Originations >$0MM", flags)


class TestFlag3_TL_CRE(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(product_bucket="TL CRE", credit_lii=80_000_000)
        flags = evaluate_flags(row)
        self.assertIn("TL-CRE >$75MM", flags)

    def test_below_threshold(self):
        row = _make_row(product_bucket="TL CRE", credit_lii=70_000_000)
        flags = evaluate_flags(row)
        self.assertNotIn("TL-CRE >$75MM", flags)

    def test_wrong_bucket(self):
        row = _make_row(product_bucket="TL PHA", credit_lii=80_000_000)
        flags = evaluate_flags(row)
        self.assertNotIn("TL-CRE >$75MM", flags)


class TestFlag4_TL_CRE_Office(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(product_bucket="TL CRE", credit_lii=15_000_000, is_office=True)
        flags = evaluate_flags(row)
        self.assertIn("TL-CRE Office >$10MM", flags)

    def test_not_office(self):
        row = _make_row(product_bucket="TL CRE", credit_lii=15_000_000, is_office=False)
        flags = evaluate_flags(row)
        self.assertNotIn("TL-CRE Office >$10MM", flags)


class TestFlag5_SBL_D(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(product_bucket="TL SBL Diversified", credit_lii=350_000_000)
        flags = evaluate_flags(row)
        self.assertIn("TL-SBL-D >$300MM", flags)

    def test_at_threshold(self):
        row = _make_row(product_bucket="TL SBL Diversified", credit_lii=300_000_000)
        flags = evaluate_flags(row)
        self.assertNotIn("TL-SBL-D >$300MM", flags)


class TestFlag8_HF_PE(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=40_000_000,
            txt_mstr_facil_collateral_desc="Hedge Fund portfolio, mixed"
        )
        flags = evaluate_flags(row)
        self.assertIn("TL-Alts HF/PE >$35MM", flags)

    def test_no_hedge_keyword(self):
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=40_000_000,
            txt_mstr_facil_collateral_desc="Real estate portfolio"
        )
        flags = evaluate_flags(row)
        self.assertNotIn("TL-Alts HF/PE >$35MM", flags)


class TestFlag9_PrivateShares(unittest.TestCase):
    def test_pha_qualifies(self):
        row = _make_row(product_bucket="TL PHA", credit_lii=40_000_000)
        flags = evaluate_flags(row)
        self.assertIn("TL-Alts Private Shares >$35MM", flags)

    def test_multi_with_privately_held(self):
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=55_000_000,
            txt_mstr_facil_collateral_desc="Privately Held shares and bonds"
        )
        flags = evaluate_flags(row)
        self.assertIn("TL-Alts Private Shares >$35MM", flags)

    def test_multi_without_keyword(self):
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=55_000_000,
            txt_mstr_facil_collateral_desc="Mixed securities"
        )
        flags = evaluate_flags(row)
        self.assertNotIn("TL-Alts Private Shares >$35MM", flags)


class TestFlag10_Unsecured(unittest.TestCase):
    def test_unsecured_bucket(self):
        row = _make_row(product_bucket="TL Unsecured", credit_lii=40_000_000)
        flags = evaluate_flags(row)
        self.assertIn("TL-Alts Unsecured >$35MM", flags)

    def test_multi_with_unsecured(self):
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=55_000_000,
            txt_mstr_facil_collateral_desc="Unsecured personal guarantee"
        )
        flags = evaluate_flags(row)
        self.assertIn("TL-Alts Unsecured >$35MM", flags)


class TestFlag14_LAL_D(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(product_bucket="LAL Diversified", credit_lii=350_000_000)
        flags = evaluate_flags(row)
        self.assertIn("LAL-D >$300MM", flags)


class TestFlag15_LAL_C(unittest.TestCase):
    def test_qualifies(self):
        row = _make_row(product_bucket="LAL Highly Conc.", credit_lii=150_000_000)
        flags = evaluate_flags(row)
        self.assertIn("LAL-C >$100MM", flags)


class TestMultipleFlags(unittest.TestCase):
    """Verify a single loan can carry multiple flags."""

    def test_cre_office_and_cre_75(self):
        """TL CRE >$75MM with office should get Flag 3 + Flag 4."""
        row = _make_row(product_bucket="TL CRE", credit_lii=80_000_000, is_office=True)
        flags = evaluate_flags(row)
        self.assertIn("TL-CRE >$75MM", flags)
        self.assertIn("TL-CRE Office >$10MM", flags)

    def test_ntc_and_bucket_flag(self):
        """NTC on TL SBL Highly Conc. over $100MM should get Flag 1 + Flag 6."""
        row = _make_row(
            product_bucket="TL SBL Highly Conc.", credit_lii=150_000_000, is_ntc=True
        )
        flags = evaluate_flags(row)
        self.assertIn("NTC > $50MM", flags)
        self.assertIn("TL-SBL-C >$100MM", flags)

    def test_non_pass_with_bucket_flag(self):
        """Non-Pass new camp on TL CRE >$75MM should get Flag 2 + Flag 3."""
        row = _make_row(
            product_bucket="TL CRE", credit_lii=80_000_000,
            focus_list="Non-Pass", NEW_CAMP_YN="Y"
        )
        flags = evaluate_flags(row)
        self.assertIn("Non-Pass Originations >$0MM", flags)
        self.assertIn("TL-CRE >$75MM", flags)

    def test_triple_flag_hedge_ntc(self):
        """TL Multi with Hedge Fund collateral, NTC, >$50MM should get multiple flags."""
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=60_000_000,
            is_ntc=True,
            txt_mstr_facil_collateral_desc="Hedge Fund, Other Secured Assets"
        )
        flags = evaluate_flags(row)
        self.assertIn("NTC > $50MM", flags)
        self.assertIn("TL-Alts HF/PE >$35MM", flags)
        self.assertIn("TL-Alts Other Secured >$50MM", flags)


class TestKnownAnswerCases(unittest.TestCase):
    """Five known-answer test loans with exact expected flag output."""

    def test_case_1_lal_diversified_over_300m(self):
        row = _make_row(product_bucket="LAL Diversified", credit_lii=400_000_000)
        flags = evaluate_flags(row)
        self.assertEqual(flags, ["LAL-D >$300MM"])

    def test_case_2_tl_cre_office_ntc(self):
        row = _make_row(
            product_bucket="TL CRE", credit_lii=80_000_000,
            is_ntc=True, is_office=True
        )
        flags = evaluate_flags(row)
        self.assertEqual(sorted(flags), sorted([
            "NTC > $50MM", "TL-CRE >$75MM", "TL-CRE Office >$10MM"
        ]))

    def test_case_3_non_pass_only(self):
        row = _make_row(
            product_bucket="RESI", credit_lii=5_000_000,
            focus_list="Non-Pass", NEW_CAMP_YN="Y"
        )
        flags = evaluate_flags(row)
        self.assertEqual(flags, ["Non-Pass Originations >$0MM"])

    def test_case_4_tl_unsecured_below_threshold(self):
        row = _make_row(product_bucket="TL Unsecured", credit_lii=30_000_000)
        flags = evaluate_flags(row)
        self.assertEqual(flags, [])

    def test_case_5_tl_multi_hedge_privately_held(self):
        row = _make_row(
            product_bucket="TL Multicollateral", credit_lii=60_000_000,
            txt_mstr_facil_collateral_desc="Hedge Fund and Privately Held shares"
        )
        flags = evaluate_flags(row)
        self.assertIn("TL-Alts HF/PE >$35MM", flags)
        self.assertIn("TL-Alts Private Shares >$35MM", flags)
        self.assertEqual(len(flags), 2)


if __name__ == "__main__":
    unittest.main()
