#!/usr/bin/env python3
"""
FRED Series Registry — Registry-Driven Macro / Market Context
==============================================================

Central metadata registry for all FRED series consumed by the credit-risk
dashboard.  Each entry carries provenance, cadence, priority, chart routing,
and transformation directives so ingestion, validation, and charting are
entirely data-driven.

Modules A-D as specified:
  A. SBL / Market-Collateral Proxy
  B. Residential / Jumbo Mortgage
  C. CRE Lending / Underwriting / Credit
  D. Case-Shiller Collateral (static seed + discovery-mode supplement)

The registry is the **single source of truth** for which series to fetch,
how to label them, and where each flows in the Excel output and chart layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple
import pandas as pd


# ---------------------------------------------------------------------------
# Core data class
# ---------------------------------------------------------------------------
@dataclass
class FREDSeriesSpec:
    """Metadata for a single FRED series."""

    category: str
    subcategory: str
    series_id: str
    display_name: str
    freq: str                          # D, W, M, Q, A
    units: str                         # e.g. "Bil. of $", "%", "Index"
    seasonal_adjustment: str           # SA, NSA, SAAR
    priority: int                      # 1=dashboard, 2=overlay/regime, 3=registry-only
    use_case: str                      # human-readable purpose
    chart_group: str                   # routing key for first-wave charts
    transformations: List[str] = field(default_factory=list)
    is_active: bool = True
    notes: str = ""
    sheet: str = ""                    # target Excel sheet
    discovery_source: str = ""         # "static" | "release:<id>" for auto-discovered


# ---------------------------------------------------------------------------
# Convenience builder
# ---------------------------------------------------------------------------
def _s(cat, sub, sid, name, freq, units, sa, pri, use, cg,
       transforms=None, sheet="", notes="", discovery="static"):
    return FREDSeriesSpec(
        category=cat, subcategory=sub, series_id=sid, display_name=name,
        freq=freq, units=units, seasonal_adjustment=sa, priority=pri,
        use_case=use, chart_group=cg,
        transformations=transforms or [],
        sheet=sheet, notes=notes, discovery_source=discovery,
    )


# ===================================================================
# MODULE A — SBL / Market-Collateral Proxy
# ===================================================================
_SBL_CAT = "SBL / Market-Collateral Proxy"
_SBL_SHEET = "FRED_SBL_Backdrop"

SBL_CORE_SERIES: List[FREDSeriesSpec] = [
    # --- Core bank-system securities inventory ---
    _s(_SBL_CAT, "Securities in Bank Credit", "INVEST",
       "Securities in Bank Credit, All Commercial Banks", "W", "Bil. of $", "SA", 1,
       "Bank-system securities inventory trend", "sbl_backdrop",
       ["pct_chg_yoy", "rolling_12m_avg", "z_score_5y"], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit", "INVESTNSA",
       "Securities in Bank Credit, All Commercial Banks (NSA)", "W", "Bil. of $", "NSA", 3,
       "Bank-system securities inventory trend (NSA variant)", "sbl_backdrop",
       ["pct_chg_yoy"], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit", "SBCDCBM027SBOG",
       "Securities in Bank Credit, Domestically Chartered Commercial Banks", "M", "Bil. of $", "SA", 2,
       "Domestic-bank securities inventory", "sbl_backdrop",
       ["pct_chg_yoy", "z_score_5y"], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit", "SBCDCBM027NBOG",
       "Securities in Bank Credit, Domestically Chartered Commercial Banks (NSA)", "M", "Bil. of $", "NSA", 3,
       "Domestic-bank securities inventory (NSA)", "sbl_backdrop",
       [], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit — Large Banks", "SBCLCBM027SBOG",
       "Securities in Bank Credit, Large Domestically Chartered Commercial Banks", "M", "Bil. of $", "SA", 1,
       "Large-bank securities inventory trend", "sbl_backdrop",
       ["pct_chg_yoy", "rolling_12m_avg", "z_score_5y"], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit — Large Banks", "SBCLCBM027NBOG",
       "Securities in Bank Credit, Large Domestically Chartered Commercial Banks (NSA)", "M", "Bil. of $", "NSA", 3,
       "Large-bank securities inventory (NSA)", "sbl_backdrop",
       [], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit — Large Banks", "SBCLCBW027SBOG",
       "Securities in Bank Credit, Large Domestically Chartered Commercial Banks (Weekly)", "W", "Bil. of $", "SA", 2,
       "Large-bank securities inventory (weekly)", "sbl_backdrop",
       ["rolling_3m_avg"], _SBL_SHEET),
    _s(_SBL_CAT, "Securities in Bank Credit — Large Banks", "SBCLCBW027NBOG",
       "Securities in Bank Credit, Large Domestically Chartered Commercial Banks (Weekly, NSA)", "W", "Bil. of $", "NSA", 3,
       "Large-bank securities inventory (weekly, NSA)", "sbl_backdrop",
       [], _SBL_SHEET),

    # --- Secondary: broker-dealer balance-sheet / leverage proxies ---
    _s(_SBL_CAT, "Broker-Dealer Leverage", "BOGZ1FU664004005Q",
       "Sec. B&D: Debt Securities & Loans; Asset, Transactions (Q, NSA)", "Q", "Mil. of $", "NSA", 2,
       "Broker-dealer leverage / customer-margin backdrop", "sbl_backdrop",
       ["pct_chg_qoq_annualized", "z_score_5y"], _SBL_SHEET),
    _s(_SBL_CAT, "Broker-Dealer Leverage", "BOGZ1FA664004005Q",
       "Sec. B&D: Debt Securities & Loans; Asset, Transactions (Q, SAAR)", "Q", "Mil. of $", "SAAR", 2,
       "Broker-dealer leverage (SAAR)", "sbl_backdrop",
       ["pct_chg_yoy"], _SBL_SHEET),
    _s(_SBL_CAT, "Broker-Dealer Leverage", "BOGZ1FL664004005A",
       "Sec. B&D: Debt Securities & Loans; Asset, Level (Annual)", "A", "Mil. of $", "NSA", 3,
       "Broker-dealer asset level", "sbl_backdrop",
       [], _SBL_SHEET),
    _s(_SBL_CAT, "Broker-Dealer Leverage", "BOGZ1FL664041005A",
       "Sec. B&D: Loans Incl. Repo (Excl. Mortgages); Asset, Level (Annual)", "A", "Mil. of $", "NSA", 3,
       "Broker-dealer repo/loan level", "sbl_backdrop",
       [], _SBL_SHEET),
    _s(_SBL_CAT, "Broker-Dealer Leverage", "BOGZ1FA664041005A",
       "Sec. B&D: Loans Incl. Repo (Excl. Mortgages); Transactions (Annual)", "A", "Mil. of $", "NSA", 3,
       "Broker-dealer repo/loan transactions", "sbl_backdrop",
       [], _SBL_SHEET),
    _s(_SBL_CAT, "Broker-Dealer Margin Proxy", "BOGZ1FU663067005A",
       "Sec. B&D: Margin Loans & Receivables; Transactions (Annual)", "A", "Mil. of $", "NSA", 2,
       "Broker-dealer customer margin loan proxy", "sbl_backdrop",
       ["pct_chg_yoy"], _SBL_SHEET,
       notes="Closest FRED proxy for broker-dealer margin-lending activity"),
]


# ===================================================================
# MODULE B — Residential / Jumbo Mortgage
# ===================================================================
_RESI_CAT = "Residential / Jumbo Mortgage"
_RESI_SHEET = "FRED_Residential_Jumbo"

RESIDENTIAL_SERIES: List[FREDSeriesSpec] = [
    # --- Jumbo pricing ---
    _s(_RESI_CAT, "Jumbo Pricing", "OBMMIJUMBO30YF",
       "30-Year Fixed Rate Jumbo Mortgage Index", "D", "%", "NSA", 1,
       "Jumbo mortgage rate benchmark", "jumbo_conditions",
       ["rolling_3m_avg", "spread_vs_MORTGAGE30US", "z_score_5y"], _RESI_SHEET),

    # --- Jumbo SLOOS demand / standards ---
    _s(_RESI_CAT, "Jumbo SLOOS Demand", "SUBLPDHMDJNQ",
       "Net % Banks Reporting Stronger Demand — Qualified Jumbo", "Q", "%", "NSA", 1,
       "Jumbo credit appetite from SLOOS", "jumbo_conditions",
       ["z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Jumbo SLOOS Demand", "SUBLPDHMDJLGNQ",
       "Net % Large Banks Reporting Stronger Demand — Qualified Jumbo", "Q", "%", "NSA", 1,
       "Jumbo credit appetite, large banks", "jumbo_conditions",
       ["z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Jumbo SLOOS Standards", "SUBLPDHMSKNQ",
       "Net % Banks Tightening Standards — Non-Qualified Jumbo", "Q", "%", "NSA", 1,
       "Jumbo underwriting standards", "jumbo_conditions",
       ["z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Jumbo SLOOS Standards", "SUBLPDHMSKLGNQ",
       "Net % Large Banks Tightening Standards — Non-Qualified Jumbo", "Q", "%", "NSA", 1,
       "Jumbo underwriting standards, large banks", "jumbo_conditions",
       ["z_score_5y"], _RESI_SHEET),

    # --- Residential loan balances / growth ---
    _s(_RESI_CAT, "Resi Balances", "ATAIEALLGSRESFRMT100",
       "Single-Family Resi Mortgage Balances (Top 100 Banks)", "Q", "Thou. of $", "NSA", 1,
       "Large-bank residential balance growth", "resi_credit_cycle",
       ["pct_chg_yoy", "pct_chg_qoq_annualized"], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Balances — Large Banks", "RRELCBM027SBOG",
       "Residential RE Loans, Large Dom. Chartered Commercial Banks (Monthly, SA)", "M", "Bil. of $", "SA", 1,
       "Large-bank residential loan balances (monthly)", "resi_credit_cycle",
       ["pct_chg_yoy", "rolling_12m_avg", "z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Balances — Large Banks", "RRELCBW027SBOG",
       "Residential RE Loans, Large Dom. Chartered Commercial Banks (Weekly, SA)", "W", "Bil. of $", "SA", 2,
       "Large-bank residential loan balances (weekly)", "resi_credit_cycle",
       ["rolling_3m_avg"], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Balances — Large Banks", "RRELCBW027NBOG",
       "Residential RE Loans, Large Dom. Chartered Commercial Banks (Weekly, NSA)", "W", "Bil. of $", "NSA", 3,
       "Large-bank residential loan balances (weekly, NSA)", "resi_credit_cycle",
       [], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Growth", "H8B1221NLGCQG",
       "Residential RE Loans, Large Banks, Quarterly Annualized Growth", "Q", "%", "SA", 1,
       "Large-bank residential growth rate", "resi_credit_cycle",
       ["z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Closed-End Resi", "CRLLCBW027SBOG",
       "Closed-End Residential Loans, Large Banks (Weekly, SA)", "W", "Bil. of $", "SA", 2,
       "Large-bank closed-end residential loans", "resi_credit_cycle",
       ["rolling_3m_avg", "pct_chg_yoy"], _RESI_SHEET),
    _s(_RESI_CAT, "Home Equity", "RHELCBM027SBOG",
       "Revolving Home Equity Loans, Large Banks (Monthly, SA)", "M", "Bil. of $", "SA", 1,
       "Home-equity utilization context", "resi_credit_cycle",
       ["pct_chg_yoy", "rolling_12m_avg"], _RESI_SHEET),
    _s(_RESI_CAT, "Home Equity", "RHELCBM027NBOG",
       "Revolving Home Equity Loans, Large Banks (Monthly, NSA)", "M", "Bil. of $", "NSA", 3,
       "Home-equity (NSA)", "resi_credit_cycle",
       [], _RESI_SHEET),
    _s(_RESI_CAT, "Home Equity", "RHELCBW027SBOG",
       "Revolving Home Equity Loans, Large Banks (Weekly, SA)", "W", "Bil. of $", "SA", 2,
       "Home-equity (weekly)", "resi_credit_cycle",
       ["rolling_3m_avg"], _RESI_SHEET),
    _s(_RESI_CAT, "Home Equity", "RHELCBW027NBOG",
       "Revolving Home Equity Loans, Large Banks (Weekly, NSA)", "W", "Bil. of $", "NSA", 3,
       "Home-equity (weekly, NSA)", "resi_credit_cycle",
       [], _RESI_SHEET),

    # --- Residential credit performance ---
    _s(_RESI_CAT, "Resi Delinquency", "DRSFRMT100S",
       "Delinquency Rate on 1-4 Resi Mortgages, Top 100 Banks (SA)", "Q", "%", "SA", 1,
       "Top-100-bank residential delinquency", "resi_credit_cycle",
       ["pct_chg_yoy", "z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Delinquency", "DRSFRMT100N",
       "Delinquency Rate on 1-4 Resi Mortgages, Top 100 Banks (NSA)", "Q", "%", "NSA", 2,
       "Top-100-bank residential delinquency (NSA)", "resi_credit_cycle",
       [], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Charge-Offs", "CORSFRMT100S",
       "Charge-Off Rate on 1-4 Resi Mortgages, Top 100 Banks (SA)", "Q", "%", "SA", 1,
       "Top-100-bank residential charge-off", "resi_credit_cycle",
       ["pct_chg_yoy", "z_score_5y"], _RESI_SHEET),
    _s(_RESI_CAT, "Resi Charge-Offs", "CORSFRMT100N",
       "Charge-Off Rate on 1-4 Resi Mortgages, Top 100 Banks (NSA)", "Q", "%", "NSA", 2,
       "Top-100-bank residential charge-off (NSA)", "resi_credit_cycle",
       [], _RESI_SHEET),
]


# ===================================================================
# MODULE C — CRE Lending / Underwriting / Credit
# ===================================================================
_CRE_CAT = "CRE Lending / Underwriting / Credit"
_CRE_SHEET = "FRED_CRE"

CRE_SERIES: List[FREDSeriesSpec] = [
    # --- CRE balance / growth ---
    _s(_CRE_CAT, "CRE Balances", "CREACBM027NBOG",
       "CRE Loans, All Commercial Banks (Monthly, NSA)", "M", "Bil. of $", "NSA", 1,
       "Bank-system CRE balance trend", "cre_cycle",
       ["pct_chg_yoy", "rolling_12m_avg", "z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE Balances — Large Banks", "CRELCBM027NBOG",
       "CRE Loans, Large Dom. Chartered Commercial Banks (Monthly, NSA)", "M", "Bil. of $", "NSA", 1,
       "Large-bank CRE balance trend", "cre_cycle",
       ["pct_chg_yoy", "rolling_12m_avg", "z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE Growth — Large Banks", "H8B3219NLGCMG",
       "CRE Loans, Large Banks, Monthly Annualized Growth", "M", "%", "SA", 1,
       "Large-bank CRE growth rate", "cre_cycle",
       ["rolling_12m_avg", "z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "Construction & Land Dev", "CLDLCBM027SBOG",
       "Construction & Land Dev Loans, Large Banks (Monthly, SA)", "M", "Bil. of $", "SA", 2,
       "Large-bank CLD balances", "cre_cycle",
       ["pct_chg_yoy"], _CRE_SHEET),
    _s(_CRE_CAT, "Construction & Land Dev", "CLDLCBM027NBOG",
       "Construction & Land Dev Loans, Large Banks (Monthly, NSA)", "M", "Bil. of $", "NSA", 3,
       "Large-bank CLD balances (NSA)", "cre_cycle",
       [], _CRE_SHEET),
    _s(_CRE_CAT, "Construction & Land Dev", "CLDLCBW027SBOG",
       "Construction & Land Dev Loans, Large Banks (Weekly, SA)", "W", "Bil. of $", "SA", 2,
       "Large-bank CLD (weekly)", "cre_cycle",
       ["rolling_3m_avg"], _CRE_SHEET),
    _s(_CRE_CAT, "Construction & Land Dev", "CLDLCBW027NBOG",
       "Construction & Land Dev Loans, Large Banks (Weekly, NSA)", "W", "Bil. of $", "NSA", 3,
       "Large-bank CLD (weekly, NSA)", "cre_cycle",
       [], _CRE_SHEET),

    # --- CRE SLOOS standards / demand ---
    _s(_CRE_CAT, "CRE SLOOS Standards", "SUBLPDRCSN",
       "Net % Banks Tightening CRE Standards — Nonfarm Nonresidential", "Q", "%", "NSA", 1,
       "CRE underwriting tightening", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Standards", "SUBLPDRCSNLGNQ",
       "Net % Large Banks Tightening CRE Standards — Nonfarm Nonresidential", "Q", "%", "NSA", 1,
       "CRE underwriting tightening, large banks", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Demand", "SUBLPDRCDN",
       "Net % Banks Reporting Stronger CRE Demand — Nonfarm Nonresidential", "Q", "%", "NSA", 1,
       "CRE origination appetite", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Demand", "SUBLPDRCDNLGNQ",
       "Net % Large Banks Reporting Stronger CRE Demand — Nonfarm Nonresidential", "Q", "%", "NSA", 1,
       "CRE origination appetite, large banks", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Standards — Multi", "SUBLPDRCSM",
       "Net % Banks Tightening CRE Standards — Multifamily", "Q", "%", "NSA", 2,
       "Multifamily CRE tightening", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Standards — Multi", "SUBLPDRCSMLGNQ",
       "Net % Large Banks Tightening CRE Standards — Multifamily", "Q", "%", "NSA", 2,
       "Multifamily CRE tightening, large banks", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Demand — Multi", "SUBLPDRCDM",
       "Net % Banks Reporting Stronger CRE Demand — Multifamily", "Q", "%", "NSA", 2,
       "Multifamily CRE demand", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Demand — Multi", "SUBLPDRCDMLGNQ",
       "Net % Large Banks Reporting Stronger CRE Demand — Multifamily", "Q", "%", "NSA", 2,
       "Multifamily CRE demand, large banks", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE SLOOS Demand — C&LD", "SUBLPDRCDC",
       "Net % Banks Reporting Stronger CRE Demand — Construction & Land Dev", "Q", "%", "NSA", 2,
       "CLD demand", "cre_standards_demand",
       ["z_score_5y"], _CRE_SHEET),

    # --- CRE credit quality ---
    _s(_CRE_CAT, "CRE Delinquency", "DRCRELEXFT100S",
       "Delinquency Rate on CRE Loans (Excl. Farmland), Top 100 Banks (SA)", "Q", "%", "SA", 1,
       "CRE delinquency performance", "cre_credit_quality",
       ["pct_chg_yoy", "z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE Delinquency", "DRCRELEXFT100N",
       "Delinquency Rate on CRE Loans (Excl. Farmland), Top 100 Banks (NSA)", "Q", "%", "NSA", 2,
       "CRE delinquency (NSA)", "cre_credit_quality",
       [], _CRE_SHEET),
    _s(_CRE_CAT, "CRE Charge-Offs", "CORCREXFT100S",
       "Charge-Off Rate on CRE Loans (Excl. Farmland), Top 100 Banks (SA)", "Q", "%", "SA", 1,
       "CRE charge-off performance", "cre_credit_quality",
       ["pct_chg_yoy", "z_score_5y"], _CRE_SHEET),
    _s(_CRE_CAT, "CRE Charge-Offs", "CORCREXFT100N",
       "Charge-Off Rate on CRE Loans (Excl. Farmland), Top 100 Banks (NSA)", "Q", "%", "NSA", 2,
       "CRE charge-off (NSA)", "cre_credit_quality",
       [], _CRE_SHEET),
    _s(_CRE_CAT, "CRE Prices", "COMREPUSQ159N",
       "Commercial Real Estate Prices for the United States (YoY %)", "Q", "%", "NSA", 1,
       "CRE collateral value trend", "cre_credit_quality",
       ["z_score_5y", "z_score_10y"], _CRE_SHEET),
]


# ===================================================================
# MODULE D — Case-Shiller Collateral (static seed)
# ===================================================================
# The static seed captures the highest-priority series.
# fred_case_shiller_discovery.py extends this dynamically via release-table API.
_CS_CAT = "Case-Shiller Collateral"
_CS_SHEET = "FRED_CaseShiller_Master"
_CS_SEL_SHEET = "FRED_CaseShiller_Selected"

# Standard release — national / composites / key metros (SA)
CASE_SHILLER_SEED_SERIES: List[FREDSeriesSpec] = [
    # -- National & composites --
    _s(_CS_CAT, "National", "CSUSHPISA",
       "S&P Case-Shiller National HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Collateral trend — national", "cs_collateral_trend",
       ["pct_chg_yoy", "pct_chg_mom", "z_score_5y", "z_score_10y"],
       _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "National", "CSUSHPINSA",
       "S&P Case-Shiller National HPI (NSA)", "M", "Index Jan 2000=100", "NSA", 2,
       "Collateral trend — national (NSA)", "cs_collateral_trend",
       ["pct_chg_yoy"], _CS_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Composite-10", "SPCS10RSA",
       "S&P Case-Shiller 10-City Composite HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Collateral trend — 10-city composite", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Composite-20", "SPCS20RSA",
       "S&P Case-Shiller 20-City Composite HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Collateral trend — 20-city composite", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),

    # -- Key wealth-market metros (SA) --
    _s(_CS_CAT, "Metro — New York", "NYXRSA",
       "Case-Shiller New York HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — NYC", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Los Angeles", "LXXRSA",
       "Case-Shiller Los Angeles HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — LA", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — San Francisco", "SFXRSA",
       "Case-Shiller San Francisco HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — SF", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Miami", "MIXRSA",
       "Case-Shiller Miami HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — Miami", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Washington DC", "WDXRSA",
       "Case-Shiller Washington DC HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — DC", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Boston", "BOXRSA",
       "Case-Shiller Boston HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — Boston", "cs_collateral_trend",
       ["pct_chg_yoy", "z_score_5y"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — San Diego", "SDXRSA",
       "Case-Shiller San Diego HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Affluent-market proxy — San Diego", "cs_collateral_trend",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Seattle", "SEXRSA",
       "Case-Shiller Seattle HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Affluent-market proxy — Seattle", "cs_collateral_trend",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Denver", "DNXRSA",
       "Case-Shiller Denver HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Affluent-market proxy — Denver", "cs_collateral_trend",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Metro — Chicago", "CHXRSA",
       "Case-Shiller Chicago HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Metro benchmark — Chicago", "cs_collateral_trend",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:199159"),

    # -- Sales-pair counts (market liquidity) --
    _s(_CS_CAT, "Sales-Pair Counts", "SPCS10RPSNSA",
       "S&P Case-Shiller 10-City Sales Pair Count (NSA)", "M", "Count", "NSA", 1,
       "Market-liquidity context — 10-city", "cs_sales_pairs",
       ["pct_chg_yoy", "rolling_12m_avg"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Sales-Pair Counts", "SPCS20RPSNSA",
       "S&P Case-Shiller 20-City Sales Pair Count (NSA)", "M", "Count", "NSA", 1,
       "Market-liquidity context — 20-city", "cs_sales_pairs",
       ["pct_chg_yoy", "rolling_12m_avg"], _CS_SEL_SHEET, discovery="release:199159"),
    _s(_CS_CAT, "Sales-Pair Counts", "NYXRPSNSA",
       "Case-Shiller New York Sales Pair Count (NSA)", "M", "Count", "NSA", 2,
       "Market-liquidity context — NYC", "cs_sales_pairs",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:199159"),
]

# Tiered indexes — key wealth-market high-tier / middle-tier seeds
CASE_SHILLER_TIERED_SEED: List[FREDSeriesSpec] = [
    # High Tier SA — priority private-bank collateral
    _s(_CS_CAT, "High Tier — New York", "NYXRHTSA",
       "Case-Shiller New York High Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — NYC high tier", "cs_tiered",
       ["pct_chg_yoy", "spread_vs_CSUSHPISA", "z_score_5y"],
       _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "High Tier — Los Angeles", "LXXRHTSA",
       "Case-Shiller Los Angeles High Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — LA high tier", "cs_tiered",
       ["pct_chg_yoy", "spread_vs_CSUSHPISA", "z_score_5y"],
       _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "High Tier — San Francisco", "SFXRHTSA",
       "Case-Shiller San Francisco High Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — SF high tier", "cs_tiered",
       ["pct_chg_yoy", "spread_vs_CSUSHPISA", "z_score_5y"],
       _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "High Tier — Miami", "MIXRHTSA",
       "Case-Shiller Miami High Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — Miami high tier", "cs_tiered",
       ["pct_chg_yoy", "spread_vs_CSUSHPISA"],
       _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "High Tier — Washington DC", "WDXRHTSA",
       "Case-Shiller Washington DC High Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — DC high tier", "cs_tiered",
       ["pct_chg_yoy", "spread_vs_CSUSHPISA"],
       _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "High Tier — Boston", "BOXRHTSA",
       "Case-Shiller Boston High Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 1,
       "Affluent-market proxy — Boston high tier", "cs_tiered",
       ["pct_chg_yoy", "spread_vs_CSUSHPISA"],
       _CS_SEL_SHEET, discovery="release:345173"),

    # Middle Tier SA — comp context
    _s(_CS_CAT, "Middle Tier — New York", "NYXRMTSA",
       "Case-Shiller New York Middle Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Mid-market comp — NYC", "cs_tiered",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "Middle Tier — Los Angeles", "LXXRMTSA",
       "Case-Shiller Los Angeles Middle Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Mid-market comp — LA", "cs_tiered",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:345173"),
    _s(_CS_CAT, "Middle Tier — San Francisco", "SFXRMTSA",
       "Case-Shiller San Francisco Middle Tier HPI (SA)", "M", "Index Jan 2000=100", "SA", 2,
       "Mid-market comp — SF", "cs_tiered",
       ["pct_chg_yoy"], _CS_SEL_SHEET, discovery="release:345173"),
]


# ===================================================================
# MASTER REGISTRY — single flat list for the ingestion engine
# ===================================================================
FRED_EXPANSION_REGISTRY: List[FREDSeriesSpec] = (
    SBL_CORE_SERIES
    + RESIDENTIAL_SERIES
    + CRE_SERIES
    + CASE_SHILLER_SEED_SERIES
    + CASE_SHILLER_TIERED_SEED
)


# ---------------------------------------------------------------------------
# Convenience accessors
# ---------------------------------------------------------------------------
def get_series_by_sheet(sheet_name: str) -> List[FREDSeriesSpec]:
    """Return all registry entries routed to a given Excel sheet."""
    return [s for s in FRED_EXPANSION_REGISTRY if s.sheet == sheet_name]


def get_series_by_priority(max_priority: int = 2) -> List[FREDSeriesSpec]:
    """Return active series at or above the given priority threshold."""
    return [s for s in FRED_EXPANSION_REGISTRY
            if s.is_active and s.priority <= max_priority]


def get_series_ids_to_fetch(max_priority: int = 3) -> List[str]:
    """Return deduplicated list of FRED series IDs to fetch."""
    seen = set()
    out = []
    for s in FRED_EXPANSION_REGISTRY:
        if s.is_active and s.priority <= max_priority and s.series_id not in seen:
            seen.add(s.series_id)
            out.append(s.series_id)
    return out


def get_chart_group_series(chart_group: str) -> List[FREDSeriesSpec]:
    """Return all series assigned to a given chart group."""
    return [s for s in FRED_EXPANSION_REGISTRY if s.chart_group == chart_group]


def registry_to_dataframe() -> pd.DataFrame:
    """Export the full registry as a DataFrame for audit / Excel output."""
    rows = [asdict(s) for s in FRED_EXPANSION_REGISTRY]
    df = pd.DataFrame(rows)
    df["transformations"] = df["transformations"].apply(lambda x: ", ".join(x) if x else "")
    return df


def validate_registry() -> Dict[str, List[str]]:
    """
    Run validation checks on the registry.
    Returns dict of issue_type → list of descriptions.
    """
    issues: Dict[str, List[str]] = {
        "duplicate_series_ids": [],
        "missing_metadata": [],
        "inactive_in_priority_1": [],
        "orphan_sheet_routing": [],
    }

    seen_ids: Dict[str, int] = {}
    valid_sheets = {
        _SBL_SHEET, _RESI_SHEET, _CRE_SHEET, _CS_SHEET, _CS_SEL_SHEET,
    }

    for spec in FRED_EXPANSION_REGISTRY:
        # Duplicate check
        if spec.series_id in seen_ids:
            issues["duplicate_series_ids"].append(
                f"{spec.series_id} appears {seen_ids[spec.series_id] + 1} times"
            )
        seen_ids[spec.series_id] = seen_ids.get(spec.series_id, 0) + 1

        # Missing metadata
        for fld in ("display_name", "freq", "units", "use_case"):
            if not getattr(spec, fld, ""):
                issues["missing_metadata"].append(
                    f"{spec.series_id}: missing {fld}"
                )

        # Inactive at priority 1
        if spec.priority == 1 and not spec.is_active:
            issues["inactive_in_priority_1"].append(spec.series_id)

        # Sheet routing
        if spec.sheet and spec.sheet not in valid_sheets:
            issues["orphan_sheet_routing"].append(
                f"{spec.series_id} → {spec.sheet}"
            )

    # Clean up: remove empty categories
    return {k: v for k, v in issues.items() if v}


# ---------------------------------------------------------------------------
# Sheets that the ingestion engine should create
# ---------------------------------------------------------------------------
OUTPUT_SHEET_MAP = {
    "FRED_SBL_Backdrop": "SBL / Market-Collateral Proxy series + derived spreads / regime flags",
    "FRED_Residential_Jumbo": "Jumbo rates, jumbo SLOOS, residential balances, delinquency, charge-offs",
    "FRED_CRE": "CRE balances, CLD, CRE SLOOS, CRE delinquency, CRE charge-offs, CRE prices",
    "FRED_CaseShiller_Master": "Full discovered Case-Shiller registry (release, tier, metro, SA/NSA, units)",
    "FRED_CaseShiller_Selected": "Curated subset used in production dashboard visuals",
}


# Quick self-test
if __name__ == "__main__":
    print(f"Total series in registry: {len(FRED_EXPANSION_REGISTRY)}")
    print(f"Unique series IDs to fetch: {len(get_series_ids_to_fetch())}")
    print(f"\nBy sheet:")
    for sheet in OUTPUT_SHEET_MAP:
        n = len(get_series_by_sheet(sheet))
        print(f"  {sheet}: {n} series")
    print(f"\nBy priority:")
    for p in (1, 2, 3):
        n = len([s for s in FRED_EXPANSION_REGISTRY if s.priority == p])
        print(f"  P{p}: {n} series")
    issues = validate_registry()
    if issues:
        print(f"\nValidation issues:")
        for k, v in issues.items():
            print(f"  {k}: {len(v)} issues")
            for item in v[:3]:
                print(f"    - {item}")
    else:
        print("\nRegistry validation: CLEAN")
