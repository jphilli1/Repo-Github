import os
import sys
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from pathlib import Path
import warnings

import requests
import pandas as pd
import numpy as np
from scipy import stats
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

# ==================================================================================
#  1. SCRIPT CONFIGURATION & SETUP
# ==================================================================================

def setup_logging(log_dir: str = "logs") -> logging.Logger:
    """Setup comprehensive logging configuration."""
    Path(log_dir).mkdir(exist_ok=True)
    log_filename = f"{log_dir}/bank_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_filename}")
    return logger

logger = setup_logging()

@dataclass
class DashboardConfig:
    """Clean configuration class to hold runtime parameters."""
    fred_api_key: str
    subject_bank_cert: int
    peer_bank_certs: List[int]
    quarters_back: int = 50
    fred_years_back: int = 25
    output_dir: str = "output"
    fdic_api_base: str = "https://banks.data.fdic.gov/api"
    fred_api_base: str = "https://api.stlouisfed.org/fred"

# ==================================================================================
#  2. GLOBAL DATA DICTIONARIES & CONSTANTS
# ==================================================================================
FDIC_FIELDS_TO_FETCH = [
    "CERT", "NAME", "REPDTE", "ASSET", "DEP", "LIAB", "EQ", "LNLSNET", "ROA", "ROE", "NIMY",
    "EEFFR", "NONIIAY", "ELNATRY", "RBCT1CER", "RBC1RWAJ", "RBCRWAJ", "RBC1AAJ", "LNATRES",
    "NCLNLS", "NTLNLS", "P3LNLS", "P9LNLS", "LNLS", "EINTEXP", "FREPP", "OTHBOR",
    "RCONLG28", # Allowance for Credit Losses on Off-Balance Sheet Credit Exposures - Note: May be sparse.
    "TIER1",    # [CORRECTED] Tier 1 Capital
    "TIER2",    # [CORRECTED] Tier 2 Capital
    # Loan Categories
    "LNCI", "LNRENROW", "LNRECONS", "LNREMULT", "LNRENROT", "LNREAG", "LNRERES", "LNRELOC",
    "LNCON", "LNCRCD", "LNAUTO", "LNOTHER", "LS", "LNAG",
    # Add series for Non-Owner-Occupied (Income-Producing) Nonfarm Nonresidential - Note: May be sparse.
    "LNREOTH", "NTREOTH", "P9REOTH", "NAREOTH", "P3REOTH",
    # NCOs
    "NTCI", "NTRECONS", "NTREMULT", "NTRENROT", "NTREAG", "NTRERES", "NTRELOC", "NTRENROW",
    "NTCON", "NTCRCD", "NTAUTO", "NTLS", "NTOTHER", "NTAG",
    # Past Due 30-89
    "P3CI", "P3RENROW", "P3RECONS", "P3REMULT", "P3RENROT", "P3REAG", "P3RERES",
    "P3RELOC", "P3CON", "P3CRCD", "P3AUTO", "P3OTHLN", "P3LS", "P3AG",
    # Past Due 90+
    "P9CI", "P9RENROW", "P9RECONS", "P9REMULT", "P9RENROT", "P9REAG", "P9RERES",
    "P9RELOC", "P9CON", "P9CRCD", "P9AUTO", "P9OTHLN", "P9LS", "P9AG",
    # Nonaccrual
    "NACI", "NARENROW", "NARECONS", "NAREMULT", "NARENROT", "NAREAG", "NARERES",
    "NARELOC", "NACON", "NACRCD", "NAAUTO", "NAOTHLN", "NALS", "NAAG"
]
FDIC_FIELD_DESCRIPTIONS = {
    # Key Balance Sheet & Income Statement
    "ASSET":    {"short": "Total Assets", "long": "Total assets held by the institution."},
    "DEP":      {"short": "Total Deposits", "long": "Total deposits held by the institution."},
    "LIAB":     {"short": "Total Liabilities", "long": "Total liabilities of the institution."},
    "EQ":       {"short": "Total Equity Capital", "long": "Total equity capital of the institution."},
    "LNLS":     {"short": "Gross Loans & Leases", "long": "Total loans and leases, gross, before deducting the allowance for loan and lease losses."},
    "LNATRES":  {"short": "Loan Loss Allowance", "long": "Allowance for Loan and Lease Losses."},
    "RCONLG28": {"short": "Allowance for Off-Balance Sheet Exposures", "long": "Allowance for Credit Losses on Off-Balance Sheet Credit Exposures."},
    "NONIIAY":  {"short": "Noninterest Income (YTD)", "long": "Total noninterest income, year-to-date."},
    "ELNATRY":  {"short": "Noninterest Expense (YTD)", "long": "Total noninterest expense, year-to-date."},
    "EINTEXP":  {"short": "Interest Expense (YTD)", "long": "Total interest expense, year-to-date."},
    "Total_ACL":{"short": "Total Allowance for Credit Losses", "long": "Sum of the allowance for on-balance-sheet loans and off-balance-sheet credit exposures."},
    "LNREOTH":  {"short": "Income-Prod. CRE Bal", "long": "Loans Secured by Other Nonfarm Nonresidential Properties (Income-Producing)."},


    # Key Ratios & Capital
    "ROA":      {"short": "ROA", "long": "Return on Assets, annualized."},
    "ROE":      {"short": "ROE", "long": "Return on Equity, annualized."},
    "NIMY":     {"short": "Net Interest Margin", "long": "Net Interest Margin, annualized, year-to-date."},
    "EEFFR":    {"short": "Efficiency Ratio", "long": "Efficiency Ratio."},
    "RBCT1CER": {"short": "CET1 Capital Ratio", "long": "Common Equity Tier 1 Capital Ratio."},
    # Capital
    "RCFD7204": {"short": "Tier 1 Capital", "long": "Tier 1 capital as defined for regulatory purposes."},
    "RCFD7205": {"short": "Tier 2 Capital", "long": "Tier 2 capital as defined for regulatory purposes."},
    
    "NTLNLS":   {"short": "Total Net Charge-Offs (YTD)", "long": "Total net charge-offs on loans and leases, year-to-date."},
    "P3LNLS":   {"short": "Total Loans PD 30-89 Days", "long": "Total loans and leases 30-89 days past due."},
    "P9LNLS":   {"short": "Total Loans PD 90+ Days", "long": "Total loans and leases 90 or more days past due."},
    # ===== DERIVED METRICS (MODIFIED) =====
    # Capital
    "TIER1":    {"short": "Tier 1 Capital", "long": "Tier 1 risk-based capital."},
    "TIER2":    {"short": "Tier 2 Capital", "long": "Tier 2 risk-based capital."},
    "RBC1AAJ":  {"short": "Tier 1 Capital (Leverage)", "long": "Tier 1 Capital (for Leverage Ratio)."},
    
    # Asset Quality - Top of House
    "NCLNLS":   {"short": "Total Noncurrent Loans", "long": "Total loans and leases past due 90 days or more plus nonaccrual loans."},
    
    # ===== DERIVED METRICS (MODIFIED & NEW) =====
    "Cost_of_Funds": {"short": "Cost of Funds", "long": "Annualized cost of interest-bearing liabilities (Quarterly Interest Expense * 4 / Average Interest-Bearing Liabilities)."},
    "Allowance_to_Gross_Loans_Rate": {"short": "Total Allowance / Gross Loans", "long": "Total Allowance for Credit Losses (on- and off-balance sheet) as a percentage of gross loans."},
    "TTM_NCO_Rate": {"short": "TTM NCO / Avg Loans", "long": "Trailing 12-Month sum of net charge-offs as a percentage of TTM average gross loans."},
    "TTM_PD30_Rate": {"short": "TTM PD 30-89 / Avg Loans", "long": "Trailing 12-Month average of loans 30-89 days past due as a percentage of TTM average gross loans."},
    "TTM_PD90_Rate": {"short": "TTM PD 90+ / Avg Loans", "long": "Trailing 12-Month average of loans 90+ days past due as a percentage of TTM average gross loans."},
    "TTM_Past_Due_Rate": {"short": "Total TTM PD Rate", "long": "Trailing 12-Month average of total loans 30+ days past due as a percentage of TTM average gross loans."},
    "Nonaccrual_to_Gross_Loans_Rate": {"short": "Nonaccrual / Gross Loans", "long": "Total nonaccrual loans as a percentage of total gross loans."},

    # Granular TTM PD Rates
    "IDB_CI_TTM_PD30_Rate": {"short": "TTM C&I PD 30-89 Rate", "long": "TTM avg C&I loans 30-89 days past due as a % of TTM avg C&I loans."},
    "IDB_CI_TTM_PD90_Rate": {"short": "TTM C&I PD 90+ Rate", "long": "TTM avg C&I loans 90+ days past due as a % of TTM avg C&I loans."},
    "IDB_CRE_TTM_PD30_Rate": {"short": "TTM CRE PD 30-89 Rate", "long": "TTM avg CRE loans 30-89 days past due as a % of TTM avg CRE loans."},
    "IDB_CRE_TTM_PD90_Rate": {"short": "TTM CRE PD 90+ Rate", "long": "TTM avg CRE loans 90+ days past due as a % of TTM avg CRE loans."},
    "IDB_Consumer_TTM_PD30_Rate": {"short": "TTM Consumer PD 30-89 Rate", "long": "TTM avg Consumer loans 30-89 days past due as a % of TTM avg Consumer loans."},
    "IDB_Consumer_TTM_PD90_Rate": {"short": "TTM Consumer PD 90+ Rate", "long": "TTM avg Consumer loans 90+ days past due as a % of TTM avg Consumer loans."},
    "IDB_Resi_TTM_PD30_Rate": {"short": "TTM Resi. PD 30-89 Rate", "long": "TTM avg Residential loans 30-89 days past due as a % of TTM avg Residential loans."},
    "IDB_Resi_TTM_PD90_Rate": {"short": "TTM Resi. PD 90+ Rate", "long": "TTM avg Residential loans 90+ days past due as a % of TTM avg Residential loans."},
    "IDB_Other_TTM_PD30_Rate": {"short": "TTM Other PD 30-89 Rate", "long": "TTM avg Other loans 30-89 days past due as a % of TTM avg Other loans."},
    "IDB_Other_TTM_PD90_Rate": {"short": "TTM Other PD 90+ Rate", "long": "TTM avg Other loans 90+ days past due as a % of TTM avg Other loans."},

    # [MODIFIED & NEW] Loan Composition and Growth Metrics
    "IDB_CI_Composition": {"short": "C&I Comp.", "long": "Commercial & Industrial (including Owner-Occupied CRE) loans as a percentage of total gross loans."},
    "IDB_CRE_Composition": {"short": "CRE Comp.", "long": "Commercial Real Estate (Construction, Multifamily, Farmland, Income-Producing) loans as a percentage of total gross loans."},
    "IDB_Consumer_Composition": {"short": "Consumer Comp.", "long": "Consumer loans as a percentage of total gross loans."},
    "IDB_Resi_Composition": {"short": "Residential Comp.", "long": "Residential Real Estate (including 1-4 Family Investor) loans as a percentage of total gross loans."},
    "IDB_Other_Composition": {"short": "Other Comp.", "long": "Other loans and leases as a percentage of total gross loans."},
    "IDB_CRE_Growth_TTM": {"short": "TTM CRE Growth", "long": "Trailing 12-Month (Year-over-Year) growth rate of the Commercial Real Estate loan portfolio."},
    "IDB_CRE_Growth_36M": {"short": "36M CRE Growth", "long": "36-Month (3-Year) growth rate of the Commercial Real Estate loan portfolio."},
    "CRE_Concentration_Capital_Risk": {"short": "CRE / (T1C + Total ACL)", "long": "Total Commercial Real Estate Loans as a percentage of Tier 1 Capital plus the Total Allowance for Credit Losses."},
    
    # ===== LOAN BALANCES =====
    "LNCI":     {"short": "C&I Loan Balances", "long": "Commercial and Industrial Loans."},
    "LNRENROW": {"short": "1-4 Family CRE Non-Owner Bal", "long": "Loans Secured by 1-4 Family Residential Properties (Non-owner Occupied)."},
    "LNRECONS": {"short": "Construction & Dev Loan Bal", "long": "Construction and Land Development Loans."},
    "LNREMULT": {"short": "Multifamily CRE Loan Bal", "long": "Loans Secured by Multifamily (5 or more) Residential Properties."},
    "LNRENROT": {"short": "Nonfarm CRE Owner-Occ Bal", "long": "Loans Secured by Nonfarm Nonresidential Properties (Owner-Occupied)."},
    "LNREAG":   {"short": "Farmland Loan Bal", "long": "Loans Secured by Farmland."},
    "LNRERES":  {"short": "1-4 Family Resi Loan Bal", "long": "Loans Secured by 1-4 Family Residential Properties."},
    "LNRELOC":  {"short": "HELOC Balances", "long": "Home Equity Lines of Credit."},
    "LNCON":    {"short": "Consumer Loan Balances", "long": "Loans to Individuals for Household, Family, and Other Personal Expenditures."},
    "LNCRCD":   {"short": "Credit Card Loan Balances", "long": "Credit Card Loans."},
    "LNAUTO":   {"short": "Auto Loan Balances", "long": "Automobile Loans."},
    "LNOTHER":  {"short": "Other Loan Balances", "long": "Other Loans."},
    "LS":       {"short": "Lease Balances", "long": "Lease Financing Receivables."},
    "LNAG":     {"short": "Agricultural Loan Balances", "long": "Agricultural Loans."},

    # ===== NET CHARGE-OFFS (YTD) BY CATEGORY =====
    "NTCI":     {"short": "C&I NCOs", "long": "Net Charge-Offs on Commercial and Industrial Loans."},
    "NTRECONS": {"short": "Construction & Dev NCOs", "long": "Net Charge-Offs on Construction and Land Development Loans."},
    "NTREMULT": {"short": "Multifamily CRE NCOs", "long": "Net Charge-Offs on Loans Secured by Multifamily Residential Properties."},
    "NTRENROT": {"short": "Nonfarm CRE Owner-Occ NCOs", "long": "Net Charge-Offs on Loans Secured by Nonfarm Nonresidential Properties (Owner-Occupied)."},
    "NTREAG":   {"short": "Farmland NCOs", "long": "Net Charge-Offs on Loans Secured by Farmland."},
    "NTRERES":  {"short": "1-4 Family Resi NCOs", "long": "Net Charge-Offs on Loans Secured by 1-4 Family Residential Properties."},
    "NTRELOC":  {"short": "HELOC NCOs", "long": "Net Charge-Offs on Home Equity Lines of Credit."},
    "NTCON":    {"short": "Consumer NCOs", "long": "Net Charge-Offs on Loans to Individuals."},
    "NTCRCD":   {"short": "Credit Card NCOs", "long": "Net Charge-Offs on Credit Card Loans."},
    "NTAUTO":   {"short": "Auto Loan NCOs", "long": "Net Charge-Offs on Automobile Loans."},
    "NTLS":     {"short": "Lease NCOs", "long": "Net Charge-Offs on Leases."},
    "NTOTHER":  {"short": "Other Loan NCOs", "long": "Net Charge-Offs on Other Loans."},
    "NTAG":     {"short": "Agricultural NCOs", "long": "Net Charge-Offs on Agricultural Loans."},

    # ===== PAST DUE 30-89 DAYS BY CATEGORY =====
    "P3CI":     {"short": "C&I PD 30-89", "long": "Commercial and Industrial Loans 30-89 Days Past Due."},
    "P3RENROW": {"short": "1-4 Fam CRE Non-Owner PD 30-89", "long": "Loans Secured by 1-4 Family Residential (Non-owner Occupied) 30-89 Days Past Due."},
    "P3RECONS": {"short": "Construction PD 30-89", "long": "Construction and Land Development Loans 30-89 Days Past Due."},
    "P3REMULT": {"short": "Multifamily PD 30-89", "long": "Loans Secured by Multifamily Residential Properties 30-89 Days Past Due."},
    "P3RENROT": {"short": "Nonfarm CRE Owner-Occ PD 30-89", "long": "Loans Secured by Nonfarm Nonresidential (Owner-Occupied) 30-89 Days Past Due."},
    "P3REAG":   {"short": "Farmland PD 30-89", "long": "Loans Secured by Farmland 30-89 Days Past Due."},
    "P3RERES":  {"short": "1-4 Family Resi PD 30-89", "long": "Loans Secured by 1-4 Family Residential Properties 30-89 Days Past Due."},
    "P3RELOC":  {"short": "HELOC PD 30-89", "long": "Home Equity Lines of Credit 30-89 Days Past Due."},
    "P3CON":    {"short": "Consumer PD 30-89", "long": "Loans to Individuals 30-89 Days Past Due."},
    "P3CRCD":   {"short": "Credit Card PD 30-89", "long": "Credit Card Loans 30-89 Days Past Due."},
    "P3AUTO":   {"short": "Auto PD 30-89", "long": "Automobile Loans 30-89 Days Past Due."},
    "P3OTHLN":  {"short": "Other Loans PD 30-89", "long": "Other Loans 30-89 Days Past Due."},
    "P3LS":     {"short": "Leases PD 30-89", "long": "Leases 30-89 Days Past Due."},
    "P3AG":     {"short": "Agricultural PD 30-89", "long": "Agricultural Loans 30-89 Days Past Due."},

    # ===== PAST DUE 90+ DAYS BY CATEGORY =====
    "P9CI":     {"short": "C&I PD 90+", "long": "Commercial and Industrial Loans 90+ Days Past Due."},
    "P9RENROW": {"short": "1-4 Fam CRE Non-Owner PD 90+", "long": "Loans Secured by 1-4 Family Residential (Non-owner Occupied) 90+ Days Past Due."},
    "P9RECONS": {"short": "Construction PD 90+", "long": "Construction and Land Development Loans 90+ Days Past Due."},
    "P9REMULT": {"short": "Multifamily PD 90+", "long": "Loans Secured by Multifamily Residential Properties 90+ Days Past Due."},
    "P9RENROT": {"short": "Nonfarm CRE Owner-Occ PD 90+", "long": "Loans Secured by Nonfarm Nonresidential (Owner-Occupied) 90+ Days Past Due."},
    "P9REAG":   {"short": "Farmland PD 90+", "long": "Loans Secured by Farmland 90+ Days Past Due."},
    "P9RERES":  {"short": "1-4 Family Resi PD 90+", "long": "Loans Secured by 1-4 Family Residential Properties 90+ Days Past Due."},
    "P9RELOC":  {"short": "HELOC PD 90+", "long": "Home Equity Lines of Credit 90+ Days Past Due."},
    "P9CON":    {"short": "Consumer PD 90+", "long": "Loans to Individuals 90+ Days Past Due."},
    "P9CRCD":   {"short": "Credit Card PD 90+", "long": "Credit Card Loans 90+ Days Past Due."},
    "P9AUTO":   {"short": "Auto PD 90+", "long": "Automobile Loans 90+ Days Past Due."},
    "P9OTHLN":  {"short": "Other Loans PD 90+", "long": "Other Loans 90+ Days Past Due."},
    "P9LS":     {"short": "Leases PD 90+", "long": "Leases 90+ Days Past Due."},
    "P9AG":     {"short": "Agricultural PD 90+", "long": "Agricultural Loans 90+ Days Past Due."},
    
    # ===== NONACCRUAL BY CATEGORY =====
    "NACI":     {"short": "C&I Nonaccrual", "long": "Commercial and Industrial Loans on Nonaccrual Status."},
    "NARENROW": {"short": "1-4 Fam CRE Non-Owner Nonaccrual", "long": "Loans Secured by 1-4 Family Residential (Non-owner Occupied) on Nonaccrual Status."},
    "NARECONS": {"short": "Construction Nonaccrual", "long": "Construction and Land Development Loans on Nonaccrual Status."},
    "NAREMULT": {"short": "Multifamily Nonaccrual", "long": "Loans Secured by Multifamily Residential Properties on Nonaccrual Status."},
    "NARENROT": {"short": "Nonfarm CRE Owner-Occ Nonaccrual", "long": "Loans Secured by Nonfarm Nonresidential (Owner-Occupied) on Nonaccrual Status."},
    "NAREAG":   {"short": "Farmland Nonaccrual", "long": "Loans Secured by Farmland on Nonaccrual Status."},
    "NARERES":  {"short": "1-4 Family Resi Nonaccrual", "long": "Loans Secured by 1-4 Family Residential Properties on Nonaccrual Status."},
    "NARELOC":  {"short": "HELOC Nonaccrual", "long": "Home Equity Lines of Credit on Nonaccrual Status."},
    "NACON":    {"short": "Consumer Nonaccrual", "long": "Loans to Individuals on Nonaccrual Status."},
    "NACRCD":   {"short": "Credit Card Nonaccrual", "long": "Credit Card Loans on Nonaccrual Status."},
    "NAAUTO":   {"short": "Auto Nonaccrual", "long": "Automobile Loans on Nonaccrual Status."},
    "NAOTHLN":  {"short": "Other Loans Nonaccrual", "long": "Other Loans on Nonaccrual Status."},
    "NALS":     {"short": "Leases Nonaccrual", "long": "Leases on Nonaccrual Status."},
    "NAAG":     {"short": "Agricultural Nonaccrual", "long": "Agricultural Loans on Nonaccrual Status."},
}

LOAN_CATEGORIES = {
    "CI":       {"balance": ["LNCI", "LNRENROT"], "nco": ["NTCI", "NTRENROT"], "pd30": ["P3CI", "P3RENROT"], "pd90": ["P9CI", "P9RENROT"], "na": ["NACI", "NARENROT"]},
    "CRE":      {"balance": ["LNRECONS", "LNREMULT", "LNREAG", "LNREOTH"], "nco": ["NTRECONS", "NTREMULT", "NTREAG", "NTREOTH"], "pd30": ["P3RECONS", "P3REMULT", "P3REAG", "P3REOTH"], "pd90": ["P9RECONS", "P9REMULT", "P9REAG", "P9REOTH"], "na": ["NARECONS", "NAREMULT", "NAREAG", "NAREOTH"]},
    "Consumer": {"balance": ["LNCON", "LNCRCD", "LNAUTO"], "nco": ["NTCON", "NTCRCD", "NTAUTO"], "pd30": ["P3CON", "P3CRCD", "P3AUTO"], "pd90": ["P9CON", "P9CRCD", "P9AUTO"], "na": ["NACON", "NACRCD", "NAAUTO"]},
    "Resi":     {"balance": ["LNRERES", "LNRELOC", "LNRENROW"], "nco": ["NTRERES", "NTRELOC", "NTRENROW"], "pd30": ["P3RERES", "P3RELOC", "P3RENROW"], "pd90": ["P9RERES", "P9RELOC", "P9RENROW"], "na": ["NARERES", "NARELOC", "NARENROW"]},
    "Other":    {"balance": ["LNOTHER", "LS", "LNAG"], "nco": ["NTOTHER", "NTLS", "NTAG"], "pd30": ["P3OTHLN", "P3LS", "P3AG"], "pd90": ["P9OTHLN", "P9LS", "P9AG"], "na": ["NAOTHLN", "NALS", "NAAG"]}
}
FRED_SERIES_TO_FETCH = {
    "Key Economic Indicators": {
        "GDPC1":    {"short": "Real GDP", "long": "Real Gross Domestic Product"},
        "A191RL1Q225SBEA": {"short": "Real GDP Growth", "long": "Real Gross Domestic Product, Percent Change from Preceding Period"},
        "UNRATE":   {"short": "Unemployment Rate", "long": "Civilian Unemployment Rate"},
        "CPIAUCSL": {"short": "CPI Inflation", "long": "Consumer Price Index for All Urban Consumers: All Items"},
        "UMCSENT":  {"short": "Consumer Sentiment", "long": "University of Michigan: Consumer Sentiment"},
        "ICSA":     {"short": "Initial Jobless Claims", "long": "Initial Claims, Insured Unemployment"},
        "USSLIND":  {"short": "Leading Economic Index", "long": "Conference Board Leading Economic Index for the United States"},
        "RSXFS":    {"short": "Retail Sales", "long": "Advance Retail Sales: Retail Trade"}
    },
    "Interest Rates & Yield Curve": {
        "FEDFUNDS": {"short": "Effective Fed Funds", "long": "Federal Funds Effective Rate"},
        "DFF":      {"short": "Fed Funds Rate", "long": "Federal Funds Effective Rate (Daily)"},
        "DPRIME":   {"short": "Bank Prime Loan Rate", "long": "Bank Prime Loan Rate"},
        "MORTGAGE30US": {"short": "30Y Mortgage Rate", "long": "30-Year Fixed Rate Mortgage Average in the United States"},
        "DGS30":    {"short": "30-Yr Treasury", "long": "Market Yield on U.S. Treasury at 30-Year Constant Maturity"},
        "DGS20":    {"short": "20-Yr Treasury", "long": "Market Yield on U.S. Treasury at 20-Year Constant Maturity"},
        "DGS10":    {"short": "10-Yr Treasury", "long": "Market Yield on U.S. Treasury at 10-Year Constant Maturity"},
        "DGS7":     {"short": "7-Yr Treasury", "long": "Market Yield on U.S. Treasury at 7-Year Constant Maturity"},
        "DGS5":     {"short": "5-Yr Treasury", "long": "Market Yield on U.S. Treasury at 5-Year Constant Maturity"},
        "DGS3":     {"short": "3-Yr Treasury", "long": "Market Yield on U.S. Treasury at 3-Year Constant Maturity"},
        "DGS2":     {"short": "2-Yr Treasury", "long": "Market Yield on U.S. Treasury at 2-Year Constant Maturity"},
        "DGS1":     {"short": "1-Yr Treasury", "long": "Market Yield on U.S. Treasury at 1-Year Constant Maturity"},
        "DGS6MO":   {"short": "6-Mo Treasury", "long": "Market Yield on U.S. Treasury at 6-Month Constant Maturity"},
        "DGS3MO":   {"short": "3-Mo Treasury", "long": "Market Yield on U.S. Treasury at 3-Month Constant Maturity"},
        "DGS1MO":   {"short": "1-Mo Treasury", "long": "Market Yield on U.S. Treasury at 1-Month Constant Maturity"},
        "T10Y2Y":   {"short": "10Y-2Y Spread", "long": "10-Year Minus 2-Year Treasury Spread"},
        "T10Y3M":   {"short": "10Y-3M Spread", "long": "10-Year Minus 3-Month Treasury Spread"}
    },
    "Credit Spreads & Lending Standards": {
        "DBAA":       {"short": "Baa Corp. Yield", "long": "Moody's Seasoned Baa Corporate Bond Yield"},
        "BAMLH0A0HYM2": {"short": "High-Yield Spread", "long": "ICE BofA US High Yield Index Option-Adjusted Spread"},
        "BAMLH0A0HYM2EY": {"short": "HY Effective Yield", "long": "ICE BofA US High Yield Index Effective Yield"},
        "BAMLH0A1HYBB": {"short": "BB-Rated Spread", "long": "ICE BofA BB US High Yield Index Option-Adjusted Spread"},
        "BAMLH0A2HYB":  {"short": "B-Rated Spread", "long": "ICE BofA B US High Yield Index Option-Adjusted Spread"},
        "BAMLH0A3HYC":  {"short": "CCC-Rated Spread", "long": "ICE BofA CCC & Lower US High Yield Index Option-Adjusted Spread"},
        "BAMLC0A0CM":   {"short": "IG Corp. Spread", "long": "ICE BofA US Corporate Index Option-Adjusted Spread"},
        "BAMLC0A1CAAAEY": {"short": "AAA-Rated Eff. Yield", "long": "ICE BofA AAA US Corporate Index Effective Yield"},
        "BAMLC0A4CBBB": {"short": "BBB-Rated Spread", "long": "ICE BofA BBB US Corporate Index Option-Adjusted Spread"},
        "DRBLACBS":   {"short": "Biz Loan Delinquency", "long": "Delinquency Rate on Business Loans, All Commercial Banks"},
        "DRTSCILM":   {"short": "C&I Lending Standards", "long": "Net Pct of Banks Tightening Standards for C&I Loans"},
        "DRTSCLCC":   {"short": "CRE Lending Standards", "long": "Net Pct of Banks Tightening Standards for CRE Loans"}
    },
    "Financial Stress & Risk": {
        "STLFSI4":      {"short": "Financial Stress Index", "long": "St. Louis Fed Financial Stress Index"},
        "VIXCLS":       {"short": "VIX", "long": "CBOE Volatility Index"},
        "NFCI":         {"short": "Nat'l Financial Conditions", "long": "Chicago Fed National Financial Conditions Index"}
    },
    "Global Benchmarks": {
        "FPCPITOTLZGWLD": {"short": "World Inflation", "long": "Inflation, CPI for World"},
        "FPCPITOTLZGHIC": {"short": "High-Income Inflation", "long": "Inflation, CPI for High-Income Countries"},
        "FPCPITOTLZGLMY": {"short": "Low/Mid-Income Inflation", "long": "Inflation, CPI for Low & Middle Income Countries"},
        "NYGDPMKTPCDWLD": {"short": "World GDP", "long": "GDP at Market Prices for World"}
    },
    "Real Estate & Housing": {
        "HOUST":         {"short": "Housing Starts", "long": "New Privately-Owned Housing Units Started"},
        "PERMIT":        {"short": "Building Permits", "long": "New Private Housing Units Authorized by Building Permits"},
        "NYXRSA": {"short": "NY Metro Home Price Index", "long": "S&P/Case-Shiller NY Home Price Index"},
        "LXXRSA": {"short": "LA Metro Home Price Index", "long": "S&P/Case-Shiller LA Home Price Index"},
        "MIXRNSA": {"short": "Miami Metro Home Price Index", "long": "S&P/Case-Shiller Miami Home Price Index"},
        "CASTHPI": {"short": "California HPI", "long": "All-Transactions House Price Index for California"},
        "FLSTHPI": {"short": "Florida HPI", "long": "All-Transactions House Price Index for Florida"}
    },
    "Banking Sector Aggregates": {
        "BUSLOANS":   {"short": "Business Loans (All Banks)", "long": "Business Loans, All Commercial Banks"},
        "REALLN":     {"short": "Real Estate Loans (All Banks)", "long": "Real Estate Loans, All Commercial Banks"},
        "CONSUMER":   {"short": "Consumer Loans (All Banks)", "long": "Consumer Loans, All Commercial Banks"},
        "DPSACBW027SBOG": {"short": "Total Deposits (All Banks)", "long": "Total Deposits, All Commercial Banks"},
        "DRCRELEXFACBS": {"short": "CRE Delinquency Rate", "long": "Delinquency Rate on CRE Loans (Excluding Farmland)"},
        "CORBLACBS": {"short": "Business Loan Charge-offs", "long": "Charge-off Rate on Business Loans, Annualized"},
        "DRALACBS": {"short": "All Loans Delinquency", "long": "Delinquency Rate on All Loans"},
        "CORALACBN": {"short": "All Loans Charge-offs", "long": "Charge-off Rate on All Loans, Annualized"}
    },
    "Leading Indicators": {
        "USSLIND": {"short": "US Leading Index", "long": "The Conference Board Leading Economic Index® (LEI) for the U.S."},
        "USALOLITONOSTSAM": {"short": "OECD CLI US", "long": "OECD Composite Leading Indicator for US"},
        "PAYEMS": {"short": "Nonfarm Employment", "long": "All Employees: Total Nonfarm"}
    },
    "Middle Market, Healthcare, & Funding Indicators": {
        "DRTSCIS": {"short": "Small Firm C&I Standards", "long": "Net Pct Banks Tightening Standards - Small Firms"},
        "SOFR3MTB3M": {"short": "SOFR-T-Bill Spread", "long": "3-Month Term SOFR vs 3-Month Treasury Bill Spread"},
        "MPCT04XXS": {"short": "Healthcare Construction", "long": "Total Construction Spending: Health Care"},
        "CEU6562000101": {"short": "Healthcare Employment", "long": "All Employees: Health Care"}
    }
}
    
# ==================================================================================
#  3. HELPER CLASSES
# ==================================================================================

class FDICDataFetcher:
    def __init__(self, config: DashboardConfig):
        self.config = config
        self.session = requests.Session()
    def fetch_all_banks(self) -> Tuple[pd.DataFrame, List[int]]:
        certs_to_fetch = [self.config.subject_bank_cert] + self.config.peer_bank_certs
        all_bank_data, failed_certs = [], []
        for cert in certs_to_fetch:
            try:
                params = {
                    "filters": f"CERT:{cert}", "fields": ",".join(FDIC_FIELDS_TO_FETCH),
                    "sort_by": "REPDTE", "sort_order": "DESC",
                    "limit": self.config.quarters_back + 4, "format": "json"
                }
                response = self.session.get(f"{self.config.fdic_api_base}/financials", params=params, timeout=30)
                response.raise_for_status()
                data = [item.get('data', {}) for item in response.json().get('data', []) if item.get('data')]
                if data:
                    df = pd.DataFrame(data)
                    df['CERT'] = cert
                    all_bank_data.append(df)
            except Exception as e:
                logger.error(f"Error fetching FDIC data for CERT {cert}: {e}")
                failed_certs.append(cert)
            time.sleep(0.2)
        
        if not all_bank_data: return pd.DataFrame(), failed_certs
        
        combined_df = pd.concat(all_bank_data, ignore_index=True)
        for field in FDIC_FIELDS_TO_FETCH:
            if field not in combined_df.columns: combined_df[field] = np.nan
        
        combined_df['REPDTE'] = pd.to_datetime(combined_df['REPDTE'])
        num_cols = [c for c in combined_df.columns if c not in ['CERT', 'NAME', 'REPDTE']]
        combined_df[num_cols] = combined_df[num_cols].apply(pd.to_numeric, errors='coerce')
        return combined_df.sort_values(['CERT', 'REPDTE']), failed_certs

class FREDDataFetcher:
    def __init__(self, config: DashboardConfig):
        self.config = config
        self.session = requests.Session()
    def fetch_all_series(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("Starting FRED data fetch...")
        endpoint_url = f"{self.config.fred_api_base}/series/observations"
        all_series_data, descriptions_list = {}, []
        start_date = (datetime.now() - timedelta(days=self.config.fred_years_back * 365)).strftime('%Y-%m-%d')
        for category, series_info in FRED_SERIES_TO_FETCH.items():
            for series_id, details in series_info.items():
                descriptions_list.append({'Series ID': series_id, 'Category': category, **details})
                try:
                    params = {'series_id': series_id, 'api_key': self.config.fred_api_key, 'file_type': 'json', 'observation_start': start_date}
                    response = self.session.get(endpoint_url, params=params, timeout=15)
                    response.raise_for_status()
                    data = response.json().get('observations', [])
                    if data:
                        df = pd.DataFrame(data)[['date', 'value']]
                        df['date'] = pd.to_datetime(df['date'])
                        df['value'] = pd.to_numeric(df['value'], errors='coerce')
                        all_series_data[series_id] = df.set_index('date')['value']
                except Exception as e:
                    logger.error(f"Failed to fetch FRED series {series_id}: {e}")
                time.sleep(0.5)
        if not all_series_data: return pd.DataFrame(), pd.DataFrame()
        final_df = pd.DataFrame(all_series_data)
        
        return final_df.reset_index(), pd.DataFrame(descriptions_list)

class BankMetricsProcessor:
    def __init__(self, config: 'DashboardConfig'):
        self.config = config

    def _get_series(self, df: pd.DataFrame, series_list: List[str]) -> pd.Series:
        total = pd.Series(0, index=df.index)
        for col in series_list:
            if col in df:
                total += df[col].fillna(0)
        return total

    def _safe_divide(self, numerator, denominator):
        if np.isscalar(numerator) and np.isscalar(denominator):
            return numerator / denominator if denominator != 0 else 0.0
        if isinstance(denominator, pd.Series):
            denominator = denominator.replace(0, np.nan)
        return (numerator / denominator).fillna(0)

    def create_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        logging.info("Creating quarterly derived metrics...")
        df_processed = df.copy()

        # [1] Convert all Year-to-Date (YTD) series to discrete quarterly values
        ytd_fields = [col for col in df_processed.columns if col.startswith('NT') or 'EINTEXP' in col]
        for cert, group in df_processed.groupby('CERT'):
            group = group.sort_values('REPDTE')
            for col in ytd_fields:
                quarterly_vals = group[col].diff()
                q1_mask = group['REPDTE'].dt.quarter == 1
                quarterly_vals.loc[q1_mask] = group.loc[q1_mask, col]
                df_processed.loc[group.index, f"{col}_Q"] = quarterly_vals

        # [2] Define key variables that will be used in multiple calculations
        gross_loans = df_processed.get('LNLS', 0)
        allowance_loans = df_processed.get('LNATRES', 0).fillna(0)
        allowance_off_bs = df_processed.get('RCONLG28', 0).fillna(0)
        tier1_capital = df_processed.get('RBC1AAJ', 0).fillna(0)

        # [3] Calculate custom loan category balances and rates
        for cat_name, cat_details in LOAN_CATEGORIES.items():
            balance = self._get_series(df_processed, cat_details.get('balance', []))
            df_processed[f'IDB_{cat_name}_Balance'] = balance
            df_processed[f'IDB_{cat_name}_Composition'] = self._safe_divide(balance, gross_loans) * 100
            
            # [RESTORED] NCO Rate for each category
            nco_q_val = self._get_series(df_processed, [f"{s}_Q" for s in cat_details.get('nco', []) if f"{s}_Q" in df_processed])
            df_processed[f'IDB_{cat_name}_NCO_Rate'] = self._safe_divide(nco_q_val, balance) * 400

        # [4] Calculate top-of-house metrics
        df_processed['Total_ACL'] = allowance_loans + allowance_off_bs
        df_processed['Allowance_to_Gross_Loans_Rate'] = self._safe_divide(df_processed['Total_ACL'], gross_loans) * 100
        
        # Cost of Funds
        df_processed['interest_bearing_liab'] = self._get_series(df_processed, ['DEP', 'FREPP', 'OTHBOR'])
        prev_interest_bearing_liab = df_processed.groupby('CERT')['interest_bearing_liab'].shift(1)
        avg_interest_bearing_liab = (df_processed['interest_bearing_liab'] + prev_interest_bearing_liab) / 2
        df_processed['Cost_of_Funds'] = self._safe_divide(df_processed.get('EINTEXP_Q', 0) * 4, avg_interest_bearing_liab) * 100
        df_processed.drop(columns=['interest_bearing_liab'], inplace=True) 

        # Nonperforming & Nonaccrual Rates
        all_na_fields = [na_field for details in LOAN_CATEGORIES.values() for na_field in details.get('na', [])]
        df_processed['Total_Nonaccrual'] = self._get_series(df_processed, all_na_fields)
        df_processed['Nonaccrual_to_Gross_Loans_Rate'] = self._safe_divide(df_processed['Total_Nonaccrual'], gross_loans) * 100
        
        # [RESTORED] NPL (Non-Performing Loan) to Gross Loans Rate
        df_processed['NPL_to_Gross_Loans_Rate'] = self._safe_divide(df_processed.get('NCLNLS', 0), gross_loans) * 100

        # CRE Concentration Ratio
        capital_at_risk = tier1_capital + df_processed['Total_ACL']
        cre_balance = df_processed.get('IDB_CRE_Balance', 0)
        df_processed['CRE_Concentration_Capital_Risk'] = self._safe_divide(cre_balance, capital_at_risk) * 100

        return df_processed

    def calculate_ttm_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        logging.info("Calculating granular TTM metrics...")
        
        all_banks_data = []
        for cert, group in df.groupby('CERT'):
            bank_df = group.sort_values("REPDTE").copy()
            
            if len(bank_df) >= 4:
                # Portfolio-wide TTM calcs
                avg_loans_ttm = bank_df['LNLS'].rolling(window=4, min_periods=4).mean()
                bank_df['TTM_NCO_Rate'] = self._safe_divide(bank_df['NTLNLS_Q'].rolling(window=4).sum(), avg_loans_ttm) * 100
                bank_df['TTM_PD30_Rate'] = self._safe_divide(bank_df['P3LNLS'].rolling(window=4).mean(), avg_loans_ttm) * 100
                bank_df['TTM_PD90_Rate'] = self._safe_divide(bank_df['P9LNLS'].rolling(window=4).mean(), avg_loans_ttm) * 100
                bank_df['TTM_Past_Due_Rate'] = bank_df['TTM_PD30_Rate'] + bank_df['TTM_PD90_Rate']
                
                # Granular IDB category TTM calcs
                for cat_name, cat_details in LOAN_CATEGORIES.items():
                    avg_cat_balance_ttm = bank_df[f'IDB_{cat_name}_Balance'].rolling(window=4).mean()
                    
                    pd30_balance = self._get_series(bank_df, cat_details.get('pd30', []))
                    avg_pd30_ttm = pd30_balance.rolling(window=4).mean()
                    bank_df[f'IDB_{cat_name}_TTM_PD30_Rate'] = self._safe_divide(avg_pd30_ttm, avg_cat_balance_ttm) * 100

                    pd90_balance = self._get_series(bank_df, cat_details.get('pd90', []))
                    avg_pd90_ttm = pd90_balance.rolling(window=4).mean()
                    bank_df[f'IDB_{cat_name}_TTM_PD90_Rate'] = self._safe_divide(avg_pd90_ttm, avg_cat_balance_ttm) * 100
                
                bank_df['IDB_CRE_Growth_TTM'] = bank_df['IDB_CRE_Balance'].pct_change(periods=4) * 100
            
            if len(bank_df) >= 12:
                bank_df['IDB_CRE_Growth_36M'] = bank_df['IDB_CRE_Balance'].pct_change(periods=12) * 100
            
            all_banks_data.append(bank_df)

        return pd.concat(all_banks_data, ignore_index=True) if all_banks_data else df
        
    def calculate_8q_averages(self, proc_df: pd.DataFrame) -> pd.DataFrame:
        """
        [RESTORED] Calculates 8-quarter rolling averages for ALL numeric metrics.
        """
        logging.info("Calculating 8-quarter averages for all numeric metrics...")
        if proc_df.empty: return pd.DataFrame()

        metrics_to_average = proc_df.select_dtypes(include=np.number).columns.tolist()
        if 'CERT' in metrics_to_average:
            metrics_to_average.remove('CERT')

        avg_results = []
        for cert, group in proc_df.groupby('CERT'):
            if len(group) < 8:
                continue
            
            group = group.sort_values('REPDTE')
            averages = group[metrics_to_average].rolling(window=8, min_periods=8).mean()
            latest_averages = averages.iloc[-1]
            
            record = {"CERT": cert, "NAME": group['NAME'].iloc[-1]}
            record.update(latest_averages.to_dict())
            avg_results.append(record)
            
        if not avg_results: return pd.DataFrame()
        
        avg_df = pd.DataFrame(avg_results).set_index('CERT')
        all_cols = avg_df.columns.tolist()
        if 'NAME' in all_cols:
            all_cols.remove('NAME')
            new_col_order = ['NAME'] + sorted(all_cols)
            avg_df = avg_df[new_col_order]
        return avg_df
        
    def create_latest_snapshot(self, proc_df: pd.DataFrame) -> pd.DataFrame:
        if proc_df.empty: return pd.DataFrame()
        latest_date = proc_df['REPDTE'].max()
        latest_data = proc_df[proc_df['REPDTE'] == latest_date].copy()
        
        snapshot = latest_data[['CERT', 'NAME', 'REPDTE']].copy()
        
        # Add all relevant metrics to the snapshot
        metrics_to_include = [
            "LNLS", "Total_ACL", "TIER1", "TIER2",
            "NPL_to_Gross_Loans_Rate", "Nonaccrual_to_Gross_Loans_Rate", "Allowance_to_Gross_Loans_Rate", "TTM_NCO_Rate",
            "TTM_PD30_Rate", "TTM_PD90_Rate", "TTM_Past_Due_Rate", "IDB_CRE_Growth_TTM", 
            "IDB_CRE_Growth_36M", "CRE_Concentration_Capital_Risk", "Cost_of_Funds"
        ]
        
        # Add compositions and granular TTM rates
        for cat in LOAN_CATEGORIES.keys():
            snapshot[f'{cat}_Composition'] = latest_data.get(f'IDB_{cat}_Composition', np.nan)
            snapshot[f'{cat}_TTM_PD30_Rate'] = latest_data.get(f'IDB_{cat}_TTM_PD30_Rate', np.nan)
            snapshot[f'{cat}_TTM_PD90_Rate'] = latest_data.get(f'IDB_{cat}_TTM_PD90_Rate', np.nan)

        # Add single value metrics
        for metric in metrics_to_include:
            snapshot[metric] = latest_data.get(metric, np.nan)

        snapshot.rename(columns={'REPDTE': 'Reporting_Date', 'LNLS': 'Gross_Loans'}, inplace=True)
        return snapshot.set_index('CERT')

class PeerAnalyzer:
    def __init__(self, config: 'DashboardConfig'):
        self.config = config
        self.metric_descriptions = FDIC_FIELD_DESCRIPTIONS.copy()

    def create_peer_comparison(self, processed_df: pd.DataFrame) -> pd.DataFrame:
        logging.info("Creating peer comparison analysis...")
        if processed_df.empty: return pd.DataFrame()

        latest_date = processed_df["REPDTE"].max()
        latest_data = processed_df[processed_df["REPDTE"] == latest_date].copy()

        subject_data = latest_data[latest_data["CERT"] == self.config.subject_bank_cert]
        peer_data = latest_data[latest_data["CERT"].isin(self.config.peer_bank_certs)]

        if subject_data.empty or peer_data.empty: return pd.DataFrame()

        metrics_to_compare = list(self.metric_descriptions.keys())
        
        comparison_list = []
        for metric in metrics_to_compare:
            if metric not in latest_data.columns: continue

            subject_value = subject_data[metric].iloc[0] if not subject_data.empty else np.nan
            peer_values = peer_data[metric].dropna()
            if peer_values.empty: continue
            
            record = {
                "Metric Code": metric,
                "Metric Name": self.metric_descriptions.get(metric, {}).get("short", metric),
                "Your_Bank": subject_value, 
                "Peer_Mean": peer_values.mean(),
                "Peer_Median": peer_values.median(), 
                "Peer_25th": peer_values.quantile(0.25),
                "Peer_75th": peer_values.quantile(0.75),
            }
            
            if pd.notna(subject_value):
                record["Your_Percentile"] = stats.percentileofscore(peer_values, subject_value, kind='rank')
            else:
                record["Your_Percentile"] = np.nan
            
            comparison_list.append(record)

        if not comparison_list: return pd.DataFrame()
        
        comparison_df = pd.DataFrame(comparison_list)
        comparison_df["Performance_Flag"] = comparison_df.apply(self._get_performance_flag, axis=1)
        return comparison_df

    def _get_performance_flag(self, row: pd.Series) -> str:
        if pd.isna(row.get("Your_Percentile")): return "N/A"
        percentile = row["Your_Percentile"]
        metric = row["Metric Code"].lower()
        if any(term in metric for term in ["nco", "npl", "past_due", "eeffr", "cost_of_funds"]):
            if percentile <= 25: return "Top Quartile"
            if percentile <= 50: return "Better than Median"
            return "Bottom Quartile"
        else:
            if percentile >= 75: return "Top Quartile"
            if percentile >= 50: return "Better than Median"
            return "Bottom Quartile"


class MacroTrendAnalyzer:
    """
    Analyzes macroeconomic time series data, calculates technical indicators,
    and generates structured output for Power BI dashboards.
    """
    def __init__(self, config: 'DashboardConfig'):
        """
        Initializes the analyzer with configuration, metadata, and technical parameters.

        Args:
            config (DashboardConfig): A configuration object containing settings like
                                      API keys, date ranges, etc.
        """
        self.config = config
        self.indicator_metadata = self._initialize_metadata()
        self.technical_params = self._set_technical_parameters()
    def _initialize_metadata(self) -> Dict:
        """
        Defines a comprehensive metadata repository for each macroeconomic indicator.
        This dictionary is the "brain" of the automated analysis, providing context
        for interpretation, classification, and commentary generation.

        Returns:
            Dict]: A nested dictionary where keys are FRED series IDs
                                       and values are dictionaries of metadata attributes.
        """
        return {
            # Leading Indicators
            "USSLIND": {
                "type": "leading", "horizon": "medium", "sectors": ["economy_wide"],
                "benchmark": {"recession": 98.0, "expansion": 102.0}, "annualized": False,
                "best_technical": "EMA", "technical_reason": "Highly responsive to economic turning points."
            },
            "T10Y2Y": { # Assuming this was an existing series
                "type": "leading", "horizon": "long", "sectors": ["banking", "real_estate", "economy_wide"],
                "benchmark": {"inversion": 0.0, "normal": 1.5}, "annualized": False,
                "best_technical": "SMA", "technical_reason": "Stable trend indicator for yield curve shape."
            },
            "USALOLITONOSTSAM": {
                "type": "leading", "horizon": "medium", "sectors": ["economy_wide", "global"],
                "benchmark": {"contraction": 99.5, "expansion": 100.5}, "annualized": False,
                "best_technical": "EMA", "technical_reason": "Captures momentum shifts in OECD economic outlook."
            },
            "DRTSCIS": {
                "type": "leading", "horizon": "short", "sectors": ["banking", "credit", "middle_market"],
                "benchmark": {"tightening": 20.0, "easing": -10.0}, "annualized": False,
                "best_technical": "Bollinger Bands", "technical_reason": "Breakouts signal significant shifts in lending sentiment."
            },

            # Banking & Credit Quality
            "DRCRELEXFACBS": {
                "type": "lagging", "horizon": "medium", "sectors": ["banking", "real_estate", "credit"],
                "benchmark": {"stress": 3.0, "normal": 1.0}, "annualized": False,
                "best_technical": "Bollinger Bands", "technical_reason": "Volatility in delinquencies indicates credit cycle stress."
            },
            "CORBLACBS": {
                "type": "lagging", "horizon": "medium", "sectors": ["banking", "credit", "corporate"],
                "benchmark": {"stress": 1.5, "normal": 0.5}, "annualized": True,
                "best_technical": "SMA", "technical_reason": "Identifies the underlying trend in realized credit losses."
            },
            "DRALACBS": {
                "type": "lagging", "horizon": "medium", "sectors": ["banking", "credit", "consumer"],
                "benchmark": {"stress": 3.5, "normal": 2.0}, "annualized": False,
                "best_technical": "SMA", "technical_reason": "Smooths quarterly data to reveal the broad credit quality trend."
            },
            "CORALACBN": {
                "type": "lagging", "horizon": "long", "sectors": ["banking", "credit", "economy_wide"],
                "benchmark": {"stress": 2.0, "normal": 0.75}, "annualized": True,
                "best_technical": "SMA", "technical_reason": "Tracks the long-term cycle of aggregate bank loan losses."
            },
            "SOFR3MTB3M": {
                "type": "coincident", "horizon": "short", "sectors": ["banking", "funding", "credit"],
                "benchmark": {"stress": 0.5, "extreme_stress": 1.0}, "annualized": False,
                "best_technical": "Bollinger Bands", "technical_reason": "Band expansion signals rising funding stress and counterparty risk."
            },

            # Employment
            "PAYEMS": {
                "type": "coincident", "horizon": "short", "sectors": ["economy_wide", "employment"],
                "benchmark": {"contraction_yoy_pct": 0.0}, "annualized": False,
                "best_technical": "RSI", "technical_reason": "Identifies momentum exhaustion in labor market growth."
            },
            "CEU6562000101": {
                "type": "coincident", "horizon": "medium", "sectors": ["healthcare", "employment"],
                "benchmark": {"contraction_yoy_pct": 0.0}, "annualized": False,
                "best_technical": "SMA", "technical_reason": "Healthcare employment is a stable trend, best captured by SMA."
            },

            # Real Estate & Construction
            "NYXRSA": {"type": "coincident", "horizon": "medium", "sectors": ["real_estate"], "benchmark": {}, "annualized": False, "best_technical": "SMA", "technical_reason": "Smooths monthly price volatility to show the primary trend."},
            "LXXRSA": {"type": "coincident", "horizon": "medium", "sectors": ["real_estate"], "benchmark": {}, "annualized": False, "best_technical": "SMA", "technical_reason": "Smooths monthly price volatility to show the primary trend."},
            "MIXRNSA": {"type": "coincident", "horizon": "medium", "sectors": ["real_estate"], "benchmark": {}, "annualized": False, "best_technical": "SMA", "technical_reason": "Smooths monthly price volatility to show the primary trend."},
            "CASTHPI": {"type": "coincident", "horizon": "medium", "sectors": ["real_estate"], "benchmark": {}, "annualized": False, "best_technical": "SMA", "technical_reason": "Ideal for smoothing quarterly state-level HPI data."},
            "FLSTHPI": {"type": "coincident", "horizon": "medium", "sectors": ["real_estate"], "benchmark": {}, "annualized": False, "best_technical": "SMA", "technical_reason": "Ideal for smoothing quarterly state-level HPI data."},
            "MPCT04XXS": {
                "type": "coincident", "horizon": "medium", "sectors": ["healthcare", "construction"],
                "benchmark": {"contraction": 0.0}, "annualized": False,
                "best_technical": "SMA", "technical_reason": "Construction spending data is volatile; SMA reveals the underlying investment trend."
            },

            # Global Benchmarks
            "FPCPITOTLZGWLD": {"type": "lagging", "horizon": "long", "sectors": ["global", "economy_wide"], "benchmark": {"high_inflation": 5.0}, "annualized": True, "best_technical": "SMA", "technical_reason": "Annual data requires a long-term moving average to identify global inflation regimes."},
            "FPCPITOTLZGHIC": {"type": "lagging", "horizon": "long", "sectors": ["global", "economy_wide"], "benchmark": {"high_inflation": 4.0}, "annualized": True, "best_technical": "SMA", "technical_reason": "Tracks inflation trends in developed economies."},
            "FPCPITOTLZGLMY": {"type": "lagging", "horizon": "long", "sectors": ["global", "economy_wide"], "benchmark": {"high_inflation": 6.0}, "annualized": True, "best_technical": "SMA", "technical_reason": "Tracks inflation trends in emerging economies."},
            "NYGDPMKTPCDWLD": {"type": "lagging", "horizon": "long", "sectors": ["global", "economy_wide"], "benchmark": {}, "annualized": False, "best_technical": "SMA", "technical_reason": "Annual data requires a long-term moving average to identify global growth regimes."},
        }

    def _set_technical_parameters(self) -> Dict:
        """
        Sets default and series-specific parameters for technical indicator calculations.
        These can be tuned to optimize for different data frequencies and volatilities.

        Returns:
            Dict]: A dictionary of parameters for each indicator.
        """
        # Default parameters are chosen based on common practice for economic data
        default_params = {
            'sma_period': 12, 'ema_period': 12,
            'bb_period': 12, 'bb_std': 2.0,
            'rsi_period': 14, 'z_score_period': 12
        }

        # Series-specific overrides can be defined here if defaults are not optimal
        # For example, for a very long-term, stable series like GDP:
        # "GDP": {'sma_period': 4,...} # 4-quarter (annual) moving average
        specific_params = {
            "T10Y2Y": {'sma_period': 50, 'ema_period': 20, 'bb_period': 50, 'rsi_period': 14, 'z_score_period': 50},
            "USSLIND": {'sma_period': 6, 'ema_period': 6, 'bb_period': 6, 'rsi_period': 14, 'z_score_period': 6},
            "PAYEMS": {'sma_period': 12, 'ema_period': 12, 'bb_period': 20, 'rsi_period': 14, 'z_score_period': 12},
        }

        # This approach allows for a flexible combination of defaults and overrides
        params = {}
        for series_id in self.indicator_metadata.keys():
            params[series_id] = default_params.copy()
            if series_id in specific_params:
                params[series_id].update(specific_params[series_id])
        return params
    def _calculate_rsi(self, series: pd.Series, period: int) -> pd.Series:
        """
        Helper function to calculate the Relative Strength Index (RSI).
        """
        delta = series.diff(1)
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()

        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def calculate_technical_indicators(self, series: pd.Series, series_id: str) -> pd.DataFrame:
        """
        Calculates a suite of technical indicators for a given time series.
        Handles mixed-frequency data by resampling to a consistent monthly frequency
        before calculation.

        Args:
            series (pd.Series): The input time series data.
            series_id (str): The FRED series ID for parameter lookup.

        Returns:
            pd.DataFrame: A DataFrame with the original value and all calculated indicators.
        """
        # --- Pre-processing: Handle mixed frequencies ---
        # Resample to a consistent monthly frequency to make indicators comparable
        # Use linear interpolation for upsampling quarterly/annual data to avoid step-changes
        series_monthly = series.resample('MS').interpolate(method='linear')

        params = self.technical_params.get(series_id, self.technical_params['default'])

        df = pd.DataFrame({'value': series_monthly})
        df.index.name = 'date'

        # Simple Moving Average (SMA)
        df = series_monthly.rolling(
            window=params['sma_period'], min_periods=1
        ).mean()

        # Exponential Moving Average (EMA)
        df[f'{series_id}_EMA'] = series_monthly.ewm(
            span=params['ema_period'], adjust=False, min_periods=1
        ).mean()

        # Bollinger Bands
        rolling_mean = series_monthly.rolling(
            window=params['bb_period'], min_periods=1
        ).mean()
        rolling_std = series_monthly.rolling(
            window=params['bb_period'], min_periods=1
        ).std()
        df = rolling_mean + (params['bb_std'] * rolling_std)
        df = rolling_mean - (params['bb_std'] * rolling_std)
        df = (df - df) / rolling_mean # Normalized width

        # Relative Strength Index (RSI)
        df = self._calculate_rsi(series_monthly, params['rsi_period'])

        # Z-Score (Standard Score)
        z_score_mean = series_monthly.rolling(
            window=params['z_score_period'], min_periods=1
        ).mean()
        z_score_std = series_monthly.rolling(
            window=params['z_score_period'], min_periods=1
        ).std()
        df = (series_monthly - z_score_mean) / z_score_std

        # Rename 'value' column to the specific series_id for clarity
        df.rename(columns={'value': series_id}, inplace=True)

        return df
    def _get_optimal_technical_indicator(self, series_id: str) -> Dict[str, Any]:
        """
        Selects the optimal technical indicator and provides reasoning based on the
        economic characteristics of the series.
    
        Args:
            series_id (str): The FRED series ID.
    
        Returns:
            Dict[str, Any]: A dictionary containing the name of the best indicator
                            and the reasoning for its selection.
        """
        # 1. This dictionary now contains the reasons for choosing each indicator.
        # This fixes the original SyntaxError.
        selection_rules = {
            "SMA": {
                "reason": "Best for identifying long-term trends in stable, less volatile series."
            },
            "EMA": {
                "reason": "Best for tracking recent price changes and identifying short-term trends."
            },
            "Bollinger Bands": {
                "reason": "Best for assessing volatility and identifying overbought/oversold conditions."
            },
            "RSI": {
                "reason": "Best for measuring momentum and identifying potential trend reversals."
            }
        }
    
        # 2. Determine which indicator is best for the given series_id.
        # We still get this from the metadata, with "SMA" as a safe default.
        metadata = self.indicator_metadata.get(series_id, {})
        best_indicator = metadata.get("best_technical", "SMA")
    
        # 3. Look up the specific reason from the rules dictionary using the best_indicator.
        # We use .get() to provide a default reason in case the indicator isn't in our rules.
        default_rule = {"reason": "Default trend-following indicator used as no specific rule was found."}
        reason = selection_rules.get(best_indicator, default_rule)["reason"]
    
        # 4. Return the final output, which is now ready to be used in your required column.
        return {
            "indicator": best_indicator,
            "reason": reason
        }
    def calculate_oecd_aggregates(self, fred_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates or extracts aggregates for OECD vs. non-OECD country groups
        using World Bank proxies available on FRED.
    
        Args:
            fred_data (pd.DataFrame): The DataFrame containing all fetched FRED series.
    
        Returns:
            pd.DataFrame: A DataFrame with the aggregated OECD and non-OECD series.
        """
        # 1. Define constants for the specific series IDs being used as proxies.
        # These are the actual FRED series for inflation in different income groups.
        HI_INCOME_SERIES = 'FPCPITOTLZGHI'   # Proxy for OECD countries
        LOW_MID_INCOME_SERIES = 'FPCPITOTLZGLM' # Proxy for Non-OECD countries
        WORLD_SERIES = 'FPCPITOTLZGOW'   # World aggregate
    
        # 2. Assign the list of required columns to the variable. This fixes the SyntaxError.
        required_cols = [HI_INCOME_SERIES, LOW_MID_INCOME_SERIES, WORLD_SERIES]
        
        # Check if all required columns are present in the input DataFrame.
        if not all(col in fred_data.columns for col in required_cols):
            print("Warning: Not all global inflation series found for OECD aggregation.")
            return pd.DataFrame()
    
        # 3. Create the output by selecting the CORRECT column for each key.
        # This fixes the logical error/ValueError.
        output_df = pd.DataFrame({
            'OECD_Proxy_Inflation': fred_data[HI_INCOME_SERIES],
            'NonOECD_Proxy_Inflation': fred_data[LOW_MID_INCOME_SERIES],
            'World_Inflation': fred_data[WORLD_SERIES]
        })
        
        # The data is annual, so forward-fill to create a step-function series
        # that can be merged with higher-frequency data.
        output_df.ffill(inplace=True)
        
        return output_df
    def _detect_frequency(self, series: pd.Series) -> str:
        """
        Infers the frequency of a time series index.
        """
        series = series.dropna()
        if len(series.index) < 3:
            return "Undetermined"
        return pd.infer_freq(series.index) or "Irregular"

    def validate_series_data(self, series_id: str, data: pd.Series) -> Dict[str, Any]:
        """
        Performs data quality checks on a time series and returns a validation summary.

        Args:
            series_id (str): The FRED series ID.
            data (pd.Series): The time series data to validate.

        Returns:
            Dict[str, Any]: A dictionary containing data quality metrics and warnings.
        """
        if data.empty:
            return {
                "status": "Error", "warning": "No data available for series.",
                "missing_data_pct": 100, "last_update": pd.NaT,
                "data_frequency": "N/A", "outliers_detected": 0
            }

        validation_results = {
            "status": "OK",
            "warning": None,
            "missing_data_pct": data.isna().sum() / len(data) * 100 if len(data) > 0 else 0,
            "last_update": data.last_valid_index(),
            "data_frequency": self._detect_frequency(data),
        }

        # Outlier detection using Z-score
        # We consider an outlier a point with a Z-score > 3 or < -3
        z_scores = np.abs(stats.zscore(data.dropna()))
        outliers = z_scores[z_scores > 3]
        validation_results["outliers_detected"] = len(outliers)

        # Flag if data appears stale (e.g., last update > 90 days ago for monthly/quarterly series)
        if pd.Timestamp.now(tz='UTC') - validation_results['last_update'] > pd.Timedelta(days=90):
            freq = validation_results['data_frequency']
            # Annual data is expected to be stale for longer periods
            if not (freq and freq.startswith(('A', 'Y'))):
                 validation_results["warning"] = "Data may be stale."
                 validation_results["status"] = "Warning"

        if validation_results["missing_data_pct"] > 50:
            validation_results["warning"] = "High percentage of missing data."
            validation_results["status"] = "Warning"

        return validation_results
    def _get_technical_signal(self, series_id: str, data: pd.DataFrame, horizon: str = 'medium') -> str:
        """
        Generates a sophisticated signal by analyzing the state, duration, and
        recent events of the optimal technical indicator.
    
        Args:
            series_id (str): The FRED series ID (e.g., 'DGS10').
            data (pd.DataFrame): DataFrame containing the series and its technical indicators.
            horizon (str): The lookback horizon ('short', 'medium', 'long') to define
                           the period for signal confirmation.
    
        Returns:
            str: A descriptive signal string.
        """
        # 1. Map the horizon string to a lookback period (number of data points)
        horizon_map = {'short': 2, 'medium': 5, 'long': 10}
        lookback = horizon_map.get(horizon, 5)
    
        # Ensure we have enough data to look back
        if len(data) < lookback + 1:
            return "Awaiting Data (Insufficient History)"
    
        # --- Determine the optimal indicator to use ---
        metadata = self.indicator_metadata.get(series_id, {})
        best_tech = metadata.get("best_technical", "SMA")
    
        # --- SMA / EMA Logic ---
        if best_tech in ["SMA", "EMA"]:
            col_name = f"{series_id}_{best_tech}"
            if col_name not in data.columns: return "Signal N/A"
    
            # Get the last two points for crossover detection
            price_now = data[series_id].iloc[-1]
            price_prev = data[series_id].iloc[-2]
            tech_now = data[col_name].iloc[-1]
            tech_prev = data[col_name].iloc[-2]
    
            if pd.isna(price_now) or pd.isna(tech_now): return "Awaiting Data"
    
            # Crossover Detection
            if price_prev <= tech_prev and price_now > tech_now:
                return f"Bullish Crossover ({best_tech})"
            if price_prev >= tech_prev and price_now < tech_now:
                return f"Bearish Crossover ({best_tech})"
    
            # Confirmed Trend Detection (checks the entire lookback period)
            if all(data[series_id].iloc[-lookback:] > data[col_name].iloc[-lookback:]):
                return f"Confirmed Bullish Trend (Above {best_tech} for {lookback} periods)"
            if all(data[series_id].iloc[-lookback:] < data[col_name].iloc[-lookback:]):
                return f"Confirmed Bearish Trend (Below {best_tech} for {lookback} periods)"
    
            return "Neutral (Consolidating)"
    
        # --- Bollinger Bands Logic ---
        elif best_tech == "Bollinger Bands":
            upper_col, lower_col = f"{series_id}_BB_Upper", f"{series_id}_BB_Lower"
            if upper_col not in data.columns or lower_col not in data.columns: return "Signal N/A"
    
            price_now = data[series_id].iloc[-1]
            upper_now, lower_now = data[upper_col].iloc[-1], data[lower_col].iloc[-1]
            
            if any(pd.isna([price_now, upper_now, lower_now])): return "Awaiting Data"
    
            # Check for breakouts or reversions
            if price_now > upper_now:
                # Check if it's a new breakout or a sustained one
                if data[series_id].iloc[-2] <= data[upper_col].iloc[-2]:
                    return "Volatility Breakout (High Stress)"
                else:
                    return "Sustained High Volatility"
            elif price_now < lower_now:
                if data[series_id].iloc[-2] >= data[lower_col].iloc[-2]:
                     return "Volatility Breakout (Low/Recession)"
                else:
                    return "Sustained Low Pressure"
            
            return "Neutral (Range-bound)"
    
        # --- RSI Logic ---
        elif best_tech == "RSI":
            rsi_col = f"{series_id}_RSI"
            if rsi_col not in data.columns: return "Signal N/A"
    
            rsi_val = data[rsi_col].iloc[-1]
            if pd.isna(rsi_val): return "Awaiting Data"
    
            # Add magnitude to the signal
            if rsi_val > 80: return "Extreme Overbought (High Momentum)"
            if rsi_val > 70: return "Overbought"
            if rsi_val < 20: return "Extreme Oversold (Low Momentum)"
            if rsi_val < 30: return "Oversold"
            
            # Check for momentum change
            avg_rsi_lookback = data[rsi_col].iloc[-lookback:].mean()
            if rsi_val > avg_rsi_lookback and rsi_val > 50: return "Gaining Bullish Momentum"
            if rsi_val < avg_rsi_lookback and rsi_val < 50: return "Gaining Bearish Momentum"
    
            return "Neutral Momentum"
            
        return "Not Implemented"

    def _generate_short_term_analysis(self, series_id: str, data: pd.DataFrame, metadata: Dict) -> pd.Series:
        """
        Generates markdown-formatted short-term (1-3 months) analysis for a series.
        This is applied row-wise to generate commentary for each point in time.
        """
        def generate_row_analysis(row):
            current_value = row[series_id]
            # Get previous row's value for change calculation
            prev_row_index = data.index.get_loc(row.name) - 1
            if prev_row_index < 0:
                change = 0.0
            else:
                prior_value = data[series_id].iloc[prev_row_index]
                change = current_value - prior_value

            if pd.isna(current_value):
                return "No data available for this period."

            tech_signal = self._get_technical_signal(series_id, data.loc[:row.name], 'short')
            annualized_text = ' (annualized)' if metadata.get('annualized', False) else ''
            
            # Simple outlook logic
            outlook = "Stable"
            if "Bullish" in tech_signal or "High" in tech_signal:
                outlook = "Improving"
            elif "Bearish" in tech_signal or "Low" in tech_signal:
                outlook = "Worsening"

            return (
                f"**Current Level**: {current_value:,.2f}{annualized_text}\n\n"
                f"**Change (MoM)**: {change:+.2f}\n\n"
                f"**Technical Signal**: {tech_signal}\n\n"
                f"**Outlook**: Short-term trend appears to be **{outlook}**."
            )
        
        # Apply the function to each row of the DataFrame
        return data.apply(generate_row_analysis, axis=1)

    # Placeholder methods for long-term and risk analysis
    def _generate_long_term_analysis(self, series_id: str, data: pd.DataFrame, metadata: Dict) -> pd.Series:
        # Similar logic to short-term, but might use longer-period moving averages
        # or compare to year-ago values.
        return pd.Series("Long-term analysis pending implementation.", index=data.index)

    def _generate_risk_assessment(self, series_id: str, data: pd.DataFrame, metadata: Dict) -> pd.Series:
        # Logic would compare current values to benchmark stress levels from metadata
        return pd.Series("Risk assessment pending implementation.", index=data.index)
    def generate_powerbi_output(self, processed_data: Dict) -> pd.DataFrame:
        """
        Generates a Power BI-optimized "long" format DataFrame from the processed
        technical indicator data. Also attaches metadata and generated commentary.

        Args:
            processed_data (Dict): A dictionary where keys are series IDs
                                                     and values are DataFrames from
                                                     calculate_technical_indicators.

        Returns:
            pd.DataFrame: A single, tidy DataFrame ready for Power BI.
        """
        all_series_dfs =
        for series_id, df in processed_data.items():
            if df.empty:
                continue

            metadata = self.indicator_metadata.get(series_id, {})
            
            # --- Generate Commentary ---
            df = self._generate_short_term_analysis(series_id, df, metadata)
            df = self._generate_long_term_analysis(series_id, df, metadata)
            df = self._generate_risk_assessment(series_id, df, metadata)

            # --- Add Metadata Columns ---
            df[f'{series_id}_Is_Annualized'] = metadata.get('annualized', False)
            df = metadata.get('type', 'N/A')
            df[f'{series_id}_Horizon'] = metadata.get('horizon', 'N/A')
            df = '|'.join(metadata.get('sectors',))
            
            # Melt the technical indicators into a long format
            id_vars = [series_id] +
            
            # Ensure the main series column is present before melting
            if series_id not in df.columns:
                # This can happen if the rename in calculate_technical_indicators failed
                # or the input df was structured differently.
                # Find the 'value' column as a fallback.
                if 'value' in df.columns:
                    df.rename(columns={'value': series_id}, inplace=True)
                else: # If no value column, we can't proceed with this series
                    continue
            
            value_vars = [col for col in df.columns if col not in id_vars and col!= series_id]

            # Create a base DataFrame with the main series value
            base_df = df[[series_id] + id_vars].copy()
            base_df['Indicator_Name'] = 'Actual Value'
            base_df.rename(columns={series_id: 'Indicator_Value'}, inplace=True)
            
            # Melt the other indicators
            if value_vars:
                melted_df = df.melt(
                    id_vars=id_vars,
                    value_vars=value_vars,
                    var_name='Indicator_Name',
                    value_name='Indicator_Value'
                )
                # Combine the base value with the melted technical indicators
                final_df = pd.concat([base_df, melted_df], ignore_index=True)
            else:
                final_df = base_df

            final_df = series_id
            all_series_dfs.append(final_df)

        if not all_series_dfs:
            return pd.DataFrame()

        # Concatenate all individual series DataFrames into one large DataFrame
        power_bi_df = pd.concat(all_series_dfs, ignore_index=True)
        
        # Further clean-up and structuring can be done here
        # For example, extracting the base series name from the Indicator_Name
        power_bi_df = power_bi_df['Indicator_Name'].apply(lambda x: x.split('_')[-1] if '_' in x else x)

        return power_bi_df



class ExcelOutputGenerator:
    """Generates the final Excel dashboard output file."""

    def __init__(self, config: 'DashboardConfig'):
        self.config = config
        # This will now be used to generate the new descriptions sheet
        self.fdic_desc_df = pd.DataFrame.from_dict(FDIC_FIELD_DESCRIPTIONS, orient='index').reset_index()
        self.fdic_desc_df.rename(columns={'index': 'Metric Code', 'short': 'Metric Name', 'long': 'Description'}, inplace=True)

    def write_excel_output(self, file_path: str, **kwargs):
        """Writes all DataFrames to a single styled Excel file with multiple sheets."""
        logging.info(f"Writing final dashboard to: {file_path}")
        with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
            for sheet_name, df in kwargs.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Determine whether to write the index based on the sheet name
                    write_index = sheet_name in ["Latest_Peer_Snapshot", "Averages_8Q_All_Metrics"]
                    df.to_excel(writer, sheet_name=sheet_name, index=write_index)

            # [NEW] Call the method to write the descriptions sheet
            self._write_metric_descriptions_sheet(writer)
            
            # Apply styling to other sheets
            self._apply_summary_styles(writer, kwargs.get("Summary_Dashboard", pd.DataFrame()))
            self._apply_snapshot_styles(writer, kwargs.get("Latest_Peer_Snapshot", pd.DataFrame()))
            self._apply_macro_analysis_styles(writer, kwargs.get("Macro_Analysis", pd.DataFrame()))
            self._write_enriched_fdic_detail(writer, kwargs.get('FDIC_Data', pd.DataFrame()))
            self._write_enriched_fred_macro(writer, kwargs.get('FRED_Data'), kwargs.get('FRED_Descriptions'))
        
        logging.info("Excel file written successfully.")
    
    def _write_metric_descriptions_sheet(self, writer):
        """[NEW] Writes the FDIC metric descriptions to a dedicated sheet."""
        sheet_name = 'FDIC_Metric_Descriptions'
        logging.info(f"Writing {sheet_name} sheet...")
        self.fdic_desc_df.to_excel(writer, sheet_name=sheet_name, index=False)
        worksheet = writer.sheets[sheet_name]
        # Set column widths for better readability
        worksheet.set_column('A:A', 35) # Metric Code
        worksheet.set_column('B:B', 35) # Metric Name
        worksheet.set_column('C:C', 80) # Description

    def _write_enriched_fdic_detail(self, writer, df: pd.DataFrame):
        if df.empty: return
        id_vars = ['CERT', 'NAME', 'REPDTE']
        value_vars = [col for col in self.fdic_desc_df['Metric Code'] if col in df.columns]
        
        fdic_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='Metric Code', value_name='Value')
        fdic_enriched = pd.merge(fdic_long, self.fdic_desc_df[['Metric Code', 'Metric Name', 'Description']], on='Metric Code', how='left')
        
        cols_order = ['CERT', 'NAME', 'REPDTE', 'Metric Name', 'Value', 'Description', 'Metric Code']
        fdic_enriched = fdic_enriched[[col for col in cols_order if col in fdic_enriched.columns]]
        
        fdic_enriched.to_excel(writer, sheet_name='FDIC_Detail', index=False)
        worksheet = writer.sheets['FDIC_Detail']
        worksheet.set_column('B:B', 25)
        worksheet.set_column('D:D', 25)
        worksheet.set_column('F:F', 50)

    def _write_enriched_fred_macro(self, writer, macro_df, macro_desc_df):
        # Handle None values by converting to empty DataFrames
        if macro_df is None:
            macro_df = pd.DataFrame()
        if macro_desc_df is None:
            macro_desc_df = pd.DataFrame()
        if macro_df.empty or macro_desc_df.empty: 
            return
        fred_long = macro_df.melt(id_vars=['date'], var_name='Series ID', value_name='Value')
        fred_enriched = pd.merge(fred_long, macro_desc_df, on='Series ID', how='left')
        fred_enriched.to_excel(writer, sheet_name='FRED_TimeSeries_Data', index=False)

    def _apply_summary_styles(self, writer, df: pd.DataFrame):
        if df.empty: return
        workbook = writer.book
        worksheet = writer.sheets['Summary_Dashboard']
        formats = {
            'green': workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'}),
            'yellow': workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'}),
            'red': workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        }
        worksheet.set_column('A:A', 30)
        worksheet.set_column('B:H', 18)
        try:
            flag_col_index = df.columns.get_loc('Performance_Flag')
            worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {'type': 'formula', 'criteria': f'SEARCH("Top Quartile", ${chr(ord("A") + flag_col_index)}2)', 'format': formats['green']})
            worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {'type': 'formula', 'criteria': f'SEARCH("Better than Median", ${chr(ord("A") + flag_col_index)}2)', 'format': formats['yellow']})
            worksheet.conditional_format(1, 0, len(df), len(df.columns) - 1, {'type': 'formula', 'criteria': f'SEARCH("Bottom Quartile", ${chr(ord("A") + flag_col_index)}2)', 'format': formats['red']})
        except KeyError:
            logger.warning("Styling skipped: 'Performance_Flag' column not found.")

    def _apply_snapshot_styles(self, writer, df: pd.DataFrame):
        if df.empty: return
        worksheet = writer.sheets.get('Latest_Peer_Snapshot')
        if not worksheet: return
        percent_format = writer.book.add_format({'num_format': '0.00%'})
        for col_num, value in enumerate(df.columns):
            if value not in ['NAME', 'REPDTE']:
                worksheet.set_column(col_num + 1, col_num + 1, 18, percent_format)
        worksheet.set_column(1, 1, 25)

    def _apply_macro_analysis_styles(self, writer, df: pd.DataFrame):
        if df.empty: return
        worksheet = writer.sheets.get('Macro_Analysis')
        if not worksheet: return
        worksheet.set_column('A:B', 25)
        worksheet.set_column('C:E', 15)
        worksheet.set_column('F:F', 40)

# ==================================================================================
#  4. MAIN DASHBOARD CLASS & EXECUTION
# ==================================================================================

class BankPerformanceDashboard:
    def __init__(self, config: 'DashboardConfig'):
        self.config = config
        self.fdic_fetcher = FDICDataFetcher(config)
        self.fred_fetcher = FREDDataFetcher(config)
        self.processor = BankMetricsProcessor(config)
        self.analyzer = PeerAnalyzer(config)
        self.macro_analyzer = MacroTrendAnalyzer(config)
        self.output_gen = ExcelOutputGenerator(config)
        Path(config.output_dir).mkdir(exist_ok=True)
        
    def _create_peer_composite(self, df: pd.DataFrame) -> pd.DataFrame:
        peer_df = df[df['CERT'].isin(self.config.peer_bank_certs)]
        if peer_df.empty: return df
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            numeric_cols = df.select_dtypes(include=np.number).columns.drop('CERT', errors='ignore')
            peer_avg = peer_df.groupby('REPDTE')[numeric_cols].mean().reset_index()
            peer_avg['CERT'] = 99999
            peer_avg['NAME'] = "Peers' Average"
            return pd.concat([df, peer_avg], ignore_index=True)
        
    def run(self) -> Dict[str, Any]:
        logging.info("Starting dashboard generation...")
        fdic_df, _ = self.fdic_fetcher.fetch_all_banks()
        if fdic_df.empty: raise ValueError("No FDIC data retrieved.")
        
        proc_df = self.processor.create_derived_metrics(fdic_df)
        proc_df_with_ttm = self.processor.calculate_ttm_metrics(proc_df)
        proc_df_with_peers = self._create_peer_composite(proc_df_with_ttm)
        
        peer_comp_df = self.analyzer.create_peer_comparison(proc_df_with_peers)
        snapshot_df = self.processor.create_latest_snapshot(proc_df_with_peers)
        avg_8q_all_metrics_df = self.processor.calculate_8q_averages(proc_df_with_peers)

        fred_df, fred_desc_df = self.fred_fetcher.fetch_all_series()
        enhanced_analyzer = MacroTrendAnalyzer(self.config)
        processed_data = {}
        validation_reports = {}
        for series_id in self.config.FRED_SERIES_TO_FETCH.keys():
            if series_id in fred_df.columns:
                validation_reports[series_id] = enhanced_analyzer.validate_series_data(
                    series_id, fred_df[series_id]
                )
                if validation_reports[series_id]['status']!= 'Error':
                    processed_data[series_id] = enhanced_analyzer.calculate_technical_indicators(
                        fred_df[series_id], series_id
                    )
        
        validation_df = pd.DataFrame.from_dict(validation_reports, orient='index')
        
        powerbi_macro_df = enhanced_analyzer.generate_powerbi_output(processed_data)

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        fname = f"{self.config.output_dir}/Bank_Performance_Dashboard_{ts}.xlsx"

        self.output_gen.write_excel_output(
           file_path=fname,
           Summary_Dashboard=peer_comp_df,
           Latest_Peer_Snapshot=snapshot_df,
           Averages_8Q_All_Metrics=avg_8q_all_metrics_df,
           FDIC_Metric_Descriptions=self.output_gen.fdic_desc_df,
           # Use the correct variable here
           Macro_Analysis=powerbi_macro_df,
           FDIC_Data=proc_df_with_peers, 
           FRED_Data=fred_df,
           FRED_Descriptions=fred_desc_df,
           Enhanced_Macro_Analysis=powerbi_macro_df,
           Data_Validation_Report=validation_df,
       )
       logging.info(f"Dashboard successfully generated: {fname}")
       # Also use the correct variable in the return statement
       return {"output_file": fname, "fdic_df": fdic_df, "fred_df": fred_df, "macro_analysis_df": powerbi_macro_df}

def load_config() -> DashboardConfig:
    """Loads configuration from a hardcoded source."""
    return DashboardConfig(
        fred_api_key="de4ecbcab88d5d5c5c330d772c08bcfe",
        subject_bank_cert=19977,
        peer_bank_certs=[26876,9396,18221,16068,22953,57919,20234,58647,26610,32541]
    )

        
def main():
    try:
        config = load_config()
        dash = BankPerformanceDashboard(config)
        run_results = dash.run()
        
        # --- FIX: New, detailed verification summary ---
        print("\n" + "="*80)
        print("DASHBOARD GENERATION COMPLETE")
        print(f"Output file located at: {run_results['output_file']}")
        print("="*80)
        print("\n--- DATA SERIES VERIFICATION ---")


    except Exception as e:
        logger.error(f"Application failed: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()