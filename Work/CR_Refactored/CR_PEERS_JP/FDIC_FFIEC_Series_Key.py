#key for FDIC & FFIEC keys

FDIC_FALLBACK_MAP = {
    # --- SBL & Fund Finance (FFIEC Bulk / Consolidated) ---
    "LNOTHPCS":    ["RCFD1545", "RCON1545", "LNOTHER"],  # Legacy SBL -> Consolidated SBL
    "LNOTHNONDEP": ["RCFDJ454", "RCONJ454"],             # Legacy Fund Fin -> Consolidated Fund Fin

    # NOTE:
    # Do NOT map 'LNRERES' or 'LNRELOC' here.
    # They are FDIC API fields and must remain sourced from FDIC, otherwise Standard Resi can go to 0
    # when FFIEC-only MDRMs are missing in the FDIC dataframe.

    # --- RI-C Disaggregated Allowance (FFIEC Bulk Only) ---
    "RCFDJ466": ["FFIEC"],  # Construction


    # --- RI-C Disaggregated Allowance (FFIEC Bulk Only) ---
    "RCFDJ466": ["FFIEC"], # Construction
    "RCFDJ467": ["FFIEC"], # Commercial RE
    "RCFDJ468": ["FFIEC"], # Residential
    "RCFDJ469": ["FFIEC"], # C&I
    "RCFDJ470": ["FFIEC"], # Credit Cards
    "RCFDJ471": ["FFIEC"], # Other Consumer
    "RCFDJ472": ["FFIEC"], # Unallocated
    "RCFDJ474": ["FFIEC"], # Other Loans (SBL Proxy)

    # --- Consumer Granular (Derived) ---
    "LNCONAUTO": ["LNAUTO"],
    "LNCONCC":   ["LNCRCD"],
    "LNCONOTHX": ["DERIVED"], # Calculated as LNCON - LNAUTO - LNCRCD

    # --- Legacy Real Estate Aliases (Mapping to standard codes) ---
    "LNREOTH":  ["LNRENROT"], # "Income Producing" -> Non-Owner Occupied Nonfarm
    "NALRERES": ["NARERES"],  # Legacy Resi NA -> Total Resi NA
    "P9RENRES": ["P9RENROT"], # Legacy Nonfarm NA -> Non-Owner Nonfarm NA
}

# [UPDATED] MASTER FETCH LIST
# Includes ALL valid API fields + Legacy placeholders (resolved later)
FDIC_FIELDS_TO_FETCH = [
    # --- Identifiers & Dates ---
    "CERT", "NAME", "REPDTE",

    # --- Balance Sheet Totals ---
    "ASSET", "LIAB", "DEP", "EQ",

    # --- Profitability ---
    "ROA", "ROE", "NIMY", "EEFFR", "NONIIAY", "ELNATRY", "EINTEXP",

    # --- Capital & RWA ---
    "RBCT1CER", "RBCT1J", "RBCT2", "RWAJ", "RBCRWAJ", "RB2LNRES",
    "EQCS", "EQSUR", "EQUP", "EQCCOMPI", "EQPP",

    # --- Reserves & Funding ---
    "LNATRES", "OTHBOR", "MUTUAL", "FREPP",

    # ==========================================================================
    # RAW BALANCE SHEET SERIES (For Direct Calculations & Liquidity)
    # ==========================================================================
    "RCFD2170",   # Total Assets (raw)
    "RCFD2948",   # Total Liabilities (raw)
    "RCFD3210",   # Total Equity Capital (raw)
    "RCFD2200",   # Total Deposits (raw)
    "RCFD1400",   # Gross Loans (raw) - backup for LNLS
    "RCFD3123",   # Allowance for Loan Losses (raw)

    # --- Liquidity Components (RESTORED) ---
    "RCFD0010",   # Cash and Balances Due from Depository Institutions
    "RCFD0081",   # Federal Funds Sold
    "RCFD1754",   # Held-to-Maturity Securities (Amortized Cost)
    "RCFD1773",   # Available-for-Sale Securities (Fair Value)
    "RCFD3545",   # Trading Assets (if applicable)

    # --- Unused Commitments (RESTORED) ---
    "RCFD3423",   # Unused Commitments
    "RCFD3814",   # Credit Card Lines (unused portion)
    "RCFD6550",   # Commercial Real Estate Commitments

    # --- Raw Income Statement (For Backup/Validation) ---
    "RIAD4340",   # Net Income
    "RIAD4107",   # Total Interest Income
    "RIAD4073",   # Total Interest Expense
    "RIAD4079",   # Total Noninterest Income
    "RIAD4093",   # Total Noninterest Expense
    "RIAD4301",   # Retained Earnings
    "RIAD4010",   # Interest Income on Loans (raw)
    "RIAD4115",   # Interest Expense on Deposits (raw)
    "RIAD4608",   # Total Charge-Offs (raw)
    "RIAD4609",   # Total Recoveries (raw)

    # --- INCOME & PROFITABILITY (Corrected Series) ---
    "ILNDOM",   # Int Inc Loans (Domestic)
    "ILNFOR",   # Int Inc Loans (Foreign)
    "EDEPDOM",  # Int Exp Deposits (Domestic)
    "EDEPFOR",  # Int Exp Deposits (Foreign)
    "ELNATR",   # Provision for Credit Losses (YTD)
    "EINTEXP",  # Total Interest Expense (YTD)

    # --- TOP HOUSE DELINQUENCY ---
    "P3LNLS",   # PD 30-89 Total
    "P9LNLS",   # PD 90+ Total


    # --- LOAN BALANCES (Authoritative) ---
    "LNLS", "LNLSNET",
    "LNCI",       # C&I
    "LNRECONS",   # Construction
    "LNREMULT",   # Multifamily
    "LNRENROW",   # Owner-Occ CRE
    "LNRENROT",   # Non-Owner CRE (Income Producing)
    "LNREAG",     # Farmland
    "LNRERES",    # 1-4 Family Resi Total
    "LNRELOC",    # HELOC
    "LNCON",      # Consumer Total
    "LNCRCD",     # Credit Cards
    "LNAUTO",     # Auto
    "LNOTHER",    # All Other Loans
    "LS",         # Leases
    "LNAG",       # Ag Loans

    # --- LEGACY BALANCES (Mapped via Fallback) ---
    "LNREOTH",    # Will map to LNRENROT
    "LNOTHPCS",   # Will map to RCFD1545
    "LNOTHNONDEP",# Will map to RCFDJ454
    "LNCONAUTO", "LNCONCC", "LNCONOTHX", # Will map/derive

    # --- NET CHARGE-OFFS ---
    "NTLNLS", "NCLNLS",
    "NTCI",
    "NTRECONS", "NTREMULT", "NTRENROT", "NTRENROW", "NTREAG", "NTRENRES", # <--- Added NTRENROW
    "NTRERES", "NTRELOC", # Other Consumer NCO

    # --- PAST DUE 30-89 ---
    "P3LNLS", "P3CI",
    "P3RECONS", "P3LREMUL", "P3RENROT", "P3RENROW", "P3REAG", "P3RENRES",
    "P3RERES", "P3RELOC",
    "P3CON", "P3CRCD", "P3AUTO", "P3LS", "P3AG", "P3OTHLN",
    "P3CONOTH",

    # --- PAST DUE 90+ ---
    "P9LNLS", "P9CI",
    "P9RECONS", "P9REMULT", "P9RENROT", "P9RENROW", "P9REAG", "P9RENRES",
    "P9RERES", "P9RELOC",
    "P9CON", "P9CRCD", "P9AUTO", "P9LS", "P9AG", "P9OTHLN",
    "P9CONOTH",

    # --- NONACCRUAL ---
    "NACI",
    "NARECONS", "NAREMULT", "NARENROT", "NARENROW", "NAREAG", "NARENRES",
    "NARERES", "NARELOC",
    "NACON", "NACRCD", "NAAUTO", "NALS", "NAAG", "NAOTHLN",
    "NACONOTH",
    "NALRERES", # Legacy

    # =============================================================================
    # NORMALIZATION EXCLUSION FIELDS (Ex-Commercial/Ex-Consumer Segments)
    # =============================================================================
    # These fields are used to create "apples-to-apples" comparisons by removing
    # Mass Market Consumer and Commercial Banking segments that MSPBNA does not
    # participate in.

    # --- 1. Domestic C&I (Remove standard business lending) ---
    "RCON1763",    # Balance: Commercial & Industrial loans to U.S. addressees (Domestic)
    "RIAD4608",    # NCO: C&I Charge-offs (Domestic)
    "RIAD4609",    # NCO: C&I Recoveries (Domestic) - subtract from charge-offs
    "RCON1606",    # PD30: C&I Past Due 30-89 (Domestic)
    "RCON1607",    # PD90: C&I Past Due 90+ (Domestic)
    "RCON1608",    # NA: C&I Nonaccrual (Domestic)
    # === C&I Granular Components ===
    "RIAD4645",
    "RIAD4646",
    "RIAD4617",
    "RIAD4618",
    # --- 2. Nondepository Financial Institutions (NDFI) - Fund Finance/Shadow Banking ---
    "RCONJ454", "RCFDJ454",    # Balance: Loans to nondepository financial institutions
    # NCO: Note - NDFI NCOs not separately reported (buried in "All Other")
    # NDFI (Past Due / Nonaccrual)
    'RCONJ458', 'RCFDJ458',    # PD30: NDFI Past Due 30-89
    'RCONJ459', 'RCFDJ459',    # PD90: NDFI Past Due 90+
    'RCONJ460', 'RCFDJ460',    # NA: NDFI Nonaccrual

    "RCFDF162",


    # --- 3. ADC Loans (Remove Construction Risk) ---
    "RCON1420",    # Balance: Construction, land development, and other land loans - Total
    "RIAD4658",    # NCO: ADC Charge-offs
    "RIAD4659",    # NCO: ADC Recoveries - subtract from charge-offs
    "RCON2759",    # PD30: ADC Past Due 30-89
    "RCON2769",    # PD90: ADC Past Due 90+
    "RCON3492",    # NA: ADC Nonaccrual

    # --- 4. Mass Market Consumer (Credit Cards, Auto, Ag) ---
    "RCFDB538",    # Balance: Credit Card Loans
    "RIADB514",    # NCO: Credit Card Charge-offs
    "RIADB515",    # NCO: Credit Card Recoveries
    "RCFDB575",    # NA: Credit Card Nonaccrual
    "RCFDK137",    # Balance: Auto Loans
    "RIADK205",    # NCO: Auto Charge-offs
    "RIADK206",    # NCO: Auto Recoveries
    "RCFDK213",    # NA: Auto Nonaccrual
    "RCFD1590",    # Balance: Agricultural Loans
    "RIAD4635",    # NCO: Agricultural Charge-offs
    "RIAD4645",    # NCO: Agricultural Recoveries
    "RCFD5341",    # NA: Agricultural Nonaccrual
    "RCON2746",    # PD30: Agricultural Past Due 30-89
    "RCON2747",    # PD90: Agricultural Past Due 90+

    # --- 5. Foreign Government & Banks (Exclude from Domestic Peer View) ---
    "RCFD2081",    # Balance: Loans to Foreign Governments
    "RCFD2005",    # Balance: Loans to Depository Institutions (Banks)

    # --- 6. C&I NCO Proxy (Alternative to segment-level calculation) ---
    "RIAD4638",    # NCO: Commercial & Industrial Net Charge-offs (Total)
    # === C&I Granular Components (Required for Verification) ===
    "RIAD4645",
    "RIAD4646",
    "RIAD4617",
    "RIAD4618",
    # --- 6. C&I NCO Proxy (Alternative to segment-level calculation) ---
    "RIAD4638",  # NCO: Commercial & Industrial Net Charge-offs (Total)

    # [FIXED] Granular Exclusion Fields (Strings only)
    "RIAD4655", "RIAD4665",  # Ag
    "RIADK129", "RIADK133",  # Auto Granular
    "RIADK205", "RIADK206",  # Other Consumer
    "RIAD4643", "RIAD4627",  # Foreign Govt
    "RIAD4644", "RIAD4628",  # All Other
    "RIADC891", "RIADC892",  # Constr 1-4
    "RIADC893", "RIADC894",  # Constr Other
    "RIAD4617", "RIAD4618",  # C&I Components

    # --- Missing Capital & RWA Fields ---
    "RWA",         # Total Risk-Weighted Assets (different from RWAJ)
    "AOBS",        # Allowance for Off-Balance Sheet Exposures
    "RBC1RWAJ",    # Tier 1 Capital Ratio
    "LEVRATIO",    # Tier 1 Leverage Ratio
    "CET1R",       # Common Equity Tier 1 Capital Ratio
    "EQO",         # Other Equity Capital Components


    # --- Missing NCO Fields ---
    "NCLNLS",      # Net Charge-Offs (alternative series)
    "NTRENROW",    # Owner-Occ CRE NCOs (MISSING - already has NTRENROT)
    "NTLSNFOO",    # Nonfarm CRE Owner-Occ NCOs (appears to be typo for NTRENROW)
    "NTCON",       # Consumer NCOs (Total)
    "NTCRCD",      # Credit Card NCOs
    "NTAUTO",      # Auto Loan NCOs
    "NTLS",        # Lease NCOs
    "NTOTHER",     # Other Loan NCOs
    "NTAG",        # Agricultural NCOs

    # --- Missing PD 30-89 Fields ---
    "P3RENRES",    # Nonfarm Nonres Income CRE 30-89 (typo in your doc as P3LRENRS)

    # --- Missing PD 90+ Fields ---
    "P9CONOTH",    # Consumer Other PD 90+

    # --- Missing Nonaccrual Fields ---
    "NARENROT",    # Non-Owner CRE Nonaccrual (MISSING - only has NARENROW)

    # --- Missing RI-C ACL Fields (Amortized Cost) ---
    "RCFDJJ04",    # Construction - Amortized cost
    "RCFDJJ05",    # Commercial RE - Amortized cost
    "RCFDJJ06",    # Residential RE - Amortized cost
    "RCFDJJ07",    # Commercial - Amortized cost
    "RCFDJJ08",    # Credit cards - Amortized cost
    "RCFDJJ09",    # Other consumer - Amortized cost
    "RCFDJJ11",    # Total - Amortized cost

    # --- Missing RI-C ACL Fields (Allowance Balance) ---
    "RCFDJJ12",    # Construction - Allowance balance
    "RCFDJJ13",    # Commercial RE - Allowance balance
    "RCFDJJ14",    # Residential RE - Allowance balance
    "RCFDJJ15",    # Commercial - Allowance balance
    "RCFDJJ16",    # Credit cards - Allowance balance
    "RCFDJJ17",    # Other consumer - Allowance balance
    "RCFDJJ19",    # Total - Allowance balance

    # --- Missing RI-C ACL Fields (% of Amortized Cost) ---
    "RCFDJJ20",    # Construction - ACL % of amortized cost
    "RCFDJJ21",    # Commercial RE - ACL % of amortized cost
    "RCFDJJ23",    # Residential RE - ACL % of amortized cost

    # --- Missing Deposit Fields ---
    "RCONJ473",    # Time deposits $100k-$250k (Domestic)
    "RCONJ474",    # Time deposits >$250k (Domestic)
]
FDIC_FIELDS_TO_FETCH =list(dict.fromkeys(FDIC_FIELDS_TO_FETCH))
DERIVED_FIELD_DEFINITIONS = {

    # Derived Liquidity Metrics (NEW)
    "Liquid_Assets": {"title": "Liquid Assets", "description": "Cash + Fed Funds Sold + AFS Securities."},
    "Liquidity_Ratio": {"title": "Liquidity Ratio", "description": "Liquid Assets / Total Assets."},
    "HQLA": {"title": "High Quality Liquid Assets", "description": "Cash + Fed Funds Sold + HTM Securities + AFS Securities."},
    "HQLA_Ratio": {"title": "HQLA Ratio", "description": "High Quality Liquid Assets / Total Assets."},
    "Cash_to_Assets": {"title": "Cash to Assets", "description": "Cash and balances due from depository institutions / Total Assets."},
    "Securities_to_Assets": {"title": "Securities to Assets", "description": "(HTM + AFS Securities) / Total Assets."},
    "Loans_to_Deposits": {"title": "Loans to Deposits", "description": "Gross Loans / Total Deposits."},

    # Capital Ratios (NEW)
    "Equity_to_Assets": {"title": "Equity to Assets", "description": "Total Equity Capital / Total Assets."},
    "Leverage_Ratio": {"title": "Leverage Ratio (Raw)", "description": "Total Assets / Total Equity Capital."},

    # Raw Profitability (NEW - backup for FDIC derived)
    "ROA_Raw": {"title": "ROA (Raw)", "description": "Return on Assets calculated from raw RIAD series (annualized)."},
    "ROE_Raw": {"title": "ROE (Raw)", "description": "Return on Equity calculated from raw RIAD series (annualized)."},
    "NIM_Raw": {"title": "Net Interest Margin (Raw)", "description": "Net Interest Margin calculated from raw RIAD series (annualized)."},
    "Efficiency_Ratio_Raw": {"title": "Efficiency Ratio (Raw)", "description": "Noninterest Expense / (Noninterest Income + Net Interest Income)."},
    "Yield_on_Loans_Raw": {"title": "Yield on Loans (Raw)", "description": "Interest income on loans (annualized) / Gross Loans."},
    "Cost_of_Deposits_Raw": {"title": "Cost of Deposits (Raw)", "description": "Interest expense on deposits (annualized) / Total Deposits."},
    "Unused_Commitment_Ratio": {"title": "Unused Commitment Ratio", "description": "Unused Commitments / Gross Loans."},


    # Additional derived metrics
    "Total_Capital": {"title": "Total Capital", "description": "Total risk-based capital available for regulatory purposes."},
    "Common_Stock_Pct": {"title": "Common Stock %", "description": "Common stock as a percentage of total equity capital."},
    "EQSUR_Pct": {"title": "EQSUR %", "description": "EQSUR as a percentage of total equity capital."},
    "Retained_Earnings_Pct": {"title": "Retained Earnings %", "description": "Retained earnings (net of EQCCOMPI) as a percentage of total equity capital."},
    "Preferred_Stock_Pct": {"title": "Preferred Stock %", "description": "Perpetual preferred stock as a percentage of total equity capital."},
    "CI_to_Capital_Risk": {"title": "C&I / Total Capital", "description": "Commercial & Industrial loans as a percentage of total capital."},

    # TTM Capital Composition
    "TTM_Common_Stock_Pct": {"title": "TTM Common Stock %", "description": "Trailing 12-month average common stock percentage of total equity."},
    "TTM_EQSUR_Pct": {"title": "TTM EQSUR %", "description": "Trailing 12-month average EQSUR percentage of total equity."},
    "TTM_Retained_Earnings_Pct": {"title": "TTM Retained Earnings %", "description": "Trailing 12-month average retained earnings percentage of total equity."},
    "TTM_Preferred_Stock_Pct": {"title": "TTM Preferred Stock %", "description": "Trailing 12-month average preferred stock percentage of total equity."},
    # Asset Quality - Top of House
    "NCLNLS":   {"title": "Total Noncurrent Loans", "description": "Total loans and leases past due 90 days or more plus nonaccrual loans."},

    # ===== DERIVED METRICS (MSPBNA V6 PRIVATE BANK VIEW) =====

    # --- 1. BANK LEVEL PROFITABILITY & COVERAGE ---
    "Cost_of_Funds": {"title": "Cost of Funds", "description": "Annualized cost of interest-bearing liabilities (Quarterly Interest Expense * 4 / Average Interest-Bearing Liabilities)."},
    "Allowance_to_Gross_Loans_Rate": {"title": "ACL / Total Loans", "description": "Total Allowance for Credit Losses as a percentage of Total Gross Loans."},
    "Risk_Adj_Allowance_Coverage": {"title": "ACL / (Loans - SBL)", "description": "Risk-Adjusted Coverage: Total Allowance divided by (Total Loans minus Securities-Based Lending). Excludes SBL as it is fully collateralized by marketable securities and requires minimal reserves."},
    "Nonaccrual_to_Gross_Loans_Rate": {"title": "Nonaccrual / Loans", "description": "Total nonaccrual loans as a percentage of total gross loans."},
    "TTM_NCO_Rate": {"title": "Total NCO Rate", "description": "Trailing 12-Month sum of Net Charge-Offs as a percentage of TTM Average Gross Loans."},

    # --- 2. LOAN COMPOSITION (PORTFOLIO MIX) ---
    "SBL_Composition": {"title": "SBL %", "description": "Securities-Based Lending (Loans for purchasing or carrying securities) as a % of Total Loans."},
    "Fund_Finance_Composition": {"title": "Fund Finance %", "description": "Loans to Nondepository Financial Institutions (PE/VC Capital Call Lines) as a % of Total Loans."},
    "Wealth_Resi_Composition": {"title": "Wealth Resi %", "description": "Wealth Residential (Jumbo 1-4 Family First Liens + HELOCs) as a % of Total Loans."},
    "Corp_CI_Composition": {"title": "Corp C&I %", "description": "Traditional Commercial & Industrial loans to operating companies as a % of Total Loans."},
    "CRE_OO_Composition": {"title": "CRE Owner-Occ %", "description": "Owner-Occupied Nonfarm Nonresidential CRE (Business Cash Flow dependent) as a % of Total Loans."},
    "CRE_Investment_Composition": {"title": "CRE Invest. %", "description": "Investment CRE (Construction, Multifamily, Non-OO Nonfarm) as a % of Total Loans."},
    "Consumer_Auto_Composition": {"title": "Auto %", "description": "Automobile loans as a % of Total Loans."},
    "Consumer_Other_Composition": {"title": "Cons. Other %", "description": "Other Consumer loans (Credit Cards, Unsecured, Other Revolving) as a % of Total Loans."},

    # --- 3. SEGMENT RISK: SECURITIES BASED LENDING (SBL) ---
    "SBL_TTM_NCO_Rate": {"title": "SBL NCO Rate", "description": "Net Charge-offs for SBL (Estimated/Allocated from All Other Loans) as a % of SBL Loans."},
    "SBL_TTM_PD30_Rate": {"title": "SBL PD 30-89", "description": "Past Due 30-89 Days for SBL (Proxy: All Other Loans) as a % of SBL Loans."},
    "SBL_NA_Rate": {"title": "SBL Nonaccrual", "description": "Nonaccrual Rate for SBL (Proxy: All Other Loans)."},

    # --- 4. SEGMENT RISK: FUND FINANCE ---
    "Fund_Finance_TTM_PD30_Rate": {"title": "Fund Fin. PD 30-89", "description": "Past Due 30-89 Days for Nondepository Fin. Institutions (Memo Item)."},
    "Fund_Finance_NA_Rate": {"title": "Fund Fin. Nonaccrual", "description": "Nonaccrual Rate for Nondepository Fin. Institutions (Memo Item)."},

    # --- 5. SEGMENT RISK: WEALTH RESIDENTIAL ---
    "Wealth_Resi_TTM_NCO_Rate": {"title": "Wealth Resi NCOs", "description": "Net Charge-offs for 1-4 Family First Liens & HELOCs as a % of Avg Segment Loans."},
    "Wealth_Resi_TTM_PD30_Rate": {"title": "Wealth Resi PD 30-89", "description": "Past Due 30-89 Days for 1-4 Family First Liens & HELOCs."},
    "Wealth_Resi_NA_Rate": {"title": "Wealth Resi Nonaccrual", "description": "Nonaccrual Rate for 1-4 Family First Liens & HELOCs."},

    # --- 6. SEGMENT RISK: COMMERCIAL & INDUSTRIAL ---
    "Corp_CI_TTM_NCO_Rate": {"title": "C&I NCO Rate", "description": "Net Charge-offs for C&I Loans as a % of Avg C&I Loans."},
    "Corp_CI_TTM_PD30_Rate": {"title": "C&I PD 30-89", "description": "Past Due 30-89 Days for C&I Loans."},
    "Corp_CI_NA_Rate": {"title": "C&I Nonaccrual", "description": "Nonaccrual Rate for C&I Loans."},


    # --- 7. SEGMENT RISK: CRE (OWNER OCCUPIED) ---
    "CRE_OO_TTM_NCO_Rate": {"title": "CRE OO NCO Rate", "description": "Net Charge-offs for Owner-Occupied CRE as a % of Avg OO CRE Loans."},
    "CRE_OO_TTM_PD30_Rate": {"title": "CRE OO PD 30-89", "description": "Past Due 30-89 Days for Owner-Occupied CRE."},
    "CRE_OO_NA_Rate": {"title": "CRE OO Nonaccrual", "description": "Nonaccrual Rate for Owner-Occupied CRE."},

    # --- 8. SEGMENT RISK: CRE (INVESTMENT) ---
    "CRE_Investment_TTM_NCO_Rate": {"title": "CRE Inv. NCO Rate", "description": "Net Charge-offs for Investment CRE (Constr/Multi/Non-OO) as a % of Avg Inv. CRE Loans."},
    "CRE_Investment_TTM_PD30_Rate": {"title": "CRE Inv. PD 30-89", "description": "Past Due 30-89 Days for Investment CRE."},
    "CRE_Investment_NA_Rate": {"title": "CRE Inv. Nonaccrual", "description": "Nonaccrual Rate for Investment CRE."},
    "CRE_Concentration_Capital_Risk": {"title": "Inv. CRE / Capital", "description": "Total Investment CRE Loans (Construction + Multifamily + Non-OO) as a percentage of Tier 1 Capital + ACL. (Proxy for Reg Concentration)."},

    # --- 9. SEGMENT RISK: CONSUMER (AUTO & OTHER) ---
    "Consumer_Auto_TTM_NCO_Rate": {"title": "Auto NCO Rate", "description": "Net Charge-offs for Auto Loans as a % of Avg Auto Loans."},
    "Consumer_Other_TTM_NCO_Rate": {"title": "Cons. Other NCOs", "description": "Net Charge-offs for Credit Cards & Other Consumer Loans as a % of Avg Segment Loans."},
    # --- 10. SEGMENT GROWTH METRICS (TTM / YEAR-OVER-YEAR) ---
    "Total_Loan_Growth_TTM": {"title": "Total Loan Growth", "description": "Trailing 12-Month (Year-over-Year) growth rate of Total Gross Loans."},
    "SBL_Growth_TTM": {"title": "SBL Growth", "description": "TTM growth rate of the Securities-Based Lending portfolio."},
    "Fund_Finance_Growth_TTM": {"title": "Fund Finance Growth", "description": "TTM growth rate of Loans to Nondepository Financial Institutions."},
    "Wealth_Resi_Growth_TTM": {"title": "Wealth Resi Growth", "description": "TTM growth rate of the Wealth Residential portfolio (1-4 Family First Liens + HELOCs)."},
    "Corp_CI_Growth_TTM": {"title": "C&I Growth", "description": "TTM growth rate of the Commercial & Industrial portfolio."},
    "CRE_OO_Growth_TTM": {"title": "CRE OO Growth", "description": "TTM growth rate of Owner-Occupied Commercial Real Estate."},
    "CRE_Investment_Growth_TTM": {"title": "CRE Inv. Growth", "description": "TTM growth rate of Investment CRE (Construction + Multifamily + Non-OO)."},
    "Consumer_Auto_Growth_TTM": {"title": "Auto Growth", "description": "TTM growth rate of the Automobile Loan portfolio."},
    "Consumer_Other_Growth_TTM": {"title": "Cons. Other Growth", "description": "TTM growth rate of Other Consumer Loans (Credit Cards + Unsecured)."},

    # 1. RI-C II: Amortized Cost (Denominator)
    # -------------------------------------------------------------------------
    "RIC_Constr_Cost": {
        "title": "Construction – Amortized Cost",
        "description": "Total amortized cost of construction and land development loans (Source: JJ04)."
    },
    "RIC_CRE_Cost": {
        "title": "CRE – Amortized Cost",
        "description": "Total amortized cost of commercial real estate loans (Source: JJ05)."
    },
    "RIC_Resi_Cost": {
        "title": "Residential – Amortized Cost",
        "description": "Total amortized cost of residential real estate loans (Source: JJ06)."
    },
    "RIC_Comm_Cost": {
        "title": "C&I – Amortized Cost",
        "description": "Total amortized cost of commercial and industrial loans (Source: JJ07)."
    },
    "RIC_Card_Cost": {
        "title": "Credit Card – Amortized Cost",
        "description": "Total amortized cost of credit card loans (Source: JJ08)."
    },
    "RIC_OthCons_Cost": {
        "title": "Other Consumer – Amortized Cost",
        "description": "Total amortized cost of other consumer loans (Source: JJ09)."
    },

    # -------------------------------------------------------------------------
    # 2. RI-C II: Allowance for Credit Losses (Numerator)
    # -------------------------------------------------------------------------
    "RIC_Constr_ACL": {
        "title": "Construction – ACL Balance",
        "description": "Allowance for credit losses allocated to construction loans (Source: JJ12)."
    },
    "RIC_CRE_ACL": {
        "title": "CRE – ACL Balance",
        "description": "Allowance for credit losses allocated to CRE loans (Source: JJ13)."
    },
    "RIC_Resi_ACL": {
        "title": "Residential – ACL Balance",
        "description": "Allowance for credit losses allocated to residential loans (Source: JJ14)."
    },
    "RIC_Comm_ACL": {
        "title": "C&I – ACL Balance",
        "description": "Allowance for credit losses allocated to C&I loans (Source: JJ15)."
    },
    "RIC_Card_ACL": {
        "title": "Credit Card – ACL Balance",
        "description": "Allowance for credit losses allocated to credit cards (Source: JJ16)."
    },
    "RIC_OthCons_ACL": {
        "title": "Other Consumer – ACL Balance",
        "description": "Allowance for credit losses allocated to other consumer loans (Source: JJ17)."
    },

    # -------------------------------------------------------------------------
    # 3. Risk Status (Nonaccrual, Past Due) - The "Risk Stack"
    # -------------------------------------------------------------------------
    "RIC_CRE_Nonaccrual": {
        "title": "CRE – Nonaccrual",
        "description": "Nonaccrual loans secured by CRE. Uses Row-Wise Max resolution: Max(Total Reported, Sum of Subcomponents) to handle reporting differences."
    },
    "RIC_CRE_PD30": {
        "title": "CRE – Past Due 30-89 Days",
        "description": "Loans secured by CRE past due 30-89 days and still accruing."
    },
    "RIC_CRE_PD90": {
        "title": "CRE – Past Due 90+ Days",
        "description": "Loans secured by CRE past due 90+ days and still accruing."
    },
    # (Repeat pattern for other segments if explicit tagging is required,
    # but the logic applies universally)

    # -------------------------------------------------------------------------
    # 4. Net Charge-Offs (TTM) - The "Velocity"
    # -------------------------------------------------------------------------
    "RIC_Constr_NCO_TTM": {
        "title": "Construction – NCO (TTM)",
        "description": "Trailing 12-Month Sum of Net Charge-Offs for construction loans (Calculated from quarterly flows)."
    },
    "RIC_CRE_NCO_TTM": {
        "title": "CRE – NCO (TTM)",
        "description": "Trailing 12-Month Sum of Net Charge-Offs for CRE loans. Resolves granularity differences row-by-row."
    },
    "RIC_Resi_NCO_TTM": {
        "title": "Residential – NCO (TTM)",
        "description": "Trailing 12-Month Sum of Net Charge-Offs for residential loans."
    },
    "RIC_Comm_NCO_TTM": {
        "title": "C&I – NCO (TTM)",
        "description": "Trailing 12-Month Sum of Net Charge-Offs for C&I loans."
    },
    "RIC_Card_NCO_TTM": {
        "title": "Credit Card – NCO (TTM)",
        "description": "Trailing 12-Month Sum of Net Charge-Offs for credit cards."
    },
    "RIC_OthCons_NCO_TTM": {
        "title": "Other Consumer – NCO (TTM)",
        "description": "Trailing 12-Month Sum of Net Charge-Offs for other consumer loans."
    },

    # -------------------------------------------------------------------------
    # 5. Advanced Risk Ratios (The "So What")
    # -------------------------------------------------------------------------

    # A. Risk-Adjusted Coverage
    "RIC_CRE_Risk_Adj_Coverage": {
        "title": "CRE – Risk-Adj Coverage (x)",
        "description": "Ratio of ACL to Nonaccrual Loans. Indicates how many dollars of reserves exist for every dollar of currently bad loans. Target > 1.0x."
    },
    "RIC_Resi_Risk_Adj_Coverage": {
        "title": "Residential – Risk-Adj Coverage (x)",
        "description": "Ratio of ACL to Nonaccrual Loans for residential portfolio."
    },
    "RIC_Comm_Risk_Adj_Coverage": {
        "title": "C&I – Risk-Adj Coverage (x)",
        "description": "Ratio of ACL to Nonaccrual Loans for C&I portfolio."
    },

    # B. Burn Rate (Years of Reserves)
    "RIC_CRE_Years_of_Reserves": {
        "title": "CRE – Years of Reserves",
        "description": "Theoretical duration reserves would last at current loss rates (ACL / TTM NCOs). Higher is better."
    },
    "RIC_Card_Years_of_Reserves": {
        "title": "Card – Years of Reserves",
        "description": "Theoretical duration reserves would last at current loss rates (ACL / TTM NCOs)."
    },

    # C. Mismatches (Allocation & Risk)
    "RIC_CRE_Alloc_Mismatch": {
        "title": "CRE – Allocation Mismatch (%)",
        "description": "Difference between Share of Total ACL and Share of Total Loans. Positive = Over-reserved relative to volume."
    },
    "RIC_CRE_Risk_Mismatch": {
        "title": "CRE – Risk Mismatch (%)",
        "description": "Difference between Share of Total ACL and Share of Total Nonaccruals. Positive = Conservative (Reserves > Risk Share); Negative = Exposed."
    },

    # D. Standard Rates
    "RIC_CRE_NCO_Rate": {
        "title": "CRE – NCO Rate (TTM) %",
        "description": "Trailing 12-Month Net Charge-Offs divided by current Amortized Cost."
    },
    "RIC_CRE_Nonaccrual_Rate": {
        "title": "CRE – Nonaccrual Rate %",
        "description": "Nonaccrual loans divided by Amortized Cost."
    },
    "RIC_CRE_Delinquency_Rate": {
        "title": "CRE – Total Delinquency %",
        "description": "Total Past Due (30-89 days + 90+ days) divided by Amortized Cost."
    },

    # =========================================================================
    # NORMALIZED METRICS (Ex-Commercial/Ex-Consumer View)
    # =========================================================================
    # These metrics strip out Mass Market Consumer (Credit Cards, Auto, Ag) and
    # Commercial Banking (C&I, NDFI, ADC) segments to create apples-to-apples
    # comparison for private banking focused on SBL, Wealth Resi, and Fund Finance.

    # --- Exclusion Balances ---
    "Excl_CI_Balance": {
        "title": "Excluded C&I Balance",
        "description": "Domestic Commercial & Industrial loans excluded from normalized view (RCON1763)."
    },
    "Excl_NDFI_Balance": {
        "title": "Excluded NDFI Balance",
        "description": "Loans to Nondepository Financial Institutions excluded from normalized view (RCONJ454)."
    },
    "Excl_ADC_Balance": {
        "title": "Excluded ADC Balance",
        "description": "Construction, land development and other land loans excluded from normalized view (RCON1420)."
    },
    "Excl_CreditCard_Balance": {
        "title": "Excluded Credit Card Balance",
        "description": "Credit Card loans excluded from normalized view (RCFDB538)."
    },
    "Excl_Auto_Balance": {
        "title": "Excluded Auto Balance",
        "description": "Auto loans excluded from normalized view (RCFDK137)."
    },
    "Excl_Ag_Balance": {
        "title": "Excluded Ag Balance",
        "description": "Agricultural loans excluded from normalized view (RCFD1590)."
    },

    # --- Total Exclusions ---
    "Excluded_Balance": {
        "title": "Total Excluded Balance",
        "description": "Sum of all loan segments excluded from normalized peer comparison (C&I + NDFI + ADC + Credit Cards + Auto + Ag)."
    },
    "Excluded_NCO": {
        "title": "Total Excluded NCOs",
        "description": "Sum of net charge-offs from excluded segments (used to normalize NCO rate)."
    },
    "Excluded_Nonaccrual": {
        "title": "Total Excluded Nonaccruals",
        "description": "Sum of nonaccrual balances from excluded segments (used to normalize NA rate)."
    },

    # --- Normalized Master Metrics ---
    "Norm_Gross_Loans": {
        "title": "Normalized Gross Loans",
        "description": "Gross Loans minus Excluded Balance. Represents the 'Private Bank comparable' loan book."
    },
    "Norm_Total_NCO": {
        "title": "Normalized NCOs (TTM)",
        "description": "Total NCOs minus Excluded NCOs. Trailing 12-month net charge-offs on the normalized portfolio."
    },
    "Norm_Total_Nonaccrual": {
        "title": "Normalized Nonaccruals",
        "description": "Total Nonaccruals minus Excluded Nonaccruals. Nonaccrual balance on the normalized portfolio."
    },

    # --- Normalized Ratios ---
    "Norm_NCO_Rate": {
        "title": "Normalized NCO Rate",
        "description": "Norm_Total_NCO / Norm_Gross_Loans. NCO rate on the private bank comparable portfolio."
    },
    "Norm_Nonaccrual_Rate": {
        "title": "Normalized Nonaccrual Rate",
        "description": "Norm_Total_Nonaccrual / Norm_Gross_Loans. Nonaccrual rate on the private bank comparable portfolio."
    },
    "Norm_Delinquency_Rate": {
        "title": "Normalized Delinquency Rate",
        "description": "Total delinquent loans (30-89 + 90+) on normalized portfolio divided by normalized gross loans."
    },
    "Norm_ACL_Coverage": {
        "title": "Normalized ACL Coverage",
        "description": "ACL / Norm_Gross_Loans. Reserve coverage on the private bank comparable portfolio."
    },
    "Norm_Loan_Yield": {
        "title": "Normalized Loan Yield",
        "description": "Interest Income on Loans (TTM) / Norm_Gross_Loans. Yield on the private bank comparable portfolio."
    },
    "Norm_Provision_Rate": {
        "title": "Normalized Provision Rate",
        "description": "Provision Expense (TTM) / Norm_Gross_Loans. Provision rate on the private bank comparable portfolio."
    },
    "Norm_Loss_Adj_Yield": {
        "title": "Normalized Loss-Adj Yield",
        "description": "Norm_Loan_Yield minus Norm_NCO_Rate. Risk-adjusted return after credit losses."
    },
    "Norm_Risk_Adj_Return": {
        "title": "Normalized Risk-Adj Return",
        "description": "Norm_Loan_Yield minus Norm_Nonaccrual_Rate. Return adjusted for credit risk exposure."
    },

}

LOAN_CATEGORIES = {
    # 1. SBL & LIQUIDITY
    "SBL": {
        "balance": ["SBL_Balance"], # Resolved in Processor
        "nco": [], "pd30": [], "pd90": [], "na": []
    },
    # 2. FUND FINANCE
    "Fund_Finance": {
        "balance": ["Fund_Finance_Balance"], # Resolved in Processor
        "nco": [], "pd30": ["P3NDFI"], "pd90": ["P9NDFI"], "na": ["NANDFI"]
    },
    # 3. WEALTH RESIDENTIAL
    "Wealth_Resi": {
        "balance": ["Wealth_Resi_Balance"], # Collapsed in Processor
        "nco": ["NTRERES", "NTRELOC"],
        "pd30": ["P3RERES", "P3RELOC"],
        "pd90": ["P9RERES", "P9RELOC"],
        "na": ["NARERES", "NARELOC"]
    },
    # 4. TRADITIONAL C&I
    "Corp_CI": {
        "balance": ["Corp_CI_Balance"],
        "nco": ["NTCI"], "pd30": ["P3CI"], "pd90": ["P9CI"], "na": ["NACI"]
    },
    # 5. CRE: OWNER-OCCUPIED
    "CRE_OO": {
        "balance": ["CRE_OO_Balance"],
        "nco": ["NTRENROW"], "pd30": ["P3RENROW"], "pd90": ["P9RENROW"], "na": ["NARENROW"]
    },
    # 6. CRE: INVESTMENT
    "CRE_Investment": {
        "balance": ["CRE_Investment_Balance"],
        "nco": ["NTREMULT", "NTRENROT"],
        "pd30": ["P3REMULT", "P3RENROT"],
        "pd90": ["P9REMULT", "P9RENROT"],
        "na": ["NAREMULT", "NARENROT"]
    },
    # 7. CONSUMER: AUTO
    "Consumer_Auto": {
        "balance": ["Consumer_Auto_Balance"],
        "nco": ["NTAUTO"], "pd30": ["P3AUTO"], "pd90": ["P9AUTO"], "na": ["NAAUTO"]
    },
    # 8. CONSUMER: OTHER
    "Consumer_Other": {
        "balance": ["Consumer_Other_Balance"], # Derived Residual in Processor
        "nco": ["NTCON", "NTCRCD"],
        "pd30": ["P3CON", "P3CRCD"],
        "pd90": ["P9CON", "P9CRCD"],
        "na": ["NACON", "NACRCD"]
    }
}
FRED_SERIES_TO_FETCH = {
    'Key Economic Indicators': {
        'GDPC1': {'short': 'Real GDP', 'long': 'Real Gross Domestic Product'},
        'A191RL1Q225SBEA': {'short': 'Real GDP Growth', 'long': 'Real Gross Domestic Product: Percent Change from Preceding Period'},
        'UNRATE': {'short': 'Unemployment Rate', 'long': 'Unemployment Rate'},
        'CPIAUCSL': {'short': 'CPI Inflation', 'long': 'Consumer Price Index for All Urban Consumers: All Items'},
        'UMCSENT': {'short': 'Consumer Sentiment', 'long': 'University of Michigan: Consumer Sentiment'},
        'ICSA': {'short': 'Initial Jobless Claims', 'long': 'Initial Claims'},
        'USSLIND': {'short': 'Leading Index', 'long': 'Leading Index for the United States'},
        'RSXFS': {'short': 'Retail Sales', 'long': 'Advance Retail Sales: Retail and Food Services'}
    },
    'Interest Rates & Yield Curve': {
        'FEDFUNDS': {'short': 'Fed Funds Rate', 'long': 'Effective Federal Funds Rate'},
        'DFF': {'short': 'Fed Funds (Daily)', 'long': 'Effective Federal Funds Rate (Daily)'},
        'DPRIME': {'short': 'Prime Rate', 'long': 'Bank Prime Loan Rate'},
        'MORTGAGE30US': {'short': '30Y Mortgage Rate', 'long': '30-Year Fixed Rate Mortgage Average in the United States'},
        'DGS30': {'short': 'UST 30Y', 'long': 'Market Yield on U.S. Treasury Securities at 30-Year Constant Maturity'},
        'DGS20': {'short': 'UST 20Y', 'long': 'Market Yield on U.S. Treasury Securities at 20-Year Constant Maturity'},
        'DGS10': {'short': 'UST 10Y', 'long': 'Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity'},
        'DGS7': {'short': 'UST 7Y', 'long': 'Market Yield on U.S. Treasury Securities at 7-Year Constant Maturity'},
        'DGS5': {'short': 'UST 5Y', 'long': 'Market Yield on U.S. Treasury Securities at 5-Year Constant Maturity'},
        'DGS3': {'short': 'UST 3Y', 'long': 'Market Yield on U.S. Treasury Securities at 3-Year Constant Maturity'},
        'DGS2': {'short': 'UST 2Y', 'long': 'Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity'},
        'DGS1': {'short': 'UST 1Y', 'long': 'Market Yield on U.S. Treasury Securities at 1-Year Constant Maturity'},
        'DGS6MO': {'short': 'UST 6M', 'long': 'Market Yield on U.S. Treasury Securities at 6-Month Constant Maturity'},
        'DGS3MO': {'short': 'UST 3M', 'long': 'Market Yield on U.S. Treasury Securities at 3-Month Constant Maturity'},
        'DGS1MO': {'short': 'UST 1M', 'long': 'Market Yield on U.S. Treasury Securities at 1-Month Constant Maturity'},
        'T10Y2Y': {'short': '10Y-2Y', 'long': '10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity'},
        'T10Y3M': {'short': '10Y-3M', 'long': '10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity'}
    },
    'Credit Spreads & Lending Standards': {
        'DBAA': {'short': "Moody's Baa Yield", 'long': "Moody's Seasoned Baa Corporate Bond Yield"},
        'DAAA': {'short': "Moody's Aaa Yield", 'long': "Moody's Seasoned Aaa Corporate Bond Yield"},
        'BAMLH0A0HYM2': {'short': 'HY OAS', 'long': 'ICE BofA US High Yield Index Option-Adjusted Spread'},
        'BAMLC0A0CM': {'short': 'IG OAS', 'long': 'ICE BofA US Corporate Index Option-Adjusted Spread'},
        'DRTSCILM': {'short': 'C&I Standards (Large/Med)', 'long': 'Net Percentage of Domestic Banks Tightening Standards for C&I Loans to Large and Middle-Market Firms'},
        'DRTSCIS': {'short': 'C&I Standards (Small)', 'long': 'Net Percentage of Domestic Banks Tightening Standards for C&I Loans to Small Firms'}
    },
    'Financial Stress & Risk': {
        'STLFSI4': {'short': 'St. Louis FSI', 'long': 'St. Louis Fed Financial Stress Index'},
        'VIXCLS': {'short': 'VIX', 'long': 'CBOE Volatility Index: VIX'},
        'NFCI': {'short': 'NFCI', 'long': 'Chicago Fed National Financial Conditions Index'}
    },
    'Investor Leverage & Market Credit': {
        'BOGZ1FL663067003Q': {
            'short': 'Broker-Dealer Credit Balances',
            'long': 'Security Brokers and Dealers; Credit Balances; Asset (Z.1 Financial Accounts). Proxy for securities-based leverage used mainly by wealthy/institutional investors.'
        },
        'BOGZ1FL663067005Q': {
            'short': 'Broker-Dealer Total Financial Assets',
            'long': 'Security Brokers and Dealers; Total Financial Assets (Z.1 Financial Accounts). Denominator for normalizing broker-dealer credit balances.'
        }
    },
    'Global Benchmarks': {
        'DEXUSEU': {'short': 'USD/EUR', 'long': 'U.S. Dollars to Euro Spot Exchange Rate'},
        'DEXJPUS': {'short': 'JPY/USD', 'long': 'Japanese Yen to U.S. Dollar Spot Exchange Rate'},
        'DEXUSUK': {'short': 'USD/GBP', 'long': 'U.S. Dollars to British Pound Sterling Spot Exchange Rate'},
        'DCOILWTICO': {'short': 'WTI Oil', 'long': 'Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma'},
        'GOLDAMGBD228NLBM': {'short': 'Gold', 'long': 'Gold Fixing Price 10:30 A.M. (London time) in London Bullion Market, based in U.S. Dollars'},
        'VIXCLS': {'short': 'VIX', 'long': 'CBOE Volatility Index: VIX'}
    },
    'Real Estate & Housing': {
        'CSUSHPINSA': {'short': 'Case-Shiller National', 'long': 'S&P CoreLogic Case-Shiller U.S. National Home Price Index'},
        'HOUST': {'short': 'Housing Starts', 'long': 'Housing Starts: Total: New Privately Owned Housing Units Started'},
        'PERMIT': {'short': 'Building Permits', 'long': 'New Private Housing Units Authorized by Building Permits'},
        'MSPUS': {'short': 'Median Sales Price', 'long': 'Median Sales Price of Houses Sold for the United States'},
        'RRVRUSQ156N': {'short': 'Vacancy Rate (Rental)', 'long': 'Rental Vacancy Rate for the United States'},
        'RCVRUSQ156N': {'short': 'Vacancy Rate (Homeowner)', 'long': 'Homeowner Vacancy Rate for the United States'}
    },
    'Banking Sector Aggregates': {
        'TOTLL': {'short': 'Total Loans & Leases', 'long': 'Total Loans and Leases, All Commercial Banks'},
        'BUSLOANS': {'short': 'Business Loans', 'long': 'Commercial and Industrial Loans, All Commercial Banks'},
        'REALLN': {'short': 'Real Estate Loans', 'long': 'Real Estate Loans, All Commercial Banks'},
        'CCLACBW027SBOG': {'short': 'Credit Card Loans', 'long': 'Consumer Loans: Credit Cards and Other Revolving Plans, All Commercial Banks'},
        'CONSUMER': {'short': 'Consumer Loans', 'long': 'Consumer Loans, All Commercial Banks'},
        'DEPALL': {'short': 'Total Deposits', 'long': 'Total Deposits, All Commercial Banks'},
        'DPSACBW027SBOG': {'short': 'Savings Deposits', 'long': 'Deposits, Savings Accounts, All Commercial Banks'},
        'DODFFSWCMI': {'short': 'Deposits: Other', 'long': 'Other Deposits, All Commercial Banks'},
        'CORBLACBS': {'short': 'Bus Loan CO Rate', 'long': 'Charge-off Rate on Business Loans, Annualized, All Commercial Banks'},
        'CORALACBS': {'short': 'All Loans CO Rate', 'long': 'Charge-off Rate on All Loans, Annualized, All Commercial Banks'},
        'CORCCLACBS': {'short': 'CC CO Rate', 'long': 'Charge-off Rate on Credit Card Loans, Annualized, All Commercial Banks'},
        'DRALACBS': {'short': 'All Loans Delinq', 'long': 'Delinquency Rate on All Loans, All Commercial Banks'},
        'DRCCLACBS': {'short': 'CC Delinq Rate', 'long': 'Delinquency Rate on Credit Card Loans, All Commercial Banks'},
        'DRCRELEXFACBS': {'short': 'CRE Delinq (ex-farm)', 'long': 'Delinquency Rate on Commercial Real Estate Loans (Excluding Farmland), All Commercial Banks'},
        'DRSFRMACBS': {'short': '1-4 Resi Delinq', 'long': 'Delinquency Rate on Single-Family Residential Mortgages, All Commercial Banks'},
        'DRSFRMT100S': {'short': '1-4 Resi Delinq (Top 100)', 'long': 'Delinquency Rate on Single-Family Residential Mortgages, Banks Ranked 1st to 100th Largest in Size by Assets (SA)'},
        'DRSFRMT100N': {'short': '1-4 Resi Delinq (Top 100, NSA)', 'long': 'Delinquency Rate on Single-Family Residential Mortgages, Banks Ranked 1st to 100th Largest in Size by Assets (NSA)'},
        'DRCCLT100S': {'short': 'Credit Card Delinq (Top 100)', 'long': 'Delinquency Rate on Credit Card Loans, Banks Ranked 1st to 100th Largest in Size by Assets (SA)'},
        'CORALACBN': {'short': 'All Loans CO Rate (NSA)', 'long': 'Charge-off Rate on All Loans, Annualized, All Commercial Banks (NSA)'}
    },
    'Leading Indicators': {
        'USSLIND': {'short': 'Leading Index', 'long': 'Leading Index for the United States'},
        'USALOLITONOSTSAM': {'short': 'US Leading Indicator', 'long': 'US Leading Index: Leading Index'},
        'PAYEMS': {'short': 'Nonfarm Payrolls', 'long': 'All Employees: Total Nonfarm Payrolls'},
        'PERMIT': {'short': 'Building Permits', 'long': 'New Private Housing Units Authorized by Building Permits'}
    },
    'Middle Market, Healthcare, & Funding Indicators': {
        'DRTSCIS': {'short': 'Small Firm C&I Standards', 'long': 'Net Pct Banks Tightening Standards - Small Firms'},
        'TCU': {'short': 'Capacity Utilization: Total Industry', 'long': 'Capacity Utilization: Total Index'},
        'INDPRO': {'short': 'Industrial Production Index', 'long': 'Industrial Production: Total Index'},
        'NEWORDER': {'short': "Manufacturers' New Orders", 'long': "Manufacturers' New Orders: Nondefense Capital Goods Excluding Aircraft"},
        'SOFR': {'short': 'SOFR', 'long': 'Secured Overnight Financing Rate'},
        'TB3MS': {'short': '3-Month T-Bill', 'long': '3-Month Treasury Bill Secondary Market Rate'},
        'SOFR3MTB3M': {'short': 'SOFR vs T-Bill Spread', 'long': 'Calculated Spread: SOFR - 3-Month T-Bill'},
        'MPCT04XXS': {'short': 'Medicare Spending', 'long': 'Medicare: Total Expenditures'}
    }
}
