"""
Enhanced Data Dictionary Validator - With FFIEC Taxonomy Support
=================================================================

This version can parse FFIEC XBRL Taxonomy files to get official
Call Report field definitions.

Place the FFIEC 031 Taxonomy ZIP in: Documentation/FFIEC_031_Taxonomy.zip
Or extract it to: Documentation/FFIEC_031_Taxonomy/

Version: 3.3-taxonomy-support
"""

import logging
import re
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import openpyxl

# Import local definitions
DERIVED_FIELD_DEFINITIONS = {}
FDIC_FIELDS_TO_FETCH = []

try:
    from FDIC_FFIEC_Series_Key import DERIVED_FIELD_DEFINITIONS
except ImportError:
    pass

try:
    from FDIC_FFIEC_Series_Key import FDIC_FIELDS_TO_FETCH
except ImportError:
    pass


# Hardcoded FDIC API field definitions
FDIC_API_DEFINITIONS = {
    'ASSET': 'Total Assets',
    'LIAB': 'Total Liabilities',
    'DEP': 'Total Deposits',
    'EQ': 'Total Equity Capital',
    'ROA': 'Return on Assets',
    'ROE': 'Return on Equity',
    'NIMY': 'Net Interest Margin',
    'EEFFR': 'Efficiency Ratio',
    'NONIIAY': 'Noninterest Income to Average Assets',
    'ELNATRY': 'Provision for Loan and Lease Losses',
    'EINTEXP': 'Total Interest Expense',
    'OTHBOR': 'Other Borrowed Money',
    'AOBS': 'Off-Balance Sheet Items',
    'CET1R': 'Common Equity Tier 1 Ratio',
    'EQO': 'Other Equity Capital',
    'LEVRATIO': 'Tier 1 Leverage Ratio',
    'LNCONAUTO': 'Consumer Auto Loans (derived)',
    'LNCONCC': 'Credit Card Loans (derived)',
    'LNCONOTHX': 'Other Consumer Loans excluding Auto and Credit Card (derived)',
    'LNOTHNONDEP': 'Fund Finance/Non-Depository Loans',
    'LNOTHPCS': 'Subscription Finance/Securities-Based Lending',
    'LNREOTH': 'Other Real Estate Loans',
    'NARELOC': 'Nonaccrual Home Equity Lines of Credit',
    'NTLSNFOO': 'Net Charge-Offs on Nonfarm Nonresidential',
    'RWA': 'Risk-Weighted Assets',
    'CERT': 'FDIC Certificate Number',
    'NAME': 'Institution Name',
    'REPDTE': 'Report Date',
    'RBCT1CER': 'Common Equity Tier 1 Capital',
    'RBCT1J': 'Tier 1 Capital',
    'RBCT2': 'Tier 2 Capital',
    'RWAJ': 'Risk-Weighted Assets',
    'RBCRWAJ': 'Total Risk-Based Capital Ratio',
    'RB2LNRES': 'Tier 2 Capital to Risk-Weighted Assets',
    'EQCS': 'Common Stock',
    'EQSUR': 'Surplus',
    'EQUP': 'Undivided Profits',
    'EQCCOMPI': 'Accumulated Other Comprehensive Income',
    'EQPP': 'Perpetual Preferred Stock',
    'LNATRES': 'Allowance for Loan and Lease Losses',
    'MUTUAL': 'Mutual Savings Bank Indicators',
    'FREPP': 'Federal Funds Purchased and Repurchase Agreements',
}


class EnhancedDataDictionaryClient:
    """Dictionary client with FFIEC Taxonomy parsing support."""

    def __init__(self, output_dir: str = "output", excel_dict_path: Optional[str] = None,
                 taxonomy_path: Optional[str] = None):
        """
        Initialize dictionary client.

        Args:
            output_dir: Output directory
            excel_dict_path: Path to Excel file
            taxonomy_path: Path to FFIEC Taxonomy ZIP or extracted folder
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True, parents=True)

        self.excel_path = self._find_excel_file(excel_dict_path)
        self.taxonomy_path = self._find_taxonomy(taxonomy_path)

        # DataFrames
        self.taxonomy_df = None     # From FFIEC XBRL taxonomy
        self.fdic_api_df = None     # Hardcoded FDIC definitions
        self.excel_df = None        # Excel reference

        self.logger = logging.getLogger(__name__)

    def _find_excel_file(self, path: Optional[str]) -> Optional[Path]:
        """Find Excel file."""
        if path:
            p = Path(path)
            if p.exists():
                return p.resolve()

        try:
            script_dir = Path(__file__).parent
        except:
            script_dir = Path.cwd()

        cwd = Path.cwd()

        searches = [
            cwd / "All Financial Reports.xlsx",
            cwd / "Documentation" / "All Financial Reports.xlsx",
            script_dir / "All Financial Reports.xlsx",
            script_dir / "Documentation" / "All Financial Reports.xlsx",
        ]

        for loc in searches:
            try:
                if loc.exists():
                    return loc.resolve()
            except:
                continue

        return None

    def _find_taxonomy(self, path: Optional[str]) -> Optional[Path]:
        """Find FFIEC Taxonomy ZIP or folder."""
        if path:
            p = Path(path)
            if p.exists():
                return p.resolve()

        try:
            script_dir = Path(__file__).parent
        except:
            script_dir = Path.cwd()

        cwd = Path.cwd()

        # Search for taxonomy ZIP or folder
        searches = [
            cwd / "FFIEC_031_Taxonomy.zip",
            cwd / "Documentation" / "FFIEC_031_Taxonomy.zip",
            cwd / "FFIEC_031_Taxonomy",
            cwd / "Documentation" / "FFIEC_031_Taxonomy",
            script_dir / "FFIEC_031_Taxonomy.zip",
            script_dir / "Documentation" / "FFIEC_031_Taxonomy.zip",
        ]

        for loc in searches:
            try:
                if loc.exists():
                    return loc.resolve()
            except:
                continue

        return None

    def load_all_dictionaries(self) -> None:
        """Load all available sources."""
        print("\n" + "="*80)
        print("  LOADING DATA DICTIONARIES")
        print("="*80 + "\n")

        # 1. FFIEC Taxonomy - Official source for Call Report codes
        print("[1/4] FFIEC Taxonomy (Official Call Report Dictionary)")
        self._load_taxonomy()

        # 2. FDIC API - Hardcoded definitions
        print("\n[2/4] FDIC API Fields")
        self._load_fdic_api_codes()

        # 3. Excel - Supplemental
        print("\n[3/4] Excel Documentation")
        self._load_excel()

        # 4. Local
        print("\n[4/4] Local Definitions")
        local_count = len(DERIVED_FIELD_DEFINITIONS)
        if local_count > 0:
            print(f"  ✓ {local_count} local definitions")

        # Summary
        tax_ct = len(self.taxonomy_df) if self.taxonomy_df is not None else 0
        fdic_ct = len(self.fdic_api_df) if self.fdic_api_df is not None else 0
        excel_ct = len(self.excel_df) if self.excel_df is not None else 0

        print("\n" + "="*80)
        print(f"  ✓ Loading Complete")
        print(f"    [1] Taxonomy: {tax_ct} entries")
        print(f"    [2] FDIC API: {fdic_ct} entries")
        print(f"    [3] Excel: {excel_ct} entries")
        print(f"    [4] Local: {local_count} entries")
        print(f"    TOTAL: {tax_ct + fdic_ct + excel_ct + local_count}")
        print("="*80 + "\n")

    def _load_taxonomy(self) -> None:
        """
        Load FFIEC XBRL Taxonomy files.

        Parses the taxonomy to extract field codes and definitions.
        """
        try:
            if self.taxonomy_path is None:
                print(f"  ℹ️  FFIEC Taxonomy not found")
                print(f"      Place FFIEC 031 Taxonomy ZIP in Documentation folder")
                print(f"      Or extract to Documentation/FFIEC_031_Taxonomy/")
                print(f"      Using basic definitions instead")
                self._load_basic_call_report_codes()
                return

            print(f"  📖 Loading taxonomy from: {self.taxonomy_path.name}")

            # Check if ZIP or folder
            if self.taxonomy_path.suffix == '.zip':
                # Extract and parse ZIP
                with zipfile.ZipFile(self.taxonomy_path) as z:
                    # Look for label XML files (these have the descriptions)
                    label_files = [f for f in z.namelist() if 'lab' in f.lower() and f.endswith('.xml')]

                    if not label_files:
                        print(f"  ⚠️  No label files found in taxonomy")
                        self._load_basic_call_report_codes()
                        return

                    # Parse first label file
                    with z.open(label_files[0]) as f:
                        self._parse_taxonomy_xml(f)
            else:
                # Parse folder
                label_files = list(self.taxonomy_path.glob('**/*lab*.xml'))

                if not label_files:
                    print(f"  ⚠️  No label files found in taxonomy folder")
                    self._load_basic_call_report_codes()
                    return

                with open(label_files[0], 'rb') as f:
                    self._parse_taxonomy_xml(f)

            if self.taxonomy_df is not None:
                print(f"  ✓ Parsed {len(self.taxonomy_df)} taxonomy entries")
            else:
                print(f"  ⚠️  Taxonomy parsing failed, using basic definitions")
                self._load_basic_call_report_codes()

        except Exception as e:
            print(f"  ❌ Error loading taxonomy: {e}")
            print(f"      Using basic definitions instead")
            self._load_basic_call_report_codes()

    def _parse_taxonomy_xml(self, file_obj):
        """Parse XBRL taxonomy label file."""
        try:
            tree = ET.parse(file_obj)
            root = tree.getroot()

            # XBRL namespace
            ns = {'link': 'http://www.xbrl.org/2003/linkbase',
                  'xlink': 'http://www.w3.org/1999/xlink'}

            rows = []

            # Find all label elements
            for label in root.findall('.//link:label', ns):
                # Get the label reference (contains the item code)
                label_ref = label.get('{http://www.w3.org/1999/xlink}label', '')

                # Get the label text (the description)
                label_text = label.text

                if label_ref and label_text:
                    # Extract item code from reference
                    # Format is usually: ffiec-fr-y-9c_RCFD2170
                    code_match = re.search(r'(RCFD|RCON|RIAD)\w+', label_ref)
                    if code_match:
                        code = code_match.group(0)
                        rows.append({
                            'CODE': code,
                            'NAME': label_text,
                            'DESC': label_text
                        })

            if rows:
                self.taxonomy_df = pd.DataFrame(rows)
                # Remove duplicates, keep first
                self.taxonomy_df = self.taxonomy_df.drop_duplicates(subset='CODE', keep='first')

        except Exception as e:
            self.logger.error(f"Taxonomy XML parsing error: {e}")

    def _load_basic_call_report_codes(self) -> None:
        """Fallback: Create basic Call Report definitions."""
        cr_codes = [f for f in FDIC_FIELDS_TO_FETCH
                   if isinstance(f, str) and f.startswith(('RCFD', 'RCON', 'RIAD'))]

        if not cr_codes:
            return

        rows = []
        for code in cr_codes:
            prefix = code[:4]
            item_code = code[4:]

            scope_map = {
                'RCFD': 'Consolidated',
                'RCON': 'Domestic Offices',
                'RIAD': 'Income Statement'
            }

            scope = scope_map.get(prefix, 'Unknown')

            rows.append({
                'CODE': code,
                'NAME': f'{scope} Item {item_code}',
                'DESC': f'Call Report {scope} - Item {item_code}'
            })

        self.taxonomy_df = pd.DataFrame(rows)
        print(f"  ✓ {len(self.taxonomy_df)} basic Call Report definitions")

    def _load_fdic_api_codes(self) -> None:
        """Load FDIC API definitions from hardcoded dict."""
        fdic_codes = [f for f in FDIC_FIELDS_TO_FETCH
                     if isinstance(f, str) and not f.startswith(('RCFD', 'RCON', 'RIAD'))]

        if not fdic_codes:
            return

        rows = []
        for code in fdic_codes:
            if code in FDIC_API_DEFINITIONS:
                rows.append({
                    'CODE': code,
                    'NAME': FDIC_API_DEFINITIONS[code],
                    'DESC': FDIC_API_DEFINITIONS[code]
                })
            else:
                rows.append({
                    'CODE': code,
                    'NAME': f'FDIC Field {code}',
                    'DESC': f'FDIC BankFind API field {code}'
                })

        self.fdic_api_df = pd.DataFrame(rows)

        defined = sum(1 for c in fdic_codes if c in FDIC_API_DEFINITIONS)
        print(f"  ✓ {len(self.fdic_api_df)} FDIC API codes")
        print(f"      ({defined} with definitions, {len(self.fdic_api_df) - defined} placeholders)")

    def _load_excel(self) -> None:
        """Load Excel - filtered to our codes."""
        try:
            if self.excel_path is None:
                print(f"  ℹ️  Excel file not found")
                return

            print(f"  📖 Loading: {self.excel_path.name}")

            wb = openpyxl.load_workbook(self.excel_path, read_only=True, data_only=True)
            sheet = wb.sheetnames[-1]
            wb.close()

            df = pd.read_excel(self.excel_path, sheet_name=sheet)

            if 'Variable' not in df.columns:
                print(f"  ⚠️  Missing 'Variable' column")
                return

            # Filter to our codes only
            our_codes = set(FDIC_FIELDS_TO_FETCH)
            df = df[df['Variable'].astype(str).str.strip().str.upper().isin(our_codes)]

            # Extract metadata
            def extract(txt):
                if pd.isna(txt):
                    return '', '', ''
                txt = str(txt)
                freq = 'YTD' if 'YTD' in txt[:30] else ('QTD' if 'QTD' in txt[:30] else '')
                unit = 'Dollar' if ' $ ' in txt[:50] else ('Percent' if ' % ' in txt[:50] else '')
                clean = re.sub(r'\(\s*(YTD|QTD|MTD)\s*,\s*(\$|%)\s*\)', '', txt).strip()
                return freq, unit, clean

            if 'Definition' in df.columns:
                df[['Freq', 'Unit', 'Clean']] = df['Definition'].apply(lambda x: pd.Series(extract(x)))
            else:
                df['Freq'], df['Unit'], df['Clean'] = '', '', df.get('Title', '')

            self.excel_df = pd.DataFrame({
                'CODE': df['Variable'].astype(str).str.strip().str.upper(),
                'NAME': df.get('Title', df['Variable']),
                'DESC': df['Clean'],
                'FREQ': df['Freq'],
                'UNIT': df['Unit']
            })

            self.excel_df = self.excel_df[self.excel_df['CODE'].str.len() > 0]

            print(f"  ✓ {len(self.excel_df)} entries")

        except Exception as e:
            print(f"  ❌ Error: {e}")

    def lookup_field(self, code: str) -> Tuple[Optional[str], Optional[str], str, Optional[str], Optional[str]]:
        """
        Lookup with priority: Taxonomy > FDIC API > Excel > Local.

        Returns: (name, desc, source, freq, unit)
        """
        code = str(code).strip().upper()

        # 1. FFIEC Taxonomy (official)
        if self.taxonomy_df is not None:
            match = self.taxonomy_df[self.taxonomy_df['CODE'] == code]
            if not match.empty:
                row = match.iloc[0]
                return (
                    row.get('NAME', code),
                    row.get('DESC', ''),
                    'FFIEC Taxonomy (Official)',
                    None, None
                )

        # 2. FDIC API
        if self.fdic_api_df is not None:
            match = self.fdic_api_df[self.fdic_api_df['CODE'] == code]
            if not match.empty:
                row = match.iloc[0]
                return (
                    row.get('NAME', code),
                    row.get('DESC', ''),
                    'FDIC API Fields',
                    None, None
                )

        # 3. Excel
        if self.excel_df is not None:
            match = self.excel_df[self.excel_df['CODE'] == code]
            if not match.empty:
                row = match.iloc[0]
                return (
                    row.get('NAME', code),
                    row.get('DESC', ''),
                    'Excel Documentation Dictionary',
                    row.get('FREQ', ''),
                    row.get('UNIT', '')
                )

        # 4. Local
        if code in DERIVED_FIELD_DEFINITIONS:
            d = DERIVED_FIELD_DEFINITIONS[code]
            return (
                d.get('title', code),
                d.get('description', ''),
                'Local DERIVED_FIELD_DEFINITIONS',
                None, None
            )

        return (None, None, 'NOT FOUND', None, None)

    def generate_comprehensive_dictionary(self) -> pd.DataFrame:
        """Generate dictionary for codes in FDIC_FIELDS_TO_FETCH."""
        print("\n" + "="*80)
        print("  GENERATING COMPREHENSIVE DICTIONARY")
        print("="*80 + "\n")

        codes = set()
        if isinstance(FDIC_FIELDS_TO_FETCH, list):
            codes.update(FDIC_FIELDS_TO_FETCH)

        codes.update(DERIVED_FIELD_DEFINITIONS.keys())

        print(f"Processing {len(codes)} codes...")

        rows = []
        for c in sorted(codes):
            name, desc, src, freq, unit = self.lookup_field(c)

            rows.append({
                'Metric_Code': c,
                'Metric_Name': name or c,
                'Description': desc or 'No description',
                'Source_of_Truth': src,
                'Frequency': freq or '',
                'Unit': unit or '',
                'Data_Type': '',
                'Schedule': '',
                'In_Taxonomy': '✓' if src == 'FFIEC Taxonomy (Official)' else '',
                'In_FDIC_API': '✓' if src == 'FDIC API Fields' else '',
                'In_Excel_Dict': '✓' if src == 'Excel Documentation Dictionary' else '',
                'In_Local_Defs': '✓' if c in DERIVED_FIELD_DEFINITIONS else '',
            })

        df = pd.DataFrame(rows)

        priority = {
            'FFIEC Taxonomy (Official)': 1,
            'FDIC API Fields': 2,
            'Excel Documentation Dictionary': 3,
            'Local DERIVED_FIELD_DEFINITIONS': 4,
            'NOT FOUND': 5
        }
        df['_pri'] = df['Source_of_Truth'].map(priority)
        df = df.sort_values(['_pri', 'Metric_Code']).drop('_pri', axis=1)

        print(f"✓ Generated {len(df)} entries\n")
        print("Source Distribution:")
        print(df['Source_of_Truth'].value_counts().to_string())

        not_found = df[df['Source_of_Truth'] == 'NOT FOUND']
        if len(not_found) > 0:
            print(f"\n⚠️  {len(not_found)} codes NOT FOUND:")
            for code in not_found['Metric_Code'].head(10):
                print(f"    - {code}")

        print("\n" + "="*80 + "\n")

        return df


def integrate_with_existing_code(output_dir: str = "output",
                                 excel_dict_path: Optional[str] = None,
                                 taxonomy_path: Optional[str] = None) -> pd.DataFrame:
    """Drop-in replacement with taxonomy support."""
    client = EnhancedDataDictionaryClient(
        output_dir=output_dir,
        excel_dict_path=excel_dict_path,
        taxonomy_path=taxonomy_path
    )
    client.load_all_dictionaries()
    return client.generate_comprehensive_dictionary()


if __name__ == "__main__":
    client = EnhancedDataDictionaryClient()
    client.load_all_dictionaries()
    df = client.generate_comprehensive_dictionary()

    out = Path("output") / "Dictionary.xlsx"
    out.parent.mkdir(exist_ok=True)
    df.to_excel(out, index=False)
    print(f"✓ Saved: {out}")