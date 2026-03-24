"""Package all corp ETL files into a single deployable text file."""
import os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(REPO)

FILES = [
    "corp_etl/__init__.py",
    "corp_etl/column_matcher.py",
    "corp_etl/file_utils.py",
    "corp_etl/main.py",
    "corp_etl/config.yaml",
    "corp_etl/tracker_matcher.py",
    "corp_etl/build_tracker_analytics.py",
    "corp_etl/readers/__init__.py",
    "corp_etl/readers/base_reader.py",
    "corp_etl/readers/dar_tracker_reader.py",
    "corp_etl/readers/lal_credit_reader.py",
    "corp_etl/readers/loan_reserve_reader.py",
    "corp_etl/taggers/__init__.py",
    "corp_etl/taggers/intermediate_tags.py",
    "corp_etl/taggers/wmlc_tagger.py",
    "corp_etl/tests/__init__.py",
    "corp_etl/tests/test_wmlc_flags.py",
    "templates/vba_modules/mod_CellClick.bas",
    "templates/vba_modules/mod_DataRefresh.bas",
    "templates/vba_modules/mod_Navigation.bas",
    "templates/vba_modules/mod_Diagnostics.bas",
    "templates/vba_modules/mod_ViewToggle.bas",
    "templates/power_query/load_tagged_data.m",
    "scripts/build_dashboard.py",
]

DELIM = "__BEGINFILE__ "

HEADER = """\
================================================================================
WMLC PIPELINE  -  CORP DEPLOYMENT PACKAGE
Generated: 2026-03-24
================================================================================

PREREQUISITES
  Python 3.10+ (3.12 recommended)
  pip install pyyaml pandas openpyxl rapidfuzz

DIRECTORY STRUCTURE
  wmlc_pipeline/
    corp_etl/           ETL package (main.py is entry point)
      readers/          File readers (CSV, XLSX with skip rows)
      taggers/          WMLC flag logic
      tests/            Unit tests
      config.yaml       All file paths (edit for your environment)
      tracker_matcher.py  Fuzzy matching standalone script
    templates/
      vba_modules/      VBA .bas files for Excel dashboard
      power_query/      M code for Power Query
    scripts/
      build_dashboard.py  Builds WMLC_Dashboard.xlsm
    output/             Created automatically

EXTRACTION
  Each file is delimited by a line starting with __BEGINFILE__ and ending with __ENDPATH__
  Save this entire file as corp_etl_payload.txt, then run:

    import os
    SEP = '__BEGINFILE__ '
    with open('corp_etl_payload.txt', 'r', encoding='utf-8') as f:
        content = f.read()
    for part in content.split(SEP)[1:]:
        header, body = part.split(' __ENDPATH__\\n', 1)
        fp = header.strip()
        os.makedirs(os.path.dirname(fp) or '.', exist_ok=True)
        with open(fp, 'w', encoding='utf-8', newline='\\n') as out:
            out.write(body)
        print(f'  Extracted: {fp}')

RUNNING THE PIPELINE
  1. Edit corp_etl/config.yaml - point to your real input files
  2. python corp_etl/main.py              -> output/loan_extract_tagged.csv
  3. python corp_etl/tracker_matcher.py   -> output/wmlc_tracker_matched.xlsx
  4. python scripts/build_dashboard.py    -> output/WMLC_Dashboard.xlsm
     (Stage 2 needs Excel + win32com; without it you get .xlsx + manual VBA import)
  5. Open .xlsm, enable macros, click Refresh Data

PYTHON UPDATE (if upgrading existing deployment)
  pip install --upgrade pyyaml pandas openpyxl rapidfuzz
  Required: pandas>=1.5  openpyxl>=3.1  pyyaml>=6.0  rapidfuzz>=3.0
  Verify:   python -c "import pandas, openpyxl, yaml, rapidfuzz; print('OK')"

CHANGELOG (2026-03-24)
  - ETL: Expanded NTC entity pattern (guarantor/trust/estate variants)
  - Tracker: 3-tier fuzzy matching (Tier1 $50MM+, Tier2 $20-50MM, Tier3 $10-20MM)
  - Tracker: LAL $10MM floor excludes small LAL from matching pool
  - Dashboard: 3 WMLC visualizations replace old 4 charts:
      Viz 1: Threshold Utilization stacked bar (below 80% / approaching / above)
      Viz 2: Threshold Distance table (30 loans nearest WMLC boundary)
      Viz 3: Flag Overlap heatmap (co-occurrence matrix with color scale)
  - VBA: Single-pass MasterRefresh with bulk array writes
  - VBA: Conditional formatting applied once (not per-cell, not per-toggle)
  - VBA: PreFlightCheck + debugSection error diagnostics
  - VBA: LAL $10MM floor in all chart/viz accumulators
  - Performance: Eliminated loan_detail 50K-row copy
  - Performance: View toggles skip formatting (reformat=False, near-instant)

================================================================================
FILES BEGIN BELOW
================================================================================
"""

out = [HEADER]

for fp in FILES:
    if os.path.exists(fp):
        with open(fp, "r", encoding="utf-8") as f:
            body = f.read()
        out.append(f"{DELIM}{fp} __ENDPATH__")
        out.append(body)
    else:
        out.append(f"{DELIM}{fp} __ENDPATH__")
        out.append(f"# File not found: {fp}\n")

payload = "\n".join(out)
payload_path = os.path.join("output", "corp_etl_payload.txt")
os.makedirs("output", exist_ok=True)
with open(payload_path, "w", encoding="utf-8", newline="\n") as f:
    f.write(payload)

# Verify
with open(payload_path, "r") as f:
    content = f.read()
count = content.count(DELIM)
size = os.path.getsize(payload_path)
print(f"Packaged {count} files into {payload_path}")
print(f"Payload size: {size:,} bytes ({size/1024:.1f} KB)")
