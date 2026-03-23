"""
build_dashboard.py — Sub-Agent 3: Excel Dashboard Builder

Reads output/loan_extract_tagged.csv and produces:
  - output/WMLC_Dashboard.xlsm  (data + formatting + named ranges + VBA + buttons)

Two-stage build:
  Stage 1: openpyxl builds the .xlsx with all data, formatting, sheets, named ranges
  Stage 2: COM automation opens the .xlsx, injects VBA modules, creates buttons, saves as .xlsm
"""

import os
import sys
import time
import subprocess
import pandas as pd
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import (
    Font, PatternFill, Alignment, Border, Side, numbers
)
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

# -- paths ------------------------------------------------------------------
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TAGGED_CSV = os.path.join(REPO, "output", "loan_extract_tagged.csv")
OUTPUT_XLSX = os.path.join(REPO, "output", "WMLC_Dashboard.xlsx")
OUTPUT_XLSM = os.path.join(REPO, "output", "WMLC_Dashboard.xlsm")
VBA_DIR = os.path.join(REPO, "templates", "vba_modules")
PQ_FILE = os.path.join(REPO, "templates", "power_query", "load_tagged_data.m")

# -- MS Color Palette -------------------------------------------------------
MS_COLORS = {
    'navy':        '002B5C',
    'blue':        '00539B',
    'light_blue':  'D6E4F0',
    'ice_blue':    'F0F4F8',
    'button_gray': 'F4F5F7',
    'border_gray': 'D9DEE3',
    'text_gray':   '6C757D',
    'white':       'FFFFFF',
}

# -- constants ---------------------------------------------------------------
PRODUCT_BUCKETS = [
    "LAL Diversified", "LAL Highly Conc.", "LAL NFPs",
    "TL SBL Diversified", "TL SBL Highly Conc.", "TL Life Insurance",
    "TL CRE", "TL Unsecured", "TL Aircraft", "TL PHA",
    "TL Other Secured", "TL Multicollateral", "RESI",
]

# Bucket ladder: list of (label, floor, ceiling) -- descending order
BUCKET_LADDER = [
    ("$1,000,000,000",  1_000_000_000, float("inf")),
    ("$750,000,000",      750_000_000,  999_999_999.99),
    ("$700,000,000",      700_000_000,  749_999_999.99),
    ("$600,000,000",      600_000_000,  699_999_999.99),
    ("$500,000,000",      500_000_000,  599_999_999.99),
    ("$400,000,000",      400_000_000,  499_999_999.99),
    ("$350,000,000",      350_000_000,  399_999_999.99),
    ("$300,000,000",      300_000_000,  349_999_999.99),
    ("$250,000,000",      250_000_000,  299_999_999.99),
    ("$200,000,000",      200_000_000,  249_999_999.99),
    ("$175,000,000",      175_000_000,  199_999_999.99),
    ("$150,000,000",      150_000_000,  174_999_999.99),
    ("$125,000,000",      125_000_000,  149_999_999.99),
    ("$100,000,000",      100_000_000,  124_999_999.99),
    ("$75,000,000",        75_000_000,   99_999_999.99),
    ("$50,000,000",        50_000_000,   74_999_999.99),
    ("$40,000,000",        40_000_000,   49_999_999.99),
    ("$35,000,000",        35_000_000,   39_999_999.99),
    ("$30,000,000",        30_000_000,   34_999_999.99),
    ("$25,000,000",        25_000_000,   29_999_999.99),
    ("$20,000,000",        20_000_000,   24_999_999.99),
    ("$15,000,000",        15_000_000,   19_999_999.99),
    ("$10,000,001",        10_000_001,   14_999_999.99),
    ("$1",                          1,   10_000_000.99),
]

# Defined-range labels
def _range_label(floor, ceil):
    if ceil == float("inf"):
        return f"${floor:,.0f}+"
    return f"${floor:,.0f}-${ceil:,.2f}"


# -- style helpers -----------------------------------------------------------
THIN_GRAY = Side(style="thin", color=MS_COLORS['border_gray'])
THIN_GRAY_BORDER = Border(left=THIN_GRAY, right=THIN_GRAY, top=THIN_GRAY, bottom=THIN_GRAY)
THIN_WHITE = Side(style="thin", color=MS_COLORS['white'])
THIN_WHITE_BORDER = Border(left=THIN_WHITE, right=THIN_WHITE, top=THIN_WHITE, bottom=THIN_WHITE)
DOUBLE_NAVY_TOP = Border(
    left=THIN_GRAY, right=THIN_GRAY,
    top=Side(style="double", color=MS_COLORS['navy']),
    bottom=THIN_GRAY
)

# Fills
NAVY_FILL = PatternFill(start_color=MS_COLORS['navy'], end_color=MS_COLORS['navy'], fill_type="solid")
BLUE_FILL = PatternFill(start_color=MS_COLORS['blue'], end_color=MS_COLORS['blue'], fill_type="solid")
LIGHT_BLUE_FILL = PatternFill(start_color=MS_COLORS['light_blue'], end_color=MS_COLORS['light_blue'], fill_type="solid")
ICE_BLUE_FILL = PatternFill(start_color=MS_COLORS['ice_blue'], end_color=MS_COLORS['ice_blue'], fill_type="solid")
BUTTON_GRAY_FILL = PatternFill(start_color=MS_COLORS['button_gray'], end_color=MS_COLORS['button_gray'], fill_type="solid")
WHITE_FILL = PatternFill(start_color=MS_COLORS['white'], end_color=MS_COLORS['white'], fill_type="solid")

# Fonts
TITLE_FONT = Font(bold=True, size=18, color=MS_COLORS['white'], name="Calibri")
SUBTITLE_FONT = Font(size=11, color=MS_COLORS['white'], name="Calibri")
INDICATOR_FONT = Font(size=10, color=MS_COLORS['text_gray'], name="Calibri")
HEADER_FONT = Font(bold=True, size=10, color=MS_COLORS['white'], name="Calibri")
DATA_FONT = Font(size=10, name="Calibri")
DATA_FONT_BOLD = Font(bold=True, size=10, name="Calibri")
ITALIC_9_GRAY = Font(italic=True, size=9, color=MS_COLORS['text_gray'], name="Calibri")
TOTAL_FONT = Font(bold=True, size=10, color=MS_COLORS['navy'], name="Calibri")
DETAIL_TITLE_FONT = Font(bold=True, size=14, color=MS_COLORS['white'], name="Calibri")
DETAIL_HEADER_FONT = Font(bold=True, size=10, color=MS_COLORS['white'], name="Calibri")
PQ_TITLE_FONT = Font(bold=True, size=14, color=MS_COLORS['white'], name="Calibri")
PQ_STEP_FONT = Font(size=11, name="Calibri")
PQ_CODE_FONT = Font(size=10, name="Consolas", color="333333")

# Legacy aliases for Summary sheet (keep compatible)
HEADER_FILL = BLUE_FILL
ALT_FILL = ICE_BLUE_FILL
TOTAL_FILL = LIGHT_BLUE_FILL
BOLD_14 = Font(bold=True, size=14, name="Calibri")
BOLD_12 = Font(bold=True, size=12, name="Calibri")
BOLD_11 = Font(bold=True, size=11, name="Calibri")
BOLD_ITALIC_11 = Font(bold=True, italic=True, size=11, name="Calibri")
ITALIC_9 = Font(italic=True, size=9, name="Calibri")
THIN = Side(style="thin", color="000000")
THIN_BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
DOUBLE_TOP = Border(left=THIN, right=THIN, top=Side(style="double", color="000000"), bottom=THIN)


# -- pivot computation -------------------------------------------------------
def compute_pivot(df, agg, subset_mask=None):
    """Return a 24x13 matrix (list of lists).
    agg: 'count' or 'sum' (of credit_lii).
    subset_mask: boolean Series to pre-filter df.
    """
    sub = df if subset_mask is None else df[subset_mask]
    matrix = []
    for label, floor, ceil in BUCKET_LADDER:
        row_mask = (sub["CREDIT_LII"] >= floor) & (sub["CREDIT_LII"] <= ceil)
        row_data = []
        for pb in PRODUCT_BUCKETS:
            cell_mask = row_mask & (sub["PRODUCT_BUCKET"] == pb)
            if agg == "count":
                row_data.append(int(cell_mask.sum()))
            else:
                row_data.append(float(sub.loc[cell_mask, "CREDIT_LII"].sum()))
        matrix.append(row_data)
    return matrix


# -- Summary sheet builder ---------------------------------------------------
def build_summary_sheet(wb, pivots):
    """Build a static Summary sheet with all 4 view matrices stacked."""
    ws = wb.create_sheet("Summary")

    headers = ["Gross Amount", "Defined Range"] + PRODUCT_BUCKETS + ["Total"]

    # View configs: (title, pivot_key, number_format)
    # Number formats: positive;negative;zero — dash for zero cells
    view_configs = [
        ("Summary Count",          1, '#,##0;-#,##0;"-"'),
        ("Summary Commitment",     2, '$#,##0;-$#,##0;"-"'),
        ("Summary Count - NEW",    3, '#,##0;-#,##0;"-"'),
        ("Summary Commitment - NEW", 4, '$#,##0;-$#,##0;"-"'),
    ]

    # Row 1: title
    ws.merge_cells("A1:P1")
    c = ws["A1"]
    c.value = "Portfolio Summary - All Views"
    c.font = BOLD_14
    c.alignment = Alignment(horizontal="center")

    current_row = 3  # start after blank row 2

    for title, pivot_key, num_fmt in view_configs:
        matrix = pivots[pivot_key]

        # Subheader row - navy fill, white text
        cell_sub = ws.cell(row=current_row, column=1, value=title)
        cell_sub.font = Font(bold=True, size=12, color=MS_COLORS['white'], name="Calibri")
        for ci in range(1, 17):
            ws.cell(row=current_row, column=ci).fill = NAVY_FILL
        current_row += 1

        # Column headers - blue fill, white text
        for ci, hdr in enumerate(headers, 1):
            cell = ws.cell(row=current_row, column=ci, value=hdr)
            cell.font = Font(bold=True, size=10, color=MS_COLORS['white'], name="Calibri")
            if ci == 2:
                cell.font = Font(bold=True, italic=True, size=10, color=MS_COLORS['white'], name="Calibri")
            cell.fill = BLUE_FILL
            cell.border = THIN_GRAY_BORDER
            cell.alignment = Alignment(horizontal="center", wrap_text=True)
        current_row += 1

        # 24 data rows - alternating white/ice_blue
        for ri, (label, floor, ceil) in enumerate(BUCKET_LADDER):
            is_alt = (ri % 2 == 1)

            # Col A
            cell_a = ws.cell(row=current_row, column=1, value=label)
            cell_a.font = DATA_FONT_BOLD
            cell_a.border = THIN_GRAY_BORDER
            cell_a.alignment = Alignment(horizontal="right")
            cell_a.fill = ICE_BLUE_FILL if is_alt else WHITE_FILL

            # Col B
            cell_b = ws.cell(row=current_row, column=2, value=_range_label(floor, ceil))
            cell_b.font = ITALIC_9_GRAY
            cell_b.border = THIN_GRAY_BORDER
            cell_b.alignment = Alignment(horizontal="left")
            cell_b.fill = ICE_BLUE_FILL if is_alt else WHITE_FILL

            # Cols C-O
            row_total = 0
            for ci, val in enumerate(matrix[ri]):
                cell = ws.cell(row=current_row, column=ci + 3, value=val)
                cell.number_format = num_fmt
                cell.border = THIN_GRAY_BORDER
                cell.alignment = Alignment(horizontal="right")
                cell.font = DATA_FONT
                cell.fill = ICE_BLUE_FILL if is_alt else WHITE_FILL
                row_total += val

            # Col P -- total
            cell_p = ws.cell(row=current_row, column=16, value=row_total)
            cell_p.number_format = num_fmt
            cell_p.border = THIN_GRAY_BORDER
            cell_p.font = TOTAL_FONT
            cell_p.alignment = Alignment(horizontal="right")
            cell_p.fill = ICE_BLUE_FILL if is_alt else WHITE_FILL

            current_row += 1

        # Total row
        ws.cell(row=current_row, column=1, value="Total").font = TOTAL_FONT
        ws.cell(row=current_row, column=1).fill = LIGHT_BLUE_FILL
        ws.cell(row=current_row, column=1).border = DOUBLE_NAVY_TOP
        ws.cell(row=current_row, column=2, value="").fill = LIGHT_BLUE_FILL
        ws.cell(row=current_row, column=2).border = DOUBLE_NAVY_TOP

        data_start_row = current_row - 24  # first data row of this matrix
        for ci in range(3, 17):
            col_total = sum(
                ws.cell(row=r, column=ci).value or 0
                for r in range(data_start_row, current_row)
            )
            cell = ws.cell(row=current_row, column=ci, value=col_total)
            cell.font = TOTAL_FONT
            cell.number_format = num_fmt
            cell.fill = LIGHT_BLUE_FILL
            cell.border = DOUBLE_NAVY_TOP
            cell.alignment = Alignment(horizontal="right")

        current_row += 2  # blank row between matrices

    # Column widths
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 32
    for ci in range(3, 17):
        ws.column_dimensions[get_column_letter(ci)].width = 16

    return ws


# -- POWER_QUERY_SETUP sheet builder -----------------------------------------
def build_pq_setup_sheet(wb):
    """Build a visible POWER_QUERY_SETUP sheet with setup instructions."""
    ws = wb.create_sheet("POWER_QUERY_SETUP")

    # Row 1: title
    ws.merge_cells("A1:D1")
    c = ws["A1"]
    c.value = "Power Query Setup - One-Time Configuration"
    c.font = PQ_TITLE_FONT
    c.fill = NAVY_FILL
    c.alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 30

    # Instructions
    steps = [
        (3, "Step 1: Go to Data > Get Data > Launch Power Query Editor"),
        (4, "Step 2: Click 'New Source' > 'Blank Query', then open the Advanced Editor"),
        (5, "Step 3: Paste the M code shown below into the Advanced Editor and click 'Done'"),
        (6, "Step 4: Name the query 'tbl_LoanData' and click 'Close & Load To...'"),
        (7, "        Choose 'Table' and load to the '_data' sheet cell A1"),
    ]
    for row_num, text in steps:
        cell = ws.cell(row=row_num, column=1, value=text)
        cell.font = PQ_STEP_FONT
        cell.alignment = Alignment(wrap_text=True)

    # M code block
    ws.cell(row=9, column=1, value="M Code (copy everything below):").font = Font(
        bold=True, size=11, name="Calibri"
    )

    # Read the M code
    m_code = ""
    if os.path.exists(PQ_FILE):
        with open(PQ_FILE, "r", encoding="utf-8") as f:
            m_code = f.read()

    m_lines = m_code.strip().split("\n")
    for i, line in enumerate(m_lines):
        row_num = 10 + i
        cell = ws.cell(row=row_num, column=1, value=line)
        cell.font = PQ_CODE_FONT
        cell.fill = BUTTON_GRAY_FILL
        cell.alignment = Alignment(wrap_text=False)

    post_code_row = 10 + len(m_lines) + 2

    post_steps = [
        (post_code_row, "Step 5: After loading, the _data sheet will be populated with loan data"),
        (post_code_row + 1, "Step 6: Click 'Refresh Data' button on Dashboard to recompute all views"),
        (post_code_row + 2, ""),
        (post_code_row + 3, "AFTER SETUP:"),
        (post_code_row + 4, "- To refresh data: click 'Refresh Data' on Dashboard (uses Power Query)"),
        (post_code_row + 5, "- Alternatively, use the corp_etl Python pipeline to produce tagged CSV"),
        (post_code_row + 6, "- Update the CSV path in _config sheet cell A3 if the file location changes"),
        (post_code_row + 7, ""),
        (post_code_row + 8, "NOTE: The DataSourcePath named range in _config!A3 must point to a valid CSV file."),
    ]
    for row_num, text in post_steps:
        cell = ws.cell(row=row_num, column=1, value=text)
        cell.font = PQ_STEP_FONT
        if "AFTER SETUP" in text:
            cell.font = Font(bold=True, size=11, name="Calibri")
        cell.alignment = Alignment(wrap_text=True)

    # Column width
    ws.column_dimensions["A"].width = 120

    return ws


# -- COM post-processing: inject VBA + create buttons -----------------------
def inject_vba_and_buttons(xlsx_path, xlsm_path, vba_dir):
    """Open .xlsx via COM, inject VBA, create buttons, save as .xlsm"""
    import win32com.client
    import winreg

    # Ensure VBA project access is enabled in registry
    try:
        key_path = r"Software\Microsoft\Office\16.0\Excel\Security"
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, key_path, 0,
                            winreg.KEY_READ | winreg.KEY_SET_VALUE)
        winreg.SetValueEx(key, "AccessVBOM", 0, winreg.REG_DWORD, 1)
        winreg.CloseKey(key)
        print("  Enabled VBA project access via registry.")
    except Exception as e:
        print(f"  Warning: Could not set registry key for VBA access: {e}")

    excel = None
    wb = None
    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.AutomationSecurity = 1  # msoAutomationSecurityLow

        abs_xlsx = os.path.abspath(xlsx_path)
        abs_xlsm = os.path.abspath(xlsm_path)

        print(f"  Opening {abs_xlsx} via COM ...")
        wb = excel.Workbooks.Open(abs_xlsx)

        vb_proj = wb.VBProject

        # Test access
        try:
            _ = vb_proj.VBComponents.Count
            print(f"  VBProject access OK ({vb_proj.VBComponents.Count} existing components)")
        except Exception as e:
            print(f"  ERROR: Cannot access VBProject: {e}")
            print("  Please enable 'Trust access to the VBA project object model' in")
            print("  File > Options > Trust Center > Trust Center Settings > Macro Settings")
            raise

        # --- Import standard modules ---
        def read_bas_code(filename):
            """Read .bas file and strip Attribute VB_Name line."""
            path = os.path.join(vba_dir, filename)
            with open(path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            # Strip Attribute VB_Name line
            lines = [l for l in lines if not l.strip().startswith("Attribute VB_Name")]
            return "".join(lines)

        # mod_ViewToggle
        print("  Injecting mod_ViewToggle ...")
        comp = vb_proj.VBComponents.Add(1)  # 1 = vbext_ct_StdModule
        comp.Name = "mod_ViewToggle"
        code = read_bas_code("mod_ViewToggle.bas")
        comp.CodeModule.AddFromString(code)

        # mod_Navigation
        print("  Injecting mod_Navigation ...")
        comp = vb_proj.VBComponents.Add(1)
        comp.Name = "mod_Navigation"
        code = read_bas_code("mod_Navigation.bas")
        comp.CodeModule.AddFromString(code)

        # mod_Diagnostics
        print("  Injecting mod_Diagnostics ...")
        comp = vb_proj.VBComponents.Add(1)
        comp.Name = "mod_Diagnostics"
        code = read_bas_code("mod_Diagnostics.bas")
        comp.CodeModule.AddFromString(code)

        # mod_DataRefresh
        print("  Injecting mod_DataRefresh ...")
        comp = vb_proj.VBComponents.Add(1)
        comp.Name = "mod_DataRefresh"
        code = read_bas_code("mod_DataRefresh.bas")
        comp.CodeModule.AddFromString(code)

        # mod_CellClick -- goes in Dashboard sheet module
        print("  Injecting mod_CellClick into Dashboard sheet module ...")
        cell_click_code = read_bas_code("mod_CellClick.bas")
        # Strip comment header lines about where to paste
        filtered_lines = []
        for line in cell_click_code.split("\n"):
            stripped = line.strip()
            if stripped.startswith("' ====") or stripped.startswith("' IMPORTANT"):
                continue
            filtered_lines.append(line)
        cell_click_code = "\n".join(filtered_lines)

        # Find Dashboard sheet's code module
        dash_sheet = wb.Sheets("Dashboard")
        dash_codename = dash_sheet.CodeName
        print(f"  Dashboard CodeName: {dash_codename}")
        for comp in vb_proj.VBComponents:
            if comp.Name == dash_codename:
                comp.CodeModule.AddFromString(cell_click_code)
                print("  Injected Worksheet_SelectionChange into Dashboard module.")
                break

        # --- Navy RGB for buttons ---
        # #002B5C = R=0, G=43, B=92 -> COM RGB = R + G*256 + B*65536
        NAVY_RGB = 0 + (43 * 256) + (92 * 65536)  # 6037248
        WHITE_RGB = 255 + (255 * 256) + (255 * 65536)

        # --- Create buttons on Dashboard ---
        print("  Creating Dashboard buttons ...")
        dash = wb.Sheets("Dashboard")

        # Row 4 area (y ~57px): view toggle buttons
        row4_buttons = [
            ("btnSummaryCount",      "Summary Count",       420, 57, 120, 24, "ShowSummaryCount"),
            ("btnSummaryCommitment", "Summary Commitment",  545, 57, 140, 24, "ShowSummaryCommitment"),
            ("btnCountNew",          "Count NEW",           690, 57, 120, 24, "ShowSummaryCountNew"),
            ("btnCommitmentNew",     "Commitment NEW",      815, 57, 140, 24, "ShowSummaryCommitmentNew"),
        ]
        # Row 5 area (y ~87px): WMLC / Reset / Refresh buttons
        row5_buttons = [
            ("btnWMLCOn",            "WMLC ON",             420, 87,  90, 24, "WMLCOn"),
            ("btnWMLCOff",           "WMLC OFF",            515, 87,  90, 24, "WMLCOff"),
            ("btnReset",             "Reset",               610, 87,  80, 24, "ResetDashboard"),
            ("btnRefreshData",       "Refresh Data",        695, 87, 110, 24, "RefreshData"),
            ("btnDiagnostics",       "Run Diagnostics",     810, 87, 130, 24, "TestRefreshCycle"),
        ]

        all_dash_buttons = row4_buttons + row5_buttons
        for name, text, left, top, width, height, macro in all_dash_buttons:
            shp = dash.Shapes.AddShape(5, left, top, width, height)  # 5 = msoShapeRoundedRectangle
            shp.Name = name
            shp.TextFrame2.TextRange.Text = text
            shp.TextFrame2.TextRange.Font.Size = 9
            shp.TextFrame2.TextRange.Font.Bold = True
            shp.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = WHITE_RGB
            shp.TextFrame2.TextRange.ParagraphFormat.Alignment = 2  # msoAlignCenter
            shp.Fill.ForeColor.RGB = NAVY_RGB
            shp.Line.Visible = 0  # False - flat design, no border
            shp.OnAction = macro

        # Diagnostics button gets a distinct dark teal color (#003366)
        try:
            diag_btn = dash.Shapes("btnDiagnostics")
            diag_btn.Fill.ForeColor.RGB = 0x00 + (0x33 * 256) + (0x66 * 65536)  # #003366
        except Exception:
            pass

        # --- Create buttons on loan_detail ---
        print("  Creating loan_detail buttons ...")
        detail = wb.Sheets("loan_detail")

        for name, text, left, top, width, height, macro in [
            ("btnBack",         "\u2190 Back to Dashboard", 5, 18, 160, 24, "BackToDashboard"),
            ("btnResetFilters", "Reset Filters",          175, 18, 110, 24, "ResetFilters"),
        ]:
            shp = detail.Shapes.AddShape(5, left, top, width, height)
            shp.Name = name
            shp.TextFrame2.TextRange.Text = text
            shp.TextFrame2.TextRange.Font.Size = 9
            shp.TextFrame2.TextRange.Font.Bold = True
            shp.TextFrame2.TextRange.Font.Fill.ForeColor.RGB = WHITE_RGB
            shp.TextFrame2.TextRange.ParagraphFormat.Alignment = 2
            shp.Fill.ForeColor.RGB = NAVY_RGB
            shp.Line.Visible = 0
            shp.OnAction = macro

        # --- Save as .xlsm ---
        print(f"  Saving as {abs_xlsm} ...")
        wb.SaveAs(abs_xlsm, FileFormat=52)  # 52 = xlOpenXMLWorkbookMacroEnabled
        print("  Saved .xlsm successfully.")

    finally:
        if wb is not None:
            try:
                wb.Close(False)
            except Exception:
                pass
        if excel is not None:
            try:
                excel.Quit()
            except Exception:
                pass
        # Release COM objects
        wb = None
        excel = None


# -- VBA_REFERENCE.txt generator --------------------------------------------
def generate_vba_reference():
    """Read all .bas files and PQ M code, assemble into output/VBA_REFERENCE.txt"""
    ref_path = os.path.join(REPO, "output", "VBA_REFERENCE.txt")
    lines = []
    lines.append("=" * 80)
    lines.append("WMLC Dashboard - VBA & Power Query Reference")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("=" * 80)
    lines.append("")
    lines.append("This file contains all VBA module source code and Power Query M code")
    lines.append("embedded in the WMLC_Dashboard.xlsm workbook.")
    lines.append("")
    lines.append("LAYOUT NOTE: Dashboard grid starts at row 7 (title=1, subtitle=2,")
    lines.append("indicator=3, buttons=4-5, headers=6, data=7-30, total=31).")
    lines.append("")

    # VBA modules
    bas_files = sorted([f for f in os.listdir(VBA_DIR) if f.endswith('.bas')])
    for bas_file in bas_files:
        bas_path = os.path.join(VBA_DIR, bas_file)
        with open(bas_path, "r", encoding="utf-8") as f:
            content = f.read()
        lines.append("-" * 80)
        lines.append(f"MODULE: {bas_file}")
        lines.append("-" * 80)
        lines.append(content)
        lines.append("")

    # Power Query
    if os.path.exists(PQ_FILE):
        with open(PQ_FILE, "r", encoding="utf-8") as f:
            pq_content = f.read()
        lines.append("-" * 80)
        lines.append("POWER QUERY: load_tagged_data.m")
        lines.append("-" * 80)
        lines.append(pq_content)
        lines.append("")

    lines.append("=" * 80)
    lines.append("END OF VBA REFERENCE")
    lines.append("=" * 80)

    with open(ref_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"  VBA_REFERENCE.txt written to {ref_path}")
    return ref_path


# -- main build --------------------------------------------------------------
def main():
    # Kill any running Excel before we start
    print("Killing any running EXCEL.EXE processes ...")
    subprocess.run(['taskkill', '/F', '/IM', 'EXCEL.EXE'], capture_output=True)
    time.sleep(2)

    # Delete old output files if they exist
    for f in [OUTPUT_XLSX, OUTPUT_XLSM]:
        if os.path.exists(f):
            try:
                os.remove(f)
                print(f"  Deleted old {os.path.basename(f)}")
            except Exception as e:
                print(f"  Warning: could not delete {f}: {e}")

    print("Reading tagged CSV ...")
    df = pd.read_csv(TAGGED_CSV)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    # Normalize column names to uppercase for consistent access
    df.columns = [c.upper() for c in df.columns]

    # Ensure boolean columns
    df["WMLC_QUALIFIED"] = df["WMLC_QUALIFIED"].astype(str).str.strip().str.lower() == "true"
    df["CREDIT_LII"] = pd.to_numeric(df["CREDIT_LII"], errors="coerce").fillna(0)

    # Masks
    mask_new = df["NEW_CAMP_YN"] == "Y"
    mask_wmlc = df["WMLC_QUALIFIED"]
    mask_new_wmlc = mask_new & mask_wmlc

    # Compute 8 pivots
    print("Computing 8 view pivots ...")
    pivots = {
        1: compute_pivot(df, "count"),
        2: compute_pivot(df, "sum"),
        3: compute_pivot(df, "count", mask_new),
        4: compute_pivot(df, "sum",   mask_new),
        5: compute_pivot(df, "count", mask_wmlc),
        6: compute_pivot(df, "sum",   mask_wmlc),
        7: compute_pivot(df, "count", mask_new_wmlc),
        8: compute_pivot(df, "sum",   mask_new_wmlc),
    }

    wb = Workbook()

    # == _data sheet (hidden) ================================================
    print("Building _data sheet ...")
    ws_data = wb.active
    ws_data.title = "_data"
    cols = list(df.columns)
    ws_data.append(cols)
    for _, row in df.iterrows():
        ws_data.append([row[c] for c in cols])

    # Create table
    last_col_letter = get_column_letter(len(cols))
    table_ref = f"A1:{last_col_letter}{len(df)+1}"
    tbl = Table(displayName="tbl_LoanData", ref=table_ref)
    tbl.tableStyleInfo = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False,
        showLastColumn=False, showRowStripes=True, showColumnStripes=False
    )
    ws_data.add_table(tbl)
    ws_data.sheet_state = "hidden"

    # == Dashboard sheet =====================================================
    print("Building Dashboard sheet ...")
    ws_dash = wb.create_sheet("Dashboard")

    # Row 1 -- title (merged A1:P1, navy fill, white 18pt bold)
    ws_dash.merge_cells("A1:P1")
    c = ws_dash["A1"]
    c.value = "Wealth Management Lending Committee \u2014 Portfolio Dashboard"
    c.font = TITLE_FONT
    c.fill = NAVY_FILL
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws_dash.row_dimensions[1].height = 36

    # Row 2 -- subtitle (merged A2:P2, blue fill, white 11pt)
    ws_dash.merge_cells("A2:P2")
    c2 = ws_dash["A2"]
    c2.value = "Credit Risk | 1st Line of Defense"
    c2.font = SUBTITLE_FONT
    c2.fill = BLUE_FILL
    c2.alignment = Alignment(horizontal="center", vertical="center")
    ws_dash.row_dimensions[2].height = 24

    # Row 3 -- date/view indicator (merged A3:P3, white bg, gray text)
    ws_dash.merge_cells("A3:P3")
    c3 = ws_dash["A3"]
    c3.value = f"As of: {datetime.now().strftime('%m/%d/%Y')} | View: Summary Commitment | WMLC: OFF"
    c3.font = INDICATOR_FONT
    c3.fill = WHITE_FILL
    c3.alignment = Alignment(horizontal="center", vertical="center")
    ws_dash.row_dimensions[3].height = 22

    # Rows 4-5 -- button area (light gray fill, height 30)
    for row_num in [4, 5]:
        ws_dash.row_dimensions[row_num].height = 30
        for ci in range(1, 17):
            cell = ws_dash.cell(row=row_num, column=ci)
            cell.fill = BUTTON_GRAY_FILL

    # Row 6 -- column headers (navy fill, white text, bold 10pt, center)
    headers = ["Gross Amount", "Defined Range"] + PRODUCT_BUCKETS + ["Total"]
    ws_dash.row_dimensions[6].height = 28
    for ci, hdr in enumerate(headers, 1):
        cell = ws_dash.cell(row=6, column=ci, value=hdr)
        cell.font = HEADER_FONT
        cell.fill = NAVY_FILL
        cell.border = THIN_WHITE_BORDER
        cell.alignment = Alignment(horizontal="center", wrap_text=True, vertical="center")

    # Rows 7-30 -- 24 data rows (alternating white/ice_blue)
    initial_matrix = pivots[2]  # Summary Commitment
    for ri, (label, floor, ceil) in enumerate(BUCKET_LADDER):
        row_num = ri + 7
        is_alt = (ri % 2 == 1)
        row_fill = ICE_BLUE_FILL if is_alt else WHITE_FILL

        # Col A -- Gross Amount
        cell_a = ws_dash.cell(row=row_num, column=1, value=label)
        cell_a.font = DATA_FONT_BOLD
        cell_a.border = THIN_GRAY_BORDER
        cell_a.alignment = Alignment(horizontal="right")
        cell_a.fill = row_fill

        # Col B -- Defined Range
        cell_b = ws_dash.cell(row=row_num, column=2, value=_range_label(floor, ceil))
        cell_b.font = ITALIC_9_GRAY
        cell_b.border = THIN_GRAY_BORDER
        cell_b.alignment = Alignment(horizontal="left")
        cell_b.fill = row_fill

        # Cols C-O -- data
        row_total = 0
        for ci, val in enumerate(initial_matrix[ri]):
            cell = ws_dash.cell(row=row_num, column=ci + 3, value=val)
            cell.number_format = '$#,##0;-$#,##0;"-"'
            cell.font = DATA_FONT
            cell.border = THIN_GRAY_BORDER
            cell.alignment = Alignment(horizontal="right")
            cell.fill = row_fill
            row_total += val

        # Col P -- total
        cell_p = ws_dash.cell(row=row_num, column=16, value=row_total)
        cell_p.number_format = '$#,##0'
        cell_p.border = THIN_GRAY_BORDER
        cell_p.font = TOTAL_FONT
        cell_p.alignment = Alignment(horizontal="right")
        cell_p.fill = LIGHT_BLUE_FILL

    # Row 31 -- Total row (bold navy text, light blue fill, double-line top)
    ws_dash.cell(row=31, column=1, value="Total").font = TOTAL_FONT
    ws_dash.cell(row=31, column=1).fill = LIGHT_BLUE_FILL
    ws_dash.cell(row=31, column=1).border = DOUBLE_NAVY_TOP
    ws_dash.cell(row=31, column=2, value="").fill = LIGHT_BLUE_FILL
    ws_dash.cell(row=31, column=2).border = DOUBLE_NAVY_TOP

    for ci in range(3, 17):
        col_total = sum(
            ws_dash.cell(row=r, column=ci).value or 0 for r in range(7, 31)
        )
        cell = ws_dash.cell(row=31, column=ci, value=col_total)
        cell.font = TOTAL_FONT
        cell.number_format = '$#,##0;-$#,##0;"-"'
        cell.fill = LIGHT_BLUE_FILL
        cell.border = DOUBLE_NAVY_TOP
        cell.alignment = Alignment(horizontal="right")

    # Column widths
    ws_dash.column_dimensions["A"].width = 18
    ws_dash.column_dimensions["B"].width = 28
    for ci in range(3, 16):
        ws_dash.column_dimensions[get_column_letter(ci)].width = 14
    ws_dash.column_dimensions["P"].width = 14

    # Freeze panes at A7
    ws_dash.freeze_panes = "A7"

    # == 8 hidden view sheets ================================================
    print("Building 8 hidden view sheets ...")
    for view_num in range(1, 9):
        ws_v = wb.create_sheet(f"_view{view_num}")
        matrix = pivots[view_num]
        for ri, row_vals in enumerate(matrix):
            for ci, val in enumerate(row_vals):
                ws_v.cell(row=ri + 1, column=ci + 1, value=val)
        ws_v.sheet_state = "hidden"

    # == loan_detail sheet ===================================================
    print("Building loan_detail sheet ...")
    ws_detail = wb.create_sheet("loan_detail")

    # Row 1 -- title (navy bg, white text)
    ws_detail.merge_cells("A1:H1")
    ws_detail["A1"].value = "Loan Detail"
    ws_detail["A1"].font = DETAIL_TITLE_FONT
    ws_detail["A1"].fill = NAVY_FILL
    ws_detail["A1"].alignment = Alignment(vertical="center")
    ws_detail.row_dimensions[1].height = 30

    # Row 2 -- button placeholder (light gray fill)
    for ci in range(1, len(cols) + 1):
        ws_detail.cell(row=2, column=ci).fill = BUTTON_GRAY_FILL
    ws_detail.row_dimensions[2].height = 30

    # Row 3 -- headers (navy bg, white text, bold 10pt, center)
    for ci, col_name in enumerate(cols, 1):
        cell = ws_detail.cell(row=3, column=ci, value=col_name)
        cell.font = DETAIL_HEADER_FONT
        cell.fill = NAVY_FILL
        cell.border = THIN_WHITE_BORDER
        cell.alignment = Alignment(horizontal="center", wrap_text=True)

    # Row 4+ -- data (alternating white/ice_blue)
    for idx, (_, row) in enumerate(df.iterrows()):
        row_num = idx + 4
        is_alt = (idx % 2 == 1)
        row_fill = ICE_BLUE_FILL if is_alt else WHITE_FILL
        for ci, col_name in enumerate(cols, 1):
            cell = ws_detail.cell(row=row_num, column=ci, value=row[col_name])
            cell.fill = row_fill
            cell.border = THIN_GRAY_BORDER
            cell.font = DATA_FONT

    # AutoFilter
    last_detail_col = get_column_letter(len(cols))
    ws_detail.auto_filter.ref = f"A3:{last_detail_col}{len(df)+3}"

    # Freeze panes
    ws_detail.freeze_panes = "A4"

    # Column widths
    for ci in range(1, len(cols) + 1):
        ws_detail.column_dimensions[get_column_letter(ci)].width = 16

    # == Summary sheet =======================================================
    print("Building Summary sheet ...")
    build_summary_sheet(wb, pivots)

    # == POWER_QUERY_SETUP sheet =============================================
    print("Building POWER_QUERY_SETUP sheet ...")
    build_pq_setup_sheet(wb)

    # == _config sheet (hidden) ==============================================
    print("Building _config sheet ...")
    ws_cfg = wb.create_sheet("_config")
    ws_cfg["A1"].value = 2   # ViewState (Summary Commitment)
    ws_cfg["A2"].value = 0   # WMLCState (OFF)
    ws_cfg["A3"].value = TAGGED_CSV  # DataSourcePath
    ws_cfg.sheet_state = "hidden"

    # == Named ranges ========================================================
    from openpyxl.workbook.defined_name import DefinedName

    dn_view = DefinedName("ViewState", attr_text="_config!$A$1")
    wb.defined_names.add(dn_view)

    dn_wmlc = DefinedName("WMLCState", attr_text="_config!$A$2")
    wb.defined_names.add(dn_wmlc)

    dn_path = DefinedName("DataSourcePath", attr_text="_config!$A$3")
    wb.defined_names.add(dn_path)

    # == Move Dashboard to first position ====================================
    wb.move_sheet("Dashboard", offset=-1)

    # == Save .xlsx (Stage 1) ================================================
    print(f"Saving .xlsx to {OUTPUT_XLSX} ...")
    wb.save(OUTPUT_XLSX)
    size = os.path.getsize(OUTPUT_XLSX)
    print(f"  Saved! Size: {size:,} bytes ({size/1024:.1f} KB)")

    # == Validation ==========================================================
    sheets = wb.sheetnames
    print(f"  Sheets: {sheets}")
    assert "Dashboard" in sheets
    assert "loan_detail" in sheets
    assert "_data" in sheets
    assert "_config" in sheets
    assert "Summary" in sheets
    assert "POWER_QUERY_SETUP" in sheets
    for i in range(1, 9):
        assert f"_view{i}" in sheets

    # Verify named ranges
    dn_names = list(wb.defined_names)
    assert "ViewState" in dn_names, f"ViewState not in {dn_names}"
    assert "WMLCState" in dn_names, f"WMLCState not in {dn_names}"
    assert "DataSourcePath" in dn_names, f"DataSourcePath not in {dn_names}"

    print("\n=== Stage 1 complete (openpyxl .xlsx) ===")

    # Quick pivot sanity check
    for v in range(1, 9):
        total = sum(sum(r) for r in pivots[v])
        print(f"  _view{v} grand total: {total:,.0f}")

    # == Generate VBA_REFERENCE.txt ==========================================
    print("\nGenerating VBA_REFERENCE.txt ...")
    generate_vba_reference()

    # == Stage 2: COM post-processing ========================================
    print("\n=== Stage 2: COM automation (VBA + buttons) ===")
    try:
        inject_vba_and_buttons(OUTPUT_XLSX, OUTPUT_XLSM, VBA_DIR)

        # Verify .xlsm
        xlsm_size = os.path.getsize(OUTPUT_XLSM)
        print(f"\n=== Dashboard build complete ===")
        print(f"  Output: {OUTPUT_XLSM}")
        print(f"  Size:   {xlsm_size:,} bytes ({xlsm_size/1024:.1f} KB)")
        print(f"  Sheets: {len(sheets)} ({', '.join(s for s in sheets if not s.startswith('_'))})")
        print(f"  Hidden: {', '.join(s for s in sheets if s.startswith('_'))}")

        # Verify ZIP contains vbaProject.bin
        import zipfile
        with zipfile.ZipFile(OUTPUT_XLSM, 'r') as zf:
            names = zf.namelist()
            has_vba = any("vbaProject" in n for n in names)
            print(f"  vbaProject.bin in ZIP: {has_vba}")
            if not has_vba:
                print("  WARNING: VBA project not found in .xlsm!")

    except Exception as e:
        print(f"\n  COM automation failed: {e}")
        print("  The .xlsx file was created successfully at:")
        print(f"    {OUTPUT_XLSX}")
        print("  You will need to manually import VBA modules.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
