# SKILL: Excel Dashboard + VBA Build

## Objective
Build a fully formatted .xlsm workbook with VBA macros, interactive dashboard,
drill-down capability, and Power Query connection using only openpyxl.

## Tools
- Python 3.x, openpyxl (with VBA support)
- NO xlsxwriter (cannot do VBA), NO win32com (not cross-platform)

## Critical: VBA Injection via openpyxl

openpyxl can preserve VBA from a template but cannot create VBA from scratch easily.
The recommended approach:

### Option A: vbaProject.bin template (PREFERRED)
1. Create a minimal .xlsm template with VBA modules in Excel manually (or use a
   pre-built one). Store as `templates/template.xlsm`.
2. In Python, load with `keep_vba=True`:
   ```python
   wb = openpyxl.load_workbook('templates/template.xlsm', keep_vba=True)
   ```
3. Modify sheets/data programmatically while preserving VBA.

### Option B: Build vbaProject.bin programmatically
1. Create VBA source code as .bas text files in `templates/vba_modules/`
2. Use a helper to construct a vbaProject.bin from these sources
3. Inject into the workbook ZIP before saving

### Option C: Two-pass approach (MOST RELIABLE)
1. Pass 1: Python builds the .xlsx with all data, formatting, named ranges, and layout
2. Pass 2: Python writes .bas files with all VBA code
3. User opens .xlsx, imports .bas files via VBA editor, saves as .xlsm
4. Document this in output/SETUP_INSTRUCTIONS.md

**Use Option C as the primary approach** — it's the most reliable without a Windows
environment. Generate the .xlsx AND the .bas files, plus clear import instructions.

## Dashboard Sheet Layout

### Row/Column Map
```
Row 1: "Current Portfolio Snapshot, $" (merged A1:P1, bold, 14pt)
Row 2: View indicator — "View: Summary Commitment | WMLC: OFF" (merged A2:P2)
Row 3: Headers
  A3: "Gross Amount" (bold)
  B3: "Defined Range" (bold, italic)
  C3: "LAL Diversified"
  D3: "LAL Highly Conc."
  E3: "LAL NFPs"
  F3: "TL SBL Diversified"
  G3: "TL SBL Highly Conc."
  H3: "TL Life Insurance"
  I3: "TL CRE"
  J3: "TL Unsecured"
  K3: "TL Aircraft"
  L3: "TL PHA"
  M3: "TL Other Secured"
  N3: "TL Multicollateral"
  O3: "RESI"
  P3: "Total"

Rows 4-27: Data rows (24 bucket rows, descending from $1B to $1)
Row 28: "Total" row

Column A: Gross amount labels (currency-formatted bucket floor)
Column B: Defined range text (e.g., "$750,000,000-$999,999,999.99")
Columns C-O: Data cells (one per product_bucket)
Column P: Row totals
```

### Formatting Specifications
- Header row (3): Bold, light green fill (#C6EFCE), thin bottom border
- Data cells: Thin borders all sides, right-aligned
- Column A: Bold, currency format ($#,##0)
- Column B: Italic, smaller font (9pt)
- Alternating row shading: very light gray (#F2F2F2) on even rows
- Non-zero cells: no special highlighting (keep it clean)
- Total row: Bold, top double-border, light yellow fill (#FFFFCC)
- Freeze panes: A4 (freeze headers)

### Data Population Strategy
Pre-compute all 8 views in Python as separate 2D arrays, store them in hidden
named ranges or a helper sheet. VBA toggles which array is displayed.

OR: Store the raw data in _data and have VBA compute COUNTIFS/SUMIFS on the fly.
The COUNTIFS approach is simpler to implement but slower on large datasets.

**Recommended:** Pre-compute 8 pivot tables in Python, write each to a hidden sheet
(_view1 through _view8). VBA copies values from the active view sheet to Dashboard.
This is fastest and most reliable.

### Hidden View Sheets (_view1 through _view8)
```
_view1: Summary Count (all loans)
_view2: Summary Commitment (all loans)
_view3: Summary Count NEW (NEW_CAMP_YN = "Y")
_view4: Summary Commitment NEW (NEW_CAMP_YN = "Y")
_view5: Summary Count (WMLC only)
_view6: Summary Commitment (WMLC only)
_view7: Summary Count NEW (WMLC only)
_view8: Summary Commitment NEW (WMLC only)
```

Each view sheet has identical layout: 24 rows × 14 columns of values.

## VBA Module Code

### mod_ViewToggle.bas
```vba
Option Explicit

' View state: 1=Count, 2=Commitment, 3=Count NEW, 4=Commitment NEW
' WMLC state: 0=OFF, 1=ON

Public Sub ShowSummaryCount()
    Range("ViewState").Value = 1
    RefreshDashboard
End Sub

Public Sub ShowSummaryCommitment()
    Range("ViewState").Value = 2
    RefreshDashboard
End Sub

Public Sub ShowSummaryCountNew()
    Range("ViewState").Value = 3
    RefreshDashboard
End Sub

Public Sub ShowSummaryCommitmentNew()
    Range("ViewState").Value = 4
    RefreshDashboard
End Sub

Public Sub WMLCOn()
    Range("WMLCState").Value = 1
    RefreshDashboard
End Sub

Public Sub WMLCOff()
    Range("WMLCState").Value = 0
    RefreshDashboard
End Sub

Public Sub ResetDashboard()
    Range("ViewState").Value = 2
    Range("WMLCState").Value = 0
    RefreshDashboard
End Sub

Private Sub RefreshDashboard()
    Dim viewIdx As Long
    Dim wmlcIdx As Long
    Dim sourceSheet As String
    Dim viewName As String
    Dim fmt As String

    viewIdx = Range("ViewState").Value
    wmlcIdx = Range("WMLCState").Value

    ' Calculate which hidden sheet to pull from
    ' _view1..4 are non-WMLC, _view5..8 are WMLC
    Dim sheetNum As Long
    sheetNum = viewIdx + (wmlcIdx * 4)
    sourceSheet = "_view" & sheetNum

    ' View name for display
    Select Case viewIdx
        Case 1: viewName = "Summary Count": fmt = "#,##0"
        Case 2: viewName = "Summary Commitment": fmt = "$#,##0"
        Case 3: viewName = "Summary Count NEW": fmt = "#,##0"
        Case 4: viewName = "Summary Commitment NEW": fmt = "$#,##0"
    End Select

    Dim wmlcText As String
    If wmlcIdx = 1 Then wmlcText = "ON" Else wmlcText = "OFF"

    ' Update header
    Sheets("Dashboard").Range("A2").Value = "View: " & viewName & " | WMLC: " & wmlcText

    ' Copy values from source sheet to Dashboard data area
    Dim dataRange As Range
    Set dataRange = Sheets("Dashboard").Range("C4:O27")  ' 24 rows x 13 cols (14 products excl total)

    Dim sourceRange As Range
    Set sourceRange = Sheets(sourceSheet).Range("A1:M24")  ' Same dimensions

    dataRange.Value = sourceRange.Value

    ' Update totals (column P and row 28)
    Dim r As Long, c As Long
    ' Row totals
    For r = 4 To 27
        Dim rowTotal As Double
        rowTotal = 0
        For c = 3 To 15
            If IsNumeric(Sheets("Dashboard").Cells(r, c).Value) Then
                rowTotal = rowTotal + Sheets("Dashboard").Cells(r, c).Value
            End If
        Next c
        Sheets("Dashboard").Cells(r, 16).Value = rowTotal
    Next r

    ' Column totals
    For c = 3 To 16
        Dim colTotal As Double
        colTotal = 0
        For r = 4 To 27
            If IsNumeric(Sheets("Dashboard").Cells(r, c).Value) Then
                colTotal = colTotal + Sheets("Dashboard").Cells(r, c).Value
            End If
        Next c
        Sheets("Dashboard").Cells(28, c).Value = colTotal
    Next c

    ' Apply number format
    dataRange.NumberFormat = fmt
    Sheets("Dashboard").Range("P4:P27").NumberFormat = fmt
    Sheets("Dashboard").Range("C28:P28").NumberFormat = fmt
End Sub
```

### mod_CellClick.bas (goes in Dashboard sheet module)
```vba
Private Sub Worksheet_SelectionChange(ByVal Target As Range)
    ' Only respond to single-cell clicks in data area
    If Target.Cells.Count > 1 Then Exit Sub
    If Target.Row < 4 Or Target.Row > 27 Then Exit Sub
    If Target.Column < 3 Or Target.Column > 15 Then Exit Sub

    Dim bucketLabel As String
    Dim productBucket As String
    Dim viewIdx As Long
    Dim wmlcIdx As Long

    bucketLabel = Sheets("Dashboard").Cells(Target.Row, 1).Value
    productBucket = Sheets("Dashboard").Cells(3, Target.Column).Value
    viewIdx = Range("ViewState").Value
    wmlcIdx = Range("WMLCState").Value

    ' Switch to loan_detail and apply filters
    Dim ws As Worksheet
    Set ws = Sheets("loan_detail")
    ws.Activate

    ' Clear existing filters
    If ws.AutoFilterMode Then ws.AutoFilter.ShowAllData

    ' Find column positions in loan_detail
    Dim colBucket As Long, colProduct As Long, colWMLC As Long, colNew As Long
    Dim h As Range
    Set h = ws.Rows(3)

    colBucket = 0: colProduct = 0: colWMLC = 0: colNew = 0
    Dim i As Long
    For i = 1 To h.Cells(1, h.Columns.Count).End(xlToLeft).Column
        Select Case LCase(Trim(ws.Cells(3, i).Value))
            Case "credit_lii_commitment_bucket": colBucket = i
            Case "product_bucket": colProduct = i
            Case "wmlc_qualified": colWMLC = i
            Case "new_camp_yn": colNew = i
        End Select
    Next i

    ' Apply filters
    If colProduct > 0 Then ws.Range("A3").AutoFilter Field:=colProduct, Criteria1:=productBucket
    If colBucket > 0 Then ws.Range("A3").AutoFilter Field:=colBucket, Criteria1:=bucketLabel

    ' WMLC filter
    If wmlcIdx = 1 And colWMLC > 0 Then
        ws.Range("A3").AutoFilter Field:=colWMLC, Criteria1:="True"
    End If

    ' NEW filter (views 3 and 4)
    If (viewIdx = 3 Or viewIdx = 4) And colNew > 0 Then
        ws.Range("A3").AutoFilter Field:=colNew, Criteria1:="Y"
    End If
End Sub
```

### mod_Navigation.bas
```vba
Option Explicit

Public Sub BackToDashboard()
    ' Clear loan_detail filters
    Dim ws As Worksheet
    Set ws = Sheets("loan_detail")
    If ws.AutoFilterMode Then
        If ws.FilterMode Then ws.ShowAllData
    End If
    Sheets("Dashboard").Activate
End Sub

Public Sub ResetFilters()
    If ActiveSheet.AutoFilterMode Then
        If ActiveSheet.FilterMode Then ActiveSheet.ShowAllData
    End If
End Sub
```

## Power Query M Code

```m
let
    DataPath = Excel.CurrentWorkbook(){[Name="DataSourcePath"]}[Content]{0}[Column1],
    Source = Csv.Document(File.Contents(DataPath), [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),

    // Resilient type assignments
    TypedTable = Table.TransformColumnTypes(PromotedHeaders, {
        {"credit_lii", type number},
        {"balance", type number},
        {"credit_limit", type number},
        {"wmlc_flag_count", Int64.Type},
        {"credit_lii_commitment_floor", Int64.Type}
    }, "en-US")
in
    TypedTable
```

## Named Ranges to Create
- `ViewState`: single cell (e.g., _config!A1), initial value = 2
- `WMLCState`: single cell (e.g., _config!A2), initial value = 0
- `DataSourcePath`: single cell (e.g., _config!A3), initial value = path to tagged CSV

## Button Creation in openpyxl
openpyxl does not natively support Form Controls or ActiveX buttons.
Use **Shapes** with text as button placeholders, and document that the user
must assign macros to them after opening.

OR: Create the buttons as simple colored cells with text (e.g., merged cells
styled as buttons) and instruct the user to assign macros.

Best approach: Write a VBA `Auto_Open` sub that creates the buttons programmatically
on first run:
```vba
Sub Auto_Open()
    ' Create buttons if they don't exist yet
    CreateDashboardButtons
End Sub
```

## Output Checklist
- [ ] output/WMLC_Dashboard.xlsx (or .xlsm if VBA injected)
- [ ] templates/vba_modules/mod_ViewToggle.bas
- [ ] templates/vba_modules/mod_CellClick.bas
- [ ] templates/vba_modules/mod_Navigation.bas
- [ ] templates/power_query/load_tagged_data.m
- [ ] output/SETUP_INSTRUCTIONS.md (how to import VBA and PQ)
- [ ] 8 hidden view sheets pre-computed correctly
- [ ] Named ranges created for ViewState, WMLCState, DataSourcePath
