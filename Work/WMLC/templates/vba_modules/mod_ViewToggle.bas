Attribute VB_Name = "mod_ViewToggle"
Option Explicit

' View state: 1=Count, 2=Commitment, 3=Count NEW, 4=Commitment NEW
' WMLC state: 0=OFF, 1=ON
' State stored in _config sheet: A1=ViewState, A2=WMLCState
'
' All button handlers call MasterRefresh(reformat:=False) for fast toggle.
' Threshold shading is static across views — only reapplied on data load.

Public Sub ShowSummaryCount()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 1
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

Public Sub ShowSummaryCommitment()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 2
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

Public Sub ShowSummaryCountNew()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 3
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

Public Sub ShowSummaryCommitmentNew()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 4
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

Public Sub WMLCOn()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A2").Value = 1
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

Public Sub WMLCOff()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A2").Value = 0
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

Public Sub ResetDashboard()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 2
    Sheets("_config").Range("A2").Value = 0
    MasterRefresh reformat:=False
    Exit Sub
ErrHandler:
    MsgBox "Error: " & Err.Description, vbExclamation
End Sub

' DEPRECATED wrapper
Public Sub RefreshDashboard()
    MasterRefresh reformat:=False
End Sub

' ============================================================
' ApplyThresholdFormatting — batch range operations
' Uses 13 Range.Interior.Color calls + 13 border calls instead of
' 600+ individual cell operations.
' ============================================================
Public Sub ApplyThresholdFormatting()
    On Error GoTo ErrHandler
    Dim ws As Worksheet
    Set ws = Sheets("Dashboard")

    Dim shadeColor As Long: shadeColor = RGB(232, 240, 254)
    Dim boldBorderColor As Long: boldBorderColor = RGB(0, 43, 92)
    Dim iceBlue As Long: iceBlue = RGB(240, 244, 248)

    ' Reset entire data area to alternating rows — batch per row
    Dim r As Long
    For r = 7 To 30
        If (r Mod 2) = 0 Then
            ws.Range(ws.Cells(r, 3), ws.Cells(r, 15)).Interior.Color = iceBlue
        Else
            ws.Range(ws.Cells(r, 3), ws.Cells(r, 15)).Interior.Color = RGB(255, 255, 255)
        End If
    Next r
    ' Reset all borders in one shot
    Dim dataArea As Range
    Set dataArea = ws.Range("C7:O30")
    dataArea.Borders(xlEdgeBottom).LineStyle = xlContinuous
    dataArea.Borders(xlEdgeBottom).Weight = xlThin
    dataArea.Borders(xlEdgeBottom).Color = RGB(217, 222, 227)
    dataArea.Borders(xlInsideHorizontal).LineStyle = xlContinuous
    dataArea.Borders(xlInsideHorizontal).Weight = xlThin
    dataArea.Borders(xlInsideHorizontal).Color = RGB(217, 222, 227)

    ' Build header-to-column mapping
    Dim headerMap As Object
    Set headerMap = CreateObject("Scripting.Dictionary")
    Dim c As Long
    For c = 3 To 15
        headerMap(Trim(CStr(ws.Cells(6, c).Value))) = c
    Next c

    ' Product -> threshold row mapping
    Dim pt As Object
    Set pt = CreateObject("Scripting.Dictionary")
    pt("LAL Diversified") = 14:      pt("LAL Highly Conc.") = 20
    pt("LAL NFPs") = 22:             pt("TL SBL Diversified") = 14
    pt("TL SBL Highly Conc.") = 20:  pt("TL Life Insurance") = 20
    pt("TL CRE") = 21:               pt("TL Unsecured") = 24
    pt("TL Aircraft") = 22:          pt("TL PHA") = 24
    pt("TL Other Secured") = 22:     pt("TL Multicollateral") = 22
    pt("RESI") = 29

    ' Apply threshold shading — ONE range call per column (not per cell)
    Dim key As Variant
    For Each key In pt.Keys
        If headerMap.Exists(CStr(key)) Then
            c = headerMap(CStr(key))
            Dim tRow As Long
            tRow = pt(CStr(key))
            ' Single range shade
            ws.Range(ws.Cells(7, c), ws.Cells(tRow, c)).Interior.Color = shadeColor
            ' Single border call
            ws.Cells(tRow, c).Borders(xlEdgeBottom).LineStyle = xlContinuous
            ws.Cells(tRow, c).Borders(xlEdgeBottom).Weight = xlMedium
            ws.Cells(tRow, c).Borders(xlEdgeBottom).Color = boldBorderColor
        End If
    Next key

    Exit Sub
ErrHandler:
    ' Silently fail — formatting is cosmetic
End Sub

Public Sub ComputeConcentrationRatios()
    On Error GoTo ErrHandler
    Dim ws As Worksheet
    Set ws = Sheets("Dashboard")

    ws.Cells(32, 1).Value = "WMLC %"
    ws.Cells(32, 1).Font.Bold = True
    ws.Cells(32, 1).Font.Color = RGB(0, 43, 92)
    ws.Cells(32, 2).Value = "% above threshold"
    ws.Cells(32, 2).Font.Italic = True
    ws.Cells(32, 2).Font.Color = RGB(108, 117, 125)

    Dim headerMap As Object
    Set headerMap = CreateObject("Scripting.Dictionary")
    Dim lalColumns As Object
    Set lalColumns = CreateObject("Scripting.Dictionary")
    Dim c As Long
    For c = 3 To 15
        Dim hdr As String
        hdr = Trim(CStr(ws.Cells(6, c).Value))
        headerMap(hdr) = c
        If InStr(hdr, "LAL") > 0 Then lalColumns(hdr) = c
    Next c

    Dim pt As Object
    Set pt = CreateObject("Scripting.Dictionary")
    pt("LAL Diversified") = 14:      pt("LAL Highly Conc.") = 20
    pt("LAL NFPs") = 22:             pt("TL SBL Diversified") = 14
    pt("TL SBL Highly Conc.") = 20:  pt("TL Life Insurance") = 20
    pt("TL CRE") = 21:               pt("TL Unsecured") = 24
    pt("TL Aircraft") = 22:          pt("TL PHA") = 24
    pt("TL Other Secured") = 22:     pt("TL Multicollateral") = 22
    pt("RESI") = 29

    Dim LAL_DENOM_ROW As Long: LAL_DENOM_ROW = 29

    ' Read data area into array for fast computation (no cell-by-cell reads)
    Dim dataVals As Variant
    dataVals = ws.Range("C7:O30").Value  ' 24 rows x 13 cols

    Dim ratios(1 To 1, 1 To 14) As Variant
    Dim key As Variant
    For Each key In pt.Keys
        If headerMap.Exists(CStr(key)) Then
            c = headerMap(CStr(key))
            Dim colIdx As Long: colIdx = c - 2  ' 1-based index into dataVals
            Dim tRow As Long: tRow = pt(CStr(key))
            Dim threshRowIdx As Long: threshRowIdx = tRow - 6  ' convert to array index

            Dim numerator As Double: numerator = 0
            Dim r As Long
            For r = 1 To threshRowIdx
                If IsNumeric(dataVals(r, colIdx)) Then numerator = numerator + CDbl(dataVals(r, colIdx))
            Next r

            Dim denominator As Double: denominator = 0
            If lalColumns.Exists(CStr(key)) Then
                Dim denomRow As Long: denomRow = LAL_DENOM_ROW - 6
                For r = 1 To denomRow
                    If IsNumeric(dataVals(r, colIdx)) Then denominator = denominator + CDbl(dataVals(r, colIdx))
                Next r
            Else
                If IsNumeric(ws.Cells(31, c).Value) Then denominator = CDbl(ws.Cells(31, c).Value)
            End If

            If denominator > 0 Then
                ratios(1, colIdx) = numerator / denominator
            Else
                ratios(1, colIdx) = 0
            End If
        End If
    Next key

    ' Weighted average for total column
    Dim totalNum As Double, totalDen As Double
    totalNum = 0: totalDen = 0
    For c = 1 To 13
        If IsNumeric(ratios(1, c)) And IsNumeric(ws.Cells(31, c + 2).Value) Then
            Dim colVal As Double: colVal = CDbl(ws.Cells(31, c + 2).Value)
            totalNum = totalNum + (CDbl(ratios(1, c)) * colVal)
            totalDen = totalDen + colVal
        End If
    Next c
    If totalDen > 0 Then ratios(1, 14) = totalNum / totalDen Else ratios(1, 14) = 0

    ' Bulk write ratios
    ws.Range("C32:P32").Value = ratios

    ' Format row 32
    Dim r32 As Range
    Set r32 = ws.Range("C32:P32")
    r32.NumberFormat = "0.0%"
    r32.Font.Bold = True
    r32.Font.Color = RGB(0, 43, 92)
    r32.Interior.Color = RGB(214, 228, 240)
    ws.Range("A32:P32").Borders(xlEdgeTop).LineStyle = xlContinuous
    ws.Range("A32:P32").Borders(xlEdgeTop).Weight = xlThin
    ws.Range("A32:P32").Borders(xlEdgeTop).Color = RGB(0, 43, 92)

    Exit Sub
ErrHandler:
    ' Silently fail
End Sub
