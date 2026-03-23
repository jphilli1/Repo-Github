Attribute VB_Name = "mod_ViewToggle"
Option Explicit

' View state: 1=Count, 2=Commitment, 3=Count NEW, 4=Commitment NEW
' WMLC state: 0=OFF, 1=ON
' State stored in _config sheet: A1=ViewState, A2=WMLCState

Public Sub ShowSummaryCount()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 1
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in ShowSummaryCount: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub ShowSummaryCommitment()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 2
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in ShowSummaryCommitment: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub ShowSummaryCountNew()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 3
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in ShowSummaryCountNew: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub ShowSummaryCommitmentNew()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 4
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in ShowSummaryCommitmentNew: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub WMLCOn()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A2").Value = 1
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in WMLCOn: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub WMLCOff()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A2").Value = 0
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in WMLCOff: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub ResetDashboard()
    On Error GoTo ErrHandler
    Sheets("_config").Range("A1").Value = 2
    Sheets("_config").Range("A2").Value = 0
    RefreshDashboard
    Exit Sub
ErrHandler:
    MsgBox "Error in ResetDashboard: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub RefreshDashboard()
    On Error GoTo ErrHandler

    Dim viewIdx As Long
    Dim wmlcIdx As Long
    Dim sourceSheet As String
    Dim viewName As String
    Dim fmt As String

    viewIdx = Sheets("_config").Range("A1").Value
    wmlcIdx = Sheets("_config").Range("A2").Value

    ' Calculate which hidden sheet to pull from
    ' _view1..4 are non-WMLC, _view5..8 are WMLC
    Dim sheetNum As Long
    sheetNum = viewIdx + (wmlcIdx * 4)
    sourceSheet = "_view" & sheetNum

    ' View name for display — format string: positive;negative;zero (dash for zero)
    Select Case viewIdx
        Case 1: viewName = "Summary Count": fmt = "#,##0;-#,##0;""-"""
        Case 2: viewName = "Summary Commitment": fmt = "$#,##0;-$#,##0;""-"""
        Case 3: viewName = "Summary Count NEW": fmt = "#,##0;-#,##0;""-"""
        Case 4: viewName = "Summary Commitment NEW": fmt = "$#,##0;-$#,##0;""-"""
    End Select

    Dim wmlcText As String
    If wmlcIdx = 1 Then wmlcText = "ON" Else wmlcText = "OFF"

    ' Update header (row 3 — date/view indicator)
    Sheets("Dashboard").Range("A3").Value = "As of: " & Format(Now, "mm/dd/yyyy") & " | View: " & viewName & " | WMLC: " & wmlcText

    ' Copy values from source sheet to Dashboard data area
    Dim dataRange As Range
    Set dataRange = Sheets("Dashboard").Range("C7:O30")  ' 24 rows x 13 cols

    Dim sourceRange As Range
    Set sourceRange = Sheets(sourceSheet).Range("A1:M24")  ' Same dimensions

    dataRange.Value = sourceRange.Value

    ' Update totals (column P and row 31)
    Dim r As Long, c As Long
    Dim rowTotal As Double
    Dim colTotal As Double

    ' Row totals (column P)
    For r = 7 To 30
        rowTotal = 0
        For c = 3 To 15
            If IsNumeric(Sheets("Dashboard").Cells(r, c).Value) Then
                rowTotal = rowTotal + Sheets("Dashboard").Cells(r, c).Value
            End If
        Next c
        Sheets("Dashboard").Cells(r, 16).Value = rowTotal
    Next r

    ' Column totals (row 31)
    For c = 3 To 16
        colTotal = 0
        For r = 7 To 30
            If IsNumeric(Sheets("Dashboard").Cells(r, c).Value) Then
                colTotal = colTotal + Sheets("Dashboard").Cells(r, c).Value
            End If
        Next r
        Sheets("Dashboard").Cells(31, c).Value = colTotal
    Next c

    ' Apply number format to data area and totals
    dataRange.NumberFormat = fmt
    Sheets("Dashboard").Range("P7:P30").NumberFormat = fmt
    Sheets("Dashboard").Range("C31:P31").NumberFormat = fmt

    ' Apply threshold shading and concentration ratios
    ApplyThresholdFormatting
    ComputeConcentrationRatios

    Exit Sub
ErrHandler:
    MsgBox "Error in RefreshDashboard: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub ApplyThresholdFormatting()
    On Error GoTo ErrHandler
    Dim ws As Worksheet
    Set ws = Sheets("Dashboard")

    Dim shadeColor As Long
    shadeColor = RGB(232, 240, 254)   ' Very light blue for threshold zone

    Dim boldBorderColor As Long
    boldBorderColor = RGB(0, 43, 92)  ' MS Navy

    Dim iceBlue As Long
    iceBlue = RGB(240, 244, 248)

    ' Reset data area to alternating rows
    Dim r As Long, c As Long
    For r = 7 To 30
        For c = 3 To 15
            If (r Mod 2) = 0 Then
                ws.Cells(r, c).Interior.Color = iceBlue
            Else
                ws.Cells(r, c).Interior.Color = RGB(255, 255, 255)
            End If
            ws.Cells(r, c).Borders(xlEdgeBottom).LineStyle = xlContinuous
            ws.Cells(r, c).Borders(xlEdgeBottom).Weight = xlThin
            ws.Cells(r, c).Borders(xlEdgeBottom).Color = RGB(217, 222, 227)
        Next c
    Next r

    ' Build header-to-column mapping from Dashboard row 6
    Dim headerMap As Object
    Set headerMap = CreateObject("Scripting.Dictionary")
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

    Dim key As Variant
    For Each key In pt.Keys
        If headerMap.Exists(CStr(key)) Then
            c = headerMap(CStr(key))
            Dim tRow As Long
            tRow = pt(CStr(key))

            ' Shade rows 7 through threshold row
            For r = 7 To tRow
                ws.Cells(r, c).Interior.Color = shadeColor
            Next r

            ' Bold bottom border at threshold row
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

    ' Row 32 labels
    ws.Cells(32, 1).Value = "WMLC %"
    ws.Cells(32, 1).Font.Bold = True
    ws.Cells(32, 1).Font.Color = RGB(0, 43, 92)
    ws.Cells(32, 2).Value = "% above threshold"
    ws.Cells(32, 2).Font.Italic = True
    ws.Cells(32, 2).Font.Color = RGB(108, 117, 125)

    ' Build header map and identify LAL columns
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

    ' Threshold map (same as ApplyThresholdFormatting)
    Dim pt As Object
    Set pt = CreateObject("Scripting.Dictionary")
    pt("LAL Diversified") = 14:      pt("LAL Highly Conc.") = 20
    pt("LAL NFPs") = 22:             pt("TL SBL Diversified") = 14
    pt("TL SBL Highly Conc.") = 20:  pt("TL Life Insurance") = 20
    pt("TL CRE") = 21:               pt("TL Unsecured") = 24
    pt("TL Aircraft") = 22:          pt("TL PHA") = 24
    pt("TL Other Secured") = 22:     pt("TL Multicollateral") = 22
    pt("RESI") = 29

    Dim LAL_DENOM_ROW As Long
    LAL_DENOM_ROW = 29  ' Sum rows 7-29 for LAL denominator ($10MM+)

    Dim key As Variant
    For Each key In pt.Keys
        If headerMap.Exists(CStr(key)) Then
            c = headerMap(CStr(key))
            Dim tRow As Long
            tRow = pt(CStr(key))

            ' Numerator: sum rows 7 through threshold row
            Dim numerator As Double
            numerator = 0
            Dim r As Long
            For r = 7 To tRow
                If IsNumeric(ws.Cells(r, c).Value) Then numerator = numerator + CDbl(ws.Cells(r, c).Value)
            Next r

            ' Denominator: LAL uses $10MM+ sum, others use Total row
            Dim denominator As Double
            denominator = 0
            If lalColumns.Exists(CStr(key)) Then
                For r = 7 To LAL_DENOM_ROW
                    If IsNumeric(ws.Cells(r, c).Value) Then denominator = denominator + CDbl(ws.Cells(r, c).Value)
                Next r
            Else
                If IsNumeric(ws.Cells(31, c).Value) Then denominator = CDbl(ws.Cells(31, c).Value)
            End If

            If denominator > 0 Then
                ws.Cells(32, c).Value = numerator / denominator
            Else
                ws.Cells(32, c).Value = 0
            End If
        End If
    Next key

    ' Total column (P=16): weighted average
    Dim totalNum As Double, totalDen As Double
    totalNum = 0: totalDen = 0
    For c = 3 To 15
        If IsNumeric(ws.Cells(32, c).Value) And IsNumeric(ws.Cells(31, c).Value) Then
            totalNum = totalNum + (CDbl(ws.Cells(32, c).Value) * CDbl(ws.Cells(31, c).Value))
            totalDen = totalDen + CDbl(ws.Cells(31, c).Value)
        End If
    Next c
    If totalDen > 0 Then ws.Cells(32, 16).Value = totalNum / totalDen

    ' Format row 32
    ws.Range(ws.Cells(32, 3), ws.Cells(32, 16)).NumberFormat = "0.0%"
    ws.Range(ws.Cells(32, 3), ws.Cells(32, 16)).Font.Bold = True
    ws.Range(ws.Cells(32, 3), ws.Cells(32, 16)).Font.Color = RGB(0, 43, 92)
    ws.Range(ws.Cells(32, 3), ws.Cells(32, 16)).Interior.Color = RGB(214, 228, 240)
    ws.Range(ws.Cells(32, 1), ws.Cells(32, 16)).Borders(xlEdgeTop).LineStyle = xlContinuous
    ws.Range(ws.Cells(32, 1), ws.Cells(32, 16)).Borders(xlEdgeTop).Weight = xlThin
    ws.Range(ws.Cells(32, 1), ws.Cells(32, 16)).Borders(xlEdgeTop).Color = RGB(0, 43, 92)

    Exit Sub
ErrHandler:
    ' Silently fail — ratios are informational
End Sub
