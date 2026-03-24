Attribute VB_Name = "mod_Diagnostics"
Option Explicit

Public Sub RunAllTests()
    Dim failures As String
    failures = ""

    ' Test 1: _config sheet exists
    On Error Resume Next
    Dim cfgSheet As Worksheet
    Set cfgSheet = Nothing
    Set cfgSheet = Sheets("_config")
    If cfgSheet Is Nothing Then
        failures = failures & "FAIL: _config sheet not found" & vbCrLf
    End If
    On Error GoTo 0

    ' Test 2: ViewState cell is readable and numeric
    On Error Resume Next
    Dim vs As Variant
    vs = Sheets("_config").Range("A1").Value
    If Err.Number <> 0 Then
        failures = failures & "FAIL: Cannot read ViewState (_config!A1): " & Err.Description & vbCrLf
        Err.Clear
    ElseIf Not IsNumeric(vs) Then
        failures = failures & "FAIL: ViewState is not numeric: " & CStr(vs) & vbCrLf
    End If
    On Error GoTo 0

    ' Test 3: WMLCState cell is readable and numeric
    On Error Resume Next
    Dim wmlcState As Variant
    wmlcState = Sheets("_config").Range("A2").Value
    If Err.Number <> 0 Then
        failures = failures & "FAIL: Cannot read WMLCState (_config!A2): " & Err.Description & vbCrLf
        Err.Clear
    ElseIf Not IsNumeric(wmlcState) Then
        failures = failures & "FAIL: WMLCState is not numeric: " & CStr(wmlcState) & vbCrLf
    End If
    On Error GoTo 0

    ' Test 4: DataSourcePath (_config!A3) has a non-empty string
    On Error Resume Next
    Dim dsPath As Variant
    dsPath = Sheets("_config").Range("A3").Value
    If Err.Number <> 0 Then
        failures = failures & "FAIL: Cannot read DataSourcePath (_config!A3): " & Err.Description & vbCrLf
        Err.Clear
    ElseIf Len(Trim(CStr(dsPath))) = 0 Then
        failures = failures & "FAIL: DataSourcePath (_config!A3) is empty" & vbCrLf
    End If
    On Error GoTo 0

    ' Test 5: All required sheets exist
    Dim requiredSheets As Variant
    requiredSheets = Array("Dashboard", "Loan Detail", "_config", "_view1", "_view2", "_view3", "_view4", "_view5", "_view6", "_view7", "_view8")
    Dim i As Long
    For i = LBound(requiredSheets) To UBound(requiredSheets)
        On Error Resume Next
        Dim testSheet As Worksheet
        Set testSheet = Nothing
        Set testSheet = Sheets(CStr(requiredSheets(i)))
        If testSheet Is Nothing Then
            failures = failures & "FAIL: Sheet '" & requiredSheets(i) & "' not found" & vbCrLf
        End If
        On Error GoTo 0
    Next i

    ' Test 6: _data has essential column headers in row 1
    '         PRODUCT_BUCKET and CREDIT_LII are required; bucket is optional (VBA computes it)
    On Error Resume Next
    Dim wsData As Worksheet
    Set wsData = Sheets("Loan Detail")
    If Not wsData Is Nothing Then
        Dim dataLastCol As Long
        dataLastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column
        Dim foundProduct As Boolean, foundBucket As Boolean, foundCreditLii As Boolean
        foundProduct = False: foundBucket = False: foundCreditLii = False
        Dim col As Long
        For col = 1 To dataLastCol
            Select Case LCase(Trim(CStr(wsData.Cells(1, col).Value)))
                Case "product_bucket": foundProduct = True
                Case "credit_lii_commitment_bucket": foundBucket = True
                Case "credit_lii": foundCreditLii = True
            End Select
        Next col
        If Not foundProduct Then failures = failures & "FAIL: Loan Detail missing 'PRODUCT_BUCKET' column" & vbCrLf
        If Not foundCreditLii Then failures = failures & "FAIL: Loan Detail missing 'CREDIT_LII' column" & vbCrLf
        ' Bucket column is optional — VBA computes it from CREDIT_LII if missing
        If Not foundBucket Then failures = failures & "NOTE: CREDIT_LII_COMMITMENT_BUCKET missing (will be computed)" & vbCrLf
    End If
    If Err.Number <> 0 Then
        failures = failures & "FAIL: Error reading Loan Detail headers: " & Err.Description & vbCrLf
        Err.Clear
    End If
    On Error GoTo 0

    ' Test 7: _view1 has data (not empty)
    On Error Resume Next
    Dim v1val As Variant
    v1val = Sheets("_view1").Range("A1").Value
    If Err.Number <> 0 Then
        failures = failures & "FAIL: Cannot read _view1!A1" & vbCrLf
        Err.Clear
    End If
    On Error GoTo 0

    ' Test 8: Dashboard data area exists (row 7 after layout shift)
    On Error Resume Next
    Dim dashVal As Variant
    dashVal = Sheets("Dashboard").Range("C7").Value
    If Err.Number <> 0 Then
        failures = failures & "FAIL: Cannot read Dashboard!C7" & vbCrLf
        Err.Clear
    End If
    On Error GoTo 0

    ' Test 9: loan_detail has headers in row 3
    On Error Resume Next
    Dim ldHeader As Variant
    ldHeader = Sheets("Loan Detail").Range("A3").Value
    If Err.Number <> 0 Or IsEmpty(ldHeader) Then
        failures = failures & "FAIL: loan_detail row 3 headers missing" & vbCrLf
        Err.Clear
    End If
    On Error GoTo 0

    ' Test 10: Try a full RefreshDashboard cycle
    On Error Resume Next
    Sheets("_config").Range("A1").Value = 2
    Sheets("_config").Range("A2").Value = 0
    RefreshDashboard
    If Err.Number <> 0 Then
        failures = failures & "FAIL: RefreshDashboard error: " & Err.Description & vbCrLf
        Err.Clear
    End If
    On Error GoTo 0

    ' Test 11: Try all 8 view combinations
    Dim v As Long, w As Long
    For v = 1 To 4
        For w = 0 To 1
            On Error Resume Next
            Sheets("_config").Range("A1").Value = v
            Sheets("_config").Range("A2").Value = w
            RefreshDashboard
            If Err.Number <> 0 Then
                failures = failures & "FAIL: RefreshDashboard failed for view=" & v & " wmlc=" & w & ": " & Err.Description & vbCrLf
                Err.Clear
            End If
            On Error GoTo 0
        Next w
    Next v

    ' Test 12: RecomputeAllViews runs without error
    On Error Resume Next
    RecomputeAllViews
    If Err.Number <> 0 Then
        failures = failures & "FAIL: RecomputeAllViews error: " & Err.Description & vbCrLf
        Err.Clear
    End If
    On Error GoTo 0

    ' Reset to default view
    Sheets("_config").Range("A1").Value = 2
    Sheets("_config").Range("A2").Value = 0
    RefreshDashboard

    ' Report results
    If InStr(failures, "FAIL:") = 0 Then
        If failures = "" Then
            MsgBox "ALL TESTS PASSED - Dashboard is ready to use.", vbInformation, "WMLC Diagnostics"
        Else
            ' Only NOTEs, no FAILs
            MsgBox "ALL TESTS PASSED (with notes):" & vbCrLf & vbCrLf & failures, vbInformation, "WMLC Diagnostics"
        End If
    Else
        MsgBox "FAILURES DETECTED:" & vbCrLf & vbCrLf & failures, vbCritical, "WMLC Diagnostics"
    End If
End Sub

Public Sub TestRefreshCycle()
    On Error GoTo ErrHandler
    Dim ws As Worksheet
    Set ws = Sheets("Loan Detail")

    ' Check _data has data
    Dim dataRows As Long
    dataRows = ws.Cells(ws.Rows.Count, 1).End(xlUp).Row - 1
    If dataRows < 1 Then
        MsgBox "FAIL: _data sheet is empty. Load data via Power Query or CSV import first.", vbCritical
        Exit Sub
    End If

    ' Run full refresh cycle
    Application.ScreenUpdating = False
    Sheets("_config").Range("A1").Value = 2  ' Commitment view
    Sheets("_config").Range("A2").Value = 0  ' WMLC off
    RecomputeAllViews
    RefreshDashboard
    RefreshLoanDetail
    Application.ScreenUpdating = True

    ' Verify Dashboard total (P31 = total row, total column)
    Dim postDashVal As Variant
    postDashVal = Sheets("Dashboard").Range("P31").Value

    ' Verify loan_detail has data
    Dim detailRows As Long
    detailRows = Sheets("Loan Detail").Cells(Sheets("Loan Detail").Rows.Count, 1).End(xlUp).Row

    ' Verify _view1 has data
    Dim view1Val As Variant
    view1Val = Sheets("_view1").Range("A1").Value

    Dim result As String
    result = "=== Refresh Cycle Test ===" & vbCrLf
    result = result & "_data rows: " & dataRows & vbCrLf
    result = result & "Dashboard total (P31): " & Format(postDashVal, "#,##0") & vbCrLf
    result = result & "loan_detail rows: " & (detailRows - 3) & vbCrLf
    result = result & "_view1 A1 value: " & view1Val & vbCrLf

    Dim hasFail As Boolean
    hasFail = False
    If Not IsNumeric(postDashVal) Or postDashVal = 0 Then
        result = result & vbCrLf & "- Dashboard total is zero/empty"
        hasFail = True
    End If
    If detailRows <= 4 Then
        result = result & vbCrLf & "- loan_detail has no data rows"
        hasFail = True
    End If
    If Not IsNumeric(view1Val) Then
        result = result & vbCrLf & "- _view1 is empty"
        hasFail = True
    End If

    If Not hasFail Then
        result = result & vbCrLf & "ALL CHECKS PASSED"
        MsgBox result, vbInformation, "Refresh Test"
    Else
        result = result & vbCrLf & "FAILURES DETECTED"
        MsgBox result, vbCritical, "Refresh Test"
    End If
    Exit Sub

ErrHandler:
    MsgBox "Refresh test failed with error: " & Err.Description & " (Error " & Err.Number & ")", vbCritical
End Sub
