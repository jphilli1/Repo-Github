Attribute VB_Name = "mod_DataRefresh"
Option Explicit

Public Sub RefreshData()
    On Error GoTo ErrHandler
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    Application.StatusBar = "Refreshing Power Query connection..."

    ' Refresh the Power Query connection (user sets this up once)
    Dim conn As WorkbookConnection
    Dim foundConn As Boolean
    foundConn = False
    For Each conn In ThisWorkbook.Connections
        If InStr(1, conn.Name, "loan_extract", vbTextCompare) > 0 Or _
           InStr(1, conn.Name, "tbl_LoanData", vbTextCompare) > 0 Then
            conn.Refresh
            foundConn = True
        End If
    Next conn

    If Not foundConn Then
        ThisWorkbook.RefreshAll
        Application.CalculateUntilAsyncQueriesDone
    End If

    Application.StatusBar = "Recomputing all views..."
    RecomputeAllViews
    RefreshDashboard

    Application.StatusBar = "Updating loan detail..."
    RefreshLoanDetail

    Application.StatusBar = "Updating Summary sheet..."
    RefreshSummary

    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False

    Dim lastRow As Long
    lastRow = Sheets("_data").Cells(Sheets("_data").Rows.Count, 1).End(xlUp).Row

    MsgBox "Data refreshed: " & (lastRow - 1) & " loans loaded." & vbCrLf & _
           "All views recomputed.", vbInformation, "Refresh Complete"
    Exit Sub

ErrHandler:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False
    MsgBox "Error in RefreshData: " & Err.Description & " (Error " & Err.Number & ")" & vbCrLf & vbCrLf & _
           "If Power Query is not set up yet, see POWER_QUERY_SETUP tab.", vbCritical
End Sub

Public Sub RefreshDataFromCSV()
    On Error GoTo ErrHandler
    Dim csvPath As String
    csvPath = Sheets("_config").Range("A3").Value

    If Len(Dir(csvPath)) = 0 Then
        MsgBox "CSV file not found: " & csvPath & vbCrLf & vbCrLf & _
               "Update the path in _config sheet cell A3.", vbExclamation, "File Not Found"
        Exit Sub
    End If

    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    Application.StatusBar = "Importing CSV (this may take a moment)..."

    Dim wsData As Worksheet
    Set wsData = Sheets("_data")
    Dim lo As ListObject
    For Each lo In wsData.ListObjects
        lo.Unlist
    Next lo
    wsData.Cells.Clear

    Dim fileNum As Integer
    Dim fileLine As String
    Dim rowNum As Long
    Dim fields() As String
    Dim colNum As Long

    fileNum = FreeFile
    Open csvPath For Input As #fileNum
    rowNum = 1

    Do While Not EOF(fileNum)
        Line Input #fileNum, fileLine
        fields = ParseCSVLine(fileLine)
        For colNum = 0 To UBound(fields)
            Dim cellVal As String
            cellVal = fields(colNum)
            If rowNum > 1 And IsNumeric(cellVal) And Len(cellVal) > 0 Then
                wsData.Cells(rowNum, colNum + 1).Value = CDbl(cellVal)
            Else
                wsData.Cells(rowNum, colNum + 1).Value = cellVal
            End If
        Next colNum
        rowNum = rowNum + 1
    Loop
    Close #fileNum

    Dim dataRows As Long
    dataRows = rowNum - 2

    Application.StatusBar = "Recomputing all views..."
    RecomputeAllViews
    RefreshDashboard

    Application.StatusBar = "Updating loan detail..."
    RefreshLoanDetail

    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False

    MsgBox "CSV imported: " & dataRows & " loans loaded." & vbCrLf & _
           "All views recomputed.", vbInformation, "Import Complete"
    Exit Sub

ErrHandler:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False
    If fileNum > 0 Then Close #fileNum
    MsgBox "Error in RefreshDataFromCSV: " & Err.Description & " (Error " & Err.Number & ")", vbCritical
End Sub

Private Function ParseCSVLine(ByVal line As String) As String()
    Dim result() As String
    Dim inQuotes As Boolean
    Dim current As String
    Dim fieldCount As Long
    Dim i As Long
    Dim c As String
    fieldCount = 0
    ReDim result(0 To 0)
    inQuotes = False
    current = ""
    For i = 1 To Len(line)
        c = Mid(line, i, 1)
        If c = """" Then
            inQuotes = Not inQuotes
        ElseIf c = "," And Not inQuotes Then
            ReDim Preserve result(0 To fieldCount)
            result(fieldCount) = current
            fieldCount = fieldCount + 1
            current = ""
        Else
            current = current & c
        End If
    Next i
    ReDim Preserve result(0 To fieldCount)
    result(fieldCount) = current
    ParseCSVLine = result
End Function

Public Sub RecomputeAllViews()
    On Error GoTo ErrHandler
    Dim wsData As Worksheet
    Set wsData = Sheets("_data")

    ' Dynamic range detection
    Dim lastRow As Long, lastCol As Long
    lastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    lastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column

    If lastRow < 2 Then
        MsgBox "No data found in _data sheet.", vbExclamation
        Exit Sub
    End If

    ' Find column indices by header name (row 1)
    Dim colProduct As Long, colBucket As Long, colCreditLii As Long
    Dim colNewCamp As Long, colWmlcQual As Long
    Dim c As Long

    colProduct = 0: colBucket = 0: colCreditLii = 0: colNewCamp = 0: colWmlcQual = 0

    For c = 1 To lastCol
        Select Case LCase(Trim(CStr(wsData.Cells(1, c).Value)))
            Case "product_bucket": colProduct = c
            Case "credit_lii_commitment_bucket": colBucket = c
            Case "credit_lii": colCreditLii = c
            Case "new_camp_yn": colNewCamp = c
            Case "wmlc_qualified": colWmlcQual = c
        End Select
    Next c

    ' --- Report missing columns and handle gracefully ---
    Dim missingCols As String
    missingCols = ""

    ' Only hard-fail if PRODUCT_BUCKET or CREDIT_LII are missing
    If colProduct = 0 Then missingCols = missingCols & "PRODUCT_BUCKET, "
    If colCreditLii = 0 Then missingCols = missingCols & "CREDIT_LII, "
    If colProduct = 0 Or colCreditLii = 0 Then
        MsgBox "Cannot find required columns in _data headers:" & vbCrLf & _
               "PRODUCT_BUCKET col=" & colProduct & vbCrLf & _
               "CREDIT_LII col=" & colCreditLii & vbCrLf & vbCrLf & _
               "Ensure you loaded the tagged CSV from the ETL pipeline.", vbCritical
        Exit Sub
    End If

    ' --- Compute CREDIT_LII_COMMITMENT_BUCKET if missing ---
    If colBucket = 0 Then
        Application.StatusBar = "Computing commitment buckets from CREDIT_LII..."

        Dim bucketCol As Long
        bucketCol = lastCol + 1
        wsData.Cells(1, bucketCol).Value = "CREDIT_LII_COMMITMENT_BUCKET"

        Dim thresholds As Variant
        thresholds = Array( _
            1000000000#, 750000000#, 700000000#, 600000000#, 500000000#, _
            400000000#, 350000000#, 300000000#, 250000000#, 200000000#, _
            175000000#, 150000000#, 125000000#, 100000000#, 75000000#, _
            50000000#, 40000000#, 35000000#, 30000000#, 25000000#, _
            20000000#, 15000000#, 10000001#, 1#)
        Dim bucketLabels As Variant
        bucketLabels = Array( _
            "$1,000,000,000", "$750,000,000", "$700,000,000", "$600,000,000", "$500,000,000", _
            "$400,000,000", "$350,000,000", "$300,000,000", "$250,000,000", "$200,000,000", _
            "$175,000,000", "$150,000,000", "$125,000,000", "$100,000,000", "$75,000,000", _
            "$50,000,000", "$40,000,000", "$35,000,000", "$30,000,000", "$25,000,000", _
            "$20,000,000", "$15,000,000", "$10,000,001", "$1")

        Dim dr As Long
        For dr = 2 To lastRow
            Dim liiVal As Double
            liiVal = 0
            On Error Resume Next
            liiVal = CDbl(wsData.Cells(dr, colCreditLii).Value)
            On Error GoTo ErrHandler

            Dim assignedBucket As String
            assignedBucket = "$1"
            Dim t As Long
            For t = LBound(thresholds) To UBound(thresholds)
                If liiVal >= thresholds(t) Then
                    assignedBucket = bucketLabels(t)
                    Exit For
                End If
            Next t
            wsData.Cells(dr, bucketCol).Value = assignedBucket
        Next dr

        colBucket = bucketCol
        lastCol = bucketCol
        missingCols = missingCols & "CREDIT_LII_COMMITMENT_BUCKET (computed from CREDIT_LII), "
    End If

    If colNewCamp = 0 Then missingCols = missingCols & "NEW_CAMP_YN (NEW views will show all loans), "
    If colWmlcQual = 0 Then missingCols = missingCols & "WMLC_QUALIFIED (WMLC filter disabled), "

    If missingCols <> "" Then
        MsgBox "Note: Some columns are missing and will be computed or disabled:" & vbCrLf & _
               missingCols & vbCrLf & vbCrLf & _
               "For full functionality, load the tagged CSV from corp_etl/main.py output.", vbExclamation
    End If

    ' Read all data into array for speed (re-read if we added a column)
    Dim dataArr As Variant
    dataArr = wsData.Range(wsData.Cells(2, 1), wsData.Cells(lastRow, lastCol)).Value
    Dim nRows As Long
    nRows = UBound(dataArr, 1)

    ' Hardcoded canonical lists — single source of truth, no Dashboard cell reads
    ' Product buckets in Dashboard column order (C through O)
    Dim products(1 To 13) As String
    products(1) = "LAL Diversified": products(2) = "LAL Highly Conc.": products(3) = "LAL NFPs"
    products(4) = "TL SBL Diversified": products(5) = "TL SBL Highly Conc.": products(6) = "TL Life Insurance"
    products(7) = "TL CRE": products(8) = "TL Unsecured": products(9) = "TL Aircraft"
    products(10) = "TL PHA": products(11) = "TL Other Secured": products(12) = "TL Multicollateral"
    products(13) = "RESI"

    ' Commitment bucket labels in descending order (rows 7-30)
    Dim buckets(1 To 24) As String
    buckets(1) = "$1,000,000,000": buckets(2) = "$750,000,000": buckets(3) = "$700,000,000"
    buckets(4) = "$600,000,000": buckets(5) = "$500,000,000": buckets(6) = "$400,000,000"
    buckets(7) = "$350,000,000": buckets(8) = "$300,000,000": buckets(9) = "$250,000,000"
    buckets(10) = "$200,000,000": buckets(11) = "$175,000,000": buckets(12) = "$150,000,000"
    buckets(13) = "$125,000,000": buckets(14) = "$100,000,000": buckets(15) = "$75,000,000"
    buckets(16) = "$50,000,000": buckets(17) = "$40,000,000": buckets(18) = "$35,000,000"
    buckets(19) = "$30,000,000": buckets(20) = "$25,000,000": buckets(21) = "$20,000,000"
    buckets(22) = "$15,000,000": buckets(23) = "$10,000,001": buckets(24) = "$1"

    Dim p As Long
    Dim b As Long

    ' Compute all 8 views
    Dim viewNum As Long
    For viewNum = 1 To 8
        Dim wsView As Worksheet
        Set wsView = Sheets("_view" & viewNum)
        wsView.Cells.Clear

        Dim isCount As Boolean
        Dim isNew As Boolean
        Dim isWmlc As Boolean

        Select Case viewNum
            Case 1: isCount = True: isNew = False: isWmlc = False
            Case 2: isCount = False: isNew = False: isWmlc = False
            Case 3: isCount = True: isNew = True: isWmlc = False
            Case 4: isCount = False: isNew = True: isWmlc = False
            Case 5: isCount = True: isNew = False: isWmlc = True
            Case 6: isCount = False: isNew = False: isWmlc = True
            Case 7: isCount = True: isNew = True: isWmlc = True
            Case 8: isCount = False: isNew = True: isWmlc = True
        End Select

        Dim viewResults(1 To 24, 1 To 13) As Double
        Dim r As Long
        ' Initialize to zero
        For b = 1 To 24
            For p = 1 To 13
                viewResults(b, p) = 0
            Next p
        Next b

        ' Single pass through data array (no cell reads in loop)
        For r = 1 To nRows
            ' Check WMLC filter — skip if WMLC view but column missing (show all)
            If isWmlc And colWmlcQual > 0 Then
                Dim wVal As String
                wVal = LCase(CStr(dataArr(r, colWmlcQual)))
                If wVal <> "true" And wVal <> "1" Then GoTo NextRow
            End If

            ' Check NEW filter — skip if NEW view but column missing (show all)
            If isNew And colNewCamp > 0 Then
                If UCase(CStr(dataArr(r, colNewCamp))) <> "Y" Then GoTo NextRow
            End If

            ' Find product index
            Dim rowProduct As String
            rowProduct = CStr(dataArr(r, colProduct))
            Dim pIdx As Long: pIdx = 0
            For p = 1 To 13
                If products(p) = rowProduct Then pIdx = p: Exit For
            Next p
            If pIdx = 0 Then GoTo NextRow

            ' Find bucket index
            Dim rowBucket As String
            rowBucket = CStr(dataArr(r, colBucket))
            Dim bIdx As Long: bIdx = 0
            For b = 1 To 24
                If buckets(b) = rowBucket Then bIdx = b: Exit For
            Next b
            If bIdx = 0 Then GoTo NextRow

            ' Accumulate
            If isCount Then
                viewResults(bIdx, pIdx) = viewResults(bIdx, pIdx) + 1
            Else
                Dim creditVal As Variant
                creditVal = dataArr(r, colCreditLii)
                If IsNumeric(creditVal) Then
                    viewResults(bIdx, pIdx) = viewResults(bIdx, pIdx) + CDbl(creditVal)
                End If
            End If
NextRow:
        Next r

        ' Write results to view sheet (single array write)
        wsView.Range(wsView.Cells(1, 1), wsView.Cells(24, 13)).Value = viewResults
    Next viewNum

    Exit Sub
ErrHandler:
    MsgBox "Error in RecomputeAllViews: " & Err.Description & " (Error " & Err.Number & ")", vbCritical
End Sub

Public Sub RefreshLoanDetail()
    On Error GoTo ErrHandler
    Dim wsData As Worksheet, wsDetail As Worksheet
    Set wsData = Sheets("_data")
    Set wsDetail = Sheets("loan_detail")

    Dim dataLastRow As Long, dataLastCol As Long
    dataLastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    dataLastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column

    If dataLastRow < 2 Then Exit Sub

    ' Turn off autofilter before clearing
    If wsDetail.AutoFilterMode Then wsDetail.AutoFilterMode = False

    ' Clear from row 3 downward (preserve rows 1-2 title/buttons)
    Dim detailLastRow As Long
    detailLastRow = wsDetail.Cells(wsDetail.Rows.Count, 1).End(xlUp).Row
    If detailLastRow >= 3 Then
        wsDetail.Range(wsDetail.Cells(3, 1), wsDetail.Cells(detailLastRow, wsDetail.Columns.Count)).Clear
    End If

    ' Copy headers to row 3
    Dim col As Long
    For col = 1 To dataLastCol
        wsDetail.Cells(3, col).Value = wsData.Cells(1, col).Value
    Next col

    ' Copy data from row 4 onward (single array write for speed)
    If dataLastRow >= 2 Then
        Dim srcRange As Range
        Set srcRange = wsData.Range(wsData.Cells(2, 1), wsData.Cells(dataLastRow, dataLastCol))
        Dim destRange As Range
        Set destRange = wsDetail.Range(wsDetail.Cells(4, 1), wsDetail.Cells(dataLastRow + 2, dataLastCol))
        destRange.Value = srcRange.Value
    End If

    ' Re-enable autofilter on the new data range
    Dim filterRange As Range
    Set filterRange = wsDetail.Range(wsDetail.Cells(3, 1), wsDetail.Cells(dataLastRow + 2, dataLastCol))
    filterRange.AutoFilter

    Exit Sub
ErrHandler:
    MsgBox "Error in RefreshLoanDetail: " & Err.Description & " (Error " & Err.Number & ")", vbCritical
End Sub

Public Sub RefreshSummary()
    On Error GoTo ErrHandler
    Dim wsSummary As Worksheet
    Set wsSummary = Nothing
    On Error Resume Next
    Set wsSummary = Sheets("Summary")
    On Error GoTo ErrHandler
    If wsSummary Is Nothing Then Exit Sub  ' Summary sheet doesn't exist — skip silently

    ' Dynamically find the 4 subheader rows by scanning column A
    Dim subheaderRows(1 To 4) As Long
    Dim viewSheets(1 To 4) As String
    viewSheets(1) = "_view1": viewSheets(2) = "_view2"
    viewSheets(3) = "_view3": viewSheets(4) = "_view4"

    Dim scanRow As Long
    Dim foundCount As Long
    foundCount = 0
    For scanRow = 1 To 120
        Dim cellVal As String
        cellVal = LCase(Trim(CStr(wsSummary.Cells(scanRow, 1).Value)))
        If cellVal = "" Then GoTo NextScan

        ' Match order matters: check "new" variants before non-new
        If InStr(cellVal, "count") > 0 And InStr(cellVal, "new") > 0 Then
            subheaderRows(3) = scanRow
            foundCount = foundCount + 1
        ElseIf InStr(cellVal, "commitment") > 0 And InStr(cellVal, "new") > 0 Then
            subheaderRows(4) = scanRow
            foundCount = foundCount + 1
        ElseIf InStr(cellVal, "summary count") > 0 And InStr(cellVal, "commitment") = 0 Then
            subheaderRows(1) = scanRow
            foundCount = foundCount + 1
        ElseIf InStr(cellVal, "summary commitment") > 0 Then
            subheaderRows(2) = scanRow
            foundCount = foundCount + 1
        End If
NextScan:
    Next scanRow

    If foundCount < 4 Then Exit Sub  ' Layout not recognized — don't crash

    ' Update each sub-matrix from its corresponding _view sheet
    Dim v As Long
    For v = 1 To 4
        Dim dataStartRow As Long
        dataStartRow = subheaderRows(v) + 2  ' skip subheader + column header row

        ' Read 24x13 from view sheet
        Dim sourceData As Variant
        sourceData = Sheets(viewSheets(v)).Range("A1:M24").Value

        ' Write to columns C-O (3-15)
        wsSummary.Range(wsSummary.Cells(dataStartRow, 3), _
                        wsSummary.Cells(dataStartRow + 23, 15)).Value = sourceData

        ' Compute row totals (column P)
        Dim sr As Long, sc As Long
        For sr = dataStartRow To dataStartRow + 23
            Dim rowSum As Double
            rowSum = 0
            For sc = 3 To 15
                If IsNumeric(wsSummary.Cells(sr, sc).Value) Then
                    rowSum = rowSum + wsSummary.Cells(sr, sc).Value
                End If
            Next sc
            wsSummary.Cells(sr, 16).Value = rowSum
        Next sr

        ' Compute column totals (total row)
        Dim totalRow As Long
        totalRow = dataStartRow + 24
        For sc = 3 To 16
            Dim colSum As Double
            colSum = 0
            For sr = dataStartRow To dataStartRow + 23
                If IsNumeric(wsSummary.Cells(sr, sc).Value) Then
                    colSum = colSum + wsSummary.Cells(sr, sc).Value
                End If
            Next sc
            wsSummary.Cells(totalRow, sc).Value = colSum
        Next sc

        ' Apply zero-as-dash format
        Dim fmt As String
        If v = 1 Or v = 3 Then
            fmt = "#,##0;-#,##0;""-"""
        Else
            fmt = "$#,##0;-$#,##0;""-"""
        End If
        wsSummary.Range(wsSummary.Cells(dataStartRow, 3), _
                        wsSummary.Cells(totalRow, 16)).NumberFormat = fmt
    Next v

    Exit Sub
ErrHandler:
    MsgBox "Error in RefreshSummary: " & Err.Description & " (Error " & Err.Number & ")", vbCritical
End Sub
