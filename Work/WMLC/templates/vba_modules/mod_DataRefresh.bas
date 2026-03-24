Attribute VB_Name = "mod_DataRefresh"
Option Explicit

' ============================================================
' MasterRefresh: SINGLE-PASS data loop for ALL computations.
' ============================================================

Private Function GetSheet(sheetName As String) As Worksheet
    On Error Resume Next
    Set GetSheet = ThisWorkbook.Sheets(sheetName)
    On Error GoTo 0
End Function

Private Function PreFlightCheck() As String
    Dim issues As String: issues = ""
    Dim requiredSheets As Variant
    requiredSheets = Array("Dashboard", "Loan Detail", "_config", "_chart_data", _
                          "_view1", "_view2", "_view3", "_view4", _
                          "_view5", "_view6", "_view7", "_view8")
    Dim i As Long
    For i = LBound(requiredSheets) To UBound(requiredSheets)
        If GetSheet(CStr(requiredSheets(i))) Is Nothing Then
            issues = issues & "Missing sheet: " & requiredSheets(i) & vbCrLf
        End If
    Next i
    Dim wsCfg As Worksheet: Set wsCfg = GetSheet("_config")
    If Not wsCfg Is Nothing Then
        If Not IsNumeric(wsCfg.Range("A1").Value) Then issues = issues & "_config!A1 not numeric" & vbCrLf
        If Not IsNumeric(wsCfg.Range("A2").Value) Then issues = issues & "_config!A2 not numeric" & vbCrLf
    End If
    Dim wsLD As Worksheet: Set wsLD = GetSheet("Loan Detail")
    If Not wsLD Is Nothing Then
        If wsLD.Cells(wsLD.Rows.Count, 1).End(xlUp).Row < 2 Then
            issues = issues & "Loan Detail is empty" & vbCrLf
        End If
    End If
    PreFlightCheck = issues
End Function

Public Sub MasterRefresh(Optional reformat As Boolean = False)
    Dim debugSection As String
    debugSection = "Pre-flight check"

    Dim preflight As String
    preflight = PreFlightCheck()
    If preflight <> "" Then
        MsgBox "Cannot refresh:" & vbCrLf & vbCrLf & preflight, vbCritical, "Pre-Flight Failed"
        Exit Sub
    End If

    On Error GoTo ErrHandler
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    Application.EnableEvents = False

    debugSection = "Loading data"
    Dim wsData As Worksheet: Set wsData = GetSheet("Loan Detail")
    Dim viewIdx As Long, wmlcIdx As Long
    viewIdx = Sheets("_config").Range("A1").Value
    wmlcIdx = Sheets("_config").Range("A2").Value

    Dim lastRow As Long, lastCol As Long
    lastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    lastCol = wsData.Cells(1, wsData.Columns.Count).End(xlToLeft).Column

    Application.StatusBar = "Loading " & (lastRow - 1) & " rows..."
    Dim dataArr As Variant
    dataArr = wsData.Range(wsData.Cells(1, 1), wsData.Cells(lastRow, lastCol)).Value

    debugSection = "Finding columns"
    Dim colProduct As Long, colCreditLii As Long, colNewCamp As Long
    Dim colWmlcQual As Long, colWmlcFlags As Long, colBorrower As Long
    Dim colBucket As Long
    Dim c As Long
    colProduct = 0: colCreditLii = 0: colNewCamp = 0
    colWmlcQual = 0: colWmlcFlags = 0: colBorrower = 0: colBucket = 0

    For c = 1 To lastCol
        Select Case LCase(Trim(CStr(dataArr(1, c))))
            Case "product_bucket": colProduct = c
            Case "credit_lii": colCreditLii = c
            Case "new_camp_yn": colNewCamp = c
            Case "wmlc_qualified": colWmlcQual = c
            Case "wmlc_flags": colWmlcFlags = c
            Case "borrower": colBorrower = c
            Case "credit_lii_commitment_bucket": colBucket = c
        End Select
    Next c

    If colProduct = 0 Or colCreditLii = 0 Then
        MsgBox "Cannot find PRODUCT_BUCKET or CREDIT_LII in headers.", vbCritical
        GoTo Cleanup
    End If

    debugSection = "Building lookups"
    Dim wsDash As Worksheet: Set wsDash = GetSheet("Dashboard")

    Dim productNames(1 To 13) As String
    Dim productMap As Object: Set productMap = CreateObject("Scripting.Dictionary")
    Dim p As Long
    For p = 1 To 13
        productNames(p) = Trim(CStr(wsDash.Cells(6, p + 2).Value))
        If productNames(p) <> "" Then productMap(productNames(p)) = p
    Next p

    Dim bucketLabels(1 To 24) As String
    Dim bucketMap As Object: Set bucketMap = CreateObject("Scripting.Dictionary")
    bucketLabels(1) = "$1,000,000,000": bucketLabels(2) = "$750,000,000"
    bucketLabels(3) = "$700,000,000": bucketLabels(4) = "$600,000,000"
    bucketLabels(5) = "$500,000,000": bucketLabels(6) = "$400,000,000"
    bucketLabels(7) = "$350,000,000": bucketLabels(8) = "$300,000,000"
    bucketLabels(9) = "$250,000,000": bucketLabels(10) = "$200,000,000"
    bucketLabels(11) = "$175,000,000": bucketLabels(12) = "$150,000,000"
    bucketLabels(13) = "$125,000,000": bucketLabels(14) = "$100,000,000"
    bucketLabels(15) = "$75,000,000": bucketLabels(16) = "$50,000,000"
    bucketLabels(17) = "$40,000,000": bucketLabels(18) = "$35,000,000"
    bucketLabels(19) = "$30,000,000": bucketLabels(20) = "$25,000,000"
    bucketLabels(21) = "$20,000,000": bucketLabels(22) = "$15,000,000"
    bucketLabels(23) = "$10,000,001": bucketLabels(24) = "$1"
    Dim b As Long
    For b = 1 To 24: bucketMap(bucketLabels(b)) = b: Next b

    Dim threshDollars As Object: Set threshDollars = CreateObject("Scripting.Dictionary")
    threshDollars("LAL Diversified") = 300000000#
    threshDollars("LAL Highly Conc.") = 100000000#
    threshDollars("LAL NFPs") = 50000000#
    threshDollars("TL SBL Diversified") = 300000000#
    threshDollars("TL SBL Highly Conc.") = 100000000#
    threshDollars("TL Life Insurance") = 100000000#
    threshDollars("TL CRE") = 75000000#
    threshDollars("TL Unsecured") = 35000000#
    threshDollars("TL Aircraft") = 50000000#
    threshDollars("TL PHA") = 35000000#
    threshDollars("TL Other Secured") = 50000000#
    threshDollars("TL Multicollateral") = 50000000#
    threshDollars("RESI") = 10000000#

    Dim lalProducts As Object: Set lalProducts = CreateObject("Scripting.Dictionary")
    lalProducts("LAL Diversified") = True
    lalProducts("LAL Highly Conc.") = True
    lalProducts("LAL NFPs") = True
    Dim LAL_FLOOR As Double: LAL_FLOOR = 10000000

    debugSection = "Initializing accumulators"
    Dim v1(1 To 24, 1 To 13) As Double, v2(1 To 24, 1 To 13) As Double
    Dim v3(1 To 24, 1 To 13) As Double, v4(1 To 24, 1 To 13) As Double
    Dim v5(1 To 24, 1 To 13) As Double, v6(1 To 24, 1 To 13) As Double
    Dim v7(1 To 24, 1 To 13) As Double, v8(1 To 24, 1 To 13) As Double

    Dim bandBelow80(1 To 13) As Double
    Dim bandApproach(1 To 13) As Double
    Dim bandAtAbove(1 To 13) As Double

    Dim distCount As Long: distCount = 0
    Dim distMax As Long: distMax = 200
    Dim distBorrower() As String, distProduct() As String
    Dim distLii() As Double, distThresh() As Double, distPct() As Double
    ReDim distBorrower(1 To distMax): ReDim distProduct(1 To distMax)
    ReDim distLii(1 To distMax): ReDim distThresh(1 To distMax): ReDim distPct(1 To distMax)

    ' Flag overlap — collect ALL unique flags (not filtered by chart gate)
    Dim allFlagNames As Object: Set allFlagNames = CreateObject("Scripting.Dictionary")

    Dim top10Count As Long: top10Count = 0
    Dim top10Max As Long: top10Max = 50
    Dim t10Borrower() As String, t10Product() As String
    Dim t10Lii() As Double, t10Flags() As String
    ReDim t10Borrower(1 To top10Max): ReDim t10Product(1 To top10Max)
    ReDim t10Lii(1 To top10Max): ReDim t10Flags(1 To top10Max)

    debugSection = "Main data loop"
    Application.StatusBar = "Processing " & (lastRow - 1) & " loans..."

    Dim r As Long
    Dim rowProduct As String, rowBucket As String, rowBorrower As String
    Dim rowFlags As String, wqStr As String
    Dim rowLii As Double, thresh As Double, rowPct As Double
    Dim rowIsNew As Boolean, rowIsWmlc As Boolean
    Dim pIdx As Long, bIdx As Long
    Dim passesChartFilter As Boolean

    For r = 2 To UBound(dataArr, 1)
        rowProduct = Trim(CStr(dataArr(r, colProduct)))
        If rowProduct = "" Then GoTo NextMasterRow

        rowLii = 0
        If IsNumeric(dataArr(r, colCreditLii)) Then rowLii = CDbl(dataArr(r, colCreditLii))

        rowBucket = ""
        If colBucket > 0 Then rowBucket = Trim(CStr(dataArr(r, colBucket)))

        rowIsNew = False
        If colNewCamp > 0 Then rowIsNew = (UCase(CStr(dataArr(r, colNewCamp))) = "Y")

        rowIsWmlc = False
        If colWmlcQual > 0 Then
            wqStr = LCase(CStr(dataArr(r, colWmlcQual)))
            rowIsWmlc = (wqStr = "true" Or wqStr = "1")
        End If

        rowFlags = ""
        If colWmlcFlags > 0 Then
            rowFlags = CStr(dataArr(r, colWmlcFlags))
            If LCase(rowFlags) = "nan" Then rowFlags = ""
        End If

        rowBorrower = ""
        If colBorrower > 0 Then rowBorrower = CStr(dataArr(r, colBorrower))

        pIdx = 0
        If productMap.Exists(rowProduct) Then pIdx = productMap(rowProduct)
        bIdx = 0
        If bucketMap.Exists(rowBucket) Then bIdx = bucketMap(rowBucket)

        ' === VIEW GRIDS (no LAL floor) ===
        If pIdx > 0 And bIdx > 0 Then
            v1(bIdx, pIdx) = v1(bIdx, pIdx) + 1
            v2(bIdx, pIdx) = v2(bIdx, pIdx) + rowLii
            If rowIsNew Then
                v3(bIdx, pIdx) = v3(bIdx, pIdx) + 1
                v4(bIdx, pIdx) = v4(bIdx, pIdx) + rowLii
            End If
            If rowIsWmlc Then
                v5(bIdx, pIdx) = v5(bIdx, pIdx) + 1
                v6(bIdx, pIdx) = v6(bIdx, pIdx) + rowLii
            End If
            If rowIsNew And rowIsWmlc Then
                v7(bIdx, pIdx) = v7(bIdx, pIdx) + 1
                v8(bIdx, pIdx) = v8(bIdx, pIdx) + rowLii
            End If
        End If

        ' === FLAG NAMES — collect from ALL WMLC rows (BEFORE chart filter) ===
        If rowIsWmlc And rowFlags <> "" Then
            Dim tempFlags() As String
            tempFlags = Split(rowFlags, "|")
            Dim tfi As Long
            For tfi = LBound(tempFlags) To UBound(tempFlags)
                Dim tfn As String: tfn = Trim(tempFlags(tfi))
                If tfn <> "" And Not allFlagNames.Exists(tfn) Then
                    allFlagNames.Add tfn, allFlagNames.Count + 1
                End If
            Next tfi
        End If

        ' === CHART FILTER (view filters + LAL $10MM floor) ===
        passesChartFilter = True
        If (viewIdx = 3 Or viewIdx = 4) And Not rowIsNew Then passesChartFilter = False
        If wmlcIdx = 1 And Not rowIsWmlc Then passesChartFilter = False
        If lalProducts.Exists(rowProduct) And rowLii < LAL_FLOOR Then passesChartFilter = False

        If Not passesChartFilter Then GoTo NextMasterRow

        ' --- Viz 1: Threshold utilization bands ---
        If pIdx > 0 And threshDollars.Exists(rowProduct) Then
            thresh = threshDollars(rowProduct)
            If rowLii >= thresh Then
                bandAtAbove(pIdx) = bandAtAbove(pIdx) + rowLii
            ElseIf rowLii >= thresh * 0.8 Then
                bandApproach(pIdx) = bandApproach(pIdx) + rowLii
            Else
                bandBelow80(pIdx) = bandBelow80(pIdx) + rowLii
            End If
        End If

        ' --- Viz 2: Threshold distance candidates ---
        If pIdx > 0 And threshDollars.Exists(rowProduct) Then
            thresh = threshDollars(rowProduct)
            If thresh > 0 Then rowPct = rowLii / thresh Else rowPct = 0
            If rowPct >= 0.7 And rowPct <= 3# Then
                If distCount < distMax Then
                    distCount = distCount + 1
                    distBorrower(distCount) = rowBorrower
                    distProduct(distCount) = rowProduct
                    distLii(distCount) = rowLii
                    distThresh(distCount) = thresh
                    distPct(distCount) = rowPct
                Else
                    Dim worstIdx As Long: worstIdx = 1
                    Dim worstDist As Double: worstDist = Abs(distPct(1) - 1#)
                    Dim di As Long
                    For di = 2 To distMax
                        If Abs(distPct(di) - 1#) > worstDist Then
                            worstDist = Abs(distPct(di) - 1#): worstIdx = di
                        End If
                    Next di
                    If Abs(rowPct - 1#) < worstDist Then
                        distBorrower(worstIdx) = rowBorrower
                        distProduct(worstIdx) = rowProduct
                        distLii(worstIdx) = rowLii
                        distThresh(worstIdx) = thresh
                        distPct(worstIdx) = rowPct
                    End If
                End If
            End If
        End If

        ' --- Top 10 ---
        If top10Count < top10Max Then
            top10Count = top10Count + 1
            t10Borrower(top10Count) = rowBorrower
            t10Product(top10Count) = rowProduct
            t10Lii(top10Count) = rowLii
            t10Flags(top10Count) = rowFlags
        Else
            Dim minIdx As Long: minIdx = 1
            Dim minVal As Double: minVal = t10Lii(1)
            Dim mi As Long
            For mi = 2 To top10Max
                If t10Lii(mi) < minVal Then minVal = t10Lii(mi): minIdx = mi
            Next mi
            If rowLii > minVal Then
                t10Borrower(minIdx) = rowBorrower
                t10Product(minIdx) = rowProduct
                t10Lii(minIdx) = rowLii
                t10Flags(minIdx) = rowFlags
            End If
        End If

NextMasterRow:
    Next r

    ' ============================================================
    ' WRITE VIEW SHEETS
    ' ============================================================
    debugSection = "Writing view sheets"
    Application.StatusBar = "Writing views..."
    Dim wsV As Worksheet
    Dim vi As Long
    For vi = 1 To 8
        Set wsV = GetSheet("_view" & vi)
        If Not wsV Is Nothing Then
            Select Case vi
                Case 1: wsV.Range("A1:M24").Value = v1
                Case 2: wsV.Range("A1:M24").Value = v2
                Case 3: wsV.Range("A1:M24").Value = v3
                Case 4: wsV.Range("A1:M24").Value = v4
                Case 5: wsV.Range("A1:M24").Value = v5
                Case 6: wsV.Range("A1:M24").Value = v6
                Case 7: wsV.Range("A1:M24").Value = v7
                Case 8: wsV.Range("A1:M24").Value = v8
            End Select
        End If
    Next vi

    ' --- Active view to Dashboard ---
    debugSection = "Writing Dashboard matrix"
    Application.StatusBar = "Updating Dashboard..."
    Dim sheetNum As Long
    sheetNum = viewIdx + (wmlcIdx * 4)
    If sheetNum < 1 Or sheetNum > 8 Then sheetNum = 2  ' Safe default

    Set wsV = GetSheet("_view" & sheetNum)
    If wsV Is Nothing Then GoTo Cleanup
    Dim sourceData As Variant
    sourceData = wsV.Range("A1:M24").Value
    wsDash.Range("C7:O30").Value = sourceData

    Dim rowTotals(1 To 24, 1 To 1) As Double
    For b = 1 To 24
        For p = 1 To 13: rowTotals(b, 1) = rowTotals(b, 1) + sourceData(b, p): Next p
    Next b
    wsDash.Range("P7:P30").Value = rowTotals

    Dim colTotals(1 To 1, 1 To 14) As Double
    For p = 1 To 13
        For b = 1 To 24: colTotals(1, p) = colTotals(1, p) + sourceData(b, p): Next b
    Next p
    For p = 1 To 13: colTotals(1, 14) = colTotals(1, 14) + colTotals(1, p): Next p
    wsDash.Range("C31:P31").Value = colTotals

    Dim viewName As String, fmt As String
    Select Case viewIdx
        Case 1: viewName = "Summary Count": fmt = "#,##0;-#,##0;""-"""
        Case 2: viewName = "Summary Commitment": fmt = "$#,##0;-$#,##0;""-"""
        Case 3: viewName = "Summary Count NEW": fmt = "#,##0;-#,##0;""-"""
        Case 4: viewName = "Summary Commitment NEW": fmt = "$#,##0;-$#,##0;""-"""
        Case Else: viewName = "Summary Commitment": fmt = "$#,##0;-$#,##0;""-"""
    End Select
    Dim wmlcText As String
    If wmlcIdx = 1 Then wmlcText = "ON" Else wmlcText = "OFF"
    wsDash.Range("A3").Value = "As of: " & Format(Now, "mm/dd/yyyy") & " | View: " & viewName & " | WMLC: " & wmlcText
    wsDash.Range("C7:P31").NumberFormat = fmt

    ' ============================================================
    ' CHART DATA
    ' ============================================================
    debugSection = "Writing chart data"
    Dim wsChart As Worksheet: Set wsChart = GetSheet("_chart_data")
    If Not wsChart Is Nothing Then
        wsChart.Cells.ClearContents
        Dim vizData(1 To 13, 1 To 4) As Variant
        For p = 1 To 13
            vizData(p, 1) = productNames(p)
            vizData(p, 2) = bandBelow80(p)
            vizData(p, 3) = bandApproach(p)
            vizData(p, 4) = bandAtAbove(p)
        Next p
        wsChart.Range("A1:D13").Value = vizData
    End If

    ' ============================================================
    ' VIZ 2: THRESHOLD DISTANCE — bulk array
    ' ============================================================
    debugSection = "Writing distance table"
    Application.StatusBar = "Building distance table..."
    Dim si As Long, sj As Long
    Dim writeCount As Long
    writeCount = distCount
    If writeCount > 30 Then writeCount = 30

    ' Sort by distance from 100%
    For si = 1 To writeCount - 1
        Dim bestIdx As Long: bestIdx = si
        Dim bestDist2 As Double: bestDist2 = Abs(distPct(si) - 1#)
        For sj = si + 1 To distCount
            If Abs(distPct(sj) - 1#) < bestDist2 Then
                bestDist2 = Abs(distPct(sj) - 1#): bestIdx = sj
            End If
        Next sj
        If bestIdx <> si Then
            Dim tmpStr As String, tmpDbl As Double
            tmpStr = distBorrower(si): distBorrower(si) = distBorrower(bestIdx): distBorrower(bestIdx) = tmpStr
            tmpStr = distProduct(si): distProduct(si) = distProduct(bestIdx): distProduct(bestIdx) = tmpStr
            tmpDbl = distLii(si): distLii(si) = distLii(bestIdx): distLii(bestIdx) = tmpDbl
            tmpDbl = distThresh(si): distThresh(si) = distThresh(bestIdx): distThresh(bestIdx) = tmpDbl
            tmpDbl = distPct(si): distPct(si) = distPct(bestIdx): distPct(bestIdx) = tmpDbl
        End If
    Next si

    wsDash.Range("A54:G83").ClearContents
    If writeCount > 0 Then
        Dim distOutput() As Variant
        ReDim distOutput(1 To writeCount, 1 To 7)
        For di = 1 To writeCount
            distOutput(di, 1) = di
            distOutput(di, 2) = distBorrower(di)
            distOutput(di, 3) = distProduct(di)
            distOutput(di, 4) = distLii(di)
            distOutput(di, 5) = distThresh(di)
            distOutput(di, 6) = distLii(di) - distThresh(di)
            distOutput(di, 7) = distPct(di)
        Next di
        wsDash.Range(wsDash.Cells(54, 1), wsDash.Cells(53 + writeCount, 7)).Value = distOutput
    End If
    wsDash.Range("D54:E83").NumberFormat = "$#,##0"
    wsDash.Range("F54:F83").NumberFormat = "$#,##0"
    wsDash.Range("G54:G83").NumberFormat = "0.0%"

    ' ============================================================
    ' VIZ 3: FLAG OVERLAP HEATMAP
    ' ============================================================
    debugSection = "Writing heatmap"
    Application.StatusBar = "Building heatmap..."
    Dim numFlags As Long
    numFlags = allFlagNames.Count
    If numFlags > 16 Then numFlags = 16

    wsDash.Range("A86:Q102").ClearContents

    If numFlags > 0 Then
        Dim flagNameArr() As String
        ReDim flagNameArr(1 To numFlags)
        Dim fk As Variant, fIdx As Long: fIdx = 1
        For Each fk In allFlagNames.Keys
            If fIdx <= numFlags Then flagNameArr(fIdx) = CStr(fk): fIdx = fIdx + 1
        Next fk

        ' Build overlap matrix — scan WMLC rows only
        Dim overlapMatrix() As Long
        ReDim overlapMatrix(1 To numFlags, 1 To numFlags)
        Dim r2 As Long
        For r2 = 2 To UBound(dataArr, 1)
            If colWmlcFlags = 0 Then Exit For
            Dim rf As String: rf = CStr(dataArr(r2, colWmlcFlags))
            If LCase(rf) = "nan" Or rf = "" Then GoTo NextHeatRow
            Dim hFlags() As String: hFlags = Split(rf, "|")
            Dim hfi As Long, hfj As Long
            Dim hIdxArr() As Long, hCount As Long: hCount = 0
            ReDim hIdxArr(1 To UBound(hFlags) - LBound(hFlags) + 1)
            For hfi = LBound(hFlags) To UBound(hFlags)
                Dim hfn As String: hfn = Trim(hFlags(hfi))
                If hfn <> "" And allFlagNames.Exists(hfn) Then
                    Dim flagNum As Long: flagNum = allFlagNames(hfn)
                    If flagNum >= 1 And flagNum <= numFlags Then
                        hCount = hCount + 1: hIdxArr(hCount) = flagNum
                    End If
                End If
            Next hfi
            For hfi = 1 To hCount
                For hfj = 1 To hCount
                    overlapMatrix(hIdxArr(hfi), hIdxArr(hfj)) = overlapMatrix(hIdxArr(hfi), hIdxArr(hfj)) + 1
                Next hfj
            Next hfi
NextHeatRow:
        Next r2

        ' Write column headers
        For fIdx = 1 To numFlags
            wsDash.Cells(86, fIdx + 1).Value = Left(flagNameArr(fIdx), 15)
        Next fIdx
        wsDash.Range(wsDash.Cells(86, 2), wsDash.Cells(86, numFlags + 1)).Font.Size = 7
        wsDash.Range(wsDash.Cells(86, 2), wsDash.Cells(86, numFlags + 1)).Font.Bold = True
        wsDash.Range(wsDash.Cells(86, 2), wsDash.Cells(86, numFlags + 1)).Font.Color = RGB(255, 255, 255)
        wsDash.Range(wsDash.Cells(86, 2), wsDash.Cells(86, numFlags + 1)).Interior.Color = RGB(0, 83, 155)

        ' Bulk write matrix
        Dim heatArr() As Variant
        ReDim heatArr(1 To numFlags, 1 To numFlags + 1)
        For hfi = 1 To numFlags
            heatArr(hfi, 1) = Left(flagNameArr(hfi), 20)
            For hfj = 1 To numFlags
                heatArr(hfi, hfj + 1) = overlapMatrix(hfi, hfj)
            Next hfj
        Next hfi
        wsDash.Range(wsDash.Cells(87, 1), wsDash.Cells(86 + numFlags, numFlags + 1)).Value = heatArr

        Dim heatRange As Range
        Set heatRange = wsDash.Range(wsDash.Cells(87, 2), wsDash.Cells(86 + numFlags, numFlags + 1))
        heatRange.NumberFormat = "#,##0"
        heatRange.HorizontalAlignment = xlCenter
        heatRange.Font.Size = 8
    End If

    ' ============================================================
    ' TOP 10 — bulk array
    ' ============================================================
    debugSection = "Writing Top 10"
    wsDash.Range("A106:E115").ClearContents
    If top10Count > 0 Then
        Dim used() As Boolean
        ReDim used(1 To top10Count)
        Dim t10Arr(1 To 10, 1 To 5) As Variant
        Dim rank As Long, actualRanks As Long: actualRanks = 0
        For rank = 1 To 10
            If rank > top10Count Then Exit For
            Dim maxI As Long: maxI = 0
            Dim maxV As Double: maxV = -1
            For mi = 1 To top10Count
                If Not used(mi) And t10Lii(mi) > maxV Then maxV = t10Lii(mi): maxI = mi
            Next mi
            If maxI > 0 Then
                used(maxI) = True
                actualRanks = actualRanks + 1
                t10Arr(rank, 1) = rank
                t10Arr(rank, 2) = t10Borrower(maxI)
                t10Arr(rank, 3) = t10Product(maxI)
                t10Arr(rank, 4) = maxV
                t10Arr(rank, 5) = t10Flags(maxI)
            End If
        Next rank
        If actualRanks > 0 Then
            wsDash.Range(wsDash.Cells(106, 1), wsDash.Cells(105 + actualRanks, 5)).Value = t10Arr
            wsDash.Range("D106:D115").NumberFormat = "$#,##0"
        End If
    End If

    ' ============================================================
    ' SUMMARY
    ' ============================================================
    debugSection = "Writing Summary"
    Application.StatusBar = "Updating Summary..."
    Dim wsSummary As Worksheet: Set wsSummary = GetSheet("Summary")
    If Not wsSummary Is Nothing Then
        Dim subRows(1 To 4) As Long
        Dim sr As Long, foundSubs As Long: foundSubs = 0
        For sr = 1 To 120
            Dim sv As String
            sv = LCase(Trim(CStr(wsSummary.Cells(sr, 1).Value)))
            If InStr(sv, "summary count") > 0 And InStr(sv, "new") = 0 And InStr(sv, "commitment") = 0 Then
                subRows(1) = sr: foundSubs = foundSubs + 1
            ElseIf InStr(sv, "summary commitment") > 0 And InStr(sv, "new") = 0 Then
                subRows(2) = sr: foundSubs = foundSubs + 1
            ElseIf InStr(sv, "count") > 0 And InStr(sv, "new") > 0 Then
                subRows(3) = sr: foundSubs = foundSubs + 1
            ElseIf InStr(sv, "commitment") > 0 And InStr(sv, "new") > 0 Then
                subRows(4) = sr: foundSubs = foundSubs + 1
            End If
        Next sr
        If foundSubs = 4 Then
            wsSummary.Range(wsSummary.Cells(subRows(1) + 2, 3), wsSummary.Cells(subRows(1) + 25, 15)).Value = v1
            wsSummary.Range(wsSummary.Cells(subRows(2) + 2, 3), wsSummary.Cells(subRows(2) + 25, 15)).Value = v2
            wsSummary.Range(wsSummary.Cells(subRows(3) + 2, 3), wsSummary.Cells(subRows(3) + 25, 15)).Value = v3
            wsSummary.Range(wsSummary.Cells(subRows(4) + 2, 3), wsSummary.Cells(subRows(4) + 25, 15)).Value = v4
            Dim sv2 As Long
            For sv2 = 1 To 4
                Dim sDataStart As Long: sDataStart = subRows(sv2) + 2
                For b = 1 To 24
                    Dim sRowTotal As Double: sRowTotal = 0
                    For p = 1 To 13
                        If IsNumeric(wsSummary.Cells(sDataStart + b - 1, p + 2).Value) Then
                            sRowTotal = sRowTotal + CDbl(wsSummary.Cells(sDataStart + b - 1, p + 2).Value)
                        End If
                    Next p
                    wsSummary.Cells(sDataStart + b - 1, 16).Value = sRowTotal
                Next b
                Dim sTotalRow As Long: sTotalRow = sDataStart + 24
                Dim sc As Long
                For sc = 3 To 16
                    Dim sColTotal As Double: sColTotal = 0
                    For b = sDataStart To sDataStart + 23
                        If IsNumeric(wsSummary.Cells(b, sc).Value) Then sColTotal = sColTotal + CDbl(wsSummary.Cells(b, sc).Value)
                    Next b
                    wsSummary.Cells(sTotalRow, sc).Value = sColTotal
                Next sc
                Dim sFmt As String
                If sv2 = 1 Or sv2 = 3 Then sFmt = "#,##0;-#,##0;""-""" Else sFmt = "$#,##0;-$#,##0;""-"""
                wsSummary.Range(wsSummary.Cells(sDataStart, 3), wsSummary.Cells(sTotalRow, 16)).NumberFormat = sFmt
            Next sv2
        End If
    End If

    ' ============================================================
    ' FORMATTING
    ' ============================================================
    debugSection = "Applying formatting"
    Application.StatusBar = "Applying formatting..."
    If reformat Then
        ApplyThresholdFormatting
        ApplyDistanceConditionalFormatting
        If numFlags > 0 Then ApplyHeatmapConditionalFormatting numFlags
    End If
    ComputeConcentrationRatios

Cleanup:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.EnableEvents = True
    Application.StatusBar = False
    Exit Sub

ErrHandler:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.EnableEvents = True
    Application.StatusBar = False
    MsgBox "Error in MasterRefresh" & vbCrLf & _
           "Section: " & debugSection & vbCrLf & _
           "Error: " & Err.Description & " (" & Err.Number & ")", vbCritical
End Sub

' ============================================================
' Conditional formatting subs
' ============================================================

Public Sub ApplyDistanceConditionalFormatting()
    On Error Resume Next
    Dim ws As Worksheet: Set ws = Sheets("Dashboard")
    Dim rngG As Range: Set rngG = ws.Range("G54:G83")
    rngG.FormatConditions.Delete
    Dim fc1 As FormatCondition
    Set fc1 = rngG.FormatConditions.Add(Type:=xlCellValue, Operator:=xlGreaterEqual, Formula1:="1")
    fc1.Interior.Color = RGB(0, 43, 92): fc1.Font.Color = RGB(255, 255, 255): fc1.StopIfTrue = True
    Dim fc2 As FormatCondition
    Set fc2 = rngG.FormatConditions.Add(Type:=xlCellValue, Operator:=xlBetween, Formula1:="0.9", Formula2:="0.9999")
    fc2.Interior.Color = RGB(212, 160, 23): fc2.Font.Color = RGB(255, 255, 255): fc2.StopIfTrue = True
    Dim fc3 As FormatCondition
    Set fc3 = rngG.FormatConditions.Add(Type:=xlCellValue, Operator:=xlBetween, Formula1:="0.8", Formula2:="0.8999")
    fc3.Interior.Color = RGB(255, 243, 205): fc3.Font.Color = RGB(0, 0, 0): fc3.StopIfTrue = True
    Dim rngF As Range: Set rngF = ws.Range("F54:F83")
    rngF.FormatConditions.Delete
    Dim fcF1 As FormatCondition
    Set fcF1 = rngF.FormatConditions.Add(Type:=xlCellValue, Operator:=xlGreaterEqual, Formula1:="0")
    fcF1.Font.Color = RGB(0, 122, 51)
    Dim fcF2 As FormatCondition
    Set fcF2 = rngF.FormatConditions.Add(Type:=xlCellValue, Operator:=xlLess, Formula1:="0")
    fcF2.Font.Color = RGB(212, 160, 23)
    On Error GoTo 0
End Sub

Public Sub ApplyHeatmapConditionalFormatting(numFlags As Long)
    On Error Resume Next
    Dim ws As Worksheet: Set ws = Sheets("Dashboard")
    Dim rng As Range
    Set rng = ws.Range(ws.Cells(87, 2), ws.Cells(86 + numFlags, numFlags + 1))
    rng.FormatConditions.Delete
    Dim cs As ColorScale
    Set cs = rng.FormatConditions.AddColorScale(ColorScaleType:=3)
    cs.ColorScaleCriteria(1).Type = xlConditionValueLowestValue
    cs.ColorScaleCriteria(1).FormatColor.Color = RGB(255, 255, 255)
    cs.ColorScaleCriteria(2).Type = xlConditionValuePercentile
    cs.ColorScaleCriteria(2).Value = 50
    cs.ColorScaleCriteria(2).FormatColor.Color = RGB(214, 228, 240)
    cs.ColorScaleCriteria(3).Type = xlConditionValueHighestValue
    cs.ColorScaleCriteria(3).FormatColor.Color = RGB(0, 43, 92)
    On Error GoTo 0
End Sub

' ============================================================
' RefreshData / RefreshDataFromCSV
' ============================================================

Public Sub RefreshData()
    On Error GoTo ErrHandler
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    Application.StatusBar = "Refreshing..."
    Dim conn As WorkbookConnection
    Dim foundConn As Boolean: foundConn = False
    For Each conn In ThisWorkbook.Connections
        If InStr(1, conn.Name, "loan_extract", vbTextCompare) > 0 Or _
           InStr(1, conn.Name, "tbl_LoanData", vbTextCompare) > 0 Then
            conn.Refresh: foundConn = True
        End If
    Next conn
    If Not foundConn Then
        ThisWorkbook.RefreshAll
        Application.CalculateUntilAsyncQueriesDone
    End If
    MasterRefresh reformat:=True
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False
    Dim lr As Long: lr = Sheets("Loan Detail").Cells(Sheets("Loan Detail").Rows.Count, 1).End(xlUp).Row
    MsgBox "Refreshed: " & (lr - 1) & " loans.", vbInformation
    Exit Sub
ErrHandler:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False
    MsgBox "Error: " & Err.Description, vbCritical
End Sub

Public Sub RefreshDataFromCSV()
    On Error GoTo ErrHandler
    Dim csvPath As String: csvPath = Sheets("_config").Range("A3").Value
    If Len(Dir(csvPath)) = 0 Then
        MsgBox "CSV not found: " & csvPath, vbExclamation: Exit Sub
    End If
    Application.ScreenUpdating = False
    Application.Calculation = xlCalculationManual
    Dim wsData As Worksheet: Set wsData = Sheets("Loan Detail")
    Dim lo As ListObject
    For Each lo In wsData.ListObjects: lo.Unlist: Next lo
    wsData.Cells.Clear
    Dim fileNum As Integer, fileLine As String, rowNum As Long
    Dim fields() As String, colNum As Long
    fileNum = FreeFile
    Open csvPath For Input As #fileNum: rowNum = 1
    Do While Not EOF(fileNum)
        Line Input #fileNum, fileLine
        fields = ParseCSVLine(fileLine)
        For colNum = 0 To UBound(fields)
            Dim cellVal As String: cellVal = fields(colNum)
            If rowNum > 1 And IsNumeric(cellVal) And Len(cellVal) > 0 Then
                wsData.Cells(rowNum, colNum + 1).Value = CDbl(cellVal)
            Else
                wsData.Cells(rowNum, colNum + 1).Value = cellVal
            End If
        Next colNum
        rowNum = rowNum + 1
    Loop
    Close #fileNum
    MasterRefresh reformat:=True
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False
    MsgBox "Imported: " & (rowNum - 2) & " loans.", vbInformation
    Exit Sub
ErrHandler:
    Application.Calculation = xlCalculationAutomatic
    Application.ScreenUpdating = True
    Application.StatusBar = False
    If fileNum > 0 Then Close #fileNum
    MsgBox "Error: " & Err.Description, vbCritical
End Sub

Private Function ParseCSVLine(ByVal line As String) As String()
    Dim result() As String, inQuotes As Boolean, current As String
    Dim fieldCount As Long, i As Long, c As String
    fieldCount = 0: ReDim result(0 To 0): inQuotes = False: current = ""
    For i = 1 To Len(line)
        c = Mid(line, i, 1)
        If c = """" Then
            inQuotes = Not inQuotes
        ElseIf c = "," And Not inQuotes Then
            ReDim Preserve result(0 To fieldCount)
            result(fieldCount) = current: fieldCount = fieldCount + 1: current = ""
        Else
            current = current & c
        End If
    Next i
    ReDim Preserve result(0 To fieldCount)
    result(fieldCount) = current
    ParseCSVLine = result
End Function

Public Sub RecomputeAllViews(): MasterRefresh reformat:=True: End Sub
Public Sub RefreshLoanDetail(): End Sub
Public Sub RefreshSummary(): End Sub
