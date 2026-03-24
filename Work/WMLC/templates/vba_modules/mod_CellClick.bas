Attribute VB_Name = "mod_CellClick"
' ============================================================
' IMPORTANT: This code goes in the Dashboard SHEET MODULE,
' NOT in a standard module. In the VBA editor:
'   1. Double-click "Sheet2 (Dashboard)" in the Project Explorer
'   2. Paste this code there
' ============================================================

Private Sub Worksheet_SelectionChange(ByVal Target As Range)
    On Error GoTo ErrHandler

    ' Only respond to single-cell clicks
    If Target.Cells.Count > 1 Then Exit Sub

    ' Dynamic data area detection
    Dim lastDataRow As Long, lastDataCol As Long
    Dim r As Long

    lastDataRow = 0
    For r = 7 To 100
        If LCase(Trim(CStr(Me.Cells(r, 1).Value))) = "total" Then
            lastDataRow = r - 1
            Exit For
        End If
        If IsEmpty(Me.Cells(r, 1).Value) Then
            lastDataRow = r - 1
            Exit For
        End If
    Next r
    If lastDataRow < 7 Then Exit Sub

    lastDataCol = 0
    Dim c As Long
    For c = 3 To Me.Cells(6, Me.Columns.Count).End(xlToLeft).Column
        If LCase(Trim(CStr(Me.Cells(6, c).Value))) = "total" Then
            lastDataCol = c - 1
            Exit For
        End If
    Next c
    If lastDataCol < 3 Then Exit Sub

    ' Check if click is in data area
    If Target.Row < 7 Or Target.Row > lastDataRow Then Exit Sub
    If Target.Column < 3 Or Target.Column > lastDataCol Then Exit Sub

    Dim bucketLabel As String
    Dim productBucket As String
    Dim viewIdx As Long
    Dim wmlcIdx As Long

    bucketLabel = Me.Cells(Target.Row, 1).Value
    productBucket = Me.Cells(6, Target.Column).Value
    viewIdx = Sheets("_config").Range("A1").Value
    wmlcIdx = Sheets("_config").Range("A2").Value

    ' Switch to "Loan Detail" (the data sheet) and apply filters directly
    Dim ws As Worksheet
    Set ws = Sheets("Loan Detail")
    ws.Activate

    ' Ensure AutoFilter is on row 1 (header row of _data)
    If Not ws.AutoFilterMode Then
        ws.Range("A1").AutoFilter
    End If

    ' Clear existing filters
    If ws.FilterMode Then ws.ShowAllData

    ' Find column positions from row 1 headers
    Dim colBucket As Long, colProduct As Long, colWMLC As Long, colNew As Long
    Dim lastCol As Long
    colBucket = 0: colProduct = 0: colWMLC = 0: colNew = 0
    lastCol = ws.Cells(1, ws.Columns.Count).End(xlToLeft).Column

    Dim i As Long
    For i = 1 To lastCol
        Select Case LCase(Trim(ws.Cells(1, i).Value))
            Case "credit_lii_commitment_bucket": colBucket = i
            Case "product_bucket": colProduct = i
            Case "wmlc_qualified": colWMLC = i
            Case "new_camp_yn": colNew = i
        End Select
    Next i

    ' Apply filters
    If colProduct > 0 Then
        ws.Range("A1").AutoFilter Field:=colProduct, Criteria1:=productBucket
    End If
    If colBucket > 0 Then
        ws.Range("A1").AutoFilter Field:=colBucket, Criteria1:=bucketLabel
    End If
    If wmlcIdx = 1 And colWMLC > 0 Then
        ws.Range("A1").AutoFilter Field:=colWMLC, Criteria1:="True"
    End If
    If (viewIdx = 3 Or viewIdx = 4) And colNew > 0 Then
        ws.Range("A1").AutoFilter Field:=colNew, Criteria1:="Y"
    End If

    Exit Sub
ErrHandler:
    MsgBox "Error in Worksheet_SelectionChange: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub
