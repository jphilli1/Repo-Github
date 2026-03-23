Attribute VB_Name = "mod_Navigation"
Option Explicit

Public Sub BackToDashboard()
    On Error GoTo ErrHandler
    Dim ws As Worksheet
    Set ws = Sheets("loan_detail")
    If ws.AutoFilterMode Then
        If ws.FilterMode Then ws.ShowAllData
    End If
    Sheets("Dashboard").Activate
    Exit Sub
ErrHandler:
    MsgBox "Error in BackToDashboard: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub ResetFilters()
    On Error GoTo ErrHandler
    If ActiveSheet.AutoFilterMode Then
        If ActiveSheet.FilterMode Then ActiveSheet.ShowAllData
    End If
    Exit Sub
ErrHandler:
    MsgBox "Error in ResetFilters: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub

Public Sub Auto_Open()
    On Error GoTo ErrHandler
    ' Run diagnostic tests on workbook open
    RunAllTests
    Exit Sub
ErrHandler:
    MsgBox "Error in Auto_Open: " & Err.Description & " (Error " & Err.Number & ")", vbExclamation
End Sub
