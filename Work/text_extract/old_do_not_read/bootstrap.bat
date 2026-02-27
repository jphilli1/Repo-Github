@echo off
REM [EXACT] — Copy this file verbatim
REM ============================================================================
REM Split-RAG Document Extractor - Bootstrap Script
REM Version: 1.0.0
REM ============================================================================
setlocal EnableDelayedExpansion
echo ============================================================================
echo Split-RAG Document Extractor - Environment Setup
echo ============================================================================
echo.
REM Get script directory (where bootstrap.bat lives)
set "SCRIPT_DIR=%~dp0"
set "SCRIPT_DIR=%SCRIPT_DIR:~0,-1%"
echo Working Directory: %SCRIPT_DIR%
echo.
REM ============================================================================
REM Step 1: Create directory structure
REM ============================================================================
echo [Step 1/4] Creating directory structure...
if not exist "%SCRIPT_DIR%\input" (
    mkdir "%SCRIPT_DIR%\input"
    echo   Created: input\
) else (
    echo   Exists:  input\
)
if not exist "%SCRIPT_DIR%\output" (
    mkdir "%SCRIPT_DIR%\output"
    echo   Created: output\
) else (
    echo   Exists:  output\
)
if not exist "%SCRIPT_DIR%\logs" (
    mkdir "%SCRIPT_DIR%\logs"
    echo   Created: logs\
) else (
    echo   Exists:  logs\
)
echo.
REM ============================================================================
REM Step 2: Check Python version
REM ============================================================================
echo [Step 2/4] Checking Python environment...
python --version 2>nul
if errorlevel 1 (
    echo ERROR: Python not found in PATH.
    echo Please ensure Python 3.11+ is installed and in PATH.
    pause
    exit /b 1
)
echo.
REM ============================================================================
REM Step 3: Audit library versions
REM ============================================================================
echo [Step 3/4] Auditing library versions...
echo.
echo Library Versions (for audit trail):
echo -----------------------------------
python -c "import sys; print(f'Python: {sys.version}')"
python -c "import pandas; print(f'pandas: {pandas.__version__}')" 2>nul || echo pandas: NOT INSTALLED
python -c "import docling; print(f'docling: {docling.__version__}')" 2>nul || echo docling: NOT INSTALLED
python -c "import pdfplumber; print(f'pdfplumber: {pdfplumber.__version__}')" 2>nul || echo pdfplumber: NOT INSTALLED
python -c "import pydantic; print(f'pydantic: {pydantic.__version__}')" 2>nul || echo pydantic: NOT INSTALLED
python -c "import openpyxl; print(f'openpyxl: {openpyxl.__version__}')" 2>nul || echo openpyxl: NOT INSTALLED
python -c "import lxml; print(f'lxml: {lxml.__version__}')" 2>nul || echo lxml: NOT INSTALLED
python -c "import PIL; print(f'PIL (Pillow): {PIL.__version__}')" 2>nul || echo PIL: NOT INSTALLED
echo.
echo -----------------------------------
REM Check for missing critical libraries
python -c "import docling" 2>nul
if errorlevel 1 (
    echo.
    echo WARNING: docling not installed. Install with:
    echo   pip install docling
    echo.
    set "MISSING_LIBS=1"
)
python -c "import pydantic" 2>nul
if errorlevel 1 (
    echo WARNING: pydantic not installed. Install with:
    echo   pip install pydantic
    echo.
    set "MISSING_LIBS=1"
)
if defined MISSING_LIBS (
    echo.
    echo Some libraries are missing. Install all dependencies with:
    echo   pip install -r "%SCRIPT_DIR%\requirements.txt"
    echo.
    choice /C YN /M "Attempt to install missing libraries now?"
    if errorlevel 2 goto :skip_install
    if errorlevel 1 (
        echo.
        echo Installing dependencies...
        pip install -r "%SCRIPT_DIR%\requirements.txt"
        echo.
    )
)
:skip_install
echo.
REM ============================================================================
REM Step 4: Run extractor
REM ============================================================================
echo [Step 4/4] Launching extractor...
echo.
python "%SCRIPT_DIR%\extractor.py" --working_dir "%SCRIPT_DIR%"
echo.
echo ============================================================================
echo Extraction complete. Check output\ for results.
echo ============================================================================
echo.
pause
