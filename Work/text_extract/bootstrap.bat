@echo off
REM AI-Native Split-RAG System v2.0 - Bootstrap Script
REM Sets environment variables and validates runtime requirements

REM Limit CPU usage for Docling/Torch to prevent system freeze
set OMP_NUM_THREADS=4

REM Check for Python 3.11.x
python --version 2>&1 | findstr "3.11" >nul
if %errorlevel% neq 0 (
    echo [CRITICAL] Python 3.11.x is REQUIRED. Found:
    python --version
    echo Please install Python 3.11 and ensure it is in your PATH.
    pause
    exit /b 1
)

REM Activate Virtual Environment if it exists
if exist ".venv\Scripts\activate.bat" (
    echo [INFO] Activating virtual environment...
    call .venv\Scripts\activate.bat
) else (
    echo [WARN] No .venv found. Running with system Python.
    echo Recommended: python -m venv .venv
)

REM Check if extractor exists before running
if exist "extractor.py" (
    echo [INFO] Starting Tier 1 Extraction Engine...
    python extractor.py
) else (
    echo [INFO] Environment ready. 'extractor.py' not found (Phase 2 pending).
)

pause