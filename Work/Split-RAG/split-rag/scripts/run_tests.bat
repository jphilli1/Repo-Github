@echo off
REM AI-Native Split-RAG System v2.0 - Windows Test Runner

echo [INFO] Installing test dependencies...
pip install pytest pytest-cov

echo [INFO] Running Test Suite...
pytest tests/ -v --cov=. --cov-report=term-missing

if %errorlevel% neq 0 (
    echo [FAIL] Tests failed.
    exit /b 1
) else (
    echo [PASS] All tests passed.
)