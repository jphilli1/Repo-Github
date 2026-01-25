#!/bin/bash
# AI-Native Split-RAG System v2.0 - Linux/Mac Test Runner

echo "[INFO] Installing test dependencies..."
pip install pytest pytest-cov

echo "[INFO] Running Test Suite..."
pytest tests/ -v --cov=. --cov-report=term-missing

if [ $? -ne 0 ]; then
    echo "[FAIL] Tests failed."
    exit 1
else
    echo "[PASS] All tests passed."
    exit 0
fi