# AI-Native Split-RAG System v2.0 - T-003 Validation
# Verifies Data Mart Export functionality

import pytest
import copilot_tier2 as tier2
import pandas as pd
from io import StringIO


def test_data_mart_basic_export(context_graph_json):
    """Test basic export generation functionality."""
    result = tier2.generate_data_mart(context_graph_json, output_format="csv")

    # Should contain success message and CSV data
    assert "SUCCESS" in result
    assert "chunk_id,page_number,content_type,content" in result
    assert "5.0%" in result  # Table content


def test_data_mart_filtering(context_graph_json):
    """Test that content type filtering works."""
    # Ask ONLY for tables
    result = tier2.generate_data_mart(context_graph_json, content_types=["table"], output_format="csv")

    assert "5.0%" in result  # Table content exists
    assert "The Borrower means" not in result  # Text content should be filtered out


def test_data_mart_invalid_input():
    """Test graceful handling of bad JSON."""
    result = tier2.generate_data_mart("{invalid_json", output_format="csv")
    assert "System Error" in result or "empty" in result.lower()


def test_data_mart_no_matches(context_graph_json):
    """Test response when no content matches filter."""
    result = tier2.generate_data_mart(context_graph_json, content_types=["non_existent_type"], output_format="csv")
    assert "No content found" in result