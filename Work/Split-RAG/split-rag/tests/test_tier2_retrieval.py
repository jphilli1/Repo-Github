# AI-Native Split-RAG System v2.0 - Tier 2 Unit Tests
# Verifies sandbox-safe retrieval logic

import pytest
import copilot_tier2 as tier2
import pandas as pd

def test_preprocess_query():
    """Test cleaning, stopword removal, and tokenization."""
    raw = "What IS the... Interest Rate??"
    # stopwords: what, is, the
    # keywords: interest, rate
    keywords = tier2.preprocess_query(raw)

    assert "what" not in keywords
    assert "is" not in keywords
    assert "interest" in keywords
    assert "rate" in keywords
    assert len(keywords) == 2

def test_simple_markdown_table():
    """Test manual markdown table generation."""
    df = pd.DataFrame([
        {"ColA": "Val1", "ColB": "Val2"},
        {"ColA": "Val3", "ColB": "Val4"}
    ])

    md = tier2.simple_markdown_table(df)

    assert "| ColA | ColB |" in md
    assert "| --- | --- |" in md
    assert "| Val1 | Val2 |" in md

def test_markdown_table_pipe_escaping():
    """Test that pipes in content are escaped."""
    df = pd.DataFrame([{"Content": "A | B"}])
    md = tier2.simple_markdown_table(df)
    assert "A \\| B" in md

def test_retrieve_context_flow(context_graph_json):
    """Test the main retrieval entry point."""
    # Query matching the table
    result = tier2.retrieve_context("rate value", context_graph_json)

    assert "**Found" in result
    assert "Rate" in result
    assert "5.0%" in result
    assert "| table |" in result # Content type column

def test_retrieve_context_no_results(context_graph_json):
    """Test query with no matches."""
    result = tier2.retrieve_context("xylophone zebra", context_graph_json)
    assert "No matches found" in result