# AI-Native Split-RAG System v2.0 - T-002 Validation
# Verifies Entity Disambiguation and Scope Separation logic (Tier 2)

import pytest
import copilot_tier2 as tier2
import json


def test_entity_resolution_borrower(context_graph_json):
    """Test 'Who is the borrower' returns the anchored entity."""
    meta, _ = tier2.load_context_graph(context_graph_json)

    query = "Who is the borrower?"
    mod_query, answer = tier2.resolve_entity_query(query, meta)

    assert answer is not None
    assert "Acme Corp" in answer
    assert "extracted from contract preamble" in answer


def test_entity_resolution_lender(context_graph_json):
    """Test 'Who is the lender' returns the anchored entity."""
    meta, _ = tier2.load_context_graph(context_graph_json)

    query = "identify the lender"
    mod_query, answer = tier2.resolve_entity_query(query, meta)

    assert answer is not None
    assert "Global Bank" in answer


def test_virtual_substitution(context_graph_json):
    """Test that queries for unknown entities get virtual substitution."""
    meta, _ = tier2.load_context_graph(context_graph_json)
    # Remove anchored entity to force substitution
    meta['borrower_entity'] = None

    query = "who is the borrower?"
    mod_query, answer = tier2.resolve_entity_query(query, meta)

    assert answer is None  # No direct answer
    assert "borrower" in mod_query  # Ensure 'borrower' keyword is preserved/added


def test_scope_separation(context_graph_json):
    """Test that primary scope items are prioritized."""
    # This tests the keyword density calculation logic
    meta, df = tier2.load_context_graph(context_graph_json)

    # Create a query that matches both primary and corpus content
    # Primary: "The Borrower means Acme Corp."
    # Corpus: "UCC Section 9-102 Definitions."

    # We'll search for "Definitions" which appears in both Header (Primary) and Corpus
    keywords = ["definitions"]
    scored_df = tier2.calculate_keyword_density(df, keywords)

    # Get top result
    top_row = scored_df.iloc[0]

    # The Header "Definitions and Interpretations" (Primary) should beat Corpus due to:
    # 1. Content Type Weight (Header=3.0 vs Text=1.0)
    # 2. Scope Multiplier (Primary=1.5x)
    assert top_row['content_type'] == 'header'
    assert top_row['source_scope'] == 'primary'