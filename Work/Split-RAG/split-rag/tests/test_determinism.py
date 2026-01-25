# AI-Native Split-RAG System v2.0 - T-001 Validation
# Verifies system determinism (Same Input -> Same Output)

import pytest
import hashlib
import json
import schema_v2 as schema


def test_document_id_determinism():
    """Test that generate_document_id returns identical hash for identical bytes."""
    content = b"This is a test document content."
    hash1 = schema.generate_document_id(content)
    hash2 = schema.generate_document_id(content)

    assert hash1 == hash2
    assert len(hash1) == 32  # MD5 length


def test_chunk_id_determinism():
    """Test that chunk IDs are deterministic based on position and content."""
    # Run 1
    id1 = schema.generate_chunk_id("doc_123", 1, 0, "Hello World")
    # Run 2 (Same inputs)
    id2 = schema.generate_chunk_id("doc_123", 1, 0, "Hello World")
    # Run 3 (Different content)
    id3 = schema.generate_chunk_id("doc_123", 1, 0, "Hello World Changed")
    # Run 4 (Different position)
    id4 = schema.generate_chunk_id("doc_123", 1, 1, "Hello World")

    assert id1 == id2
    assert id1 != id3
    assert id1 != id4


def test_lineage_trace_determinism():
    """Test strict lineage trace generation."""
    t1 = schema.generate_lineage_trace("filehash", 1, [0.0, 0.0, 100.0, 100.0], "docling")
    t2 = schema.generate_lineage_trace("filehash", 1, [0.0, 0.0, 100.0, 100.0], "docling")
    t3 = schema.generate_lineage_trace("filehash", 1, None, "pdfplumber")

    assert t1 == t2
    assert t1 != t3
    assert len(t1) == 64  # SHA-256 length


def test_graph_serialization_determinism(minimal_context_graph):
    """Test that JSON serialization is consistent."""
    json1 = minimal_context_graph.to_json()
    json2 = minimal_context_graph.to_json()

    assert json1 == json2

    # Reload and verify equality
    data = json.loads(json1)
    assert data['document_id'] == minimal_context_graph.document_id