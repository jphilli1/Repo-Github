# AI-Native Split-RAG System v2.0 - Test Fixtures
# Provides shared data and setup for all tests.

import sys
import pytest
import json
import shutil
from pathlib import Path

# Add project root to path to allow importing modules
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

import schema_v2 as schema

@pytest.fixture
def root_dir():
    return project_root

@pytest.fixture
def temp_output_dir(root_dir):
    """Creates a temporary output directory that is cleaned up after tests."""
    temp_dir = root_dir / "tests" / "temp_output"
    temp_dir.mkdir(parents=True, exist_ok=True)
    yield temp_dir
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

@pytest.fixture
def sample_config():
    return {
        "paths": {"input_directory": "input", "output_directory": "output"},
        "extraction_settings": {
            "primary_engine": "docling",
            "enable_ocr": True,
            "enable_table_detection": True
        }
    }

@pytest.fixture
def sample_rules():
    return {
        "entities": {
            "borrower": {"patterns": ["Borrower: (?P<entity>.+)"]},
            "lender": {"patterns": ["Lender: (?P<entity>.+)"]},
            "guarantor": {"patterns": ["Guarantor: (?P<entity>.+)"]}
        },
        "priority_order": ["explicit_definition"],
        "stopwords": ["the", "is", "at"]
    }

@pytest.fixture
def minimal_context_graph():
    """Returns a valid ContextGraph with a single node."""
    meta = schema.NodeMetadata(
        page_number=1,
        source_scope="primary",
        extraction_method="docling",
        is_active=True
    )
    node = schema.ContextNode(
        chunk_id="a1b2c3d4e5f67890a1b2c3d4e5f67890", # 32 char hex
        content_type="text",
        content="This is a minimal test node.",
        metadata=meta,
        lineage_trace="a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890a1b2c3d4e5f67890" # 64 char hex
    )
    return schema.ContextGraph(
        document_id="1234567890abcdef1234567890abcdef",
        filename="minimal.pdf",
        processed_at=schema.ContextGraph.get_current_timestamp(),
        nodes=[node]
    )

@pytest.fixture
def comprehensive_context_graph():
    """Returns a complex ContextGraph with multiple node types for robust testing."""
    nodes = []

    # 1. Header
    nodes.append(schema.ContextNode(
        chunk_id="11111111111111111111111111111111",
        content_type="header",
        content="Definitions and Interpretations",
        metadata=schema.NodeMetadata(page_number=1, source_scope="primary", extraction_method="docling"),
        lineage_trace="f" * 64
    ))

    # 2. Text (Primary)
    nodes.append(schema.ContextNode(
        chunk_id="22222222222222222222222222222222",
        content_type="text",
        content="The Borrower means Acme Corp.",
        metadata=schema.NodeMetadata(page_number=1, source_scope="primary", extraction_method="docling"),
        lineage_trace="f" * 64
    ))

    # 3. Table
    nodes.append(schema.ContextNode(
        chunk_id="33333333333333333333333333333333",
        content_type="table",
        content="| Rate | Value |\n| --- | --- |\n| Base | 5.0% |",
        metadata=schema.NodeMetadata(page_number=2, source_scope="primary", extraction_method="docling"),
        lineage_trace="f" * 64
    ))

    # 4. Corpus Scope (e.g. Reference Law)
    nodes.append(schema.ContextNode(
        chunk_id="44444444444444444444444444444444",
        content_type="text",
        content="UCC Section 9-102 Definitions.",
        metadata=schema.NodeMetadata(page_number=50, source_scope="corpus", extraction_method="docling"),
        lineage_trace="f" * 64
    ))

    return schema.ContextGraph(
        document_id="abcdefabcdefabcdefabcdefabcdefab",
        filename="comprehensive_loan.pdf",
        processed_at="2023-10-27T10:00:00Z",
        borrower_entity="Acme Corp",
        lender_entity="Global Bank",
        guarantor_entity="Acme Holdings",
        nodes=nodes
    )

@pytest.fixture
def context_graph_json(comprehensive_context_graph):
    return comprehensive_context_graph.to_json()