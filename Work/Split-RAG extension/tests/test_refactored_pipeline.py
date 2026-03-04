"""
Split-RAG Extension — Refactored Pipeline Integration Tests

Tests the v2.0 schema_v2.py → extractor.py → relationship_manager.py pipeline.
Validates:
    - schema_v2 ContextNode / ContextGraph / ExtractionMetrics
    - DocumentKnowledgeGraph (networkx DKG) construction + entity extraction
    - HybridRetrievalRouter (TF-IDF + subgraph scoping)
    - Bounding box preservation (non-negotiable audit requirement)
    - copilot_tier2 sandbox retrieval
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import schema_v2 as schema
from relationship_manager import (
    DocumentKnowledgeGraph,
    HybridRetrievalRouter,
    load_entity_keywords,
    _make_entity_id,
)
from copilot_tier2 import (
    preprocess_query,
    load_context_graph,
    resolve_entity_query,
    calculate_keyword_density,
    retrieve_context,
    get_document_summary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_context_graph() -> schema.ContextGraph:
    """Build a synthetic ContextGraph matching schema_v2."""
    doc_id = "a" * 32
    file_hash = "b" * 32

    nodes = [
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 1, 0, "CREDIT QUALITY REVIEW"),
            content_type="header",
            content="CREDIT QUALITY REVIEW",
            metadata=schema.NodeMetadata(
                page_number=1,
                bbox=[72.0, 50.0, 540.0, 80.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 1, [72.0, 50.0, 540.0, 80.0], "pdfplumber"),
        ),
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 1, 1, "The NPL ratio increased"),
            content_type="text",
            content="The NPL ratio increased to 1.23% in Q3 2024, reflecting deteriorating market conditions. Provision coverage remains adequate at 145%.",
            metadata=schema.NodeMetadata(
                page_number=1,
                bbox=[72.0, 90.0, 540.0, 160.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 1, [72.0, 90.0, 540.0, 160.0], "pdfplumber"),
        ),
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 1, 2, "| Metric | Value"),
            content_type="table",
            content="| Metric | Value | Change |\n| --- | --- | --- |\n| NPL Ratio | 1.23% | +0.15% |\n| Provision Coverage | 145% | -3% |\n| Tier 1 Capital | 12.5% | +0.2% |",
            metadata=schema.NodeMetadata(
                page_number=1,
                bbox=[72.0, 170.0, 540.0, 300.0],
                table_shape=[4, 3],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 1, [72.0, 170.0, 540.0, 300.0], "pdfplumber"),
        ),
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 2, 3, "PRICING AND TERMS"),
            content_type="header",
            content="PRICING AND TERMS",
            metadata=schema.NodeMetadata(
                page_number=2,
                bbox=[72.0, 50.0, 540.0, 80.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 2, [72.0, 50.0, 540.0, 80.0], "pdfplumber"),
        ),
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 2, 4, "The facility carries"),
            content_type="text",
            content="The facility carries a spread of SOFR + 250 basis points with a floor rate of 3.50%. Total commitment fee is 25 basis points on the unused portion. Maturity date is December 31, 2027.",
            metadata=schema.NodeMetadata(
                page_number=2,
                bbox=[72.0, 90.0, 540.0, 180.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 2, [72.0, 90.0, 540.0, 180.0], "pdfplumber"),
        ),
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 3, 5, "REGULATORY COMPLIANCE"),
            content_type="header",
            content="REGULATORY COMPLIANCE",
            metadata=schema.NodeMetadata(
                page_number=3,
                bbox=[72.0, 50.0, 540.0, 80.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 3, [72.0, 50.0, 540.0, 80.0], "pdfplumber"),
        ),
        schema.ContextNode(
            chunk_id=schema.generate_chunk_id(doc_id, 3, 6, "Basel III requirements"),
            content_type="text",
            content="Basel III requirements mandate minimum Tier 1 capital ratio of 6%. Current ratio stands at 12.5%, well above regulatory minimums. Stress test results under adverse scenario show capital depletion to 8.2%.",
            metadata=schema.NodeMetadata(
                page_number=3,
                bbox=[72.0, 90.0, 540.0, 200.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=schema.generate_lineage_trace(file_hash, 3, [72.0, 90.0, 540.0, 200.0], "pdfplumber"),
        ),
    ]

    return schema.ContextGraph(
        document_id=doc_id,
        filename="test_report.pdf",
        processed_at="2026-01-01T00:00:00Z",
        borrower_entity="Acme Corp",
        lender_entity="Big Bank",
        nodes=nodes,
        metrics=schema.ExtractionMetrics(
            total_pages=3,
            total_nodes=7,
            tables_extracted=1,
            headers_extracted=3,
            conflicts_detected=0,
            extraction_time_seconds=1.5,
            primary_engine_used=False,
            fallback_triggered=True,
            fallback_engine="pdfplumber",
        ),
    )


@pytest.fixture
def entity_keywords() -> dict:
    return {
        "financial_metric": ["NPL ratio", "provision coverage", "tier 1 capital"],
        "pricing": ["SOFR", "basis points", "spread", "commitment fee"],
        "regulatory": ["Basel III", "stress test"],
    }


@pytest.fixture
def built_dkg(sample_context_graph, entity_keywords):
    """Build DKG and extract entities."""
    dkg = DocumentKnowledgeGraph(
        entity_keywords=entity_keywords,
        fuzzy_threshold=80,
    )
    dkg.build_from_context_graph(sample_context_graph)
    dkg.extract_entities()
    return dkg


@pytest.fixture
def router(built_dkg):
    """Build retrieval router on top of DKG."""
    r = HybridRetrievalRouter(built_dkg, top_k=15)
    r.build_index()
    return r


# ---------------------------------------------------------------------------
# §1 schema_v2 tests
# ---------------------------------------------------------------------------

class TestSchemaV2:
    def test_context_node_validation(self):
        doc_id = "a" * 32
        chunk_id = schema.generate_chunk_id(doc_id, 1, 0, "test")
        lineage = schema.generate_lineage_trace("h" * 32, 1, [0.0, 0.0, 100.0, 50.0], "pdfplumber")
        node = schema.ContextNode(
            chunk_id=chunk_id,
            content_type="text",
            content="Sample text",
            metadata=schema.NodeMetadata(
                page_number=1,
                bbox=[0.0, 0.0, 100.0, 50.0],
                source_scope="primary",
                extraction_method="pdfplumber",
            ),
            lineage_trace=lineage,
        )
        assert len(node.chunk_id) == 32
        assert len(node.lineage_trace) == 64

    def test_context_graph_serialization(self, sample_context_graph):
        json_str = sample_context_graph.to_json()
        data = json.loads(json_str)
        assert data["document_id"] == "a" * 32
        assert len(data["nodes"]) == 7
        assert data["metrics"]["fallback_triggered"] is True
        assert data["borrower_entity"] == "Acme Corp"

    def test_extraction_metrics_fallback_engine(self, sample_context_graph):
        assert sample_context_graph.metrics.fallback_engine == "pdfplumber"

    def test_chunk_id_determinism(self):
        id1 = schema.generate_chunk_id("doc1", 1, 0, "test")
        id2 = schema.generate_chunk_id("doc1", 1, 0, "test")
        assert id1 == id2
        assert len(id1) == 32

    def test_lineage_trace_determinism(self):
        t1 = schema.generate_lineage_trace("h", 1, [1.0, 2.0, 3.0, 4.0], "pdfplumber")
        t2 = schema.generate_lineage_trace("h", 1, [1.0, 2.0, 3.0, 4.0], "pdfplumber")
        assert t1 == t2
        assert len(t1) == 64

    def test_node_metadata_pypdfium2_method(self):
        meta = schema.NodeMetadata(
            page_number=1,
            bbox=[10.0, 20.0, 300.0, 400.0],
            source_scope="primary",
            extraction_method="pypdfium2",
        )
        assert meta.extraction_method == "pypdfium2"


# ---------------------------------------------------------------------------
# §2 DKG (relationship_manager.py) tests
# ---------------------------------------------------------------------------

class TestDocumentKnowledgeGraph:
    def test_has_document_node(self, built_dkg):
        doc_nodes = [
            n for n, d in built_dkg.graph.nodes(data=True)
            if d.get("node_type") == "document"
        ]
        assert len(doc_nodes) == 1

    def test_has_page_nodes(self, built_dkg):
        page_nodes = [
            n for n, d in built_dkg.graph.nodes(data=True)
            if d.get("node_type") == "page"
        ]
        assert len(page_nodes) == 3

    def test_has_section_nodes(self, built_dkg):
        section_nodes = [
            n for n, d in built_dkg.graph.nodes(data=True)
            if d.get("node_type") == "section"
        ]
        assert len(section_nodes) >= 3

    def test_has_chunk_nodes(self, built_dkg):
        chunks = [
            n for n, d in built_dkg.graph.nodes(data=True)
            if d.get("node_type") == "chunk"
        ]
        assert len(chunks) == 7

    def test_has_page_edges(self, built_dkg):
        edges = [
            (u, v) for u, v, d in built_dkg.graph.edges(data=True)
            if d.get("edge_type") == "HAS_PAGE"
        ]
        assert len(edges) == 3

    def test_has_section_edges(self, built_dkg):
        edges = [
            (u, v) for u, v, d in built_dkg.graph.edges(data=True)
            if d.get("edge_type") == "HAS_SECTION"
        ]
        assert len(edges) >= 3

    def test_has_next_block_edges(self, built_dkg):
        edges = [
            (u, v) for u, v, d in built_dkg.graph.edges(data=True)
            if d.get("edge_type") == "NEXT_BLOCK"
        ]
        assert len(edges) == 6  # 7 chunks, 6 sequential edges

    def test_contains_table_edge(self, built_dkg):
        edges = [
            (u, v) for u, v, d in built_dkg.graph.edges(data=True)
            if d.get("edge_type") == "CONTAINS_TABLE"
        ]
        assert len(edges) == 1

    def test_entity_extraction(self, built_dkg):
        entity_nodes = [
            n for n, d in built_dkg.graph.nodes(data=True)
            if d.get("node_type") == "entity"
        ]
        assert len(entity_nodes) > 0

    def test_mentioned_in_edges(self, built_dkg):
        edges = [
            (u, v) for u, v, d in built_dkg.graph.edges(data=True)
            if d.get("edge_type") == "MENTIONED_IN"
        ]
        assert len(edges) > 0

    def test_bbox_preserved_on_all_chunks(self, built_dkg):
        """AUDITABILITY: Every chunk node must have bbox attribute."""
        for nid, data in built_dkg.graph.nodes(data=True):
            if data.get("node_type") == "chunk":
                bbox = data.get("bbox")
                assert bbox is not None, f"Node {nid} missing bbox"
                assert len(bbox) == 4, f"Node {nid} has invalid bbox: {bbox}"

    def test_section_subgraphs(self, built_dkg):
        subgraphs = built_dkg.get_section_subgraphs()
        assert len(subgraphs) >= 3

    def test_table_subgraph(self, built_dkg):
        tsg = built_dkg.get_table_subgraph()
        table_chunks = [
            n for n, d in tsg.nodes(data=True)
            if d.get("content_type") == "table"
        ]
        assert len(table_chunks) == 1


# ---------------------------------------------------------------------------
# §3 HybridRetrievalRouter tests
# ---------------------------------------------------------------------------

class TestHybridRetrievalRouter:
    def test_build_index(self, router):
        assert router._fitted

    def test_route_pricing(self, router):
        route, _ = router.route_query("What is the interest rate and spread?")
        assert route == "tables_pricing"

    def test_route_regulatory(self, router):
        route, _ = router.route_query("Basel III stress test results")
        assert route == "regulatory"

    def test_route_fallback(self, router):
        route, _ = router.route_query("xyzzy foobar nonsense")
        assert route == "full_graph"

    def test_query_returns_results(self, router):
        results = router.query("NPL ratio credit quality")
        assert len(results) > 0
        assert results[0]["score"] > 0

    def test_query_results_have_bbox(self, router):
        """Bounding-box provenance must be present in results."""
        results = router.query("credit quality review")
        has_bbox = any(
            r.get("bbox") is not None and len(r["bbox"]) == 4
            for r in results
        )
        assert has_bbox

    def test_query_results_have_route_label(self, router):
        results = router.query("NPL ratio")
        assert all("route" in r for r in results)

    def test_bounding_box_citations(self, router):
        results = router.query("credit quality")
        citations = HybridRetrievalRouter.get_bounding_box_citations(results)
        assert isinstance(citations, list)
        if citations:
            assert "x0" in citations[0]
            assert "y0" in citations[0]
            assert "page_number" in citations[0]

    def test_format_results_markdown(self, router):
        results = router.query("NPL ratio")
        md = HybridRetrievalRouter.format_results_markdown(results)
        assert "Score" in md
        assert "BBox" in md

    def test_pricing_query_finds_sofr(self, router):
        results = router.query("SOFR spread basis points")
        top_texts = " ".join(r["content"].lower() for r in results[:3])
        assert "sofr" in top_texts or "spread" in top_texts or "basis" in top_texts


# ---------------------------------------------------------------------------
# §4 copilot_tier2 tests
# ---------------------------------------------------------------------------

class TestCopilotTier2:
    def test_preprocess_query(self):
        tokens = preprocess_query("What is the NPL ratio for Q3?")
        assert "npl" in tokens
        assert "ratio" in tokens
        assert "the" not in tokens

    def test_load_context_graph(self, sample_context_graph):
        json_str = sample_context_graph.to_json()
        meta, df = load_context_graph(json_str)
        assert meta["filename"] == "test_report.pdf"
        assert len(df) == 7

    def test_resolve_entity_query_borrower(self, sample_context_graph):
        json_str = sample_context_graph.to_json()
        meta, _ = load_context_graph(json_str)
        _, answer = resolve_entity_query("Who is the borrower?", meta)
        assert "Acme Corp" in answer

    def test_retrieve_context(self, sample_context_graph):
        json_str = sample_context_graph.to_json()
        result = retrieve_context("NPL ratio", json_str)
        assert "NPL" in result or "npl" in result.lower()

    def test_retrieve_context_bbox_in_output(self, sample_context_graph):
        json_str = sample_context_graph.to_json()
        meta, df = load_context_graph(json_str)
        keywords = preprocess_query("credit quality")
        scored = calculate_keyword_density(df, keywords)
        # Verify bbox column exists in scored output
        assert "bbox" in scored.columns or "bbox" in df.columns

    def test_document_summary(self, sample_context_graph):
        json_str = sample_context_graph.to_json()
        summary = get_document_summary(json_str)
        assert "test_report.pdf" in summary
        assert "Acme Corp" in summary


# ---------------------------------------------------------------------------
# §5 Determinism tests
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_chunk_id_determinism(self):
        for _ in range(10):
            id1 = schema.generate_chunk_id("doc1", 5, 12, "content")
            id2 = schema.generate_chunk_id("doc1", 5, 12, "content")
            assert id1 == id2

    def test_document_id_determinism(self):
        data = b"sample pdf bytes"
        assert schema.generate_document_id(data) == schema.generate_document_id(data)

    def test_dkg_determinism(self, sample_context_graph, entity_keywords):
        dkg1 = DocumentKnowledgeGraph(entity_keywords=entity_keywords)
        dkg1.build_from_context_graph(sample_context_graph)

        dkg2 = DocumentKnowledgeGraph(entity_keywords=entity_keywords)
        dkg2.build_from_context_graph(sample_context_graph)

        assert set(dkg1.graph.nodes) == set(dkg2.graph.nodes)
        assert set(dkg1.graph.edges) == set(dkg2.graph.edges)

    def test_entity_id_determinism(self):
        id1 = _make_entity_id("financial_metric", "NPL ratio")
        id2 = _make_entity_id("financial_metric", "NPL ratio")
        assert id1 == id2


# ---------------------------------------------------------------------------
# §6 NEXT_BLOCK edge, parent Section context, JSON citation payload tests
# ---------------------------------------------------------------------------

class TestNextBlockAndSectionContext:
    def test_has_next_block_edges(self, built_dkg):
        """NEXT_BLOCK edges must exist alongside NEXT_CHUNK."""
        edges = [
            (u, v) for u, v, d in built_dkg.graph.edges(data=True)
            if d.get("edge_type") == "NEXT_BLOCK"
        ]
        assert len(edges) == 6  # 7 chunks, 6 sequential edges

    def test_query_results_have_parent_section(self, router):
        """Every result should be enriched with parent_section."""
        results = router.query("credit quality NPL ratio")
        assert len(results) > 0
        assert all("parent_section" in r for r in results)

    def test_parent_section_is_correct_label(self, router):
        """Chunk under CREDIT QUALITY REVIEW header should have that section."""
        results = router.query("NPL ratio provision coverage")
        matched = [r for r in results if r.get("parent_section") == "CREDIT QUALITY REVIEW"]
        assert len(matched) > 0

    def test_citation_payload_structure(self, router):
        """get_citation_payload returns correct JSON structure."""
        payload = router.get_citation_payload("SOFR spread basis points")
        assert "query" in payload
        assert "route" in payload
        assert "result_count" in payload
        assert "synthesized_text" in payload
        assert "citations" in payload
        assert isinstance(payload["citations"], list)

    def test_citation_payload_has_bbox(self, router):
        """Citations in payload must include bbox coordinates."""
        payload = router.get_citation_payload("credit quality NPL ratio")
        for c in payload["citations"]:
            assert "bbox" in c
            assert "page_number" in c
            assert "content_type" in c

    def test_citation_payload_has_section(self, router):
        """Citations should include the section label."""
        payload = router.get_citation_payload("NPL ratio")
        has_section = any(c.get("section") is not None for c in payload["citations"])
        assert has_section

    def test_synthesized_text_not_empty(self, router):
        """Synthesized text should contain content from results."""
        payload = router.get_citation_payload("Basel III stress test")
        assert len(payload["synthesized_text"]) > 0
        assert "Basel" in payload["synthesized_text"] or "stress" in payload["synthesized_text"]

    def test_schema_no_docling_method(self):
        """Schema extraction_method must not accept 'docling'."""
        from pydantic import ValidationError as PydanticValidationError
        with pytest.raises(PydanticValidationError):
            schema.NodeMetadata(
                page_number=1,
                bbox=[0.0, 0.0, 100.0, 50.0],
                source_scope="primary",
                extraction_method="docling",
            )
