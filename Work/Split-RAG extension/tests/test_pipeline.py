"""
Split-RAG Extension — Pipeline Integration Tests

Tests the full pipeline: schemas, graph construction, TF-IDF retrieval, and routing.
Does NOT require actual PDFs — uses synthetic ChunkMetadata objects.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Ensure the parent package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from schemas import (
    ChunkMetadata,
    DocumentGraph,
    EntityNode,
    GraphEdge,
    RetrievalResult,
    generate_document_id,
    generate_lineage_trace,
    generate_node_id,
)
from graph_service import GraphConstructionService
from retrieval_service import RetrievalService
from split_rag_router import SplitRAGRouter
from entity_matcher import load_entity_keywords, scan_text_for_entities


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_chunks() -> list[ChunkMetadata]:
    """Synthetic chunks simulating a 3-page financial document."""
    return [
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 1, 0, "CREDIT QUALITY REVIEW"),
            source_file_name="test.pdf",
            page_number=1,
            chunk_type="header",
            bounding_boxes=[(72.0, 50.0, 540.0, 80.0)],
            raw_text="CREDIT QUALITY REVIEW",
            reading_order_index=0,
            extraction_method="pdfplumber",
        ),
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 1, 1, "The NPL ratio increased"),
            source_file_name="test.pdf",
            page_number=1,
            chunk_type="paragraph",
            bounding_boxes=[(72.0, 90.0, 540.0, 160.0)],
            raw_text="The NPL ratio increased to 1.23% in Q3 2024, reflecting deteriorating market conditions in the commercial real estate sector. Provision coverage remains adequate at 145%.",
            reading_order_index=1,
            extraction_method="pdfplumber",
        ),
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 1, 2, "| Metric | Value"),
            source_file_name="test.pdf",
            page_number=1,
            chunk_type="table",
            bounding_boxes=[(72.0, 170.0, 540.0, 300.0)],
            raw_text="| Metric | Value | Change |\n| --- | --- | --- |\n| NPL Ratio | 1.23% | +0.15% |\n| Provision Coverage | 145% | -3% |\n| Tier 1 Capital | 12.5% | +0.2% |",
            reading_order_index=2,
            extraction_method="pdfplumber",
        ),
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 2, 3, "PRICING AND TERMS"),
            source_file_name="test.pdf",
            page_number=2,
            chunk_type="header",
            bounding_boxes=[(72.0, 50.0, 540.0, 80.0)],
            raw_text="PRICING AND TERMS",
            reading_order_index=3,
            extraction_method="pdfplumber",
        ),
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 2, 4, "The facility carries"),
            source_file_name="test.pdf",
            page_number=2,
            chunk_type="paragraph",
            bounding_boxes=[(72.0, 90.0, 540.0, 180.0)],
            raw_text="The facility carries a spread of SOFR + 250 basis points with a floor rate of 3.50%. Total commitment fee is 25 basis points on the unused portion. Maturity date is December 31, 2027.",
            reading_order_index=4,
            extraction_method="pdfplumber",
        ),
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 3, 5, "REGULATORY COMPLIANCE"),
            source_file_name="test.pdf",
            page_number=3,
            chunk_type="header",
            bounding_boxes=[(72.0, 50.0, 540.0, 80.0)],
            raw_text="REGULATORY COMPLIANCE",
            reading_order_index=5,
            extraction_method="pdfplumber",
        ),
        ChunkMetadata(
            node_id=generate_node_id("test.pdf", 3, 6, "Basel III requirements"),
            source_file_name="test.pdf",
            page_number=3,
            chunk_type="paragraph",
            bounding_boxes=[(72.0, 90.0, 540.0, 200.0)],
            raw_text="Basel III requirements mandate minimum Tier 1 capital ratio of 6%. Current ratio stands at 12.5%, well above regulatory minimums. Stress test results under adverse scenario show capital depletion to 8.2%, still above the 4.5% CET1 minimum.",
            reading_order_index=6,
            extraction_method="pdfplumber",
        ),
    ]


@pytest.fixture
def entity_keywords() -> dict:
    return {
        "financial_metric": ["NPL ratio", "provision coverage", "tier 1 capital"],
        "pricing": ["SOFR", "basis points", "spread", "commitment fee"],
        "regulatory": ["Basel III", "stress test"],
    }


@pytest.fixture
def built_graph(sample_chunks, entity_keywords):
    """Build a graph from sample chunks and return the service."""
    svc = GraphConstructionService(
        entity_keywords=entity_keywords,
        fuzzy_threshold=80,
    )
    svc.build_graph(sample_chunks, document_id="abc123", filename="test.pdf")
    svc.extract_entities()
    return svc


# ---------------------------------------------------------------------------
# Schema tests
# ---------------------------------------------------------------------------

class TestSchemas:
    def test_chunk_metadata_validation(self):
        chunk = ChunkMetadata(
            node_id="abcd1234abcd1234abcd1234abcd1234",
            source_file_name="test.pdf",
            page_number=1,
            chunk_type="paragraph",
            bounding_boxes=[(10.0, 20.0, 300.0, 40.0)],
            raw_text="Sample text content.",
            extraction_method="pdfplumber",
        )
        assert chunk.node_id == "abcd1234abcd1234abcd1234abcd1234"
        assert chunk.chunk_type == "paragraph"
        assert len(chunk.bounding_boxes) == 1

    def test_chunk_metadata_empty_id_fails(self):
        with pytest.raises(Exception):
            ChunkMetadata(
                node_id="",
                source_file_name="test.pdf",
                page_number=1,
                chunk_type="paragraph",
                raw_text="text",
            )

    def test_generate_node_id_deterministic(self):
        id1 = generate_node_id("f.pdf", 1, 0, "hello")
        id2 = generate_node_id("f.pdf", 1, 0, "hello")
        assert id1 == id2
        assert len(id1) == 32  # MD5 hex

    def test_generate_lineage_trace(self):
        trace = generate_lineage_trace("hash123", 1, [10.0, 20.0, 300.0, 40.0], "docling")
        assert len(trace) == 64  # SHA-256 hex

    def test_document_graph_serialization(self, sample_chunks):
        dg = DocumentGraph(
            document_id="abc123def456abc123def456abc123de",
            filename="test.pdf",
            chunks=sample_chunks,
            total_pages=3,
        )
        json_str = dg.to_json()
        data = json.loads(json_str)
        assert data["filename"] == "test.pdf"
        assert len(data["chunks"]) == 7

    def test_retrieval_result_model(self):
        rr = RetrievalResult(
            node_id="abc123",
            raw_text="Some content",
            score=0.85,
            page_number=1,
            chunk_type="paragraph",
            bounding_boxes=[(10.0, 20.0, 300.0, 40.0)],
        )
        assert rr.score == 0.85


# ---------------------------------------------------------------------------
# Graph construction tests
# ---------------------------------------------------------------------------

class TestGraphService:
    def test_graph_has_document_node(self, built_graph):
        doc_nodes = [
            n for n, d in built_graph.graph.nodes(data=True)
            if d.get("node_type") == "document"
        ]
        assert len(doc_nodes) == 1

    def test_graph_has_page_nodes(self, built_graph):
        page_nodes = [
            n for n, d in built_graph.graph.nodes(data=True)
            if d.get("node_type") == "page"
        ]
        assert len(page_nodes) == 3  # 3 pages

    def test_graph_has_section_nodes(self, built_graph):
        section_nodes = [
            n for n, d in built_graph.graph.nodes(data=True)
            if d.get("node_type") == "section"
        ]
        assert len(section_nodes) >= 3  # 3 headers = 3 sections

    def test_graph_has_chunk_nodes(self, built_graph):
        chunk_nodes = [
            n for n, d in built_graph.graph.nodes(data=True)
            if d.get("node_type") == "chunk"
        ]
        assert len(chunk_nodes) == 7

    def test_graph_has_edges(self, built_graph):
        assert built_graph.graph.number_of_edges() > 0

    def test_has_page_edges(self, built_graph):
        page_edges = [
            (u, v) for u, v, d in built_graph.graph.edges(data=True)
            if d.get("edge_type") == "HAS_PAGE"
        ]
        assert len(page_edges) == 3

    def test_has_section_edges(self, built_graph):
        section_edges = [
            (u, v) for u, v, d in built_graph.graph.edges(data=True)
            if d.get("edge_type") == "HAS_SECTION"
        ]
        assert len(section_edges) >= 3

    def test_entity_extraction(self, built_graph):
        entity_nodes = [
            n for n, d in built_graph.graph.nodes(data=True)
            if d.get("node_type") == "entity"
        ]
        assert len(entity_nodes) > 0

    def test_section_subgraphs(self, built_graph):
        subgraphs = built_graph.get_section_subgraphs()
        assert len(subgraphs) >= 3

    def test_table_subgraph(self, built_graph):
        table_sg = built_graph.get_table_subgraph()
        table_chunks = [
            n for n, d in table_sg.nodes(data=True)
            if d.get("chunk_type") == "table"
        ]
        assert len(table_chunks) == 1


# ---------------------------------------------------------------------------
# Retrieval tests
# ---------------------------------------------------------------------------

class TestRetrievalService:
    def test_build_index(self, built_graph):
        svc = RetrievalService()
        count = svc.build_index(built_graph.graph)
        assert count == 7
        assert svc.is_fitted

    def test_query_returns_results(self, built_graph):
        svc = RetrievalService()
        svc.build_index(built_graph.graph)
        results = svc.query("NPL ratio credit quality")
        assert len(results) > 0
        assert results[0].score > 0

    def test_query_ranking_quality(self, built_graph):
        svc = RetrievalService()
        svc.build_index(built_graph.graph)
        results = svc.query("NPL ratio")
        # The table and paragraph about NPL should score highest
        top_texts = " ".join(r.raw_text.lower() for r in results[:3])
        assert "npl" in top_texts

    def test_pricing_query(self, built_graph):
        svc = RetrievalService()
        svc.build_index(built_graph.graph)
        results = svc.query("SOFR spread basis points pricing")
        assert len(results) > 0
        # Header gets 3x boost so may rank first; check top 3 results
        top_texts = " ".join(r.raw_text.lower() for r in results[:3])
        assert "sofr" in top_texts or "spread" in top_texts or "basis" in top_texts

    def test_empty_query(self, built_graph):
        svc = RetrievalService()
        svc.build_index(built_graph.graph)
        results = svc.query("")
        # Empty query should still work (returns low-relevance results or empty)
        assert isinstance(results, list)

    def test_bounding_boxes_preserved(self, built_graph):
        svc = RetrievalService()
        svc.build_index(built_graph.graph)
        results = svc.query("credit quality review")
        has_bbox = any(len(r.bounding_boxes) > 0 for r in results)
        assert has_bbox


# ---------------------------------------------------------------------------
# Router tests
# ---------------------------------------------------------------------------

class TestSplitRAGRouter:
    def test_route_pricing_query(self, built_graph):
        retrieval = RetrievalService()
        retrieval.build_index(built_graph.graph)
        router = SplitRAGRouter(built_graph, retrieval)
        router.initialize()

        route_name, _ = router.route_query("What is the interest rate and spread?")
        assert route_name == "tables_pricing"

    def test_route_regulatory_query(self, built_graph):
        retrieval = RetrievalService()
        retrieval.build_index(built_graph.graph)
        router = SplitRAGRouter(built_graph, retrieval)
        router.initialize()

        route_name, _ = router.route_query("Basel III stress test results")
        assert route_name == "regulatory"

    def test_route_fallback(self, built_graph):
        retrieval = RetrievalService()
        retrieval.build_index(built_graph.graph)
        router = SplitRAGRouter(built_graph, retrieval)
        router.initialize()

        route_name, _ = router.route_query("xyzzy foobar nonsense")
        assert route_name == "full_graph"

    def test_full_query(self, built_graph):
        retrieval = RetrievalService()
        retrieval.build_index(built_graph.graph)
        router = SplitRAGRouter(built_graph, retrieval)
        router.initialize()

        results = router.query("What is the NPL ratio?")
        assert len(results) > 0
        assert results[0].subgraph_label is not None

    def test_format_results_markdown(self, built_graph):
        retrieval = RetrievalService()
        retrieval.build_index(built_graph.graph)
        router = SplitRAGRouter(built_graph, retrieval)
        router.initialize()

        results = router.query("NPL ratio")
        md = SplitRAGRouter.format_results_markdown(results)
        assert "Score" in md
        assert "Page" in md

    def test_bounding_box_citations(self, built_graph):
        retrieval = RetrievalService()
        retrieval.build_index(built_graph.graph)
        router = SplitRAGRouter(built_graph, retrieval)
        router.initialize()

        results = router.query("credit quality")
        citations = SplitRAGRouter.get_bounding_box_citations(results)
        assert isinstance(citations, list)
        if citations:
            assert "x0" in citations[0]
            assert "page_number" in citations[0]


# ---------------------------------------------------------------------------
# Entity matcher tests
# ---------------------------------------------------------------------------

class TestEntityMatcher:
    def test_load_default_keywords(self):
        kw = load_entity_keywords(Path("nonexistent.json"))
        assert "financial_metric" in kw
        assert len(kw["financial_metric"]) > 0

    def test_scan_text_exact(self):
        text = "The NPL ratio is 1.23% and provision coverage stands at 145%."
        kw = {"financial_metric": ["NPL ratio", "provision coverage"]}
        results = scan_text_for_entities(text, kw, fuzzy_threshold=80)
        labels = [r[1] for r in results]
        assert "NPL ratio" in labels
        assert "provision coverage" in labels

    def test_scan_text_no_match(self):
        text = "The weather is sunny today."
        kw = {"financial_metric": ["NPL ratio"]}
        results = scan_text_for_entities(text, kw, fuzzy_threshold=90)
        assert len(results) == 0


# ---------------------------------------------------------------------------
# Determinism test (T-001 equivalent)
# ---------------------------------------------------------------------------

class TestDeterminism:
    def test_node_id_determinism(self):
        """Same input always produces same node_id."""
        for _ in range(10):
            id1 = generate_node_id("doc.pdf", 5, 12, "test content here")
            id2 = generate_node_id("doc.pdf", 5, 12, "test content here")
            assert id1 == id2

    def test_document_id_determinism(self):
        """Same file bytes always produce same document_id."""
        data = b"sample pdf bytes content"
        id1 = generate_document_id(data)
        id2 = generate_document_id(data)
        assert id1 == id2

    def test_graph_determinism(self, sample_chunks, entity_keywords):
        """Two graph builds from identical chunks produce identical structure."""
        svc1 = GraphConstructionService(entity_keywords=entity_keywords)
        svc1.build_graph(sample_chunks, "docid", "test.pdf")

        svc2 = GraphConstructionService(entity_keywords=entity_keywords)
        svc2.build_graph(sample_chunks, "docid", "test.pdf")

        assert set(svc1.graph.nodes) == set(svc2.graph.nodes)
        assert set(svc1.graph.edges) == set(svc2.graph.edges)


# ---------------------------------------------------------------------------
# Cross-page isolation tests
# ---------------------------------------------------------------------------

class TestPageStateReset:
    """
    Verify that prev_chunk_id and current_section_id are reset at each
    page boundary. This prevents chunks on page N+1 from incorrectly
    linking to page N's last chunk or section.
    """

    def test_no_cross_page_next_block_edges(self, built_graph):
        """NEXT_BLOCK edges must NOT span across pages."""
        for u, v, d in built_graph.graph.edges(data=True):
            if d.get("edge_type") == "NEXT_BLOCK":
                u_page = built_graph.graph.nodes[u].get("page_number")
                v_page = built_graph.graph.nodes[v].get("page_number")
                assert u_page == v_page, (
                    f"Cross-page NEXT_BLOCK edge: {u} (p{u_page}) → {v} (p{v_page})"
                )

    def test_next_block_count_per_page(self, built_graph):
        """
        Each page's NEXT_BLOCK count = (chunks_on_page - 1).
        Page 1: 3 chunks (header, para, table) → 2 edges
        Page 2: 2 chunks (header, para) → 1 edge
        Page 3: 2 chunks (header, para) → 1 edge
        Total: 4 NEXT_BLOCK edges (was 6 when cross-page)
        """
        next_block_edges = [
            (u, v) for u, v, d in built_graph.graph.edges(data=True)
            if d.get("edge_type") == "NEXT_BLOCK"
        ]
        assert len(next_block_edges) == 4

    def test_headerless_page2_attaches_to_page_node(self):
        """
        When page 2 has no header, its chunks should attach to the page
        node (not to page 1's section).
        """
        chunks = [
            ChunkMetadata(
                node_id=generate_node_id("test.pdf", 1, 0, "SECTION ONE HEADER"),
                source_file_name="test.pdf",
                page_number=1,
                chunk_type="header",
                bounding_boxes=[(72.0, 50.0, 540.0, 80.0)],
                raw_text="SECTION ONE HEADER",
                reading_order_index=0,
                extraction_method="pdfplumber",
            ),
            ChunkMetadata(
                node_id=generate_node_id("test.pdf", 1, 1, "Page 1 content"),
                source_file_name="test.pdf",
                page_number=1,
                chunk_type="paragraph",
                bounding_boxes=[(72.0, 90.0, 540.0, 160.0)],
                raw_text="Page 1 content under the section header.",
                reading_order_index=1,
                extraction_method="pdfplumber",
            ),
            # Page 2 has NO header — just a paragraph
            ChunkMetadata(
                node_id=generate_node_id("test.pdf", 2, 2, "Page 2 orphan content"),
                source_file_name="test.pdf",
                page_number=2,
                chunk_type="paragraph",
                bounding_boxes=[(72.0, 50.0, 540.0, 140.0)],
                raw_text="Page 2 orphan content with no header.",
                reading_order_index=2,
                extraction_method="pdfplumber",
            ),
        ]

        svc = GraphConstructionService()
        svc.build_graph(chunks, document_id="test123", filename="test.pdf")

        page2_chunk_id = chunks[2].node_id
        page2_node = "PAGE_test123_2"
        page1_section = f"SEC_{chunks[0].node_id}"

        # The page 2 chunk should be a child of the PAGE_2 node
        parents = list(svc.graph.predecessors(page2_chunk_id))
        parent_types = {
            p: svc.graph.nodes[p].get("node_type") for p in parents
        }

        # Must be attached to page node, NOT to page 1's section
        assert page2_node in parents, (
            f"Page 2 chunk should attach to {page2_node}, got parents: {parents}"
        )
        assert page1_section not in parents, (
            f"Page 2 chunk must NOT attach to page 1 section {page1_section}"
        )

    def test_no_cross_page_reading_order(self):
        """First chunk on page 2 must NOT have a NEXT_BLOCK edge from page 1's last chunk."""
        chunks = [
            ChunkMetadata(
                node_id=generate_node_id("test.pdf", 1, 0, "Page 1 last chunk"),
                source_file_name="test.pdf",
                page_number=1,
                chunk_type="paragraph",
                bounding_boxes=[(72.0, 50.0, 540.0, 120.0)],
                raw_text="Page 1 last chunk content here.",
                reading_order_index=0,
                extraction_method="pdfplumber",
            ),
            ChunkMetadata(
                node_id=generate_node_id("test.pdf", 2, 1, "Page 2 first chunk"),
                source_file_name="test.pdf",
                page_number=2,
                chunk_type="paragraph",
                bounding_boxes=[(72.0, 50.0, 540.0, 120.0)],
                raw_text="Page 2 first chunk content here.",
                reading_order_index=1,
                extraction_method="pdfplumber",
            ),
        ]

        svc = GraphConstructionService()
        svc.build_graph(chunks, document_id="cross123", filename="test.pdf")

        page1_chunk = chunks[0].node_id
        page2_chunk = chunks[1].node_id

        # There should be NO NEXT_BLOCK edge from page1_chunk to page2_chunk
        edge_data = svc.graph.get_edge_data(page1_chunk, page2_chunk)
        assert edge_data is None or edge_data.get("edge_type") != "NEXT_BLOCK", (
            "Cross-page NEXT_BLOCK edge found — page state was not reset"
        )

    def test_edge_naming_consistency(self, built_graph):
        """All sequential edges must use NEXT_BLOCK (not NEXT_CHUNK)."""
        for u, v, d in built_graph.graph.edges(data=True):
            assert d.get("edge_type") != "NEXT_CHUNK", (
                f"Found legacy NEXT_CHUNK edge: {u} → {v}"
            )

    def test_chunk_count_property(self, built_graph):
        """Verify chunk_count property (renamed from document_count)."""
        svc = RetrievalService()
        svc.build_index(built_graph.graph)
        assert svc.chunk_count == 7
