"""
Split-RAG Extension — Phase 4: Deterministic SPLIT-RAG Orchestrator

Replaces LLM "agents" with deterministic Functional Sub-Routers:
    1. Partitioning  — networkx graph split by sections / entity clusters
    2. Routing       — regex + TF-IDF keyword scoring to select the target subgraph
    3. Retrieval     — scoped TF-IDF search within the routed subgraph
    4. Aggregation   — merge results with bounding-box provenance

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.
"""

from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

import networkx as nx

from graph_service import GraphConstructionService
from retrieval_service import RetrievalService
from schemas import RetrievalResult

logger = logging.getLogger("SplitRAG.Router")


# ---------------------------------------------------------------------------
# Route definitions (deterministic keyword → subgraph mapping)
# ---------------------------------------------------------------------------

ROUTE_RULES: List[Dict] = [
    {
        "name": "tables_pricing",
        "description": "Routes to table-heavy and pricing-related content",
        "keywords": [
            "cost", "price", "pricing", "fee", "rate", "amount",
            "total", "spread", "margin", "basis points", "sofr",
            "libor", "interest", "payment", "amortization",
        ],
        "subgraph_type": "table",
    },
    {
        "name": "financial_metrics",
        "description": "Routes to financial metric and ratio content",
        "keywords": [
            "ratio", "npl", "coverage", "adequacy", "leverage",
            "capital", "roi", "return", "tier 1", "risk-weighted",
            "provision", "impairment", "write-off",
        ],
        "subgraph_type": "entity",
        "entity_type": "financial_metric",
    },
    {
        "name": "regulatory",
        "description": "Routes to regulatory and compliance content",
        "keywords": [
            "regulation", "regulatory", "compliance", "basel",
            "dodd-frank", "volcker", "ifrs", "cecl", "stress test",
            "examination", "audit", "supervisory",
        ],
        "subgraph_type": "entity",
        "entity_type": "regulatory",
    },
    {
        "name": "contract_terms",
        "description": "Routes to contractual term and covenant content",
        "keywords": [
            "covenant", "default", "maturity", "collateral",
            "guarantee", "subordination", "prepayment", "lien",
            "borrower", "lender", "agreement", "facility",
        ],
        "subgraph_type": "entity",
        "entity_type": "contract_term",
    },
]


class SplitRAGRouter:
    """
    Deterministic SPLIT-RAG orchestrator.

    Replaces multi-agent LLM routing with:
    - Regex/keyword-based route selection
    - Scoped TF-IDF retrieval within networkx subgraphs
    - Result aggregation with provenance metadata
    """

    def __init__(
        self,
        graph_service: GraphConstructionService,
        retrieval_service: RetrievalService,
        *,
        route_rules: Optional[List[Dict]] = None,
        fallback_top_k: int = 15,
    ) -> None:
        self._graph_service = graph_service
        self._retrieval_service = retrieval_service
        self._route_rules = route_rules or ROUTE_RULES
        self._fallback_top_k = fallback_top_k

        # Pre-compute section subgraphs
        self._section_subgraphs: Dict[str, nx.DiGraph] = {}
        self._table_subgraph: Optional[nx.DiGraph] = None

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Pre-compute subgraphs for routing. Call after graph is built."""
        self._section_subgraphs = self._graph_service.get_section_subgraphs()
        self._table_subgraph = self._graph_service.get_table_subgraph()
        logger.info(
            "Router initialized: %d section subgraphs, table subgraph with %d nodes",
            len(self._section_subgraphs),
            self._table_subgraph.number_of_nodes() if self._table_subgraph else 0,
        )

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route_query(self, query: str) -> Tuple[str, nx.DiGraph]:
        """
        Determine which subgraph to search based on the query.

        Algorithm:
            1. Lowercase + tokenize query
            2. Score each route rule by keyword overlap
            3. Select highest-scoring route
            4. Return (route_name, target_subgraph)
            5. Fallback to full graph if no route matches
        """
        query_lower = query.lower()
        query_tokens = set(re.findall(r"\w+", query_lower))

        best_route: Optional[Dict] = None
        best_score = 0

        for rule in self._route_rules:
            score = 0
            for kw in rule["keywords"]:
                kw_tokens = set(kw.lower().split())
                if kw_tokens & query_tokens:
                    score += 1
                elif kw.lower() in query_lower:
                    score += 2  # Exact phrase match gets higher score

            if score > best_score:
                best_score = score
                best_route = rule

        if best_route and best_score > 0:
            subgraph = self._resolve_subgraph(best_route)
            if subgraph and subgraph.number_of_nodes() > 0:
                logger.info(
                    "Routed query to '%s' (score=%d, %d nodes)",
                    best_route["name"],
                    best_score,
                    subgraph.number_of_nodes(),
                )
                return best_route["name"], subgraph

        # Fallback: full graph
        logger.info("No specific route matched — using full graph.")
        return "full_graph", self._graph_service.graph

    def _resolve_subgraph(self, route: Dict) -> Optional[nx.DiGraph]:
        """Resolve a route rule to its corresponding networkx subgraph."""
        sg_type = route.get("subgraph_type", "section")

        if sg_type == "table":
            return self._table_subgraph

        if sg_type == "entity":
            entity_type = route.get("entity_type", "")
            return self._graph_service.get_entity_subgraph(entity_type)

        if sg_type == "section":
            # Find best matching section by label
            query_keywords = set(kw.lower() for kw in route.get("keywords", []))
            best_sec = None
            best_overlap = 0
            for label, sg in self._section_subgraphs.items():
                label_tokens = set(label.lower().split())
                overlap = len(label_tokens & query_keywords)
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_sec = sg
            return best_sec

        return None

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def query(
        self,
        query_text: str,
        top_k: Optional[int] = None,
    ) -> List[RetrievalResult]:
        """
        Full SPLIT-RAG query pipeline:
            1. Route query to target subgraph
            2. Execute scoped TF-IDF search
            3. Tag results with subgraph label
            4. Return ranked results
        """
        k = top_k or self._fallback_top_k
        route_name, target_subgraph = self.route_query(query_text)

        # Scoped retrieval within the target subgraph
        results = self._retrieval_service.query_subgraph(
            query_text, target_subgraph, top_k=k
        )

        # Tag results with subgraph provenance
        for r in results:
            r.subgraph_label = route_name

        # If scoped search returned too few results, supplement from full graph
        if len(results) < 3 and route_name != "full_graph":
            logger.info("Supplementing sparse results from full graph.")
            full_results = self._retrieval_service.query(query_text, top_k=k)
            existing_ids = {r.node_id for r in results}
            for fr in full_results:
                if fr.node_id not in existing_ids:
                    fr.subgraph_label = "full_graph_supplement"
                    results.append(fr)
                    if len(results) >= k:
                        break

        return results[:k]

    # ------------------------------------------------------------------
    # Multi-hop retrieval
    # ------------------------------------------------------------------

    def multi_hop_query(
        self,
        query_text: str,
        hops: int = 2,
        top_k: Optional[int] = None,
    ) -> List[RetrievalResult]:
        """
        Multi-hop retrieval: use initial results to expand the search.

        Hop 1: Standard routed query
        Hop 2+: Extract key terms from top results, re-query adjacent sections
        """
        k = top_k or self._fallback_top_k
        all_results: List[RetrievalResult] = []
        seen_ids: set = set()

        # Hop 1
        hop1 = self.query(query_text, top_k=k)
        for r in hop1:
            if r.node_id not in seen_ids:
                all_results.append(r)
                seen_ids.add(r.node_id)

        # Subsequent hops
        for hop in range(1, hops):
            if not all_results:
                break

            # Extract salient terms from top results
            expansion_text = " ".join(
                r.raw_text[:200] for r in all_results[:5]
            )
            expanded_query = f"{query_text} {expansion_text}"

            hop_results = self.query(expanded_query, top_k=k)
            for r in hop_results:
                if r.node_id not in seen_ids:
                    r.subgraph_label = f"hop_{hop + 1}"
                    all_results.append(r)
                    seen_ids.add(r.node_id)

        # Re-sort by score
        all_results.sort(key=lambda r: r.score, reverse=True)
        return all_results[:k]

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    @staticmethod
    def format_results_markdown(results: List[RetrievalResult]) -> str:
        """Format retrieval results as Markdown with provenance metadata."""
        if not results:
            return "No relevant content found."

        lines = [
            "| # | Score | Page | Type | Route | Text (preview) |",
            "|---|-------|------|------|-------|----------------|",
        ]
        for i, r in enumerate(results, 1):
            preview = r.raw_text[:100].replace("\n", " ").replace("|", "\\|")
            bbox_str = ""
            if r.bounding_boxes:
                b = r.bounding_boxes[0]
                bbox_str = f" [bbox: {b[0]:.0f},{b[1]:.0f},{b[2]:.0f},{b[3]:.0f}]"
            lines.append(
                f"| {i} | {r.score:.4f} | {r.page_number} | "
                f"{r.chunk_type} | {r.subgraph_label or '-'} | "
                f"{preview}{bbox_str} |"
            )

        return "\n".join(lines)

    @staticmethod
    def get_bounding_box_citations(
        results: List[RetrievalResult],
    ) -> List[Dict]:
        """
        Extract bounding-box citation data for UI overlay rendering.
        Returns a list of dicts suitable for frontend consumption.
        """
        citations = []
        for r in results:
            for bbox in r.bounding_boxes:
                citations.append({
                    "node_id": r.node_id,
                    "page_number": r.page_number,
                    "x0": bbox[0],
                    "y0": bbox[1],
                    "x1": bbox[2],
                    "y1": bbox[3],
                    "chunk_type": r.chunk_type,
                    "score": r.score,
                    "text_preview": r.raw_text[:80],
                })
        return citations
