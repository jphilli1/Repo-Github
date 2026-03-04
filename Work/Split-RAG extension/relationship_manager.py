"""
Split-RAG System v2.0 — Relationship Manager (Refactored)
"The Gatekeeper" → Document Knowledge Graph Builder + Hybrid Retrieval Router

COMPLETE REFACTOR: Replaced folder-scanning logic with:
    1. In-Memory networkx.DiGraph Document Knowledge Graph (DKG)
    2. Structural topology: DOC → PAGE → SECTION → CHUNK edges
    3. Rule-based entity extraction via RapidFuzz fuzzy matching
    4. TF-IDF hybrid retrieval routing via scikit-learn
    5. Every node preserves [x0, y0, x1, y1] bounding box coordinates

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.

CP-001: All functions have explicit return type hints
CP-002: Specific exception handling
CP-003: Pathlib used for all file ops
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import networkx as nx
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import schema_v2 as schema

logger = logging.getLogger("SplitRAG.RelationshipManager")


# ---------------------------------------------------------------------------
# RapidFuzz availability
# ---------------------------------------------------------------------------
_RAPIDFUZZ_AVAILABLE: bool = False
try:
    from rapidfuzz import fuzz as _fuzz
    _RAPIDFUZZ_AVAILABLE = True
except ImportError:
    _fuzz = None


# ============================================================================
# §1  DOCUMENT KNOWLEDGE GRAPH (DKG) CONSTRUCTION
# ============================================================================

class DocumentKnowledgeGraph:
    """
    In-memory Document Knowledge Graph using networkx.DiGraph.

    Structural Hierarchy:
        DOC_<id>
          └── PAGE_<n>
                └── SEC_<id>  (inferred from header nodes)
                      └── CHUNK_<id>

    Edge types: HAS_PAGE, HAS_SECTION, HAS_CHILD, NEXT_BLOCK,
                CONTAINS_TABLE, MENTIONED_IN

    AUDITABILITY: Every chunk node retains its bounding box [x0,y0,x1,y1]
    as a node attribute. This is non-negotiable for frontend visual citations.
    """

    def __init__(
        self,
        *,
        entity_keywords: Optional[Dict[str, List[str]]] = None,
        fuzzy_threshold: int = 85,
    ) -> None:
        self.graph: nx.DiGraph = nx.DiGraph()
        self._entity_keywords = entity_keywords or {}
        self._fuzzy_threshold = fuzzy_threshold

    # ------------------------------------------------------------------
    # §1.1  Topological Construction
    # ------------------------------------------------------------------

    def build_from_context_graph(self, ctx: schema.ContextGraph) -> nx.DiGraph:
        """
        Build the DKG from a validated ContextGraph (schema_v2).

        Loops through ContextNode models, creates graph nodes with full
        metadata (including bbox), and wires structural edges.
        """
        self.graph.clear()

        doc_node = f"DOC_{ctx.document_id}"
        self.graph.add_node(
            doc_node,
            node_type="document",
            label=ctx.filename,
            document_id=ctx.document_id,
            borrower_entity=ctx.borrower_entity,
            lender_entity=ctx.lender_entity,
            guarantor_entity=ctx.guarantor_entity,
        )

        # Group nodes by page
        pages: Dict[int, List[schema.ContextNode]] = defaultdict(list)
        for node in ctx.nodes:
            if node.metadata.is_active:
                pages[node.metadata.page_number].append(node)

        prev_chunk_id: Optional[str] = None
        current_section_id: Optional[str] = None

        for page_no in sorted(pages.keys()):
            # Reset per-page state: each page starts with clean context
            prev_chunk_id = None
            current_section_id = None

            page_id = f"PAGE_{ctx.document_id}_{page_no}"
            self.graph.add_node(
                page_id,
                node_type="page",
                page_number=page_no,
                document_id=ctx.document_id,
            )
            self.graph.add_edge(doc_node, page_id, edge_type="HAS_PAGE")

            for node in pages[page_no]:
                chunk_id = node.chunk_id

                # === AUDITABILITY: bbox stored as node attribute ===
                self.graph.add_node(
                    chunk_id,
                    node_type="chunk",
                    content_type=node.content_type,
                    content=node.content,
                    page_number=node.metadata.page_number,
                    bbox=node.metadata.bbox,
                    table_shape=node.metadata.table_shape,
                    source_scope=node.metadata.source_scope,
                    extraction_method=node.metadata.extraction_method,
                    lineage_trace=node.lineage_trace,
                    is_active=node.metadata.is_active,
                    conflict_detected=node.metadata.conflict_detected,
                )

                # Section detection: headers start new sections
                if node.content_type == "header":
                    section_id = f"SEC_{chunk_id}"
                    self.graph.add_node(
                        section_id,
                        node_type="section",
                        label=node.content.strip()[:120],
                        page_number=node.metadata.page_number,
                    )
                    self.graph.add_edge(page_id, section_id, edge_type="HAS_SECTION")
                    current_section_id = section_id
                    # Also link header chunk to its section
                    self.graph.add_edge(section_id, chunk_id, edge_type="HAS_CHILD")
                elif current_section_id:
                    self.graph.add_edge(current_section_id, chunk_id, edge_type="HAS_CHILD")
                else:
                    self.graph.add_edge(page_id, chunk_id, edge_type="HAS_CHILD")

                # Table-specific edge
                if node.content_type == "table" and current_section_id:
                    self.graph.add_edge(
                        current_section_id, chunk_id, edge_type="CONTAINS_TABLE"
                    )

                # Reading-order edge (NEXT_BLOCK per architecture spec)
                if prev_chunk_id:
                    self.graph.add_edge(prev_chunk_id, chunk_id, edge_type="NEXT_BLOCK")
                prev_chunk_id = chunk_id

        logger.info(
            "DKG built: %d nodes, %d edges from %s",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
            ctx.filename,
        )
        return self.graph

    # ------------------------------------------------------------------
    # §1.2  Rule-Based Entity Extraction (RapidFuzz)
    # ------------------------------------------------------------------

    def extract_entities(self) -> List[Dict[str, Any]]:
        """
        Scan chunk node text for domain-specific entity keywords.

        Uses RapidFuzz partial_ratio when available; falls back to exact
        substring matching. Creates ENTITY nodes and MENTIONED_IN edges.
        """
        if not self._entity_keywords:
            return []

        entities_found: List[Dict[str, Any]] = []
        entity_registry: Dict[str, Dict[str, Any]] = {}

        chunk_nodes = [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("node_type") == "chunk"
        ]

        for category, keywords in self._entity_keywords.items():
            for keyword in keywords:
                kw_lower = keyword.lower()
                for chunk_id, chunk_data in chunk_nodes:
                    text = chunk_data.get("content", "").lower()
                    matched = False
                    score = 0.0

                    if _RAPIDFUZZ_AVAILABLE:
                        ratio = _fuzz.partial_ratio(kw_lower, text)
                        if ratio >= self._fuzzy_threshold:
                            matched = True
                            score = float(ratio)
                    else:
                        if kw_lower in text:
                            matched = True
                            score = 100.0

                    if matched:
                        entity_id = _make_entity_id(category, keyword)
                        if entity_id not in entity_registry:
                            entity_registry[entity_id] = {
                                "entity_id": entity_id,
                                "label": keyword,
                                "type": category,
                                "score": score,
                                "chunk_ids": [chunk_id],
                            }
                        else:
                            if chunk_id not in entity_registry[entity_id]["chunk_ids"]:
                                entity_registry[entity_id]["chunk_ids"].append(chunk_id)
                            entity_registry[entity_id]["score"] = max(
                                entity_registry[entity_id]["score"], score
                            )

                        # Add entity node + MENTIONED_IN edge
                        self.graph.add_node(
                            entity_id,
                            node_type="entity",
                            label=keyword,
                            entity_type=category,
                        )
                        self.graph.add_edge(
                            entity_id, chunk_id, edge_type="MENTIONED_IN"
                        )

        entities_found = list(entity_registry.values())
        logger.info("Entity extraction: %d unique entities found", len(entities_found))
        return entities_found

    # ------------------------------------------------------------------
    # §1.3  Subgraph Access (for SPLIT-RAG routing)
    # ------------------------------------------------------------------

    def get_section_subgraphs(self) -> Dict[str, nx.DiGraph]:
        """Partition graph into per-section subgraphs."""
        subgraphs: Dict[str, nx.DiGraph] = {}
        for nid, data in self.graph.nodes(data=True):
            if data.get("node_type") == "section":
                descendants = nx.descendants(self.graph, nid) | {nid}
                sub = self.graph.subgraph(descendants).copy()
                label = data.get("label", nid)
                subgraphs[label] = sub
        return subgraphs

    def get_table_subgraph(self) -> nx.DiGraph:
        """Return subgraph containing only table chunks and their ancestors."""
        table_ids: Set[str] = set()
        for nid, data in self.graph.nodes(data=True):
            if data.get("content_type") == "table":
                table_ids.add(nid)
                table_ids.update(nx.ancestors(self.graph, nid))
        if not table_ids:
            return nx.DiGraph()
        return self.graph.subgraph(table_ids).copy()

    def get_entity_subgraph(self, entity_type: str) -> nx.DiGraph:
        """Return subgraph for a specific entity type and connected chunks."""
        relevant: Set[str] = set()
        for nid, data in self.graph.nodes(data=True):
            if data.get("node_type") == "entity" and data.get("entity_type") == entity_type:
                relevant.add(nid)
                relevant.update(nx.descendants(self.graph, nid))
                relevant.update(nx.ancestors(self.graph, nid))
        if not relevant:
            return nx.DiGraph()
        return self.graph.subgraph(relevant).copy()

    def get_chunk_nodes(self) -> List[Tuple[str, Dict[str, Any]]]:
        """Return all chunk-type nodes with their data."""
        return [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("node_type") == "chunk"
        ]


# ============================================================================
# §2  HYBRID RETRIEVAL ROUTING (TF-IDF + networkx subgraph scoping)
# ============================================================================

# Content-type weight multipliers (defaults, overridable via config.json "weights" section)
DEFAULT_CONTENT_TYPE_WEIGHTS: Dict[str, float] = {
    "header": 3.0,
    "table": 2.5,
    "kv_pair": 2.0,
    "image_caption": 1.5,
    "text": 1.0,
}

DEFAULT_PRIMARY_SCOPE_MULTIPLIER: float = 1.5


def load_weights_from_config(
    config_path: Path = Path("config.json"),
) -> Tuple[Dict[str, float], float]:
    """
    Load content-type weights and scope multiplier from config.json.
    Falls back to built-in defaults if config is missing or incomplete.
    """
    weights = dict(DEFAULT_CONTENT_TYPE_WEIGHTS)
    scope_mult = DEFAULT_PRIMARY_SCOPE_MULTIPLIER
    try:
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            weights_cfg = cfg.get("weights", {})
            if "content_type" in weights_cfg:
                weights.update(weights_cfg["content_type"])
            scope_cfg = weights_cfg.get("scope", {})
            if "primary" in scope_cfg:
                scope_mult = float(scope_cfg["primary"])
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not load weights from config: %s", exc)
    return weights, scope_mult

# Route definitions: keyword patterns → subgraph targets
ROUTE_RULES: List[Dict[str, Any]] = [
    {
        "name": "tables_pricing",
        "keywords": [
            "cost", "price", "pricing", "fee", "rate", "amount",
            "total", "spread", "margin", "basis points", "sofr",
            "libor", "interest", "payment", "amortization",
        ],
        "subgraph_type": "table",
    },
    {
        "name": "financial_metrics",
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
        "keywords": [
            "covenant", "default", "maturity", "collateral",
            "guarantee", "subordination", "prepayment", "lien",
            "borrower", "lender", "agreement", "facility",
        ],
        "subgraph_type": "entity",
        "entity_type": "contract_term",
    },
]


class HybridRetrievalRouter:
    """
    Deterministic retrieval router using TF-IDF + networkx subgraph scoping.

    Replaces LLM-based query decomposition with:
        1. Keyword-based route selection → target subgraph
        2. TfidfVectorizer + cosine_similarity within scoped subgraph
        3. Content-type and scope weighting
        4. Bounding-box provenance in every result
    """

    def __init__(
        self,
        dkg: DocumentKnowledgeGraph,
        *,
        max_features: int = 10000,
        ngram_range: Tuple[int, int] = (1, 2),
        top_k: int = 15,
        min_score: float = 0.01,
        route_rules: Optional[List[Dict[str, Any]]] = None,
        content_type_weights: Optional[Dict[str, float]] = None,
        primary_scope_multiplier: Optional[float] = None,
    ) -> None:
        self._dkg = dkg
        self._max_features = max_features
        self._ngram_range = ngram_range
        self._top_k = top_k
        self._min_score = min_score
        self._route_rules = route_rules or ROUTE_RULES
        self._content_type_weights = content_type_weights or dict(DEFAULT_CONTENT_TYPE_WEIGHTS)
        self._primary_scope_multiplier = (
            primary_scope_multiplier
            if primary_scope_multiplier is not None
            else DEFAULT_PRIMARY_SCOPE_MULTIPLIER
        )

        # Full-graph index
        self._vectorizer: Optional[TfidfVectorizer] = None
        self._tfidf_matrix = None
        self._node_ids: List[str] = []
        self._node_data: List[Dict[str, Any]] = []
        self._fitted = False

        # Pre-computed subgraphs
        self._section_subgraphs: Dict[str, nx.DiGraph] = {}
        self._table_subgraph: Optional[nx.DiGraph] = None

    # ------------------------------------------------------------------
    # Index Building
    # ------------------------------------------------------------------

    def build_index(self) -> int:
        """
        Pre-compute subgraphs and validate DKG has indexable chunks.

        IMPORTANT: No global TfidfVectorizer is fitted here. Each query
        instantiates and fits a localized vectorizer scoped to the target
        subgraph (via _query_graph). This ensures term-frequency matching
        is highly specific to the routed partition, eliminating noise from
        irrelevant document sections.
        """
        # Count indexable chunks (for return value and validation)
        chunk_count = 0
        for _nid, data in self._dkg.graph.nodes(data=True):
            if data.get("node_type") == "chunk" and data.get("content", "").strip():
                chunk_count += 1

        if chunk_count == 0:
            self._fitted = False
            return 0

        self._fitted = True

        # Pre-compute subgraphs for routing
        self._section_subgraphs = self._dkg.get_section_subgraphs()
        self._table_subgraph = self._dkg.get_table_subgraph()

        logger.info(
            "Retrieval index ready: %d indexable chunks, %d section subgraphs",
            chunk_count,
            len(self._section_subgraphs),
        )
        return chunk_count

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def route_query(self, query: str) -> Tuple[str, nx.DiGraph]:
        """
        Route a query to the most relevant subgraph using keyword matching.
        Falls back to full graph if no route matches.
        """
        query_lower = query.lower()
        query_tokens = set(re.findall(r"\w+", query_lower))

        best_route: Optional[Dict[str, Any]] = None
        best_score = 0

        for rule in self._route_rules:
            score = 0
            for kw in rule["keywords"]:
                kw_tokens = set(kw.lower().split())
                if kw_tokens & query_tokens:
                    score += 1
                elif kw.lower() in query_lower:
                    score += 2
            if score > best_score:
                best_score = score
                best_route = rule

        if best_route and best_score > 0:
            subgraph = self._resolve_subgraph(best_route)
            if subgraph and subgraph.number_of_nodes() > 0:
                logger.info(
                    "Routed to '%s' (score=%d, %d nodes)",
                    best_route["name"], best_score, subgraph.number_of_nodes(),
                )
                return best_route["name"], subgraph

        return "full_graph", self._dkg.graph

    def _resolve_subgraph(self, route: Dict[str, Any]) -> Optional[nx.DiGraph]:
        sg_type = route.get("subgraph_type", "section")
        if sg_type == "table":
            return self._table_subgraph
        if sg_type == "entity":
            return self._dkg.get_entity_subgraph(route.get("entity_type", ""))
        return None

    # ------------------------------------------------------------------
    # Query Execution
    # ------------------------------------------------------------------

    def query(
        self, query_text: str, top_k: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Full hybrid retrieval pipeline:
            1. Route query → target subgraph
            2. Build scoped TF-IDF index on subgraph
            3. Compute cosine similarity with content-type weighting
            4. Return ranked results with bounding-box provenance
        """
        if not self._fitted:
            logger.warning("Index not built — call build_index() first.")
            return []

        k = top_k or self._top_k
        route_name, target_subgraph = self.route_query(query_text)

        # Build scoped index
        results = self._query_subgraph(query_text, target_subgraph, k)

        # Tag results with route provenance
        for r in results:
            r["route"] = route_name

        # Supplement from full graph if sparse
        if len(results) < 3 and route_name != "full_graph":
            full_results = self._query_graph(query_text, self._dkg.graph, k)
            seen = {r["chunk_id"] for r in results}
            for fr in full_results:
                if fr["chunk_id"] not in seen:
                    fr["route"] = "full_graph_supplement"
                    results.append(fr)
                    if len(results) >= k:
                        break

        results = results[:k]
        return self._enrich_with_section_context(results)

    def _query_subgraph(
        self, query_text: str, subgraph: nx.DiGraph, top_k: int
    ) -> List[Dict[str, Any]]:
        """Build temporary TF-IDF index on subgraph and query it."""
        return self._query_graph(query_text, subgraph, top_k)

    def _query_graph(
        self, query_text: str, graph: nx.DiGraph, top_k: int
    ) -> List[Dict[str, Any]]:
        """
        Execute LOCALIZED TF-IDF query against a specific subgraph's chunk nodes.

        SPLIT-RAG DESIGN: The TfidfVectorizer is instantiated and fit ONLY on
        the raw_text of nodes within the given networkx subgraph. This localized
        vector space ensures highly specific term-frequency matching, eliminating
        noise from irrelevant document sections. The vectorizer is NOT shared
        across subgraphs.

        Results include bounding_box attributes for frontend citation overlay.
        """
        node_ids: List[str] = []
        node_data: List[Dict[str, Any]] = []
        corpus: List[str] = []

        for nid, data in graph.nodes(data=True):
            if data.get("node_type") != "chunk":
                continue
            text = data.get("content", "")
            if not text.strip():
                continue
            node_ids.append(nid)
            node_data.append(data)
            corpus.append(text)

        if not corpus:
            return []

        vectorizer = TfidfVectorizer(
            max_features=self._max_features,
            ngram_range=self._ngram_range,
            stop_words="english",
            sublinear_tf=True,
        )
        tfidf_matrix = vectorizer.fit_transform(corpus)
        query_vec = vectorizer.transform([query_text])
        raw_scores = cosine_similarity(query_vec, tfidf_matrix).flatten()

        # Apply content-type and scope weighting
        weighted_scores = np.zeros_like(raw_scores)
        for i, (score, data) in enumerate(zip(raw_scores, node_data)):
            if score < self._min_score:
                continue
            ctype = data.get("content_type", "text")
            scope = data.get("source_scope", "primary")
            type_w = self._content_type_weights.get(ctype, 1.0)
            scope_w = self._primary_scope_multiplier if scope == "primary" else 1.0
            weighted_scores[i] = score * type_w * scope_w

        # Rank and return
        top_indices = np.argsort(weighted_scores)[::-1][:top_k]
        results: List[Dict[str, Any]] = []
        for idx in top_indices:
            ws = float(weighted_scores[idx])
            if ws < self._min_score:
                break
            data = node_data[idx]
            results.append({
                "chunk_id": node_ids[idx],
                "content": data.get("content", ""),
                "score": ws,
                "page_number": data.get("page_number", 0),
                "content_type": data.get("content_type", "text"),
                "bbox": data.get("bbox"),
                "source_scope": data.get("source_scope", "primary"),
                "extraction_method": data.get("extraction_method", "unknown"),
                "lineage_trace": data.get("lineage_trace"),
                "route": "direct",
            })

        return results

    # ------------------------------------------------------------------
    # Bounding-Box Citation Export
    # ------------------------------------------------------------------

    def _get_parent_section(self, chunk_id: str) -> Optional[Dict[str, Any]]:
        """
        Walk up the DKG to find the parent Section node for a given chunk.
        Returns section data dict or None if no section parent exists.
        """
        for pred in self._dkg.graph.predecessors(chunk_id):
            pred_data = self._dkg.graph.nodes.get(pred, {})
            if pred_data.get("node_type") == "section":
                return {"section_id": pred, **pred_data}
        return None

    def _enrich_with_section_context(
        self, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich each result with its parent Section label and siblings.
        Provides surrounding context for the retrieval result.
        """
        for r in results:
            section_info = self._get_parent_section(r["chunk_id"])
            if section_info:
                r["parent_section"] = section_info.get("label", "")
                r["parent_section_id"] = section_info.get("section_id", "")
            else:
                r["parent_section"] = None
                r["parent_section_id"] = None
        return results

    @staticmethod
    def get_bounding_box_citations(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract UI-ready bounding-box citation data from retrieval results.
        Returns list of dicts for frontend overlay rendering.
        """
        citations: List[Dict[str, Any]] = []
        for r in results:
            bbox = r.get("bbox")
            if bbox and len(bbox) == 4:
                citations.append({
                    "chunk_id": r["chunk_id"],
                    "page_number": r["page_number"],
                    "x0": bbox[0],
                    "y0": bbox[1],
                    "x1": bbox[2],
                    "y1": bbox[3],
                    "content_type": r["content_type"],
                    "score": r["score"],
                    "text_preview": r["content"][:80],
                })
        return citations

    def get_citation_payload(
        self, query_text: str, top_k: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Full retrieval with JSON citation payload.

        Returns a structured dict with:
            - synthesized_text: concatenated top-K results with section context
            - citations: array of {page_number, bbox, content_type, section, text_preview}
            - query: original query
            - route: matched route name
        """
        results = self.query(query_text, top_k=top_k)
        results = self._enrich_with_section_context(results)

        # Build synthesized text from results with section headers
        text_parts: List[str] = []
        current_section: Optional[str] = None
        for r in results:
            sec = r.get("parent_section")
            if sec and sec != current_section:
                text_parts.append(f"\n## {sec}\n")
                current_section = sec
            text_parts.append(r["content"])

        synthesized = "\n\n".join(text_parts).strip()

        # Build citations array
        citations: List[Dict[str, Any]] = []
        for r in results:
            citation: Dict[str, Any] = {
                "chunk_id": r["chunk_id"],
                "page_number": r["page_number"],
                "content_type": r["content_type"],
                "score": r["score"],
                "text_preview": r["content"][:120],
                "section": r.get("parent_section"),
            }
            bbox = r.get("bbox")
            if bbox and len(bbox) == 4:
                citation["bbox"] = {
                    "x0": bbox[0],
                    "y0": bbox[1],
                    "x1": bbox[2],
                    "y1": bbox[3],
                }
            else:
                citation["bbox"] = None
            citations.append(citation)

        route = results[0]["route"] if results else "none"

        return {
            "query": query_text,
            "route": route,
            "result_count": len(results),
            "synthesized_text": synthesized,
            "citations": citations,
        }

    @staticmethod
    def format_results_markdown(results: List[Dict[str, Any]]) -> str:
        """Format retrieval results as a Markdown evidence table."""
        if not results:
            return "No relevant content found."
        lines = [
            "| # | Score | Page | Type | Route | BBox | Text (preview) |",
            "|---|-------|------|------|-------|------|----------------|",
        ]
        for i, r in enumerate(results, 1):
            preview = r["content"][:80].replace("\n", " ").replace("|", "\\|")
            bbox = r.get("bbox")
            bbox_str = (
                f"[{bbox[0]:.0f},{bbox[1]:.0f},{bbox[2]:.0f},{bbox[3]:.0f}]"
                if bbox and len(bbox) == 4
                else "—"
            )
            lines.append(
                f"| {i} | {r['score']:.4f} | {r['page_number']} | "
                f"{r['content_type']} | {r.get('route', '-')} | "
                f"{bbox_str} | {preview} |"
            )
        return "\n".join(lines)


# ============================================================================
# §3  PIPELINE INTEGRATION — process_document
# ============================================================================

def load_entity_keywords(rules_path: Path) -> Dict[str, List[str]]:
    """Load entity keyword dictionaries from rules.json."""
    if not rules_path.exists():
        return _DEFAULT_ENTITY_KEYWORDS
    try:
        with open(rules_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        kw = data.get("entity_keywords", {})
        return kw if kw else _DEFAULT_ENTITY_KEYWORDS
    except (json.JSONDecodeError, OSError):
        return _DEFAULT_ENTITY_KEYWORDS


_DEFAULT_ENTITY_KEYWORDS: Dict[str, List[str]] = {
    "financial_metric": [
        "NPL ratio", "net interest margin", "capital adequacy",
        "loan-to-value", "debt service coverage", "return on equity",
        "tier 1 capital", "leverage ratio", "provision coverage",
    ],
    "contract_term": [
        "maturity date", "interest rate", "principal amount",
        "collateral", "covenant", "default event", "prepayment",
        "amortization", "guarantee", "subordination",
    ],
    "regulatory": [
        "Basel III", "Basel IV", "Dodd-Frank", "Volcker Rule",
        "IFRS 9", "CECL", "stress test", "risk-weighted assets",
    ],
    "pricing": [
        "SOFR", "LIBOR", "spread", "basis points", "margin",
        "commitment fee", "origination fee", "prepayment penalty",
    ],
}


def process_document(
    context_graph_json_path: Path,
    rules_path: Path = Path("rules.json"),
    config_path: Path = Path("config.json"),
) -> Tuple[DocumentKnowledgeGraph, HybridRetrievalRouter]:
    """
    Full pipeline: load ContextGraph JSON → build DKG → extract entities → build index.

    Returns:
        (dkg, router) ready for query execution
    """
    # Load ContextGraph JSON
    with open(context_graph_json_path, "r", encoding="utf-8") as f:
        raw = f.read()
    ctx = schema.ContextGraph.model_validate_json(raw)

    # Load config for retrieval settings
    retrieval_cfg: Dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        retrieval_cfg = cfg.get("retrieval", {})

    # Load entity keywords
    entity_keywords = load_entity_keywords(rules_path)
    graph_cfg = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            graph_cfg = json.load(f).get("graph", {})

    # Build DKG
    dkg = DocumentKnowledgeGraph(
        entity_keywords=entity_keywords,
        fuzzy_threshold=graph_cfg.get("fuzzy_threshold", 85),
    )
    dkg.build_from_context_graph(ctx)
    dkg.extract_entities()

    # Load configurable weights
    ct_weights, scope_mult = load_weights_from_config(config_path)

    # Build retrieval router
    ngram = retrieval_cfg.get("ngram_range", [1, 2])
    router = HybridRetrievalRouter(
        dkg,
        max_features=retrieval_cfg.get("max_features", 10000),
        ngram_range=tuple(ngram),
        top_k=retrieval_cfg.get("top_k", 15),
        min_score=retrieval_cfg.get("min_score", 0.01),
        content_type_weights=ct_weights,
        primary_scope_multiplier=scope_mult,
    )
    router.build_index()

    return dkg, router


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entity_id(category: str, keyword: str) -> str:
    raw = f"ENTITY_{category}_{keyword}".lower()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


# ============================================================================
# CLI Entry Point (for standalone testing)
# ============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Split-RAG Relationship Manager — DKG + Hybrid Retrieval"
    )
    parser.add_argument("--graph-json", type=Path, required=True, help="ContextGraph JSON path")
    parser.add_argument("--rules", type=Path, default=Path("rules.json"))
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    parser.add_argument("--query", type=str, default=None)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    dkg, router = process_document(args.graph_json, args.rules, args.config)
    print(f"DKG: {dkg.graph.number_of_nodes()} nodes, {dkg.graph.number_of_edges()} edges")

    if args.query:
        results = router.query(args.query)
        print(router.format_results_markdown(results))
        citations = router.get_bounding_box_citations(results)
        if citations:
            print(f"\nBounding-box citations: {len(citations)} regions")
