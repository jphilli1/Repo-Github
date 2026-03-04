"""
Split-RAG Extension — Phase 2: In-Memory Document Knowledge Graph (DKG)

GraphConstructionService:
  - Builds a networkx.DiGraph from ChunkMetadata objects
  - Structural hierarchy: Document -> Page -> Section -> Chunk
  - Edges: HAS_PAGE, HAS_SECTION, HAS_CHILD, NEXT_BLOCK, CONTAINS_TABLE, MENTIONED_IN
  - Optional rule-based entity extraction via RapidFuzz

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections import defaultdict
from typing import Dict, List, Optional, Set, Tuple

import networkx as nx

from schemas import ChunkMetadata, EntityNode, GraphEdge

logger = logging.getLogger("SplitRAG.Graph")


class GraphConstructionService:
    """Builds and manages an in-memory Document Knowledge Graph using networkx."""

    def __init__(
        self,
        *,
        entity_keywords: Optional[Dict[str, List[str]]] = None,
        fuzzy_threshold: int = 85,
    ) -> None:
        self.graph: nx.DiGraph = nx.DiGraph()
        self._entity_keywords = entity_keywords or {}
        self._fuzzy_threshold = fuzzy_threshold
        self._rapidfuzz_available = self._probe_rapidfuzz()

    # ------------------------------------------------------------------
    # RapidFuzz probe
    # ------------------------------------------------------------------

    @staticmethod
    def _probe_rapidfuzz() -> bool:
        try:
            from rapidfuzz import fuzz  # noqa: F401
            return True
        except ImportError:
            logger.info("RapidFuzz not available — entity matching will use exact regex only.")
            return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build_graph(
        self,
        chunks: List[ChunkMetadata],
        document_id: str,
        filename: str,
    ) -> nx.DiGraph:
        """
        Construct the full DKG from a list of ChunkMetadata objects.

        Hierarchy:
            DOC_<id>
              └── PAGE_<n>
                    └── SECTION_<id> (inferred from headers)
                          └── CHUNK_<id>
        """
        self.graph.clear()

        # --- Document root node ---
        doc_node = f"DOC_{document_id}"
        self.graph.add_node(
            doc_node,
            node_type="document",
            label=filename,
            document_id=document_id,
        )

        # --- Group chunks by page ---
        pages: Dict[int, List[ChunkMetadata]] = defaultdict(list)
        for chunk in chunks:
            pages[chunk.page_number].append(chunk)

        # --- Build page & section hierarchy ---
        prev_chunk_id: Optional[str] = None
        current_section_id: Optional[str] = None

        for page_no in sorted(pages.keys()):
            # Reset per-page state: each page starts with clean context
            prev_chunk_id = None
            current_section_id = None

            page_node = f"PAGE_{document_id}_{page_no}"
            self.graph.add_node(
                page_node,
                node_type="page",
                page_number=page_no,
                document_id=document_id,
            )
            self.graph.add_edge(doc_node, page_node, edge_type="HAS_PAGE")

            page_chunks = sorted(pages[page_no], key=lambda c: c.reading_order_index)

            for chunk in page_chunks:
                chunk_node = chunk.node_id

                # Store full metadata on the graph node
                self.graph.add_node(
                    chunk_node,
                    node_type="chunk",
                    chunk_type=chunk.chunk_type,
                    raw_text=chunk.raw_text,
                    page_number=chunk.page_number,
                    bounding_boxes=[list(b) for b in chunk.bounding_boxes],
                    source_file_name=chunk.source_file_name,
                    extraction_method=chunk.extraction_method,
                    lineage_trace=chunk.lineage_trace,
                    source_scope=chunk.source_scope,
                    reading_order=chunk.reading_order_index,
                )

                # Section detection: headers start new sections
                if chunk.chunk_type in ("header", "title"):
                    section_id = f"SEC_{chunk.node_id}"
                    self.graph.add_node(
                        section_id,
                        node_type="section",
                        label=chunk.raw_text.strip()[:120],
                        page_number=chunk.page_number,
                    )
                    self.graph.add_edge(page_node, section_id, edge_type="HAS_SECTION")
                    current_section_id = section_id

                # Attach chunk to section or page
                if current_section_id:
                    self.graph.add_edge(
                        current_section_id, chunk_node, edge_type="HAS_CHILD"
                    )
                else:
                    self.graph.add_edge(page_node, chunk_node, edge_type="HAS_CHILD")

                # Table-specific edge
                if chunk.chunk_type == "table" and current_section_id:
                    self.graph.add_edge(
                        current_section_id, chunk_node, edge_type="CONTAINS_TABLE"
                    )

                # Sequential reading-order edge (per-page only)
                if prev_chunk_id:
                    self.graph.add_edge(
                        prev_chunk_id, chunk_node, edge_type="NEXT_BLOCK"
                    )
                prev_chunk_id = chunk_node

        logger.info(
            "Graph built: %d nodes, %d edges",
            self.graph.number_of_nodes(),
            self.graph.number_of_edges(),
        )
        return self.graph

    def extract_entities(self) -> List[EntityNode]:
        """
        Scan chunk nodes for keyword matches using RapidFuzz (fuzzy) or regex (exact).
        Creates MENTIONED_IN edges from entity nodes to chunk nodes.
        """
        if not self._entity_keywords:
            return []

        entities_found: List[EntityNode] = []
        entity_registry: Dict[str, EntityNode] = {}

        chunk_nodes = [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("node_type") == "chunk"
        ]

        for category, keywords in self._entity_keywords.items():
            for keyword in keywords:
                kw_lower = keyword.lower()

                for chunk_id, chunk_data in chunk_nodes:
                    text = chunk_data.get("raw_text", "").lower()
                    matched = False
                    score = 0.0

                    if self._rapidfuzz_available:
                        from rapidfuzz import fuzz

                        # Check partial ratio for substring fuzzy match
                        ratio = fuzz.partial_ratio(kw_lower, text)
                        if ratio >= self._fuzzy_threshold:
                            matched = True
                            score = float(ratio)
                    else:
                        # Exact substring fallback
                        if kw_lower in text:
                            matched = True
                            score = 100.0

                    if matched:
                        entity_id = _entity_id(category, keyword)
                        if entity_id not in entity_registry:
                            ent = EntityNode(
                                entity_id=entity_id,
                                entity_label=keyword,
                                entity_type=category,
                                matched_score=score,
                                source_chunk_ids=[chunk_id],
                            )
                            entity_registry[entity_id] = ent
                        else:
                            if chunk_id not in entity_registry[entity_id].source_chunk_ids:
                                entity_registry[entity_id].source_chunk_ids.append(chunk_id)
                            entity_registry[entity_id].matched_score = max(
                                entity_registry[entity_id].matched_score, score
                            )

                        # Add entity node + edge to graph
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
        logger.info("Entity extraction found %d unique entities", len(entities_found))
        return entities_found

    # ------------------------------------------------------------------
    # Subgraph access (used by SPLIT-RAG router)
    # ------------------------------------------------------------------

    def get_section_subgraphs(self) -> Dict[str, nx.DiGraph]:
        """
        Partition the graph into per-section subgraphs.
        Returns {section_label: subgraph}.
        """
        subgraphs: Dict[str, nx.DiGraph] = {}
        section_nodes = [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("node_type") == "section"
        ]
        for sec_id, sec_data in section_nodes:
            descendant_ids = nx.descendants(self.graph, sec_id) | {sec_id}
            sub = self.graph.subgraph(descendant_ids).copy()
            label = sec_data.get("label", sec_id)
            subgraphs[label] = sub
        return subgraphs

    def get_chunk_nodes(self) -> List[Tuple[str, Dict]]:
        """Return all chunk-type nodes with their data."""
        return [
            (nid, data)
            for nid, data in self.graph.nodes(data=True)
            if data.get("node_type") == "chunk"
        ]

    def get_table_subgraph(self) -> nx.DiGraph:
        """Return a subgraph containing only table chunks and their ancestors."""
        table_ids: Set[str] = set()
        for nid, data in self.graph.nodes(data=True):
            if data.get("chunk_type") == "table":
                table_ids.add(nid)
                table_ids.update(nx.ancestors(self.graph, nid))
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entity_id(category: str, keyword: str) -> str:
    raw = f"ENTITY_{category}_{keyword}".lower()
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
