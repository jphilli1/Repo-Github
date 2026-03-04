"""
Split-RAG Extension — Main Entry Point

End-to-end pipeline:
    1. Ingest PDF → ChunkMetadata with bounding boxes
    2. Build networkx Document Knowledge Graph
    3. Extract entities via RapidFuzz / regex
    4. Build TF-IDF index
    5. Route and execute queries via deterministic SPLIT-RAG

Usage:
    python main.py --file input/document.pdf --query "What is the NPL ratio?"
    python main.py --file input/document.pdf --interactive
    python main.py --file input/document.pdf --export-graph output/graph.json

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional

from schemas import DocumentGraph, RetrievalResult
from ingestion_service import DocumentIngestionService
from graph_service import GraphConstructionService
from retrieval_service import RetrievalService
from split_rag_router import SplitRAGRouter
from entity_matcher import load_entity_keywords


def setup_logging(config: Dict) -> None:
    log_cfg = config.get("logging", {})
    logging.basicConfig(
        level=getattr(logging, log_cfg.get("level", "INFO")),
        format=log_cfg.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    )


def load_config(config_path: Path) -> Dict:
    if not config_path.exists():
        logging.warning("Config not found at %s — using defaults.", config_path)
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_rules(rules_path: Path) -> Dict:
    if not rules_path.exists():
        logging.warning("Rules not found at %s — using defaults.", rules_path)
        return {}
    with open(rules_path, "r", encoding="utf-8") as f:
        return json.load(f)


class SplitRAGPipeline:
    """
    Orchestrates the full Split-RAG Extension pipeline.
    """

    def __init__(
        self,
        config_path: Path = Path("config.json"),
        rules_path: Path = Path("rules.json"),
    ) -> None:
        self.config = load_config(config_path)
        self.rules = load_rules(rules_path)
        setup_logging(self.config)

        self.logger = logging.getLogger("SplitRAG.Pipeline")

        # Services
        ing_cfg = self.config.get("ingestion", {})
        self.ingestion = DocumentIngestionService(
            use_docling=ing_cfg.get("use_docling", True),
            enable_ocr=ing_cfg.get("enable_ocr", False),
            enable_tables=ing_cfg.get("enable_table_detection", True),
            use_gpu=ing_cfg.get("use_gpu", False),
            entity_scan_pages=ing_cfg.get("entity_scan_pages", 20),
            entity_rules=self.rules,
        )

        graph_cfg = self.config.get("graph", {})
        entity_keywords = load_entity_keywords(rules_path)
        self.graph_service = GraphConstructionService(
            entity_keywords=entity_keywords,
            fuzzy_threshold=graph_cfg.get("fuzzy_threshold", 85),
        )

        ret_cfg = self.config.get("retrieval", {})
        ngram = ret_cfg.get("ngram_range", [1, 2])
        self.retrieval = RetrievalService(
            max_features=ret_cfg.get("max_features", 10000),
            ngram_range=tuple(ngram),
            top_k=ret_cfg.get("top_k", 15),
            min_score=ret_cfg.get("min_score", 0.01),
        )

        router_cfg = self.config.get("router", {})
        self.router = SplitRAGRouter(
            self.graph_service,
            self.retrieval,
            fallback_top_k=router_cfg.get("fallback_top_k", 15),
        )

        self._document_graph: Optional[DocumentGraph] = None

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def ingest(self, file_path: Path) -> DocumentGraph:
        """Phase 1+2: Ingest document and build knowledge graph."""
        self.logger.info("=== Phase 1: Ingesting %s ===", file_path.name)
        chunks, entities = self.ingestion.ingest(file_path)

        if not chunks:
            self.logger.error("No chunks extracted from %s", file_path.name)
            raise RuntimeError(f"Ingestion produced zero chunks for {file_path}")

        from schemas import generate_document_id
        file_bytes = file_path.read_bytes()
        doc_id = generate_document_id(file_bytes)

        self.logger.info(
            "Extracted %d chunks, entities: %s", len(chunks), entities
        )

        # Phase 2: Build graph
        self.logger.info("=== Phase 2: Building Document Knowledge Graph ===")
        self.graph_service.build_graph(chunks, doc_id, file_path.name)

        # Entity extraction
        self.logger.info("=== Phase 2b: Entity Extraction ===")
        entity_nodes = self.graph_service.extract_entities()

        # Build TF-IDF index on full graph
        self.logger.info("=== Phase 3: Building TF-IDF Index ===")
        indexed = self.retrieval.build_index(self.graph_service.graph)
        self.logger.info("Indexed %d chunk documents", indexed)

        # Initialize router subgraphs
        self.router.initialize()

        # Build DocumentGraph container
        from schemas import GraphEdge
        edges = []
        for u, v, data in self.graph_service.graph.edges(data=True):
            edges.append(GraphEdge(
                source_id=str(u),
                target_id=str(v),
                edge_type=data.get("edge_type", "HAS_CHILD"),
            ))

        self._document_graph = DocumentGraph(
            document_id=doc_id,
            filename=file_path.name,
            borrower_entity=entities.get("borrower"),
            chunks=chunks,
            entities=entity_nodes,
            edges=edges,
            total_pages=max((c.page_number for c in chunks), default=0),
        )

        self.logger.info(
            "Pipeline ready: %d chunks, %d entities, %d edges, %d pages",
            len(chunks),
            len(entity_nodes),
            len(edges),
            self._document_graph.total_pages,
        )
        return self._document_graph

    def query(
        self, query_text: str, top_k: int = 15
    ) -> List[RetrievalResult]:
        """Phase 4: Route and execute a query."""
        if not self.retrieval.is_fitted:
            raise RuntimeError("Pipeline not initialized — call ingest() first.")
        return self.router.query(query_text, top_k=top_k)

    def multi_hop_query(
        self, query_text: str, hops: int = 2, top_k: int = 15
    ) -> List[RetrievalResult]:
        """Multi-hop reasoning query."""
        if not self.retrieval.is_fitted:
            raise RuntimeError("Pipeline not initialized — call ingest() first.")
        return self.router.multi_hop_query(query_text, hops=hops, top_k=top_k)

    def export_graph(self, output_path: Path) -> None:
        """Export the DocumentGraph to JSON."""
        if self._document_graph is None:
            raise RuntimeError("No document loaded — call ingest() first.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(self._document_graph.to_json())
        self.logger.info("Graph exported to %s", output_path)

    def get_citations(self, results: List[RetrievalResult]) -> List[Dict]:
        """Get UI bounding-box citation data from results."""
        return SplitRAGRouter.get_bounding_box_citations(results)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split-RAG Extension — API-Free Local RAG Pipeline"
    )
    parser.add_argument(
        "--file", type=Path, required=True, help="Path to PDF file to ingest"
    )
    parser.add_argument(
        "--config", type=Path, default=Path("config.json"), help="Config file path"
    )
    parser.add_argument(
        "--rules", type=Path, default=Path("rules.json"), help="Rules file path"
    )
    parser.add_argument(
        "--query", type=str, default=None, help="Query to execute after ingestion"
    )
    parser.add_argument(
        "--multi-hop", action="store_true", help="Use multi-hop retrieval"
    )
    parser.add_argument(
        "--top-k", type=int, default=15, help="Number of results to return"
    )
    parser.add_argument(
        "--export-graph", type=Path, default=None, help="Export graph JSON to path"
    )
    parser.add_argument(
        "--interactive", action="store_true", help="Enter interactive query mode"
    )
    args = parser.parse_args()

    pipeline = SplitRAGPipeline(config_path=args.config, rules_path=args.rules)

    # Ingest
    doc_graph = pipeline.ingest(args.file)
    print(f"\nDocument: {doc_graph.filename}")
    print(f"Pages: {doc_graph.total_pages}")
    print(f"Chunks: {len(doc_graph.chunks)}")
    print(f"Entities: {len(doc_graph.entities)}")
    if doc_graph.borrower_entity:
        print(f"Borrower: {doc_graph.borrower_entity}")

    # Export
    if args.export_graph:
        pipeline.export_graph(args.export_graph)

    # Single query
    if args.query:
        print(f"\n--- Query: {args.query} ---\n")
        if args.multi_hop:
            results = pipeline.multi_hop_query(args.query, top_k=args.top_k)
        else:
            results = pipeline.query(args.query, top_k=args.top_k)
        print(SplitRAGRouter.format_results_markdown(results))

        citations = pipeline.get_citations(results)
        if citations:
            print(f"\nBounding-box citations: {len(citations)} regions")

    # Interactive mode
    if args.interactive:
        print("\n=== Interactive Query Mode (type 'quit' to exit) ===\n")
        while True:
            try:
                user_input = input("Query> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if user_input.lower() in ("quit", "exit", "q"):
                break
            if not user_input:
                continue

            results = pipeline.query(user_input, top_k=args.top_k)
            print(SplitRAGRouter.format_results_markdown(results))
            print()


if __name__ == "__main__":
    main()
