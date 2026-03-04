"""
Split-RAG Extension — Main Entry Point (v2)

End-to-end pipeline using v2 architecture:
    1. Extract PDF → ContextGraph via extractor.py (pdfplumber + pypdfium2)
    2. Build networkx Document Knowledge Graph via relationship_manager.py
    3. Extract entities via RapidFuzz / regex
    4. Build TF-IDF index (localized per-subgraph)
    5. Route and execute queries via deterministic hybrid retrieval

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

import schema_v2 as schema
from extractor import load_config, load_rules, setup_logging, process_file, run_extraction, extract_entities
from relationship_manager import (
    DocumentKnowledgeGraph,
    HybridRetrievalRouter,
    load_entity_keywords,
    load_weights_from_config,
)


class SplitRAGPipeline:
    """
    Orchestrates the full Split-RAG Extension v2 pipeline.

    Tier 1 — extractor.py: pdfplumber primary, pypdfium2 fallback
    Tier 2 — relationship_manager.py: DKG + hybrid retrieval router
    """

    def __init__(
        self,
        config_path: Path = Path("config.json"),
        rules_path: Path = Path("rules.json"),
    ) -> None:
        self.config_path = config_path
        self.rules_path = rules_path
        self._setup_logging(config_path)
        self.logger = logging.getLogger("SplitRAG.Pipeline")

        # Load config for retrieval settings
        self._raw_config = self._load_json(config_path)
        self._rules = self._load_json(rules_path)

        self._dkg: Optional[DocumentKnowledgeGraph] = None
        self._router: Optional[HybridRetrievalRouter] = None
        self._context_graph: Optional[schema.ContextGraph] = None

    @staticmethod
    def _load_json(path: Path) -> Dict:
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def _setup_logging(config_path: Path) -> None:
        cfg = {}
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        log_cfg = cfg.get("logging", {})
        logging.basicConfig(
            level=getattr(logging, log_cfg.get("level", "INFO")),
            format=log_cfg.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        )

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def ingest(self, file_path: Path) -> schema.ContextGraph:
        """
        Phase 1: Extract PDF → ContextGraph
        Phase 2: Build DKG + entity extraction + retrieval index
        """
        import hashlib

        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        file_bytes = file_path.read_bytes()
        file_hash = hashlib.md5(file_bytes).hexdigest()
        doc_id = schema.generate_document_id(file_bytes)

        self.logger.info("=== Phase 1: Extracting %s ===", file_path.name)

        # Entity anchoring
        ext_cfg = self._raw_config.get("extraction_settings", {})
        max_pages = ext_cfg.get("max_pages_for_entity_scan", 20)
        entities = extract_entities(file_path, max_pages, self._rules)

        # Extraction (pdfplumber → pypdfium2 fallback)
        nodes, primary_success, fallback_triggered = run_extraction(
            file_path, doc_id, file_hash, self.logger
        )

        if not nodes:
            raise RuntimeError(f"Ingestion produced zero nodes for {file_path}")

        self.logger.info("Extracted %d nodes, entities: %s", len(nodes), entities)

        # Assemble ContextGraph
        import time
        fallback_engine_name = "pypdfium2" if fallback_triggered else None
        metrics = schema.ExtractionMetrics(
            total_pages=max((n.metadata.page_number for n in nodes), default=0),
            total_nodes=len(nodes),
            tables_extracted=sum(1 for n in nodes if n.content_type == "table"),
            headers_extracted=sum(1 for n in nodes if n.content_type == "header"),
            conflicts_detected=0,
            extraction_time_seconds=0.0,
            primary_engine_used=primary_success,
            fallback_triggered=fallback_triggered,
            fallback_engine=fallback_engine_name,
        )

        self._context_graph = schema.ContextGraph(
            document_id=doc_id,
            filename=file_path.name,
            processed_at=schema.ContextGraph.get_current_timestamp(),
            borrower_entity=entities.get("borrower"),
            lender_entity=entities.get("lender"),
            guarantor_entity=entities.get("guarantor"),
            nodes=nodes,
            metrics=metrics,
        )

        # Phase 2: Build DKG
        self.logger.info("=== Phase 2: Building Document Knowledge Graph ===")
        entity_keywords = load_entity_keywords(self.rules_path)
        graph_cfg = self._raw_config.get("graph", {})

        self._dkg = DocumentKnowledgeGraph(
            entity_keywords=entity_keywords,
            fuzzy_threshold=graph_cfg.get("fuzzy_threshold", 85),
        )
        self._dkg.build_from_context_graph(self._context_graph)
        self._dkg.extract_entities()

        # Phase 3: Build retrieval index
        self.logger.info("=== Phase 3: Building Retrieval Index ===")
        ct_weights, scope_mult = load_weights_from_config(self.config_path)
        ret_cfg = self._raw_config.get("retrieval", {})
        ngram = ret_cfg.get("ngram_range", [1, 2])

        self._router = HybridRetrievalRouter(
            self._dkg,
            max_features=ret_cfg.get("max_features", 10000),
            ngram_range=tuple(ngram),
            top_k=ret_cfg.get("top_k", 15),
            min_score=ret_cfg.get("min_score", 0.01),
            content_type_weights=ct_weights,
            primary_scope_multiplier=scope_mult,
        )
        indexed = self._router.build_index()
        self.logger.info("Indexed %d chunks", indexed)

        self.logger.info(
            "Pipeline ready: %d nodes, %d pages",
            len(nodes),
            self._context_graph.metrics.total_pages if self._context_graph.metrics else 0,
        )
        return self._context_graph

    def query(
        self, query_text: str, top_k: int = 15
    ) -> List[Dict]:
        """Route and execute a query."""
        if not self._router or not self._router._fitted:
            raise RuntimeError("Pipeline not initialized — call ingest() first.")
        return self._router.query(query_text, top_k=top_k)

    def get_citation_payload(
        self, query_text: str, top_k: int = 15
    ) -> Dict:
        """Full retrieval with JSON citation payload."""
        if not self._router or not self._router._fitted:
            raise RuntimeError("Pipeline not initialized — call ingest() first.")
        return self._router.get_citation_payload(query_text, top_k=top_k)

    def export_graph(self, output_path: Path) -> None:
        """Export the ContextGraph to JSON."""
        if self._context_graph is None:
            raise RuntimeError("No document loaded — call ingest() first.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(self._context_graph.to_json())
        self.logger.info("Graph exported to %s", output_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Split-RAG Extension — API-Free Local RAG Pipeline (v2)"
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
    ctx = pipeline.ingest(args.file)
    print(f"\nDocument: {ctx.filename}")
    print(f"Pages: {ctx.metrics.total_pages if ctx.metrics else 'N/A'}")
    print(f"Nodes: {len(ctx.nodes)}")
    if ctx.borrower_entity:
        print(f"Borrower: {ctx.borrower_entity}")

    # Export
    if args.export_graph:
        pipeline.export_graph(args.export_graph)

    # Single query
    if args.query:
        print(f"\n--- Query: {args.query} ---\n")
        results = pipeline.query(args.query, top_k=args.top_k)
        print(HybridRetrievalRouter.format_results_markdown(results))

        citations = HybridRetrievalRouter.get_bounding_box_citations(results)
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
            print(HybridRetrievalRouter.format_results_markdown(results))
            print()


if __name__ == "__main__":
    main()
