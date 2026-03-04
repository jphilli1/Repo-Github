"""
Split-RAG Extension — Phase 3: Local TF-IDF Retrieval Engine

Uses scikit-learn TfidfVectorizer + cosine_similarity for lexical search.
No neural embeddings, no torch, no external APIs.

ABSOLUTE CONSTRAINT: No torch, transformers, llama-index, neo4j, openai, google-genai.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import networkx as nx
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from schemas import ChunkMetadata, RetrievalResult

logger = logging.getLogger("SplitRAG.Retrieval")


# ---------------------------------------------------------------------------
# Content-type weight multipliers (defaults, overridable via config.json)
# ---------------------------------------------------------------------------
DEFAULT_CHUNK_TYPE_WEIGHTS: Dict[str, float] = {
    "header": 3.0,
    "title": 3.0,
    "table": 2.5,
    "table_row": 2.0,
    "kv_pair": 2.0,
    "list_item": 1.2,
    "paragraph": 1.0,
    "image_caption": 0.8,
    "footer": 0.3,
}

DEFAULT_SCOPE_BOOST: Dict[str, float] = {
    "primary": 1.5,
    "corpus": 1.0,
}


def load_weights_from_config(
    config_path: Path = Path("config.json"),
) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Load content-type weights and scope boost from config.json.
    Falls back to built-in defaults if config is missing or incomplete.
    """
    chunk_weights = dict(DEFAULT_CHUNK_TYPE_WEIGHTS)
    scope_boost = dict(DEFAULT_SCOPE_BOOST)
    try:
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            weights_cfg = cfg.get("weights", {})
            if "content_type" in weights_cfg:
                chunk_weights.update(weights_cfg["content_type"])
            if "scope" in weights_cfg:
                scope_boost.update(weights_cfg["scope"])
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not load weights from config: %s", exc)
    return chunk_weights, scope_boost


class RetrievalService:
    """
    TF-IDF based retrieval engine operating on networkx graph chunk nodes.

    Workflow:
        1. Extract text corpus from graph chunk nodes
        2. Fit TfidfVectorizer on corpus
        3. Query → TF-IDF vector → cosine similarity ranking
        4. Apply content-type and scope weighting
        5. Return top-K RetrievalResult objects with bounding-box metadata
    """

    def __init__(
        self,
        *,
        max_features: int = 10000,
        ngram_range: Tuple[int, int] = (1, 2),
        top_k: int = 15,
        min_score: float = 0.01,
        chunk_type_weights: Optional[Dict[str, float]] = None,
        scope_boost: Optional[Dict[str, float]] = None,
    ) -> None:
        self._max_features = max_features
        self._ngram_range = ngram_range
        self._top_k = top_k
        self._min_score = min_score
        self._chunk_type_weights = chunk_type_weights or dict(DEFAULT_CHUNK_TYPE_WEIGHTS)
        self._scope_boost = scope_boost or dict(DEFAULT_SCOPE_BOOST)

        self._vectorizer: Optional[TfidfVectorizer] = None
        self._tfidf_matrix = None
        self._node_ids: List[str] = []
        self._node_data: List[Dict] = []
        self._fitted = False

    # ------------------------------------------------------------------
    # Index building
    # ------------------------------------------------------------------

    def build_index(self, graph: nx.DiGraph) -> int:
        """
        Build the TF-IDF index from all chunk nodes in the graph.
        Returns the number of indexed documents.
        """
        self._node_ids = []
        self._node_data = []
        corpus: List[str] = []

        for nid, data in graph.nodes(data=True):
            if data.get("node_type") != "chunk":
                continue
            text = data.get("raw_text", "")
            if not text.strip():
                continue
            self._node_ids.append(nid)
            self._node_data.append(data)
            corpus.append(text)

        if not corpus:
            logger.warning("No chunk text found — index is empty.")
            self._fitted = False
            return 0

        self._vectorizer = TfidfVectorizer(
            max_features=self._max_features,
            ngram_range=self._ngram_range,
            stop_words="english",
            sublinear_tf=True,
        )
        self._tfidf_matrix = self._vectorizer.fit_transform(corpus)
        self._fitted = True

        logger.info(
            "TF-IDF index built: %d documents, %d features",
            len(corpus),
            len(self._vectorizer.vocabulary_),
        )
        return len(corpus)

    def build_index_from_subgraph(self, subgraph: nx.DiGraph) -> int:
        """Build TF-IDF index restricted to a specific subgraph."""
        return self.build_index(subgraph)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def query(
        self,
        query_text: str,
        top_k: Optional[int] = None,
    ) -> List[RetrievalResult]:
        """
        Execute a TF-IDF similarity search against the indexed corpus.

        Steps:
            1. Vectorize query
            2. Compute cosine similarity
            3. Apply chunk-type and scope weighting
            4. Return sorted results with bounding-box provenance
        """
        if not self._fitted or self._vectorizer is None:
            logger.warning("Index not built — call build_index() first.")
            return []

        k = top_k or self._top_k

        # Transform query
        query_vec = self._vectorizer.transform([query_text])
        raw_scores = cosine_similarity(query_vec, self._tfidf_matrix).flatten()

        # Apply weights
        weighted_scores = np.zeros_like(raw_scores)
        for i, (score, data) in enumerate(zip(raw_scores, self._node_data)):
            if score < self._min_score:
                continue
            ctype = data.get("chunk_type", "paragraph")
            scope = data.get("source_scope", "primary")
            type_w = self._chunk_type_weights.get(ctype, 1.0)
            scope_w = self._scope_boost.get(scope, 1.0)
            weighted_scores[i] = score * type_w * scope_w

        # Rank
        top_indices = np.argsort(weighted_scores)[::-1][:k]

        results: List[RetrievalResult] = []
        for idx in top_indices:
            ws = float(weighted_scores[idx])
            if ws < self._min_score:
                break
            data = self._node_data[idx]
            bboxes = data.get("bounding_boxes", [])
            bbox_tuples = [tuple(b) for b in bboxes] if bboxes else []

            results.append(
                RetrievalResult(
                    node_id=self._node_ids[idx],
                    raw_text=data.get("raw_text", ""),
                    score=ws,
                    page_number=data.get("page_number", 0),
                    chunk_type=data.get("chunk_type", "paragraph"),
                    bounding_boxes=bbox_tuples,
                    source_file_name=data.get("source_file_name", ""),
                )
            )

        logger.info(
            "Query '%s' returned %d results (top score: %.4f)",
            query_text[:60],
            len(results),
            results[0].score if results else 0.0,
        )
        return results

    # ------------------------------------------------------------------
    # Scoped query (for SPLIT-RAG router)
    # ------------------------------------------------------------------

    def query_subgraph(
        self,
        query_text: str,
        subgraph: nx.DiGraph,
        top_k: Optional[int] = None,
    ) -> List[RetrievalResult]:
        """
        Build a temporary index on a subgraph and query it.
        Used by the SPLIT-RAG router for targeted retrieval.
        """
        temp_service = RetrievalService(
            max_features=self._max_features,
            ngram_range=self._ngram_range,
            top_k=top_k or self._top_k,
            min_score=self._min_score,
            chunk_type_weights=self._chunk_type_weights,
            scope_boost=self._scope_boost,
        )
        count = temp_service.build_index(subgraph)
        if count == 0:
            return []
        return temp_service.query(query_text, top_k=top_k)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    def is_fitted(self) -> bool:
        return self._fitted

    @property
    def chunk_count(self) -> int:
        """Number of indexed chunk nodes (not unique documents)."""
        return len(self._node_ids)

    def get_vocabulary_size(self) -> int:
        if self._vectorizer and hasattr(self._vectorizer, "vocabulary_"):
            return len(self._vectorizer.vocabulary_)
        return 0
