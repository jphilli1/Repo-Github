# AI-Native Split-RAG System v2.0 - Tier 2 Retrieval Engine
# "The Consumer" - Constrained Sandbox Logic
# Memory Limit: ~256MB | Network Access: None
# Allowed: Standard Library + pandas ONLY
# CANON_002: pandas is the ONLY permitted external dependency.

"""
Copilot Studio Tier 2: Deterministic Document Query Engine
Version: 2.0.0

SANDBOX CONSTRAINTS:
- External dependencies: pandas only
- Standard library: all permitted
- No file system writes
- No network access
- Max execution: 30 seconds
"""

import json
import re
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict

import pandas as pd


# ============================================================================
# CONFIGURATION
# ============================================================================

STOPWORDS = frozenset({
    "the", "is", "at", "which", "on", "and", "a", "an", "of", "to", "in",
    "for", "with", "by", "from", "as", "or", "be", "are", "was", "were",
    "been", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "must", "shall",
})

CONTENT_TYPE_WEIGHTS: Dict[str, float] = {
    "header": 3.0,
    "table": 2.5,
    "kv_pair": 2.0,
    "image_caption": 1.5,
    "text": 1.0,
}

PRIMARY_SCOPE_MULTIPLIER: float = 1.5
MAX_RESULTS: int = 15
SYSTEM_PREFIX: str = (
    "SYSTEM: You are a deterministic analysis assistant. Your response must "
    "be based strictly on the provided facts. Do not be creative. Do not "
    "hallucinate. Use temperature 0 behavior for this interaction.\n\n---\n\n"
)


# ============================================================================
# CORE FUNCTIONS
# ============================================================================

def simple_markdown_table(df: pd.DataFrame) -> str:
    """Generate Markdown table without tabulate dependency."""
    if df.empty:
        return "No data available."
    columns = df.columns.tolist()
    md = "| " + " | ".join(columns) + " |\n"
    md += "| " + " | ".join(["---"] * len(columns)) + " |\n"
    for _, row in df.iterrows():
        clean_row = []
        for val in row:
            s = str(val).replace("|", "\\|")
            if len(s) > 500:
                s = s[:497] + "..."
            clean_row.append(s)
        md += "| " + " | ".join(clean_row) + " |\n"
    return md


def preprocess_query(query: str) -> List[str]:
    """Lowercase, remove punctuation, filter stopwords, deduplicate."""
    q = re.sub(r"[^\w\s]", "", query.lower())
    tokens = q.split()
    return list(dict.fromkeys(t for t in tokens if t not in STOPWORDS))


def load_context_graph(json_string: str) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """Parse ContextGraph JSON, flatten metadata into DataFrame."""
    try:
        data = json.loads(json_string)
    except json.JSONDecodeError:
        return {}, pd.DataFrame()

    graph_meta = {
        "document_id": data.get("document_id"),
        "filename": data.get("filename"),
        "borrower_entity": data.get("borrower_entity"),
        "lender_entity": data.get("lender_entity"),
        "guarantor_entity": data.get("guarantor_entity"),
        "schema_version": data.get("schema_version"),
    }

    nodes = data.get("nodes", [])
    if not nodes:
        return graph_meta, pd.DataFrame()

    flat_nodes = []
    for n in nodes:
        meta = n.get("metadata", {})
        row = {
            "chunk_id": n.get("chunk_id"),
            "content_type": n.get("content_type"),
            "content": n.get("content", ""),
            "page_number": meta.get("page_number"),
            "bbox": meta.get("bbox"),
            "source_scope": meta.get("source_scope", "primary"),
            "is_active": meta.get("is_active", True),
            "content_lower": n.get("content", "").lower(),
        }
        flat_nodes.append(row)

    return graph_meta, pd.DataFrame(flat_nodes)


# ============================================================================
# RETRIEVAL
# ============================================================================

def calculate_keyword_density(
    df: pd.DataFrame, keywords: List[str]
) -> pd.DataFrame:
    """Score nodes by keyword density with content-type and scope weighting."""
    if df.empty or not keywords:
        df["score"] = 0.0
        return df

    active_df = df[df["is_active"] == True].copy()
    active_df["score"] = 0.0

    for kw in keywords:
        active_df["score"] += active_df["content_lower"].str.count(re.escape(kw))

    active_df["type_weight"] = active_df["content_type"].map(CONTENT_TYPE_WEIGHTS).fillna(1.0)
    active_df["score"] *= active_df["type_weight"]
    active_df.loc[active_df["source_scope"] == "primary", "score"] *= PRIMARY_SCOPE_MULTIPLIER

    return active_df.sort_values(by="score", ascending=False)


def resolve_entity_query(
    query: str, graph_meta: Dict[str, Any]
) -> Tuple[str, Optional[str]]:
    """Check if query targets an anchored entity and return direct answer."""
    q = query.lower()
    for role in ("borrower", "lender", "guarantor"):
        if role in q and any(w in q for w in ("who", "name", "identify")):
            entity = graph_meta.get(f"{role}_entity")
            if entity:
                return query, f"The {role.title()} is **{entity}** (extracted from contract preamble)."
            return query + f" {role}", None
    return query, None


# ============================================================================
# ENTRY POINTS
# ============================================================================

def retrieve_context(
    query: str, context_json_str: str, max_results: int = MAX_RESULTS
) -> str:
    """Mode A: Conversational retrieval for Copilot Studio."""
    try:
        meta, df = load_context_graph(context_json_str)
        if df.empty:
            return f"{SYSTEM_PREFIX}Error: Context Graph is empty or invalid."

        mod_query, direct_ans = resolve_entity_query(query, meta)
        if direct_ans:
            return direct_ans

        keywords = preprocess_query(mod_query)
        if not keywords:
            return f"{SYSTEM_PREFIX}Please provide more specific keywords."

        scored = calculate_keyword_density(df, keywords)
        results = scored[scored["score"] > 0].head(max_results)

        if results.empty:
            return f"{SYSTEM_PREFIX}No matches for: {', '.join(keywords)}"

        display = results[["score", "content_type", "page_number", "bbox", "content"]].copy()

        output = f"{SYSTEM_PREFIX}**Found {len(display)} results for '{mod_query}'**\n\n"
        output += simple_markdown_table(display)
        return output

    except Exception as e:
        return f"{SYSTEM_PREFIX}System Error: {str(e)}"


def generate_data_mart(
    context_json_str: str,
    content_types: Optional[List[str]] = None,
    output_format: str = "csv",
) -> str:
    """Mode B: Data Mart Export (CSV only in sandbox)."""
    try:
        if content_types is None:
            content_types = ["table", "kv_pair"]

        meta, df = load_context_graph(context_json_str)
        if df.empty:
            return "Error: Context Graph is empty."

        mask = df["content_type"].isin(content_types) & (df["is_active"] == True)
        filtered = df[mask]

        if filtered.empty:
            return f"No content for types: {', '.join(content_types)}"

        export = filtered[["chunk_id", "page_number", "content_type", "bbox", "content"]]
        return f"SUCCESS: {len(export)} rows.\n\n{export.to_csv(index=False)}"

    except Exception as e:
        return f"System Error: {str(e)}"


def get_document_summary(json_str: str) -> str:
    """Returns structural summary of the document."""
    try:
        meta, df = load_context_graph(json_str)
        if df.empty:
            return "Empty graph."

        stats = df["content_type"].value_counts().to_string()
        entities = (
            f"Borrower: {meta.get('borrower_entity')}\n"
            f"Lender: {meta.get('lender_entity')}\n"
            f"Guarantor: {meta.get('guarantor_entity')}"
        )
        bbox_count = df["bbox"].apply(lambda x: x is not None and len(x) == 4).sum()

        return (
            f"{SYSTEM_PREFIX}## Document Summary\n\n"
            f"**File:** {meta.get('filename')}\n\n"
            f"**Entities:**\n{entities}\n\n"
            f"**Node Stats:**\n{stats}\n\n"
            f"**Nodes with bounding boxes:** {bbox_count}/{len(df)}\n"
        )
    except Exception as e:
        return f"Error: {str(e)}"


# ============================================================================
# SELF-TEST
# ============================================================================

if __name__ == "__main__":
    mock_json = json.dumps({
        "document_id": "a" * 32,
        "filename": "test_loan.pdf",
        "processed_at": "2026-01-01T00:00:00Z",
        "borrower_entity": "Acme Corp",
        "lender_entity": "Big Bank",
        "nodes": [
            {
                "chunk_id": "b" * 32,
                "content_type": "header",
                "content": "Definitions",
                "lineage_trace": "c" * 64,
                "metadata": {
                    "page_number": 1,
                    "bbox": [72.0, 50.0, 540.0, 80.0],
                    "source_scope": "primary",
                    "extraction_method": "pdfplumber",
                },
            },
            {
                "chunk_id": "d" * 32,
                "content_type": "text",
                "content": "The Interest Rate is SOFR + 250 basis points.",
                "lineage_trace": "e" * 64,
                "metadata": {
                    "page_number": 2,
                    "bbox": [72.0, 100.0, 540.0, 150.0],
                    "source_scope": "primary",
                    "extraction_method": "pdfplumber",
                },
            },
            {
                "chunk_id": "f" * 32,
                "content_type": "table",
                "content": "| Item | Cost |\n| --- | --- |\n| Tax | 100 |",
                "lineage_trace": "a1" + "b" * 62,
                "metadata": {
                    "page_number": 3,
                    "bbox": [72.0, 200.0, 540.0, 350.0],
                    "source_scope": "primary",
                    "extraction_method": "pdfplumber",
                },
            },
        ],
    })

    print("=" * 60)
    print("[Test 1] Preprocessing")
    print(preprocess_query("What is the Interest Rate?"))

    print("\n[Test 2] Entity Query")
    meta, _ = load_context_graph(mock_json)
    print(resolve_entity_query("who is the borrower?", meta))

    print("\n[Test 3] Retrieval")
    print(retrieve_context("interest rate", mock_json))

    print("\n[Test 4] Document Summary")
    print(get_document_summary(mock_json))

    print("\n--- Self-Test Complete ---")
