# [EXACT] — Complete copilot_tier2.py implementation
# This entire script must be copied into Copilot Studio as a single code block
# Per CANON_002: Only pandas external dependency. All stdlib permitted.

"""
Copilot Studio Tier 2: Deterministic Document Query Engine
Version: 1.0.0

SANDBOX CONSTRAINTS:
- External dependencies: pandas only
- Standard library: all permitted
- No file system writes
- No network access
- Max execution: 30 seconds
"""

import json
import re
import sys
import io
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict

import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

SYSTEM_PREFIX = """SYSTEM: You are a deterministic analysis assistant. Your response must be based strictly on the provided facts. Do not be creative. Do not hallucinate. Use temperature 0 behavior for this interaction.

---

"""

STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "are", "was", "were", "be", "been",
    "being", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "must", "shall", "can", "this",
    "that", "these", "those", "it", "its", "what", "which", "who", "whom",
    "how", "when", "where", "why", "all", "each", "every", "both", "few",
    "more", "most", "other", "some", "such", "no", "nor", "not", "only",
    "own", "same", "so", "than", "too", "very", "just", "also", "now",
}

MAX_OUTPUT_CHARS = 6000
NODE_TYPE_BOOST = {"table": 0.25, "section": 0.10}
HIGH_CONFIDENCE_BOOST = 0.10


# ============================================================================
# CORE FUNCTIONS
# ============================================================================


def load_context_graph(json_content: str) -> Dict[str, Any]:
    """
    Parse JSON content into context graph structure.

    Args:
        json_content: Raw JSON string from SharePoint file

    Returns:
        Parsed context graph dictionary

    Raises:
        ValueError: If JSON is invalid or missing required fields
    """
    try:
        data = json.loads(json_content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {str(e)}")

    if "nodes" not in data:
        raise ValueError("Missing 'nodes' field in context graph")

    return data


def get_node_content(node: Dict[str, Any]) -> str:
    """Extract text content from a node based on its type."""
    if node.get("text") and node["text"].get("text"):
        return node["text"]["text"]
    elif node.get("table") and node["table"].get("csv_text"):
        return node["table"]["csv_text"]
    elif node.get("title"):
        return node["title"]
    return ""


def get_node_page(node: Dict[str, Any]) -> Optional[int]:
    """Extract page number from node location."""
    location = node.get("location")
    if location and isinstance(location, dict):
        return location.get("page")
    return None


def get_node_confidence(node: Dict[str, Any]) -> float:
    """Extract confidence from node provenance."""
    provenance = node.get("provenance")
    if provenance and isinstance(provenance, dict):
        return provenance.get("confidence", 0.0)
    return 0.0


def get_document_node(nodes: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find the document node in the node list."""
    for node in nodes:
        if node.get("node_type") == "document":
            return node
    return None


def get_document_name(data: Dict[str, Any]) -> str:
    """Extract document name from context graph."""
    nodes = data.get("nodes", [])
    doc_node = get_document_node(nodes)
    if doc_node:
        if doc_node.get("title"):
            return doc_node["title"]
        file_ref = doc_node.get("file_ref", {})
        if file_ref.get("file_name"):
            return file_ref["file_name"]
    return "Unknown"


def extract_keywords(query: str) -> List[str]:
    """
    Extract search keywords from user query.

    Algorithm:
        1. Convert to lowercase
        2. Split on non-word characters
        3. Remove stopwords
        4. Keep tokens with length >= 3
        5. Return unique keywords

    Example:
        Input: "What is the NPL ratio for Q3 2024?"
        Output: ["npl", "ratio", "2024"]
    """
    query_lower = query.lower()
    tokens = re.split(r"\W+", query_lower)

    keywords = [
        token
        for token in tokens
        if token and len(token) >= 3 and token not in STOPWORDS
    ]

    seen = set()
    unique_keywords = []
    for kw in keywords:
        if kw not in seen:
            seen.add(kw)
            unique_keywords.append(kw)

    return unique_keywords


def score_nodes(nodes: List[Dict[str, Any]], keywords: List[str]) -> pd.DataFrame:
    """
    Score nodes by keyword relevance using density-based ranking.

    Algorithm:
        1. Create DataFrame from nodes
        2. Extract content and normalize to lowercase
        3. Count keyword hits in each node
        4. Calculate word count
        5. Compute density = hits / word_count
        6. Apply node_type boost
        7. Apply high confidence boost
        8. Final score = density * (1 + type_boost) * (1 + confidence_boost)

    Returns:
        DataFrame with columns: [original node fields..., score]
    """
    if not nodes:
        return pd.DataFrame()

    # Filter to content-bearing nodes (chunk, table, section)
    content_nodes = [
        n for n in nodes if n.get("node_type") in ("chunk", "table", "section")
    ]

    if not content_nodes:
        return pd.DataFrame()

    # Build records for DataFrame
    records = []
    for node in content_nodes:
        content = get_node_content(node)
        records.append({
            "node_id": node.get("node_id", ""),
            "node_type": node.get("node_type", ""),
            "title": node.get("title"),
            "parent_id": node.get("parent_id"),
            "content": content,
            "page": get_node_page(node),
            "confidence": get_node_confidence(node),
            "raw_node": node,
        })

    df = pd.DataFrame(records)

    if df.empty:
        return df

    df["content_lower"] = df["content"].str.lower()

    def count_hits(text: str, kws: List[str]) -> int:
        if not isinstance(text, str):
            return 0
        return sum(text.count(kw) for kw in kws)

    df["raw_hits"] = df["content_lower"].apply(lambda x: count_hits(x, keywords))
    df["word_count"] = df["content_lower"].str.split().str.len().clip(lower=1)
    df["density"] = df["raw_hits"] / df["word_count"]
    df["type_boost"] = df["node_type"].map(NODE_TYPE_BOOST).fillna(0)
    df["confidence_boost"] = df["confidence"].apply(
        lambda x: HIGH_CONFIDENCE_BOOST if x >= 0.9 else 0
    )
    df["score"] = df["density"] * (1 + df["type_boost"]) * (1 + df["confidence_boost"])
    df = df.sort_values("score", ascending=False)

    return df


def select_coherent_nodes(
    scored_df: pd.DataFrame, max_chars: int = MAX_OUTPUT_CHARS
) -> pd.DataFrame:
    """
    Select top-scoring nodes from best parent section.

    Strategy:
        1. Find parent_id with highest aggregate score
        2. Return only nodes from that section
        3. Order: section → chunk → table
        4. Truncate if exceeds max_chars
    """
    if scored_df.empty:
        return scored_df

    relevant = scored_df[scored_df["score"] > 0].copy()

    if relevant.empty:
        return scored_df.head(3)

    # Group by parent_id and find best section
    section_scores = relevant.groupby("parent_id")["score"].sum()
    best_section = section_scores.idxmax()

    section_nodes = relevant[relevant["parent_id"] == best_section].copy()

    type_order = {"section": 0, "chunk": 1, "table": 2}
    section_nodes["type_order"] = section_nodes["node_type"].map(type_order).fillna(3)
    section_nodes = section_nodes.sort_values(
        ["type_order", "score"], ascending=[True, False]
    )

    selected = []
    total_chars = 0

    for _, row in section_nodes.iterrows():
        content_len = len(row["content"]) if row["content"] else 0
        if total_chars + content_len > max_chars and selected:
            if row["node_type"] != "section":
                continue
        selected.append(row)
        total_chars += content_len

    return pd.DataFrame(selected)


def format_evidence_table(nodes_df: pd.DataFrame) -> str:
    """
    Format node metadata as Markdown evidence table.

    Columns: node_id (truncated) | page | type | confidence | chars
    """
    if nodes_df.empty:
        return "No relevant nodes found."

    lines = ["| Node ID | Page | Type | Confidence | Chars |"]
    lines.append("|---|---|---|---|---|")

    for _, row in nodes_df.iterrows():
        node_id = str(row.get("node_id", "N/A"))[:16] + "..."
        page = row.get("page") or "N/A"
        ntype = row.get("node_type", "unknown")
        conf = row.get("confidence", 0)
        chars = len(row.get("content", "")) if row.get("content") else 0

        lines.append(f"| {node_id} | {page} | {ntype} | {conf:.2f} | {chars} |")

    return "\n".join(lines)


def format_excerpts(nodes_df: pd.DataFrame) -> str:
    """Format node content as readable excerpts."""
    if nodes_df.empty:
        return ""

    excerpts = []

    for _, row in nodes_df.iterrows():
        ntype = row.get("node_type", "chunk")
        content = row.get("content", "")
        title = row.get("title")

        if ntype == "section":
            excerpts.append(f"## {content if content else title}")
        elif ntype == "table":
            section_title = title or "Data Table"
            excerpts.append(f"**[Table: {section_title}]**\n\n```\n{content}\n```")
        else:
            excerpts.append(content)

    return "\n\n".join(excerpts)


def get_parent_title(nodes: List[Dict[str, Any]], parent_id: str) -> str:
    """Look up parent node title by ID."""
    for node in nodes:
        if node.get("node_id") == parent_id:
            return node.get("title") or "Unknown Section"
    return "Unknown Section"


def query_context_graph(json_content: str, user_query: str) -> str:
    """
    Main entry point for Copilot Studio queries.

    Args:
        json_content: Raw JSON string from SharePoint
        user_query: Natural language query from user

    Returns:
        Formatted Markdown response with SYSTEM prefix
    """
    try:
        context_graph = load_context_graph(json_content)
    except ValueError as e:
        return f"{SYSTEM_PREFIX}**Error:** {str(e)}"

    nodes = context_graph.get("nodes", [])
    doc_name = get_document_name(context_graph)

    if not nodes:
        return f"{SYSTEM_PREFIX}**No content found in document.**"

    keywords = extract_keywords(user_query)

    if not keywords:
        return f"{SYSTEM_PREFIX}**Query too broad.** Please ask a more specific question about: {doc_name}"

    scored_df = score_nodes(nodes, keywords)
    selected_df = select_coherent_nodes(scored_df)

    evidence = format_evidence_table(selected_df)
    excerpts = format_excerpts(selected_df)

    matched_section = "None"
    if not selected_df.empty:
        first_row = selected_df.iloc[0]
        parent_id = first_row.get("parent_id")
        if parent_id:
            matched_section = get_parent_title(nodes, parent_id)
        elif first_row.get("title"):
            matched_section = first_row.get("title")

    output = f"""{SYSTEM_PREFIX}**Document:** {doc_name}
**Query Keywords:** {', '.join(keywords)}
**Matched Section:** {matched_section}

### Evidence Sources

{evidence}

### Relevant Content

{excerpts}
"""

    return output


def get_document_summary(json_content: str) -> str:
    """Generate a summary of the document structure."""
    try:
        context_graph = load_context_graph(json_content)
    except ValueError as e:
        return f"{SYSTEM_PREFIX}**Error:** {str(e)}"

    nodes = context_graph.get("nodes", [])
    stats = context_graph.get("stats", {})
    doc_name = get_document_name(context_graph)

    # Find document node for metadata
    doc_node = get_document_node(nodes)
    file_ref = doc_node.get("file_ref", {}) if doc_node else {}

    type_counts = defaultdict(int)
    sections = set()
    for node in nodes:
        type_counts[node.get("node_type", "unknown")] += 1
        if node.get("node_type") == "section" and node.get("title"):
            sections.add(node["title"])

    output = f"""{SYSTEM_PREFIX}## Document Summary

**Name:** {doc_name}
**Type:** {file_ref.get('file_ext', 'unknown').upper()}
**Size:** {file_ref.get('file_size_bytes', 'N/A')} bytes
**Extractor:** {stats.get('extractor_used', 'unknown')}
**Pages:** {stats.get('page_count', 'N/A')}

### Content Statistics

| Node Type | Count |
|---|---|
"""

    for ntype, count in sorted(type_counts.items()):
        output += f"| {ntype} | {count} |\n"

    output += f"""
**Total Nodes:** {len(nodes)}
**Total Edges:** {len(context_graph.get('edges', []))}
**Total Characters:** {stats.get('total_chars', 'N/A')}

### Sections Found

"""

    for section in sorted(sections):
        output += f"- {section}\n"

    return output


def list_sections(json_content: str) -> str:
    """List all sections in the document with their content counts."""
    try:
        context_graph = load_context_graph(json_content)
    except ValueError as e:
        return f"{SYSTEM_PREFIX}**Error:** {str(e)}"

    nodes = context_graph.get("nodes", [])
    doc_name = get_document_name(context_graph)

    # Build section stats by looking at parent relationships
    section_stats = defaultdict(lambda: {"count": 0, "chars": 0, "types": defaultdict(int)})

    # First, identify all section nodes
    section_nodes = {n["node_id"]: n for n in nodes if n.get("node_type") == "section"}

    for node in nodes:
        parent_id = node.get("parent_id")
        if parent_id and parent_id in section_nodes:
            section_title = section_nodes[parent_id].get("title", "Unknown")
            section_stats[section_title]["count"] += 1
            content = get_node_content(node)
            section_stats[section_title]["chars"] += len(content)
            section_stats[section_title]["types"][node.get("node_type", "unknown")] += 1

    output = f"""{SYSTEM_PREFIX}## Sections in {doc_name}

| Section | Nodes | Characters | Content Types |
|---|---|---|---|
"""

    for section, stats in sorted(section_stats.items()):
        types_str = ", ".join(f"{t}:{c}" for t, c in stats["types"].items())
        output += f"| {section[:50]} | {stats['count']} | {stats['chars']} | {types_str} |\n"

    return output


def search_tables(json_content: str, search_term: str = "") -> str:
    """Find and return tables from the document."""
    try:
        context_graph = load_context_graph(json_content)
    except ValueError as e:
        return f"{SYSTEM_PREFIX}**Error:** {str(e)}"

    nodes = context_graph.get("nodes", [])
    doc_name = get_document_name(context_graph)

    tables = [n for n in nodes if n.get("node_type") == "table"]

    if search_term:
        search_lower = search_term.lower()
        tables = [
            t for t in tables if search_lower in get_node_content(t).lower()
        ]

    if not tables:
        return f"{SYSTEM_PREFIX}**No tables found** matching criteria."

    output = f"""{SYSTEM_PREFIX}## Tables in {doc_name}

Found **{len(tables)}** table(s).

"""

    for i, table in enumerate(tables[:5], 1):
        # Find parent section title
        parent_id = table.get("parent_id")
        section_title = get_parent_title(nodes, parent_id) if parent_id else "Unknown"
        content = get_node_content(table)

        if len(content) > 1000:
            content = content[:1000] + "\n\n... (truncated)"

        output += f"""### Table {i}: {section_title}


    if len(tables) > 5:
        output += f"*Showing 5 of {len(tables)} tables. Refine your search for specific results.*"

    return output


# ============================================================================
# COPILOT STUDIO ENTRY POINT
# ============================================================================


def main(
    json_path: str = None, file_content_str: str = None, user_query: str = ""
) -> str:
    """
    Copilot Studio entry point.

    Args:
        json_path: Path to JSON file (if file system accessible)
        file_content_str: Raw JSON string (if passed directly)
        user_query: User's natural language query

    Returns:
        Formatted response string
    """
    if file_content_str:
        json_content = file_content_str
    elif json_path:
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                json_content = f.read()
        except Exception as e:
            return f"{SYSTEM_PREFIX}**Error reading file:** {str(e)}"
    else:
        return f"{SYSTEM_PREFIX}**Error:** No document provided."

    query_lower = user_query.lower().strip()

    if query_lower in ["summary", "overview", "describe"]:
        return get_document_summary(json_content)

    if query_lower in ["sections", "list sections", "toc"]:
        return list_sections(json_content)

    if query_lower.startswith("tables") or query_lower.startswith("find tables"):
        search_term = (
            user_query.replace("tables", "").replace("find tables", "").strip()
        )
        return search_tables(json_content, search_term)

    return query_context_graph(json_content, user_query)


# ============================================================================
# FOR DIRECT TESTING
# ============================================================================

if __name__ == "__main__":
    sample_json = '''
    {
        "schema_version": "1.0.0",
        "run_id": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
        "created_utc": "2025-01-22T10:30:00Z",
        "nodes": [
            {
                "node_id": "1111111111111111111111111111111111111111111111111111111111111111",
                "node_type": "document",
                "title": "test_report.pdf",
                "parent_id": null,
                "file_ref": {
                    "file_path": "input/test_report.pdf",
                    "file_name": "test_report.pdf",
                    "file_ext": "pdf",
                    "file_size_bytes": 12345,
                    "file_modified_utc": "2025-01-20T08:00:00Z",
                    "file_sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                },
                "location": null,
                "provenance": {
                    "source_tier": "tier1_local",
                    "extractor_version": "1.0.0",
                    "created_utc": "2025-01-22T10:30:00Z",
                    "confidence": 1.0,
                    "warnings": []
                },
                "text": null,
                "table": null,
                "image": null,
                "tags": []
            },
            {
                "node_id": "2222222222222222222222222222222222222222222222222222222222222222",
                "node_type": "section",
                "title": "Credit Quality",
                "parent_id": "1111111111111111111111111111111111111111111111111111111111111111",
                "file_ref": {
                    "file_path": "input/test_report.pdf",
                    "file_name": "test_report.pdf",
                    "file_ext": "pdf",
                    "file_size_bytes": 12345,
                    "file_modified_utc": "2025-01-20T08:00:00Z",
                    "file_sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                },
                "location": {"page": 1},
                "provenance": {
                    "source_tier": "tier1_local",
                    "extractor_version": "1.0.0",
                    "created_utc": "2025-01-22T10:30:00Z",
                    "confidence": 0.95,
                    "warnings": []
                },
                "text": null,
                "table": null,
                "image": null,
                "tags": []
            },
            {
                "node_id": "3333333333333333333333333333333333333333333333333333333333333333",
                "node_type": "chunk",
                "title": null,
                "parent_id": "2222222222222222222222222222222222222222222222222222222222222222",
                "file_ref": {
                    "file_path": "input/test_report.pdf",
                    "file_name": "test_report.pdf",
                    "file_ext": "pdf",
                    "file_size_bytes": 12345,
                    "file_modified_utc": "2025-01-20T08:00:00Z",
                    "file_sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                },
                "location": {"page": 1},
                "provenance": {
                    "source_tier": "tier1_local",
                    "extractor_version": "1.0.0",
                    "created_utc": "2025-01-22T10:30:00Z",
                    "confidence": 0.95,
                    "warnings": []
                },
                "text": {
                    "text": "The NPL ratio increased to 1.23% in Q3 2024, reflecting market conditions.",
                    "char_start": 0,
                    "char_end": 75,
                    "token_estimate": 18
                },
                "table": null,
                "image": null,
                "tags": []
            }
        ],
        "edges": [
            {
                "edge_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "from_id": "1111111111111111111111111111111111111111111111111111111111111111",
                "to_id": "2222222222222222222222222222222222222222222222222222222222222222",
                "relation": "contains",
                "weight": 1.0,
                "metadata": {}
            },
            {
                "edge_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "from_id": "2222222222222222222222222222222222222222222222222222222222222222",
                "to_id": "3333333333333333333333333333333333333333333333333333333333333333",
                "relation": "contains",
                "weight": 1.0,
                "metadata": {}
            }
        ],
        "stats": {
            "total_nodes": 3,
            "nodes_by_type": {"document": 1, "section": 1, "chunk": 1},
            "total_edges": 2,
            "total_chars": 75,
            "extraction_duration_ms": 1500,
            "extractor_used": "docling",
            "page_count": 5
        }
    }
    '''

    print("=" * 60)
    print("TEST: NPL Query")
    print("=" * 60)
    print(main(file_content_str=sample_json, user_query="What is the NPL ratio?"))

