#!/usr/bin/env python3
"""Bundle src/ Python files and docs/claude/ Markdown files into LLM_CONTEXT.txt."""

import os
import sys


def collect_files(base_dir, extension):
    """Recursively collect files with the given extension, sorted by path."""
    results = []
    for dirpath, _, filenames in os.walk(base_dir):
        for f in sorted(filenames):
            if f.endswith(extension):
                results.append(os.path.join(dirpath, f))
    results.sort()
    return results


def main():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(repo_root, "LLM_CONTEXT.txt")

    src_dir = os.path.join(repo_root, "src")
    docs_dir = os.path.join(repo_root, "docs", "claude")

    py_files = collect_files(src_dir, ".py") if os.path.isdir(src_dir) else []
    md_files = collect_files(docs_dir, ".md") if os.path.isdir(docs_dir) else []
    all_files = py_files + md_files

    if not all_files:
        print("No files found to bundle.", file=sys.stderr)
        sys.exit(1)

    with open(output_path, "w", encoding="utf-8") as out:
        for i, fpath in enumerate(all_files):
            relpath = os.path.relpath(fpath, repo_root).replace(os.sep, "/")
            if i > 0:
                out.write("\n\n")
            out.write(f"--- FILE: {relpath} ---\n\n")
            with open(fpath, "r", encoding="utf-8") as f:
                out.write(f.read())

    size = os.path.getsize(output_path)
    print(f"Bundled {len(py_files)} Python + {len(md_files)} Markdown files")
    print(f"Written: LLM_CONTEXT.txt ({size:,} bytes)")


if __name__ == "__main__":
    main()
