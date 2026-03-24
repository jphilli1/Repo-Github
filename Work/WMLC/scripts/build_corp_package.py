#!/usr/bin/env python3
"""Build the corp deployment package.

Packs ONLY the files safe for corporate deployment:
  corp_etl/   (all .py and .yaml files)
  WMLC_Dashboard.xlsm
  CORP_DEPLOYMENT.md
  UNPACK_INSTRUCTIONS.txt

NEVER includes: agents/, skills/, specs/, proxy_data/,
LLM_CONTEXT.txt, bundle_context.py, or any .claude/ content.
"""
import base64
import io
import os
import sys
import tarfile
from datetime import datetime
from pathlib import Path

_CORP_SAFE_DIRS = {"corp_etl"}
_CORP_SAFE_FILES = {
    "WMLC_Dashboard.xlsm",
    "CORP_DEPLOYMENT.md",
    "UNPACK_INSTRUCTIONS.txt",
}
_NEVER_INCLUDE = {
    ".claude", "agents", "skills", "specs", "proxy_data",
    "output", "logs", "LLM_CONTEXT.txt", "bundle_context.py",
    "__pycache__", ".git", "venv", ".venv",
}

def main():
    script_path = Path(__file__).resolve()
    repo_root = script_path.parent.parent if script_path.parent.name == "scripts" else script_path.parent
    output_dir = repo_root / "output"
    output_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_path = output_dir / f"corp_etl_payload_{timestamp}.txt"

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        # Pack corp_etl/ directory
        corp_etl = repo_root / "corp_etl"
        if corp_etl.is_dir():
            tar.add(corp_etl, arcname="corp_etl")
            print(f"  + corp_etl/ ({sum(1 for _ in corp_etl.rglob('*') if _.is_file())} files)")

        # Pack safe root-level files
        for fname in _CORP_SAFE_FILES:
            fpath = repo_root / fname
            if fpath.exists():
                tar.add(fpath, arcname=fname)
                print(f"  + {fname}")
            else:
                print(f"  ! Missing: {fname}", file=sys.stderr)

    encoded = base64.b64encode(buf.getvalue()).decode("ascii")
    output_path.write_text(encoded, encoding="ascii")

    size_kb = output_path.stat().st_size / 1000
    print(f"\nCorp package written: {output_path.name} ({size_kb:.0f}KB)")
    print(f"  Decode on corp: PowerShell -> $raw -replace '\\s','' -> [Convert]::FromBase64String")

if __name__ == "__main__":
    main()
