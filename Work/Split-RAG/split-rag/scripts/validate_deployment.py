# AI-Native Split-RAG System v2.0 - Deployment Validator
# Scans Tier 2 code for forbidden imports before deployment.

import ast
import sys
from pathlib import Path

# Config
TIER2_FILE = "copilot_tier2.py"
FORBIDDEN_IMPORTS = {
    "torch", "transformers", "pydantic", "sklearn",
    "scipy", "requests", "nltk", "spacy", "tabulate",
    "numpy"  # Pandas uses numpy, but direct import in Tier 2 is often unnecessary/restricted
}


def check_imports(file_path):
    print(f"Scanning {file_path} for forbidden imports...")

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = ast.parse(f.read(), filename=str(file_path))
    except Exception as e:
        print(f"[ERROR] Could not parse file: {e}")
        return False

    violations = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split('.')[0] in FORBIDDEN_IMPORTS:
                    violations.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.split('.')[0] in FORBIDDEN_IMPORTS:
                violations.append(node.module)

    if violations:
        print(f"[FAIL] Forbidden imports found: {', '.join(violations)}")
        print("Violation of CANON_002: Tier 2 must rely on StdLib + Pandas only.")
        return False

    print("[PASS] No forbidden imports found.")
    return True


if __name__ == "__main__":
    # Determine path (relative to script or cwd)
    base_path = Path(__file__).resolve().parent.parent
    target = base_path / TIER2_FILE

    if not target.exists():
        # Try cwd
        target = Path(TIER2_FILE)

    if not target.exists():
        print(f"[ERROR] Could not find {TIER2_FILE}")
        sys.exit(1)

    success = check_imports(target)
    sys.exit(0 if success else 1)