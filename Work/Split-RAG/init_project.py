import os
import sys
from pathlib import Path


def create_structure():
    """
    Creates the AI-Native Split-RAG v2.0 directory structure
    and ALL placeholder files (including tests, scripts, and runners).
    """
    root = Path.cwd() / "split-rag"

    # 1. Directory Structure
    directories = [
        root / "input",
        root / "input" / "_MANUAL_UPLOADS",
        root / "output",
        root / "logs",
        root / "quarantine",
        root / "tests",
        root / "tests" / "fixtures",
        root / "scripts",  # New Scripts Directory
    ]

    # 2. File Scaffolding
    files = [
        # Core Components
        root / "schema_v2.py",
        root / "extractor.py",
        root / "relationship_manager.py",
        root / "copilot_tier2.py",

        # Helper Scripts
        root / "install_deps.py",

        # Test Suite
        root / "tests" / "__init__.py",
        root / "tests" / "conftest.py",
        root / "tests" / "test_determinism.py",
        root / "tests" / "test_disambiguation.py",
        root / "tests" / "test_data_mart.py",
        root / "tests" / "test_tier2_retrieval.py",

        # Runners & Validators (New)
        root / "scripts" / "run_tests.bat",
        root / "scripts" / "run_tests.sh",
        root / "scripts" / "validate_deployment.py"
    ]

    print(f"Initializing project structure at: {root}")

    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            print(f"  [+] Created directory: {directory.relative_to(Path.cwd())}")
        except Exception as e:
            print(f"  [!] Error creating {directory}: {e}")

    for file_path in files:
        try:
            if not file_path.exists():
                file_path.touch()
                print(f"  [+] Created placeholder: {file_path.relative_to(Path.cwd())}")
            else:
                print(f"  [.] Exists: {file_path.relative_to(Path.cwd())}")
        except Exception as e:
            print(f"  [!] Error creating {file_path}: {e}")

    print("\nProject structure updated.")


if __name__ == "__main__":
    create_structure()