# AI-Native Split-RAG System v2.0 - Relationship Manager
# "The Gatekeeper" - Filters, Validates, and Indexes Relationship Docs
# Tier 1 Component (Runs in Local Python Environment)

import json
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional
import pandas as pd
import pdfplumber
from tqdm import tqdm

# --- Configuration Constants ---
CURRENT_YEAR = datetime.now().year
PRIOR_YEAR = CURRENT_YEAR - 1
CRITICAL_PRIOR_DOCS = {"LOAN MODIFICATION", "ANNUAL REVIEW"}


# --- Helper Functions ---

def load_config(config_path: Path) -> Dict:
    with open(config_path, 'r') as f:
        return json.load(f)


def load_rules(rules_path: Path) -> Dict:
    with open(rules_path, 'r') as f:
        return json.load(f)


def load_master_list(master_path: Path) -> set:
    """
    Loads the 'life_time_info' master file.
    Returns a set of normalized Relationship Names for fast lookup.
    """
    if not master_path or not master_path.exists():
        return set()

    try:
        if master_path.suffix == '.csv':
            df = pd.read_csv(master_path)
        elif master_path.suffix in ['.xlsx', '.xls']:
            df = pd.read_excel(master_path)
        else:
            return set()

        # Adjust column name to match your actual Master File
        target_col = next((c for c in df.columns if 'Relationship' in c or 'Client' in c), None)
        if target_col:
            return set(df[target_col].astype(str).str.strip().str.upper())
    except Exception as e:
        print(f"Error loading Master List: {e}")

    return set()


def fast_scan_document(file_path: Path, rules: Dict) -> Dict[str, Any]:
    """
    Peeks at the first 3 pages to extract Date, Type, and Entity.
    """
    info = {
        "doc_type": "UNKNOWN",
        "doc_date": None,
        "extracted_entity": None,
        "filename": file_path.name
    }

    text_buffer = ""
    try:
        if file_path.suffix.lower() == '.pdf':
            with pdfplumber.open(file_path) as pdf:
                scan_pages = min(3, len(pdf.pages))
                for i in range(scan_pages):
                    text = pdf.pages[i].extract_text()
                    if text: text_buffer += text + "\n"

        # 1. Detect Document Type
        type_rules = rules["entities"].get("document_type", {}).get("patterns", [])
        for pattern in type_rules:
            match = re.search(pattern, text_buffer, re.IGNORECASE | re.MULTILINE)
            if match:
                info["doc_type"] = match.group("entity").upper().strip()
                break

        # 2. Detect Date
        date_rules = rules["entities"].get("effective_date", {}).get("patterns", [])
        for pattern in date_rules:
            match = re.search(pattern, text_buffer, re.IGNORECASE | re.MULTILINE)
            if match:
                date_str = match.group("entity").strip()
                try:
                    for fmt in ["%m/%d/%Y", "%Y-%m-%d", "%B %d, %Y"]:
                        try:
                            dt = datetime.strptime(date_str, fmt)
                            info["doc_date"] = dt.year
                            break
                        except ValueError:
                            continue
                except:
                    pass
                break

        # 3. Detect Entity
        borrower_rules = rules["entities"].get("borrower", {}).get("patterns", [])
        for pattern in borrower_rules:
            match = re.search(pattern, text_buffer, re.IGNORECASE | re.MULTILINE)
            if match:
                info["extracted_entity"] = match.group("entity").strip()
                break

    except Exception as e:
        # File might be corrupt or password protected
        pass

    return info


def create_doc_entry(file_path, meta, reason, validation_status, source="folder"):
    return {
        "filename": file_path.name,
        "filepath": str(file_path),
        "type": meta["doc_type"],
        "year": meta["doc_date"] or "Unknown",
        "reason": reason,
        "validation_status": validation_status,
        "entity_in_doc": meta["extracted_entity"],
        "source": source
    }


# --- Main Logic ---

def process_universe(config_path: Path, rules_path: Path, master_file_path: Optional[Path]):
    config = load_config(config_path)
    rules = load_rules(rules_path)

    input_dir = Path(config['paths']['input_directory'])
    manual_dir = Path(config['paths'].get('manual_input_directory', 'input/_MANUAL_UPLOADS'))
    output_dir = Path(config['paths']['output_directory'])

    # 1. Load Universe (Master List)
    master_universe = load_master_list(master_file_path)
    print(f"Universe Loaded: {len(master_universe)} relationships known.")

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "relationships": {},
        "orphaned_files": []
    }

    # --- PASS 1: Process Structured Folders ---
    print("\n--- PASS 1: Scanning Relationship Folders ---")
    if input_dir.exists():
        for folder in input_dir.iterdir():
            if not folder.is_dir() or folder.name.startswith(('_', '.')): continue

            rel_name = folder.name
            print(f"Scanning Folder: {rel_name}")

            # Init entry
            status = "Verified" if rel_name.upper() in master_universe else "Unverified (Folder Only)"
            manifest["relationships"][rel_name] = {
                "status": status,
                "document_count": 0,
                "documents": []
            }

            for file_path in folder.iterdir():
                if file_path.suffix.lower() != '.pdf': continue

                meta = fast_scan_document(file_path, rules)

                # Logic: Current Year OR Prior Year Critical
                doc_year = meta["doc_date"]
                keep = False
                reason = "Outdated"

                if doc_year == CURRENT_YEAR:
                    keep = True
                    reason = "Current Year"
                elif doc_year == PRIOR_YEAR and any(c in meta["doc_type"] for c in CRITICAL_PRIOR_DOCS):
                    keep = True
                    reason = "Prior Year Critical"

                if keep:
                    val_status = "Verified"
                    if meta["extracted_entity"] and meta["extracted_entity"].upper() not in rel_name.upper():
                        val_status = "Potential Mismatch"

                    doc_entry = create_doc_entry(file_path, meta, reason, val_status, "folder")
                    manifest["relationships"][rel_name]["documents"].append(doc_entry)

            manifest["relationships"][rel_name]["document_count"] = len(
                manifest["relationships"][rel_name]["documents"])

    # --- PASS 2: Process Manual Uploads ---
    print("\n--- PASS 2: Scanning Manual Uploads ---")
    if manual_dir.exists():
        for file_path in manual_dir.iterdir():
            if file_path.suffix.lower() != '.pdf': continue

            print(f"Processing Manual File: {file_path.name}")
            meta = fast_scan_document(file_path, rules)
            entity = meta["extracted_entity"]

            # Universe Check: Where does this file belong?
            matched_rel_key = None

            if entity:
                # 1. Try to match against existing folders in Manifest
                for rel_key in manifest["relationships"].keys():
                    if entity.upper() in rel_key.upper() or rel_key.upper() in entity.upper():
                        matched_rel_key = rel_key
                        break

                # 2. If no folder, try Master Universe
                if not matched_rel_key:
                    # Simple check if the extracted entity exists in master list
                    # This is O(N) but fine for Tier 1
                    for known_rel in master_universe:
                        if entity.upper() == known_rel:
                            matched_rel_key = known_rel
                            # Create new entry in manifest if folder didn't exist
                            if matched_rel_key not in manifest["relationships"]:
                                manifest["relationships"][matched_rel_key] = {
                                    "status": "Verified (Master List)",
                                    "document_count": 0,
                                    "documents": []
                                }
                            break

            # Add to Manifest or Orphan
            if matched_rel_key:
                print(f"  -> Matched to Universe: {matched_rel_key}")
                doc_entry = create_doc_entry(file_path, meta, "Manual Upload", "Verified via Entity Extraction",
                                             "manual")
                manifest["relationships"][matched_rel_key]["documents"].append(doc_entry)
                manifest["relationships"][matched_rel_key]["document_count"] += 1
            else:
                print(f"  -> No Universe Match. Flagging as Orphan.")
                doc_entry = create_doc_entry(file_path, meta, "Manual Upload", "Unmatched Entity", "manual")
                manifest["orphaned_files"].append(doc_entry)

    # Final Save
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "relationship_manifest.json"
    with open(out_path, 'w') as f:
        json.dump(manifest, f, indent=2)

    print(f"\nManifest Generation Complete: {out_path}")


if __name__ == "__main__":
    # Adjust paths as needed for your local test
    process_universe(
        Path("config.json"),
        Path("rules.json"),
        Path("master_list.xlsx")
    )