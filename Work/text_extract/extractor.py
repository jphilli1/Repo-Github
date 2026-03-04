# AI-Native Split-RAG System v2.0 - Tier 1 Extraction Engine
# "The Factory" - Heavy Processing Logic
# CP-002: Specific exception handling required
# CP-003: Pathlib used for all file ops

import sys
import json
import argparse
import hashlib
import time
import shutil
import logging
import re
import traceback
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass

# Approved Dependencies
# NOTE: Torch/Transformers NOT imported directly (handled by Docling)
from pydantic import ValidationError
import pdfplumber
from tqdm import tqdm
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling.datamodel.pipeline_options import PdfPipelineOptions, TableFormerMode
from docling.datamodel.base_models import InputFormat

# Internal Imports
import schema_v2 as schema

# --- Configuration & Setup ---

@dataclass
class Config:
    input_dir: Path
    output_dir: Path
    log_dir: Path
    quarantine_dir: Path
    primary_engine: str
    fallback_engine: str
    enable_ocr: bool
    enable_table_detection: bool
    max_pages_scan: int
    docling_use_gpu: bool
    docling_table_mode: str
    conflict_threshold: float
    keep_all_policy: bool

def load_config(config_path: Path) -> Config:
    try:
        with open(config_path, 'r') as f:
            data = json.load(f)

        paths = data['paths']
        ext_set = data['extraction_settings']
        doc_set = data.get('docling_settings', {})
        val_set = data.get('validation_settings', {})

        return Config(
            input_dir=Path(paths['input_directory']),
            output_dir=Path(paths['output_directory']),
            log_dir=Path(paths['log_directory']),
            quarantine_dir=Path(paths['quarantine_directory']),
            primary_engine=ext_set.get('primary_engine', 'docling'),
            fallback_engine=ext_set.get('fallback_engine', 'pdfplumber'),
            enable_ocr=ext_set.get('enable_ocr', True),
            enable_table_detection=ext_set.get('enable_table_detection', True),
            max_pages_scan=ext_set.get('max_pages_for_entity_scan', 20),
            docling_use_gpu=doc_set.get('use_gpu', False),
            docling_table_mode=doc_set.get('table_mode', 'accurate'),
            conflict_threshold=val_set.get('conflict_threshold_levenshtein', 0.3),
            keep_all_policy=val_set.get('enable_keep_all_policy', True)
        )
    except Exception as e:
        sys.exit(f"CRITICAL: Failed to load config: {e}")

def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("SplitRAG_Factory")
    logger.setLevel(logging.INFO)

    # File Handler
    fh = logging.FileHandler(log_dir / "extraction.log")
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    # Console Handler
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

# --- Entity Anchoring ---

def load_rules(rules_path: Path) -> Dict:
    with open(rules_path, 'r') as f:
        return json.load(f)

def extract_entities(file_path: Path, max_pages: int, rules: Dict) -> Dict[str, Optional[str]]:
    """
    Scans document preamble using regex patterns to find Borrower, Lender, Guarantor.
    Uses pdfplumber for fast raw text access before heavy processing.
    """
    entities = {"borrower": None, "lender": None, "guarantor": None}

    try:
        text_buffer = ""
        with pdfplumber.open(file_path) as pdf:
            # Scan only first N pages for entities
            scan_limit = min(len(pdf.pages), max_pages)
            for i in range(scan_limit):
                page_text = pdf.pages[i].extract_text()
                if page_text:
                    text_buffer += page_text + "\n"

        # Apply Regex Rules
        for role in ["borrower", "lender", "guarantor"]:
            role_config = rules["entities"].get(role, {})
            patterns = role_config.get("patterns", [])

            for pattern in patterns:
                match = re.search(pattern, text_buffer, re.IGNORECASE | re.MULTILINE)
                if match:
                    # Clean the entity name
                    raw_entity = match.group("entity").strip().strip('",.')
                    entities[role] = raw_entity
                    break # Stop after first match for this role based on priority order

    except Exception:
        # Don't fail the whole pipeline if entity extraction fails; just return Nones
        pass

    return entities

# --- Extraction Logic ---

def process_with_docling(file_path: Path, config: Config, doc_id: str, file_hash: str) -> List[schema.ContextNode]:
    """
    Primary Extraction using Docling.
    """
    nodes = []

    # Setup Pipeline Options
    pipeline_options = PdfPipelineOptions()
    pipeline_options.do_ocr = config.enable_ocr
    pipeline_options.do_table_structure = config.enable_table_detection
    if config.docling_table_mode == "accurate":
        pipeline_options.table_structure_options.mode = TableFormerMode.ACCURATE
    else:
        pipeline_options.table_structure_options.mode = TableFormerMode.FAST

    # Configure Accelerator (CPU default per architecture spec)
    # Docling manages torch internally
    if config.docling_use_gpu:
        pipeline_options.accelerator_options.device = "cuda" # Explicitly request CUDA if configured
    else:
        pipeline_options.accelerator_options.device = "cpu"

    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
        }
    )

    try:
        # Convert
        doc_result = converter.convert(file_path)
        doc = doc_result.document

        # Iterate Body
        chunk_idx = 0
        for item in doc.texts:
            # Map Docling types to Schema types
            # Note: Docling structure is complex; simplified mapping for implementation
            c_type = "text"
            if item.label == "section_header":
                c_type = "header"
            elif item.label == "caption":
                c_type = "image_caption"

            page_no = item.prov[0].page_no if item.prov else 1
            bbox = item.prov[0].bbox.as_tuple() if item.prov else None
            # Normalize bbox to list of floats if present
            bbox_list = [float(x) for x in bbox] if bbox else None

            # Generate IDs
            chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, item.text)
            lineage = schema.generate_lineage_trace(file_hash, page_no, bbox_list, "docling")

            meta = schema.NodeMetadata(
                page_number=page_no,
                bbox=bbox_list,
                source_scope="primary", # Default, logic to detect "corpus" would go here
                extraction_method="docling",
                is_active=True
            )

            node = schema.ContextNode(
                chunk_id=chunk_id,
                content_type=c_type,
                content=item.text,
                metadata=meta,
                lineage_trace=lineage
            )
            nodes.append(node)
            chunk_idx += 1

        # Iterate Tables
        for table in doc.tables:
            page_no = table.prov[0].page_no if table.prov else 1
            # Export to Markdown
            md_content = table.export_to_markdown()

            chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, md_content)
            lineage = schema.generate_lineage_trace(file_hash, page_no, None, "docling")

            meta = schema.NodeMetadata(
                page_number=page_no,
                table_shape=[table.data.num_rows, table.data.num_cols],
                source_scope="primary",
                extraction_method="docling",
                is_active=True
            )

            node = schema.ContextNode(
                chunk_id=chunk_id,
                content_type="table",
                content=md_content,
                metadata=meta,
                lineage_trace=lineage
            )
            nodes.append(node)
            chunk_idx += 1

    except Exception as e:
        raise RuntimeError(f"Docling conversion failed: {str(e)}") from e

    return nodes

def process_with_fallback(file_path: Path, doc_id: str, file_hash: str) -> List[schema.ContextNode]:
    """
    Fallback Extraction using pdfplumber.
    """
    nodes = []
    chunk_idx = 0

    try:
        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                page_no = i + 1
                text = page.extract_text()

                if text:
                    # Simple chunking by paragraphs for fallback
                    paragraphs = text.split('\n\n')
                    for para in paragraphs:
                        if not para.strip(): continue

                        chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, para)
                        lineage = schema.generate_lineage_trace(file_hash, page_no, None, "pdfplumber")

                        meta = schema.NodeMetadata(
                            page_number=page_no,
                            source_scope="primary",
                            extraction_method="pdfplumber",
                            is_active=True # Fallback becomes active if primary failed
                        )

                        node = schema.ContextNode(
                            chunk_id=chunk_id,
                            content_type="text",
                            content=para,
                            metadata=meta,
                            lineage_trace=lineage
                        )
                        nodes.append(node)
                        chunk_idx += 1

                # Simple table extraction
                tables = page.extract_tables()
                for table in tables:
                    # Convert list of lists to simple markdown
                    md_table = "\n".join([" | ".join(map(str, row)) for row in table])

                    chunk_id = schema.generate_chunk_id(doc_id, page_no, chunk_idx, md_table)
                    lineage = schema.generate_lineage_trace(file_hash, page_no, None, "pdfplumber")

                    meta = schema.NodeMetadata(
                        page_number=page_no,
                        source_scope="primary",
                        extraction_method="pdfplumber",
                        is_active=True
                    )

                    node = schema.ContextNode(
                        chunk_id=chunk_id,
                        content_type="table",
                        content=md_table,
                        metadata=meta,
                        lineage_trace=lineage
                    )
                    nodes.append(node)
                    chunk_idx += 1

    except Exception as e:
        raise RuntimeError(f"Fallback pdfplumber failed: {str(e)}") from e

    return nodes

# --- Main Pipeline ---

def process_file(file_path: Path, config: Config, rules: Dict, logger: logging.Logger) -> bool:
    start_time = time.time()

    # 1. File Hash & ID
    try:
        file_bytes = file_path.read_bytes()
        file_hash = hashlib.md5(file_bytes).hexdigest() # Used for lineage
        doc_id = schema.generate_document_id(file_bytes)
    except Exception as e:
        logger.error(f"Failed to read file {file_path}: {e}")
        return False

    # 2. Entity Anchoring
    logger.info(f"Scanning entities for {file_path.name}...")
    entities = extract_entities(file_path, config.max_pages_scan, rules)

    # 3. Extraction
    nodes = []
    primary_success = False
    fallback_triggered = False

    logger.info(f"Starting Primary Extraction ({config.primary_engine}) for {file_path.name}...")
    try:
        nodes = process_with_docling(file_path, config, doc_id, file_hash)
        primary_success = True
    except Exception as e:
        logger.warning(f"Primary engine failed for {file_path.name}: {e}")
        primary_success = False

    if not primary_success:
        logger.info(f"Triggering Fallback Engine ({config.fallback_engine})...")
        try:
            nodes = process_with_fallback(file_path, doc_id, file_hash)
            fallback_triggered = True
        except Exception as e:
            logger.critical(f"Both engines failed for {file_path.name}. Quarantining.")
            handle_quarantine(file_path, config.quarantine_dir, str(e))
            return False

    # 4. Metrics & Graph Assembly
    metrics = schema.ExtractionMetrics(
        total_pages=max([n.metadata.page_number for n in nodes], default=0),
        total_nodes=len(nodes),
        tables_extracted=len([n for n in nodes if n.content_type == "table"]),
        headers_extracted=len([n for n in nodes if n.content_type == "header"]),
        conflicts_detected=0, # Placeholder for Phase 2 implementation
        extraction_time_seconds=time.time() - start_time,
        primary_engine_used=primary_success,
        fallback_triggered=fallback_triggered
    )

    graph = schema.ContextGraph(
        document_id=doc_id,
        filename=file_path.name,
        processed_at=schema.ContextGraph.get_current_timestamp(),
        borrower_entity=entities['borrower'],
        lender_entity=entities['lender'],
        guarantor_entity=entities['guarantor'],
        nodes=nodes,
        metrics=metrics
    )

    # 5. Validation & Save
    output_path = config.output_dir / f"{file_path.stem}_v2.json"
    try:
        # Validate via Pydantic model dump
        json_output = graph.to_json()
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(json_output)
        logger.info(f"Successfully processed {file_path.name} -> {output_path}")
        return True
    except Exception as e:
        logger.error(f"Serialization failed for {file_path.name}: {e}")
        handle_quarantine(file_path, config.quarantine_dir, f"Serialization Error: {e}")
        return False

def handle_quarantine(file_path: Path, quarantine_dir: Path, error_msg: str):
    quarantine_dir.mkdir(exist_ok=True)
    dest = quarantine_dir / file_path.name

    # Move file
    try:
        shutil.move(str(file_path), str(dest))
    except Exception:
        pass # Already moved or permission issue

    # Write Report
    report_path = quarantine_dir / f"{file_path.name}_failure_report.json"
    report = {
        "filename": file_path.name,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error_msg,
        "stack_trace": traceback.format_exc()
    }
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Split-RAG Tier 1 Extraction Engine")
    parser.add_argument("--config", type=Path, default=Path("config.json"))
    parser.add_argument("--rules", type=Path, default=Path("rules.json"))
    parser.add_argument("--file", type=Path, help="Process single file")
    parser.add_argument("--reprocess", action="store_true", help="Force reprocessing")
    args = parser.parse_args()

    # Load Config
    if not args.config.exists():
        sys.exit(f"Config file not found: {args.config}")
    config = load_config(args.config)
    rules = load_rules(args.rules)
    logger = setup_logging(config.log_dir)

    # Setup Dirs
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.quarantine_dir.mkdir(parents=True, exist_ok=True)

    # File Discovery
    files_to_process = []
    if args.file:
        files_to_process = [args.file]
    else:
        # Scan input dir
        supported_ext = {'.pdf', '.docx', '.xlsx', '.pptx'}
        for f in config.input_dir.iterdir():
            if f.suffix.lower() in supported_ext and not f.name.startswith(('~', '.')):
                files_to_process.append(f)

    logger.info(f"Found {len(files_to_process)} files to process.")

    # Manifest Management (Simplistic for Phase 2)
    manifest_path = config.output_dir / "processing_manifest.json"
    manifest = {}
    if manifest_path.exists():
        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)
        except Exception:
            logger.warning("Could not load manifest. Starting fresh.")

    # Process Loop
    for file_path in tqdm(files_to_process, desc="Processing"):
        try:
            # Check manifest
            file_hash = hashlib.md5(file_path.read_bytes()).hexdigest()
            if not args.reprocess and file_hash in manifest:
                logger.info(f"Skipping {file_path.name} (already processed)")
                continue

            success = process_file(file_path, config, rules, logger)

            if success:
                manifest[file_hash] = str(config.output_dir / f"{file_path.stem}_v2.json")
                # Update manifest immediately
                with open(manifest_path, 'w') as f:
                    json.dump(manifest, f, indent=2)

        except Exception as e:
            logger.error(f"Unexpected error loop on {file_path.name}: {e}")

if __name__ == "__main__":
    main()