"""
PDF Parser Engine
=================
Main orchestrator that combines block extraction, state machine parsing,
validation, and output formatting into a complete PDF parsing pipeline.

Usage:
    engine = ParserEngine(config)
    result = engine.parse("path/to/exam.pdf")
    # result is a ParseResult with structured JSON output

Architecture:
    PDF → BlockExtractor → ContentBlocks → StateMachineParser →
    ParsedQuestions → ValidationEngine → ParseResult (JSON)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from . import __version__
from .block_extractor import BlockExtractor
from .models import (
    ExamMetadata,
    ParseResult,
    ParseVersion,
    ValidationReport,
)
from .state_machine import StateMachineParser
from .validator import ValidationEngine

logger = logging.getLogger(__name__)


@dataclass
class ParserConfig:
    """Configuration for the parser engine."""

    # Image settings
    image_format: str = "png"
    min_image_size: int = 50
    image_dpi: int = 150

    # Output settings
    output_dir: str = "output"
    image_base_dir: str = "storage/questions"

    # Exam metadata
    exam_name: str = ""
    exam_provider: str = ""
    exam_version: str = ""
    exam_id: Optional[str] = None

    # Processing
    page_range: Optional[tuple[int, int]] = None

    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = None

    # Version tracking
    save_raw_blocks: bool = True
    save_snapshots: bool = True


class ParserEngine:
    """
    Main PDF parsing engine.

    Orchestrates the full pipeline:
        1. Block extraction (text + images)
        2. State machine parsing (structure detection)
        3. Anomaly detection
        4. Validation
        5. Output formatting

    Thread-safe for parallel PDF processing.
    """

    def __init__(self, config: Optional[ParserConfig] = None):
        self.config = config or ParserConfig()
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging based on config."""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)

        # Configure root logger for the parser package
        parser_logger = logging.getLogger("parser")
        parser_logger.setLevel(log_level)

        # Console handler
        if not parser_logger.handlers:
            console = logging.StreamHandler()
            console.setLevel(log_level)
            formatter = logging.Formatter(
                "[%(asctime)s] %(levelname)-8s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            console.setFormatter(formatter)
            parser_logger.addHandler(console)

        # File handler
        if self.config.log_file:
            log_dir = Path(self.config.log_file).parent
            log_dir.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(
                self.config.log_file, encoding="utf-8"
            )
            file_handler.setLevel(log_level)
            file_handler.setFormatter(logging.Formatter(
                "[%(asctime)s] %(levelname)-8s %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            parser_logger.addHandler(file_handler)

    def parse(
        self,
        pdf_path: str,
        progress_callback: Optional[callable] = None,
    ) -> ParseResult:
        """
        Parse a PDF file into structured question entities.

        Args:
            pdf_path: Path to the PDF file to parse.
            progress_callback: Callback(page_num, total_pages) called on each page.

        Returns:
            ParseResult containing all questions, metadata, and validation.

        Raises:
            FileNotFoundError: If PDF file doesn't exist.
            RuntimeError: If PDF cannot be opened.
        """
        pdf_path = os.path.abspath(pdf_path)

        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        start_time = time.time()
        logger.info(f"Starting parse of: {pdf_path}")

        # ── Step 1: Compute file metadata ─────────────────────────────
        exam_metadata = self._build_exam_metadata(pdf_path)

        # ── Step 2: Setup image output directory ──────────────────────
        exam_id = self.config.exam_id or self._generate_exam_id(pdf_path)
        image_dir = Path(self.config.image_base_dir) / exam_id
        image_dir.mkdir(parents=True, exist_ok=True)

        # ── Step 3: Extract blocks ────────────────────────────────────
        logger.info("Phase 1: Block extraction")
        extractor = BlockExtractor(
            image_output_dir=str(image_dir),
            image_format=self.config.image_format,
            min_image_size=self.config.min_image_size,
            dpi=self.config.image_dpi,
        )

        blocks = extractor.extract(
            pdf_path,
            page_range=self.config.page_range,
            progress_callback=progress_callback,
        )

        exam_metadata.total_pages = extractor.get_page_count(pdf_path)

        # ── Step 4: State machine parsing ─────────────────────────────
        logger.info("Phase 2: State machine parsing")
        parser = StateMachineParser()
        questions = parser.parse(blocks)

        # ── Step 5: Validation ────────────────────────────────────────
        logger.info("Phase 3: Validation")
        validator = ValidationEngine()
        validation = validator.validate(questions)

        # ── Step 6: Build result ──────────────────────────────────────
        parse_version = ParseVersion(
            parser_version=__version__,
            raw_block_count=len(blocks),
            structured_question_count=len(questions),
        )

        result = ParseResult(
            exam=exam_metadata,
            parse_version=parse_version,
            questions=questions,
            validation=validation,
        )

        elapsed = time.time() - start_time
        logger.info(
            f"Parse complete in {elapsed:.2f}s — "
            f"{len(questions)} questions extracted"
        )

        # ── Step 7: Save output ───────────────────────────────────────
        output_dir = Path(self.config.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Save main JSON output
        output_file = output_dir / f"{exam_id}_parsed.json"
        self._save_json(result, output_file)

        # Save raw blocks snapshot if enabled
        if self.config.save_raw_blocks:
            raw_file = output_dir / f"{exam_id}_raw_blocks.json"
            self._save_raw_blocks(blocks, raw_file)

        # Save validation report
        report_file = output_dir / f"{exam_id}_validation.json"
        self._save_json_dict(validation.model_dump(), report_file)

        logger.info(f"Output saved to: {output_dir}")

        return result

    def _build_exam_metadata(self, pdf_path: str) -> ExamMetadata:
        """Build exam metadata from file info and config."""
        file_size = os.path.getsize(pdf_path)
        file_hash = self._compute_file_hash(pdf_path)

        return ExamMetadata(
            name=self.config.exam_name or Path(pdf_path).stem,
            provider=self.config.exam_provider,
            version=self.config.exam_version,
            source_pdf=os.path.basename(pdf_path),
            file_hash=file_hash,
            file_size_bytes=file_size,
        )

    def _compute_file_hash(self, filepath: str) -> str:
        """Compute SHA-256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _generate_exam_id(self, pdf_path: str) -> str:
        """Generate a deterministic exam ID from file path and hash."""
        name = Path(pdf_path).stem
        # Clean the name for filesystem use
        clean_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in name
        )
        return clean_name[:50]

    def _save_json(self, result: ParseResult, filepath: Path):
        """Save ParseResult to JSON file."""
        try:
            data = result.model_dump()
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Saved JSON output: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")

    def _save_json_dict(self, data: dict, filepath: Path):
        """Save a dict to JSON file."""
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Saved JSON: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save JSON: {e}")

    def _save_raw_blocks(self, blocks: list, filepath: Path):
        """Save raw extracted blocks snapshot."""
        try:
            data = [b.model_dump() for b in blocks]
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            logger.info(f"Saved raw blocks snapshot: {filepath}")
        except Exception as e:
            logger.error(f"Failed to save raw blocks: {e}")
