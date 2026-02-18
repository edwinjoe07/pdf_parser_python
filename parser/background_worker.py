"""
Background Parser Worker
========================
Handles page-level PDF parsing with checkpointing, pause/resume,
and crash recovery.

Architecture:
    - Each exam gets its own worker thread
    - Worker processes one page at a time
    - After each page: saves completed questions, commits, updates checkpoint
    - Checks DB status before each page (for pause detection)
    - On resume: replays earlier pages to rebuild state machine, then continues
    - On failure: marks exam as failed, preserves already-parsed data

Usage:
    # Start new parsing
    spawn_worker(exam_id, pdf_path, config, start_from_page=0)

    # Resume from checkpoint
    spawn_worker(exam_id, pdf_path, config, start_from_page=current_page + 1)
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import traceback
from pathlib import Path
from typing import Optional

import fitz  # PyMuPDF

from . import database as db
from . import storage
from .block_extractor import BlockExtractor
from .engine import ParserConfig
from .state_machine import StateMachineParser
from .validator import ValidationEngine

logger = logging.getLogger(__name__)

# ─── Active Worker Registry ──────────────────────────────────────────────────

_active_workers: dict[int, "BackgroundParserWorker"] = {}
_workers_lock = threading.Lock()


def get_worker(exam_id: int) -> Optional["BackgroundParserWorker"]:
    """Get the active worker for an exam, if any."""
    with _workers_lock:
        return _active_workers.get(exam_id)


def spawn_worker(
    exam_id: int,
    pdf_path: str,
    config: ParserConfig,
    start_from_page: int = 0,
) -> threading.Thread:
    """
    Spawn a background parsing worker thread.

    Args:
        exam_id: Database ID of the exam.
        pdf_path: Absolute path to the PDF file.
        config: Parser configuration.
        start_from_page: Page to start processing from (0 = start fresh).

    Returns:
        The spawned Thread object.
    """
    worker = BackgroundParserWorker(exam_id, pdf_path, config)

    thread = threading.Thread(
        target=worker.run,
        args=(start_from_page,),
        daemon=True,
        name=f"parser-worker-exam-{exam_id}",
    )
    thread.start()

    logger.info(
        f"Spawned worker thread for exam_id={exam_id}, "
        f"start_from_page={start_from_page}"
    )
    return thread


class BackgroundParserWorker:
    """
    Background worker for page-by-page PDF parsing with checkpointing.

    Features:
        - Page-level commits (each page is a checkpoint)
        - Pause support (checks DB status before each page)
        - Crash recovery (resumes from last committed checkpoint)
        - Idempotent saves (delete-before-insert per question number)
    """

    def __init__(self, exam_id: int, pdf_path: str, config: ParserConfig):
        self.exam_id = exam_id
        self.pdf_path = os.path.abspath(pdf_path)
        self.config = config
        self._stop_requested = False

    def request_stop(self):
        """Signal the worker to stop gracefully after the current page."""
        self._stop_requested = True

    def run(self, start_from_page: int = 0):
        """
        Main entry point. Runs in a background thread.

        Args:
            start_from_page: 1-indexed page to start from.
                             0 means start fresh from page 1.
        """
        # Register in active workers
        with _workers_lock:
            _active_workers[self.exam_id] = self

        try:
            self._run_internal(start_from_page)
        finally:
            # Unregister
            with _workers_lock:
                _active_workers.pop(self.exam_id, None)

    # ─── Core Parsing Loop ────────────────────────────────────────────────

    def _run_internal(self, start_from_page: int):
        """Internal parsing loop with full error handling."""
        doc = None
        try:
            # Mark as processing
            db.update_exam(self.exam_id, status="processing", last_error=None)

            # Resolve exam info
            exam = db.get_exam(self.exam_id)
            if not exam:
                logger.error(f"Exam {self.exam_id} not found in DB")
                return

            exam_name = exam["name"]

            # Setup image directory
            image_dir = storage.get_exam_image_dir(exam_name)

            # Setup block extractor
            extractor = BlockExtractor(
                image_output_dir=str(image_dir),
                image_format=self.config.image_format,
                min_image_size=self.config.min_image_size,
                dpi=self.config.image_dpi,
            )

            # Setup state machine
            state_machine = StateMachineParser()
            state_machine.reset()

            # Open PDF
            doc = fitz.open(self.pdf_path)
            total_pages = doc.page_count

            # Update total pages
            db.update_exam(self.exam_id, total_pages=total_pages)

            logger.info(
                f"Exam {self.exam_id}: Starting parse of {total_pages} pages "
                f"(start_from_page={start_from_page})"
            )

            global_order = 0
            process_from = max(1, start_from_page)

            # ── Replay Phase (rebuild state machine for resume) ───────
            if process_from > 1:
                global_order = self._replay_pages(
                    doc, extractor, state_machine, process_from - 1
                )

            # ── Cleanup partial data for pages we're about to re-process
            db.delete_questions_for_page_range(self.exam_id, process_from)

            # ── Main Processing Loop ──────────────────────────────────
            for page_num in range(process_from, total_pages + 1):
                # Check for stop signal (in-memory, fast)
                if self._stop_requested:
                    db.update_exam(self.exam_id, status="paused")
                    logger.info(
                        f"Exam {self.exam_id}: Paused at page {page_num} "
                        f"(stop requested)"
                    )
                    return

                # Check DB status (for pause via API)
                current_status = self._check_status()
                if current_status not in ("processing",):
                    logger.info(
                        f"Exam {self.exam_id}: Status is '{current_status}', "
                        f"stopping at page {page_num}"
                    )
                    return

                # Extract blocks for this page
                blocks = extractor.extract_from_page(
                    doc, page_num, global_order)
                global_order += len(blocks)

                # Feed blocks to state machine
                prev_count = len(state_machine.questions)
                for block in blocks:
                    state_machine._process_block(block)

                # Get newly finalized questions
                new_questions = state_machine.questions[prev_count:]

                # Save with idempotency (delete + insert for each question)
                for q in new_questions:
                    self._save_question(q, image_dir, exam_name)

                # Update checkpoint
                db.update_exam(self.exam_id, current_page=page_num)

                if new_questions:
                    logger.info(
                        f"Exam {self.exam_id}: Page {page_num}/{total_pages} — "
                        f"{len(new_questions)} question(s) saved"
                    )

            # ── Finalize last question ────────────────────────────────
            if state_machine.current_question:
                prev_count = len(state_machine.questions)
                state_machine.finalize()
                final_questions = state_machine.questions[prev_count:]
                for q in final_questions:
                    self._save_question(q, image_dir, exam_name)

            # ── Run validation ────────────────────────────────────────
            total_questions = len(state_machine.questions)
            parsed_numbers = sorted(
                {q.question_number for q in state_machine.questions}
            )

            # Raw-text scan: find all question anchors across ALL pages
            raw_detected = self._raw_scan_question_numbers(doc)

            # Run full validation engine on structured questions
            validator = ValidationEngine()
            validation_report = validator.validate(state_machine.questions)

            # Build detailed validation with per-question diagnostics
            validation_data = self._build_validation_data(
                state_machine.questions,
                raw_detected,
                validation_report,
                total_pages,
            )

            validation_json_str = json.dumps(
                validation_data, ensure_ascii=False, default=str
            )

            # ── Mark completed ────────────────────────────────────────
            db.update_exam(
                self.exam_id,
                status="completed",
                current_page=total_pages,
                total_questions=total_questions,
                validation_json=validation_json_str,
            )

            logger.info(
                f"Exam {self.exam_id}: Parsing COMPLETED — "
                f"{total_questions} questions from {total_pages} pages"
            )

        except Exception as e:
            tb = traceback.format_exc()
            error_msg = f"{e}\n{tb}"
            logger.error(f"Exam {self.exam_id}: Parsing FAILED — {error_msg}")

            # Mark as failed, preserve already-parsed data
            db.update_exam(
                self.exam_id,
                status="failed",
                last_error=error_msg[:5000],
            )

        finally:
            if doc:
                try:
                    doc.close()
                except Exception:
                    pass

    # ─── Replay (Rebuild State Machine on Resume) ─────────────────────────

    def _replay_pages(
        self,
        doc: fitz.Document,
        extractor: BlockExtractor,
        state_machine: StateMachineParser,
        up_to_page: int,
    ) -> int:
        """
        Replay pages 1..up_to_page through the state machine WITHOUT
        saving to DB. This rebuilds the state machine's internal state
        so that processing can continue correctly from the next page.

        Args:
            doc: Open PDF document.
            extractor: Block extractor instance (also warms up image caches).
            state_machine: State machine to feed blocks into.
            up_to_page: Last page to replay (inclusive, 1-indexed).

        Returns:
            The global_order counter after replay.
        """
        logger.info(
            f"Exam {self.exam_id}: Replaying pages 1-{up_to_page} "
            f"to rebuild state machine"
        )

        global_order = 0
        for page_num in range(1, up_to_page + 1):
            blocks = extractor.extract_from_page(doc, page_num, global_order)
            global_order += len(blocks)
            for block in blocks:
                state_machine._process_block(block)

        logger.info(
            f"Exam {self.exam_id}: Replay complete — "
            f"{len(state_machine.questions)} questions in state machine"
        )
        return global_order

    # ─── Question Persistence ─────────────────────────────────────────────

    def _save_question(self, q, image_dir: Path, exam_name: str):
        """
        Save a single ParsedQuestion to DB with idempotency.
        Deletes any existing question with the same number first.
        """
        q_dict = q.model_dump()
        q_dict = self._remap_question_images(q_dict, image_dir)

        # Idempotency: delete existing question with same number
        db.delete_question_by_exam_and_number(
            self.exam_id, q.question_number
        )

        # Insert
        db.insert_single_question(self.exam_id, q_dict)

    def _remap_question_images(self, q_dict: dict, image_dir: Path) -> dict:
        """
        Remap image paths in a question dict to be relative to project root.
        This mirrors the logic in crud._remap_image_paths but for a single question.
        """
        project_root = storage.get_project_root()

        for key in ("question_images", "answer_images", "explanation_images"):
            q_dict[key] = [
                self._resolve_image_path(p, image_dir, project_root)
                for p in q_dict.get(key, [])
            ]

        for opt in q_dict.get("options", []):
            opt["images"] = [
                self._resolve_image_path(p, image_dir, project_root)
                for p in opt.get("images", [])
            ]

        return q_dict

    def _resolve_image_path(
        self, image_ref: str, image_dir: Path, project_root: Path
    ) -> str:
        """Resolve an image reference to a relative path from project root."""
        if not image_ref:
            return image_ref

        # Try to find the actual file
        candidates = [
            project_root / image_ref,
            image_dir / Path(image_ref).name,
            image_dir / image_ref,
            project_root / "output" / "questions" / image_ref,
        ]

        for c in candidates:
            if c.exists() and c.is_file():
                try:
                    return str(c.relative_to(project_root))
                except ValueError:
                    return str(c)

        # File not found — keep original reference
        return image_ref

    # ─── Helpers ──────────────────────────────────────────────────────────

    def _check_status(self) -> str:
        """Check the current exam status from DB."""
        exam = db.get_exam(self.exam_id)
        if not exam:
            return "cancelled"
        return exam.get("status", "processing")

    # ─── Raw-Text Question Scan ───────────────────────────────────────────

    # Matches "Question: 1", "Question 42", etc. — same pattern as state_machine
    _QUESTION_ANCHOR = re.compile(
        r"(?:^|\n)\s*Question\s*:?\s*(\d+)", re.IGNORECASE
    )

    def _raw_scan_question_numbers(self, doc: fitz.Document) -> dict[int, int]:
        """
        Scan the entire PDF for question number anchors using raw text.

        Returns:
            dict mapping question_number → page_number where it was detected.
        """
        detected: dict[int, int] = {}
        for page_idx in range(doc.page_count):
            text = doc[page_idx].get_text("text")
            for m in self._QUESTION_ANCHOR.finditer(text):
                q_num = int(m.group(1))
                if q_num not in detected:
                    detected[q_num] = page_idx + 1  # 1-indexed
        return detected

    # ─── Validation Report Builder ────────────────────────────────────────

    def _build_validation_data(
        self,
        parsed_questions: list,
        raw_detected: dict[int, int],
        validation_report,
        total_pages: int,
    ) -> dict:
        """
        Build comprehensive validation data combining:
        - Raw text scan (all question numbers the PDF mentions)
        - Structured parse results
        - Per-question anomaly reasons for failures

        Returns a rich dict with:
            raw_detected_count, parsed_count, missing_questions (with reasons),
            per-question anomalies, duplicate info, etc.
        """
        # Numbers that were successfully parsed/structured
        parsed_numbers = {q.question_number for q in parsed_questions}

        # Build per-question anomaly map from parsed questions
        per_question_anomalies: dict[int, list[dict]] = {}
        for q in parsed_questions:
            if q.anomalies:
                per_question_anomalies[q.question_number] = [
                    {
                        "type": a.type.value,
                        "severity": a.severity,
                        "message": a.message,
                        "context": a.context,
                    }
                    for a in q.anomalies
                ]

        # Classify parsed questions by completeness
        fully_structured = []
        partially_structured = []
        for q in parsed_questions:
            is_complete = q.has_question_text and q.has_answer
            if is_complete:
                fully_structured.append(q.question_number)
            else:
                reasons = []
                if not q.has_question_text:
                    reasons.append("missing_question_text")
                if not q.has_answer:
                    reasons.append("missing_answer")
                if not q.has_explanation:
                    reasons.append("missing_explanation")
                partially_structured.append({
                    "question_number": q.question_number,
                    "page_start": q.page_start,
                    "page_end": q.page_end,
                    "has_question_text": q.has_question_text,
                    "has_answer": q.has_answer,
                    "has_explanation": q.has_explanation,
                    "option_count": len(q.options),
                    "image_count": q.image_count,
                    "reasons": reasons,
                    "anomalies": per_question_anomalies.get(
                        q.question_number, []
                    ),
                })

        # Questions detected in raw scan but NOT in parsed output
        raw_numbers = set(raw_detected.keys())
        missing_numbers = sorted(raw_numbers - parsed_numbers)

        missing_questions = []
        for q_num in missing_numbers:
            page = raw_detected.get(q_num, 0)
            reason = self._diagnose_missing_question(q_num, page, raw_detected)
            missing_questions.append({
                "question_number": q_num,
                "page_detected": page,
                "reason": reason,
            })

        # Sequence gaps — numbers expected in range but not in raw scan either
        all_numbers = raw_numbers | parsed_numbers
        if all_numbers:
            min_num = min(all_numbers)
            max_num = max(all_numbers)
            expected = set(range(min_num, max_num + 1))
            sequence_gaps = sorted(expected - all_numbers)
        else:
            sequence_gaps = []

        # Duplicates (detected by raw scan)
        from collections import Counter
        raw_number_list = list(raw_detected.keys())
        # Actually re-scan for duplicates (raw_detected dedupes to first)
        dup_numbers = sorted(
            validation_report.duplicate_question_numbers
        )

        return {
            "summary": {
                "raw_detected_count": len(raw_detected),
                "parsed_count": len(parsed_questions),
                "fully_structured_count": len(fully_structured),
                "partially_structured_count": len(partially_structured),
                "missing_lost_count": len(missing_questions),
                "sequence_gap_count": len(sequence_gaps),
                "duplicate_count": len(dup_numbers),
                "total_pages": total_pages,
                "success_rate": validation_report.success_rate,
            },
            "fully_structured": sorted(fully_structured),
            "partially_structured": partially_structured,
            "missing_questions": missing_questions,
            "sequence_gaps": sequence_gaps,
            "duplicate_question_numbers": dup_numbers,
            "questions_missing_answer": sorted(
                validation_report.questions_missing_answer
            ),
            "questions_missing_explanation": sorted(
                validation_report.questions_missing_explanation
            ),
            "anomaly_breakdown": validation_report.anomaly_breakdown,
            "per_question_anomalies": {
                str(k): v for k, v in per_question_anomalies.items()
            },
        }

    def _diagnose_missing_question(
        self,
        q_num: int,
        page: int,
        raw_detected: dict[int, int],
    ) -> str:
        """
        Try to diagnose WHY a question was detected in raw text
        but not parsed by the state machine.
        """
        # Check if it's surrounded by parsed questions on the same page
        same_page_questions = [
            n for n, p in raw_detected.items() if p == page and n != q_num
        ]

        reasons = []

        # Possible: question text is split across pages or malformed
        if not same_page_questions:
            reasons.append(
                "Sole question on page — may have non-standard formatting"
            )

        # Possible: the anchor was in noise text (header/footer)
        reasons.append(
            "Question anchor detected in raw text but state machine could not "
            "build a complete question structure — likely malformed layout, "
            "split across page boundaries, or header/footer noise"
        )

        return "; ".join(reasons)
