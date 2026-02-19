"""
CRUD Service Layer
==================
High-level operations that coordinate SQLite + filesystem.
Every mutation updates both the database AND the filesystem atomically.
This is the ONLY layer that should be called from API endpoints.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

from . import database as db
from . import storage
from .engine import ParserConfig, ParserEngine
from .models import ParseResult

logger = logging.getLogger(__name__)


# ─── Upload & Parse (Main Flow) ──────────────────────────────────────────────


def upload_and_parse(
    pdf_path: str,
    exam_name: str = "",
    exam_provider: str = "",
    exam_version: str = "",
    original_filename: str = "",
    progress_callback=None,
) -> dict:
    """
    Full upload→parse→persist pipeline.

    Steps:
        1. Save PDF to uploads/raw_pdfs/
        2. Insert exam record into SQLite
        3. Parse PDF (extract blocks, run FSM, validate)
        4. Save images to uploads/images/{exam_name}/
        5. Insert questions/options/images into SQLite
        6. Return {exam_id, total_questions}

    Args:
        pdf_path: Absolute path to the uploaded PDF file.
        exam_name: Human-readable exam name (defaults to filename stem).
        exam_provider: Optional provider string.
        exam_version: Optional version string.
        original_filename: Original upload filename.
        progress_callback: Optional callback(current_page, total_pages).

    Returns:
        dict with exam_id and total_questions.
    """
    pdf_path = os.path.abspath(pdf_path)
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    filename = original_filename or os.path.basename(pdf_path)
    name = exam_name or Path(filename).stem

    # ── Diagnostics: log DB path ──────────────────────────────────────
    db_path = db.get_db_path()
    logger.info(f"[upload_and_parse] DB path: {db_path}")
    logger.info(f"[upload_and_parse] Starting pipeline for: {filename}")

    # ── Step 1: Save PDF persistently ─────────────────────────────────
    stored_pdf_path = storage.save_pdf(pdf_path, filename)
    logger.info(f"[upload_and_parse] PDF stored at: {stored_pdf_path}")

    # ── Step 2: Setup image directory ─────────────────────────────────
    image_dir = storage.get_exam_image_dir(name)
    image_rel_dir = storage.get_exam_image_dir_relative(name)
    logger.info(f"[upload_and_parse] Image directory: {image_dir}")

    # ── Step 3: Parse PDF ─────────────────────────────────────────────
    config = ParserConfig(
        output_dir=str(storage.get_project_root() / "output"),
        image_base_dir=str(image_dir.parent),  # parent = uploads/images/
        exam_name=name,
        exam_provider=exam_provider,
        exam_version=exam_version,
        exam_id=storage._sanitize_name(name),
    )

    engine = ParserEngine(config)
    result: ParseResult = engine.parse(
        pdf_path, progress_callback=progress_callback)

    parsed_count = len(result.questions)
    logger.info(f"[upload_and_parse] Parsed question count: {parsed_count}")

    # ── Hard validation: reject empty parse ───────────────────────────
    if parsed_count == 0:
        logger.error(
            "[upload_and_parse] Parser returned ZERO questions — aborting")
        raise ValueError(
            "Parser returned zero questions. PDF may be empty, corrupt, or in an unsupported format.")

    # ── Step 4: Remap image paths to uploads/images/{exam}/ ──────────
    exam_id_dir = Path(config.image_base_dir) / config.exam_id
    questions_data = _remap_image_paths(
        result, exam_id_dir, image_dir, name
    )

    # ── Step 5 + 6: Insert into SQLite (atomic) ──────────────────────
    exam_id = None
    try:
        exam_id = db.insert_exam(
            name=name,
            file_path=stored_pdf_path,
            source_pdf=result.exam.source_pdf,
            file_hash=result.exam.file_hash,
            file_size_bytes=result.exam.file_size_bytes,
            total_pages=result.exam.total_pages,
            total_questions=parsed_count,
            provider=exam_provider,
            version=exam_version,
            parser_version=result.parse_version.parser_version,
        )
        logger.info(f"[upload_and_parse] Inserted exam row id={exam_id}")

        db.bulk_insert_questions(exam_id, questions_data)
        logger.info(
            f"[upload_and_parse] Bulk-inserted {parsed_count} questions for exam_id={exam_id}")

    except Exception as e:
        # ── Rollback: remove partial DB data + filesystem artifacts ───
        logger.error(
            f"[upload_and_parse] DB insert failed: {e}", exc_info=True)
        if exam_id is not None:
            try:
                db.delete_exam(exam_id)
            except Exception:
                pass
        # Clean up stored PDF
        try:
            storage.delete_pdf(filename)
        except Exception:
            pass
        # Clean up images
        try:
            storage.delete_exam_images(name)
        except Exception:
            pass
        raise RuntimeError(f"Database insert failed, rolled back: {e}") from e

    # ── Step 7: Verify stored count matches parsed count ─────────────
    stored_count = db.count_exam_questions(exam_id)
    logger.info(
        f"[upload_and_parse] VERIFICATION — parsed={parsed_count}, "
        f"stored_in_db={stored_count}, exam_id={exam_id}, db_path={db_path}"
    )

    if stored_count != parsed_count:
        logger.error(
            f"[upload_and_parse] MISMATCH: parsed {parsed_count} but "
            f"only {stored_count} stored in DB!"
        )

    return {
        "exam_id": exam_id,
        "parsed_questions": parsed_count,
        "stored_questions": stored_count,
        "db_path": db_path,
    }


def _remap_image_paths(
    result: ParseResult,
    source_dir: Path,
    target_dir: Path,
    exam_name: str,
) -> list[dict]:
    """
    Move images from engine output dir to uploads/images/{exam}/
    and return questions data with updated relative paths.
    """
    project_root = storage.get_project_root()
    questions_data = []

    for q in result.questions:
        q_dict = q.model_dump()

        # Process each image list
        q_dict["question_images"] = [
            _move_and_relativize(p, source_dir, target_dir, project_root)
            for p in q_dict.get("question_images", [])
        ]
        q_dict["answer_images"] = [
            _move_and_relativize(p, source_dir, target_dir, project_root)
            for p in q_dict.get("answer_images", [])
        ]
        q_dict["explanation_images"] = [
            _move_and_relativize(p, source_dir, target_dir, project_root)
            for p in q_dict.get("explanation_images", [])
        ]

        for opt in q_dict.get("options", []):
            opt["images"] = [
                _move_and_relativize(p, source_dir, target_dir, project_root)
                for p in opt.get("images", [])
            ]

        questions_data.append(q_dict)

    return questions_data


def _move_and_relativize(
    image_ref: str,
    source_dir: Path,
    target_dir: Path,
    project_root: Path,
) -> str:
    """
    Given an image reference (relative path like 'questions/exam/img.png'
    or just a filename), ensure the file is in target_dir and return
    a relative path from project root.
    """
    if not image_ref:
        return image_ref

    # Try to find the actual file
    candidates = [
        project_root / image_ref,
        source_dir / Path(image_ref).name,
        source_dir / image_ref,
        project_root / "output" / "questions" / image_ref,
        project_root / "storage" / "questions" / image_ref,
    ]

    src_file = None
    for c in candidates:
        if c.exists() and c.is_file():
            src_file = c
            break

    if not src_file:
        # File not found — keep original reference
        logger.warning(f"Image not found for move: {image_ref}")
        return image_ref

    dest_file = target_dir / src_file.name

    # Move if source != destination
    if src_file.resolve() != dest_file.resolve():
        shutil.copy2(str(src_file), str(dest_file))

    # Return relative path from project root
    try:
        return str(dest_file.relative_to(project_root))
    except ValueError:
        return str(dest_file)


# ─── Read Operations (from SQLite only) ──────────────────────────────────────


def get_exam(exam_id: int) -> Optional[dict]:
    """
    Get full exam with all questions from SQLite.
    Never re-parses. Returns None if not found.
    """
    exam = db.get_exam(exam_id)
    if not exam:
        return None

    questions = db.get_exam_questions(exam_id)

    # Try to load stored validation report
    validation_data = None
    validation_json_str = exam.get("validation_json", "")
    if validation_json_str:
        try:
            import json
            validation_data = json.loads(validation_json_str)
        except (json.JSONDecodeError, TypeError):
            pass

    # Build validation section from stored report or compute from questions
    if validation_data and "summary" in validation_data:
        summary = validation_data["summary"]
        validation = {
            "total_questions_detected": summary.get(
                "raw_detected_count", len(questions)
            ),
            "structured_successfully": summary.get(
                "fully_structured_count", len(questions)
            ),
            "missing_question_numbers": sorted(
                [q["question_number"]
                 for q in validation_data.get("missing_questions", [])]
            ),
            "duplicate_question_numbers": validation_data.get(
                "duplicate_question_numbers", []
            ),
            "questions_missing_answer": validation_data.get(
                "questions_missing_answer", []
            ),
            "questions_missing_explanation": validation_data.get(
                "questions_missing_explanation", []
            ),
            "failed_to_structure": [
                q.get("question_number", 0)
                for q in validation_data.get("partially_structured", [])
            ],
            "missing_lost_count": summary.get("missing_lost_count", 0),
            "anomaly_breakdown": validation_data.get(
                "anomaly_breakdown", {}
            ),
            "success_rate": summary.get("success_rate", 0.0),
            # Enhanced validation data for missing questions UI
            "missing_questions": validation_data.get(
                "missing_questions", []
            ),
            "partially_structured": validation_data.get(
                "partially_structured", []
            ),
            "sequence_gaps": validation_data.get(
                "sequence_gaps", []
            ),
            "per_question_anomalies": validation_data.get(
                "per_question_anomalies", {}
            ),
            "fully_structured_count": summary.get(
                "fully_structured_count", 0
            ),
        }
    else:
        # Fallback: basic validation from DB data
        fully_structured = sum(
            1 for q in questions
            if (q.get("question_text") or "").strip()
            and (q.get("answer_text") or "").strip()
        )
        validation = {
            "total_questions_detected": len(questions),
            "structured_successfully": fully_structured,
            "missing_question_numbers": [],
            "duplicate_question_numbers": [],
            "questions_missing_answer": [
                q.get("question_number", 0) for q in questions
                if not (q.get("answer_text") or "").strip()
            ],
            "questions_missing_explanation": [
                q.get("question_number", 0) for q in questions
                if not (q.get("explanation_text") or "").strip()
            ],
            "failed_to_structure": [],
            "missing_lost_count": 0,
            "anomaly_breakdown": {},
            "success_rate": (
                round(fully_structured / len(questions) * 100, 2)
                if questions else 0.0
            ),
            "missing_questions": [],
            "partially_structured": [],
            "sequence_gaps": [],
            "per_question_anomalies": {},
            "fully_structured_count": fully_structured,
        }

    # Build response in the format the UI expects
    return {
        "exam": {
            "id": exam["id"],
            "name": exam["name"],
            "source_pdf": exam.get("source_pdf", ""),
            "file_hash": exam.get("file_hash", ""),
            "file_size_bytes": exam.get("file_size_bytes", 0),
            "total_pages": exam.get("total_pages", 0),
            "provider": exam.get("provider", ""),
            "version": exam.get("version", ""),
            "status": exam.get("status", ""),
            "current_page": exam.get("current_page", 0),
            "created_at": exam.get("created_at", ""),
            "original_filename": exam.get("original_filename", ""),
        },
        "parse_version": {
            "parser_version": exam.get("parser_version", "1.0.0"),
        },
        "questions": [_format_question(q) for q in questions],
        "validation": validation,
    }


def get_exam_question(exam_id: int, question_number: int) -> Optional[dict]:
    """
    Get a single question by exam_id + question_number.
    Returns UI-compatible dict or None.
    """
    q = db.get_question_by_number(exam_id, question_number)
    if not q:
        return None
    return _format_question(q)


def list_exams() -> list[dict]:
    """List all exams (summary)."""
    return db.list_exams()


# ─── Update Operations ───────────────────────────────────────────────────────


def update_question(question_id: int, **fields) -> bool:
    """Update question text fields in SQLite."""
    return db.update_question(question_id, **fields)


def update_option(option_id: int, **fields) -> bool:
    """Update an option in SQLite."""
    return db.update_option(option_id, **fields)


def replace_image(
    image_id: int,
    new_file_path: str,
    exam_name: str,
) -> bool:
    """
    Replace an image:
      1. Delete old file from filesystem
      2. Save new file to correct folder
      3. Update image_path in SQLite
    """
    # Save new file
    new_rel_path = storage.save_image(new_file_path, exam_name)

    # Update DB — returns old path
    old_path = db.update_image(image_id, image_path=new_rel_path)

    # Delete old file
    if old_path and old_path != new_rel_path:
        storage.delete_image_file(old_path)

    return True


def update_image_section(
    image_id: int,
    new_section: str,
    new_option_key: str = None,
) -> bool:
    """
    Move an image to a different section (e.g., question → explanation).
    Updates SQLite only — no file duplication needed.
    """
    return db.update_image(
        image_id, section=new_section, option_key=new_option_key
    ) is not None


# ─── Delete Operations ───────────────────────────────────────────────────────


def delete_exam(exam_id: int) -> bool:
    """
    Delete an exam:
      1. Get exam info + all image paths
      2. Delete from SQLite (cascades to questions/options/images)
      3. Delete all image files from filesystem
      4. Delete exam image folder if empty
      5. Optionally delete the PDF
    """
    exam = db.get_exam(exam_id)
    if not exam:
        return False

    # Collect all image paths before deletion
    image_paths = db.get_exam_image_paths(exam_id)

    # Delete from DB (cascade)
    db.delete_exam(exam_id)

    # Delete image files
    storage.delete_image_files(image_paths)

    # Cleanup empty directory
    exam_name = exam.get("name", "")
    if exam_name:
        storage.cleanup_empty_exam_dir(exam_name)

    # Optionally delete the PDF
    file_path = exam.get("file_path", "")
    if file_path:
        abs_path = storage.get_project_root() / file_path
        if abs_path.exists():
            abs_path.unlink()
            logger.info(f"Deleted PDF: {file_path}")

    logger.info(f"Deleted exam id={exam_id} with {len(image_paths)} images")
    return True


def delete_question(question_id: int) -> bool:
    """
    Delete a question:
      1. Delete from SQLite (cascade handles options/images records)
      2. Delete associated image files
      3. Update exam total_questions
    """
    # Get question to find exam_id
    q = db.get_question(question_id)
    if not q:
        return False

    exam_id = q["exam_id"]

    # Delete and get image paths
    image_paths = db.delete_question(question_id)

    # Delete image files
    storage.delete_image_files(image_paths)

    # Update exam question count
    remaining = db.get_exam_questions(exam_id)
    db.update_exam(exam_id, total_questions=len(remaining))

    # Cleanup empty dirs
    exam = db.get_exam(exam_id)
    if exam and not remaining:
        storage.cleanup_empty_exam_dir(exam["name"])

    return True


def delete_image(image_id: int) -> bool:
    """
    Delete an image:
      1. Delete record from SQLite
      2. Delete file from filesystem
    """
    image_path = db.delete_image(image_id)
    if image_path:
        storage.delete_image_file(image_path)
        return True
    return False


# ─── Create Operations ───────────────────────────────────────────────────────


def add_question(
    exam_id: int,
    question_number: int,
    question_text: str = "",
    question_type: str = "mcq",
    answer_text: str = "",
    explanation_text: str = "",
    page_start: int = 0,
    page_end: int = 0,
) -> int:
    """
    Manually add a new question.
    Returns the new question_id.
    """
    question_id = db.insert_question(
        exam_id=exam_id,
        question_number=question_number,
        question_type=question_type,
        question_text=question_text,
        answer_text=answer_text,
        explanation_text=explanation_text,
        page_start=page_start,
        page_end=page_end,
    )

    # Update exam count
    questions = db.get_exam_questions(exam_id)
    db.update_exam(exam_id, total_questions=len(questions))

    return question_id


def add_option(
    question_id: int,
    option_key: str,
    option_text: str = "",
    is_correct: bool = False,
) -> int:
    """Add a new option to a question. Returns option_id."""
    return db.insert_option(
        question_id=question_id,
        option_key=option_key,
        option_text=option_text,
        is_correct=is_correct,
    )


def add_image(
    question_id: int,
    section: str,
    file_path: str,
    exam_name: str,
    option_key: str = None,
    block_order: int = 0,
) -> int:
    """
    Add a new image:
      1. Save file to uploads/images/{exam_name}/
      2. Insert record in SQLite
    Returns image_id.
    """
    # Save file
    rel_path = storage.save_image(file_path, exam_name)

    # Insert DB record
    return db.insert_image(
        question_id=question_id,
        section=section,
        image_path=rel_path,
        block_order=block_order,
        option_key=option_key,
    )


def enrich_result_with_blocks(result_dict: dict) -> dict:
    """
    Walk all questions in a result dict and add ``blocks`` structure
    so the UI can render them. Mutates in-place and returns the dict.
    """
    for q in result_dict.get("questions", []):
        if "blocks" not in q:
            _question_to_blocks(q)
    return result_dict


def _question_to_blocks(q: dict) -> dict:
    """
    Convert a flat question dict into the ``blocks`` format that the UI
    expects. The UI reads ``q.blocks.question``, ``q.blocks.options``, etc.
    Each section is a list of ``{type, content, page_number, order_index}`` objects.
    """
    blocks: dict[str, list[dict]] = {
        "question": [],
        "options": [],
        "answer": [],
        "explanation": [],
    }
    page = q.get("page_start", 1)
    idx = 0

    # Question text
    if q.get("question_text"):
        blocks["question"].append({
            "type": "text",
            "content": q["question_text"],
            "page_number": page,
            "order_index": idx,
        })
        idx += 1

    # Question images
    for img in q.get("question_images", []):
        blocks["question"].append({
            "type": "image",
            "content": img,
            "page_number": page,
            "order_index": idx,
        })
        idx += 1

    # Options
    for opt in q.get("options", []):
        key = opt.get("key", "")
        text = opt.get("text", "")
        content = f"{key}. {text}" if text else f"{key}."
        blocks["options"].append({
            "type": "text",
            "content": content,
            "page_number": page,
            "order_index": idx,
        })
        idx += 1
        for img in opt.get("images", []):
            blocks["options"].append({
                "type": "image",
                "content": img,
                "page_number": page,
                "order_index": idx,
            })
            idx += 1

    # Answer
    if q.get("answer_text"):
        blocks["answer"].append({
            "type": "text",
            "content": q["answer_text"],
            "page_number": page,
            "order_index": idx,
        })
        idx += 1
    for img in q.get("answer_images", []):
        blocks["answer"].append({
            "type": "image",
            "content": img,
            "page_number": page,
            "order_index": idx,
        })
        idx += 1

    # Explanation
    if q.get("explanation_text"):
        blocks["explanation"].append({
            "type": "text",
            "content": q["explanation_text"],
            "page_number": page,
            "order_index": idx,
        })
        idx += 1
    for img in q.get("explanation_images", []):
        blocks["explanation"].append({
            "type": "image",
            "content": img,
            "page_number": page,
            "order_index": idx,
        })
        idx += 1

    q["blocks"] = blocks
    return q


# ─── Helpers ─────────────────────────────────────────────────────────────────



def _format_question(q: dict) -> dict:
    """
    Format a hydrated question dict into the exact structure the UI expects.

    Output:
        question_number, question_text, question_images,
        options[{key, text, is_correct, images}],
        answer_text, answer_images,
        explanation_text, explanation_images
    """
    result = {
        "question_number": q.get("question_number", 0),
        "question_type": q.get("question_type", "mcq"),
        "question_text": q.get("question_text", ""),
        "question_images": q.get("question_images", []),
        "options": q.get("options", []),
        "answer_text": q.get("answer_text", ""),
        "answer_images": q.get("answer_images", []),
        "explanation_text": q.get("explanation_text", ""),
        "explanation_images": q.get("explanation_images", []),
        "page_start": q.get("page_start", 0),
        "page_end": q.get("page_end", 0),
        "anomalies": [],
        "raw_text": q.get("raw_text", ""),
        "anomaly_score": 0,
        "has_question_text": bool(q.get("question_text", "").strip()),
        "has_answer": bool(q.get("answer_text", "").strip()),
        "has_explanation": bool(q.get("explanation_text", "").strip()),
        "image_count": (
            len(q.get("question_images", []))
            + len(q.get("answer_images", []))
            + len(q.get("explanation_images", []))
            + sum(len(o.get("images", [])) for o in q.get("options", []))
        ),
    }
    return _question_to_blocks(result)
