"""
SQLite Database Layer
=====================
Persistent storage for parsed exam data.
All parsed questions, options, and image references are stored in SQLite.
No in-memory caching — always reads from disk.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default database path: project_root/database.sqlite
_DEFAULT_DB_PATH = str(Path(__file__).parent.parent / "database.sqlite")


def get_db_path() -> str:
    """Return the configured database path."""
    return os.environ.get("PARSER_DB_PATH", _DEFAULT_DB_PATH)


@contextmanager
def get_connection(db_path: str = None):
    """
    Context manager for database connections.
    Ensures proper commit/rollback and connection cleanup.
    """
    db_path = db_path or get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: str = None):
    """
    Initialize the database schema.
    Safe to call multiple times — uses IF NOT EXISTS.
    """
    db_path = db_path or get_db_path()
    logger.info(f"Initializing database at: {db_path}")

    with get_connection(db_path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS exams (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                name TEXT NOT NULL,
                file_path TEXT,
                source_pdf TEXT,
                file_hash TEXT,
                file_size_bytes INTEGER DEFAULT 0,
                total_pages INTEGER DEFAULT 0,
                total_questions INTEGER DEFAULT 0,
                provider TEXT DEFAULT '',
                version TEXT DEFAULT '',
                parser_version TEXT DEFAULT '1.0.0',
                result_json TEXT DEFAULT '',
                original_filename TEXT DEFAULT '',
                status TEXT DEFAULT 'pending',
                current_page INTEGER DEFAULT 0,
                last_error TEXT DEFAULT NULL,
                validation_json TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exam_id INTEGER NOT NULL,
                question_number INTEGER NOT NULL,
                question_type TEXT DEFAULT 'mcq',
                question_text TEXT DEFAULT '',
                answer_text TEXT DEFAULT '',
                explanation_text TEXT DEFAULT '',
                page_start INTEGER DEFAULT 0,
                page_end INTEGER DEFAULT 0,
                raw_text TEXT DEFAULT '',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(exam_id) REFERENCES exams(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER NOT NULL,
                option_key TEXT NOT NULL,
                option_text TEXT DEFAULT '',
                is_correct INTEGER DEFAULT 0,
                FOREIGN KEY(question_id) REFERENCES questions(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS question_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                question_id INTEGER NOT NULL,
                section TEXT NOT NULL,
                option_key TEXT,
                image_path TEXT NOT NULL,
                block_order INTEGER DEFAULT 0,
                FOREIGN KEY(question_id) REFERENCES questions(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_questions_exam_id
                ON questions(exam_id);
            CREATE INDEX IF NOT EXISTS idx_questions_exam_number
                ON questions(exam_id, question_number);
            CREATE INDEX IF NOT EXISTS idx_options_question_id
                ON options(question_id);
            CREATE INDEX IF NOT EXISTS idx_images_question_id
                ON question_images(question_id);
            CREATE INDEX IF NOT EXISTS idx_images_section
                ON question_images(question_id, section);
        """)

    # ── Migrations for existing databases ────────────────────────────
    _migrate_add_columns(db_path)

    logger.info("Database schema initialized successfully")


def _migrate_add_columns(db_path: str = None):
    """Add columns that may be missing in older databases."""
    db_path = db_path or get_db_path()
    with get_connection(db_path) as conn:
        # Read existing columns
        cols = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(exams)").fetchall()
        }
        if "job_id" not in cols:
            conn.execute("ALTER TABLE exams ADD COLUMN job_id TEXT DEFAULT ''")
            logger.info("Migrated: added exams.job_id")
        if "result_json" not in cols:
            conn.execute(
                "ALTER TABLE exams ADD COLUMN result_json TEXT DEFAULT ''")
            logger.info("Migrated: added exams.result_json")
        if "original_filename" not in cols:
            conn.execute(
                "ALTER TABLE exams ADD COLUMN original_filename TEXT DEFAULT ''")
            logger.info("Migrated: added exams.original_filename")
        if "status" not in cols:
            conn.execute(
                "ALTER TABLE exams ADD COLUMN status TEXT DEFAULT 'pending'")
            logger.info("Migrated: added exams.status")
        if "current_page" not in cols:
            conn.execute(
                "ALTER TABLE exams ADD COLUMN current_page INTEGER DEFAULT 0")
            logger.info("Migrated: added exams.current_page")
        if "last_error" not in cols:
            conn.execute(
                "ALTER TABLE exams ADD COLUMN last_error TEXT DEFAULT NULL")
            logger.info("Migrated: added exams.last_error")
        if "validation_json" not in cols:
            conn.execute(
                "ALTER TABLE exams ADD COLUMN validation_json TEXT DEFAULT ''")
            logger.info("Migrated: added exams.validation_json")


# ─── Exam CRUD ────────────────────────────────────────────────────────────────


def insert_exam(
    name: str,
    file_path: str = "",
    source_pdf: str = "",
    file_hash: str = "",
    file_size_bytes: int = 0,
    total_pages: int = 0,
    total_questions: int = 0,
    provider: str = "",
    version: str = "",
    parser_version: str = "1.0.0",
    job_id: str = "",
    result_json: str = "",
    original_filename: str = "",
    db_path: str = None,
) -> int:
    """Insert a new exam record. Returns the exam_id."""
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            """INSERT INTO exams
               (name, file_path, source_pdf, file_hash, file_size_bytes,
                total_pages, total_questions, provider, version, parser_version,
                job_id, result_json, original_filename)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, file_path, source_pdf, file_hash, file_size_bytes,
             total_pages, total_questions, provider, version, parser_version,
             job_id, result_json, original_filename),
        )
        exam_id = cursor.lastrowid
        logger.info(f"Inserted exam id={exam_id} name={name!r}")
        return exam_id


def get_exam(exam_id: int, db_path: str = None) -> Optional[dict]:
    """Fetch a single exam by ID."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM exams WHERE id = ?", (exam_id,)
        ).fetchone()
        return dict(row) if row else None


def list_exams(db_path: str = None) -> list[dict]:
    """List all exams."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM exams ORDER BY created_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def update_exam(exam_id: int, db_path: str = None, **fields) -> bool:
    """Update exam fields. Returns True if row was found."""
    if not fields:
        return False
    allowed = {
        "name", "file_path", "source_pdf", "file_hash", "file_size_bytes",
        "total_pages", "total_questions", "provider", "version",
        "job_id", "result_json", "original_filename",
        "status", "current_page", "last_error", "validation_json",
    }
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return False

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [exam_id]

    with get_connection(db_path) as conn:
        cursor = conn.execute(
            f"UPDATE exams SET {set_clause} WHERE id = ?", values
        )
        return cursor.rowcount > 0


def delete_exam(exam_id: int, db_path: str = None) -> bool:
    """Delete an exam and all cascading data. Returns True if row existed."""
    with get_connection(db_path) as conn:
        cursor = conn.execute("DELETE FROM exams WHERE id = ?", (exam_id,))
        return cursor.rowcount > 0


def get_exam_by_job_id(job_id: str, db_path: str = None) -> Optional[dict]:
    """Fetch a single exam by its API job_id."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM exams WHERE job_id = ?", (job_id,)
        ).fetchone()
        return dict(row) if row else None


def update_exam_result_json(exam_id: int, result_json: str, db_path: str = None):
    """Store the full result JSON blob for an exam."""
    with get_connection(db_path) as conn:
        conn.execute(
            "UPDATE exams SET result_json = ? WHERE id = ?",
            (result_json, exam_id),
        )


def count_exam_questions(exam_id: int, db_path: str = None) -> int:
    """Return the count of questions stored in DB for an exam."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM questions WHERE exam_id = ?",
            (exam_id,),
        ).fetchone()
        return row["cnt"] if row else 0


# ─── Page-Level Checkpointing Helpers ─────────────────────────────────────────


def delete_questions_for_page_range(
    exam_id: int, from_page: int, db_path: str = None
):
    """
    Delete all questions with page_start >= from_page for an exam.
    CASCADE will handle options and images.
    Used on resume to clean up partially-saved page data.
    """
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM questions WHERE exam_id = ? AND page_start >= ?",
            (exam_id, from_page),
        )
        if cursor.rowcount > 0:
            logger.info(
                f"Deleted {cursor.rowcount} questions with page_start >= {from_page} "
                f"for exam_id={exam_id}"
            )


def delete_question_by_exam_and_number(
    exam_id: int, question_number: int, db_path: str = None
) -> bool:
    """
    Delete a specific question by exam_id + question_number.
    Used for idempotent re-insertion during page-level checkpointing.
    CASCADE handles options and images.
    """
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            "DELETE FROM questions WHERE exam_id = ? AND question_number = ?",
            (exam_id, question_number),
        )
        return cursor.rowcount > 0


def insert_single_question(exam_id: int, q: dict, db_path: str = None) -> int:
    """
    Insert a single question with its options and images.
    Each call opens/commits/closes its own connection (page-level checkpoint).
    Returns the new question_id.
    """
    with get_connection(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO questions
               (exam_id, question_number, question_type, question_text,
                answer_text, explanation_text, page_start, page_end, raw_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                exam_id,
                q.get("question_number", 0),
                q.get("question_type", "mcq"),
                q.get("question_text", ""),
                q.get("answer_text", ""),
                q.get("explanation_text", ""),
                q.get("page_start", 0),
                q.get("page_end", 0),
                q.get("raw_text", ""),
            ),
        )
        question_id = cursor.lastrowid

        # Insert options
        for opt in q.get("options", []):
            cursor.execute(
                """INSERT INTO options
                   (question_id, option_key, option_text, is_correct)
                   VALUES (?, ?, ?, ?)""",
                (
                    question_id,
                    opt.get("key", ""),
                    opt.get("text", ""),
                    1 if opt.get("is_correct", False) else 0,
                ),
            )
            # Insert option images
            for idx, img_path in enumerate(opt.get("images", [])):
                cursor.execute(
                    """INSERT INTO question_images
                       (question_id, section, option_key, image_path, block_order)
                       VALUES (?, 'option', ?, ?, ?)""",
                    (question_id, opt.get("key", ""), img_path, idx),
                )

        # Insert question images
        for idx, img_path in enumerate(q.get("question_images", [])):
            cursor.execute(
                """INSERT INTO question_images
                   (question_id, section, option_key, image_path, block_order)
                   VALUES (?, 'question', NULL, ?, ?)""",
                (question_id, img_path, idx),
            )

        # Insert answer images
        for idx, img_path in enumerate(q.get("answer_images", [])):
            cursor.execute(
                """INSERT INTO question_images
                   (question_id, section, option_key, image_path, block_order)
                   VALUES (?, 'answer', NULL, ?, ?)""",
                (question_id, img_path, idx),
            )

        # Insert explanation images
        for idx, img_path in enumerate(q.get("explanation_images", [])):
            cursor.execute(
                """INSERT INTO question_images
                   (question_id, section, option_key, image_path, block_order)
                   VALUES (?, 'explanation', NULL, ?, ?)""",
                (question_id, img_path, idx),
            )

        return question_id


# ─── Question CRUD ────────────────────────────────────────────────────────────


def insert_question(
    exam_id: int,
    question_number: int,
    question_type: str = "mcq",
    question_text: str = "",
    answer_text: str = "",
    explanation_text: str = "",
    page_start: int = 0,
    page_end: int = 0,
    raw_text: str = "",
    db_path: str = None,
) -> int:
    """Insert a question. Returns question_id."""
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            """INSERT INTO questions
               (exam_id, question_number, question_type, question_text,
                answer_text, explanation_text, page_start, page_end, raw_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (exam_id, question_number, question_type, question_text,
             answer_text, explanation_text, page_start, page_end, raw_text),
        )
        return cursor.lastrowid


def bulk_insert_questions(exam_id: int, questions: list[dict], db_path: str = None):
    """
    Bulk insert questions with options and images in a single transaction.
    Each dict in `questions` should match ParsedQuestion structure.
    """
    db_path = db_path or get_db_path()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    try:
        cursor = conn.cursor()

        for q in questions:
            cursor.execute(
                """INSERT INTO questions
                   (exam_id, question_number, question_type, question_text,
                    answer_text, explanation_text, page_start, page_end, raw_text)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    exam_id,
                    q.get("question_number", 0),
                    q.get("question_type", "mcq"),
                    q.get("question_text", ""),
                    q.get("answer_text", ""),
                    q.get("explanation_text", ""),
                    q.get("page_start", 0),
                    q.get("page_end", 0),
                    q.get("raw_text", ""),
                ),
            )
            question_id = cursor.lastrowid

            # Insert options
            for opt in q.get("options", []):
                cursor.execute(
                    """INSERT INTO options
                       (question_id, option_key, option_text, is_correct)
                       VALUES (?, ?, ?, ?)""",
                    (
                        question_id,
                        opt.get("key", ""),
                        opt.get("text", ""),
                        1 if opt.get("is_correct", False) else 0,
                    ),
                )
                option_id = cursor.lastrowid

                # Insert option images
                for idx, img_path in enumerate(opt.get("images", [])):
                    cursor.execute(
                        """INSERT INTO question_images
                           (question_id, section, option_key, image_path, block_order)
                           VALUES (?, 'option', ?, ?, ?)""",
                        (question_id, opt.get("key", ""), img_path, idx),
                    )

            # Insert question images
            for idx, img_path in enumerate(q.get("question_images", [])):
                cursor.execute(
                    """INSERT INTO question_images
                       (question_id, section, option_key, image_path, block_order)
                       VALUES (?, 'question', NULL, ?, ?)""",
                    (question_id, img_path, idx),
                )

            # Insert answer images
            for idx, img_path in enumerate(q.get("answer_images", [])):
                cursor.execute(
                    """INSERT INTO question_images
                       (question_id, section, option_key, image_path, block_order)
                       VALUES (?, 'answer', NULL, ?, ?)""",
                    (question_id, img_path, idx),
                )

            # Insert explanation images
            for idx, img_path in enumerate(q.get("explanation_images", [])):
                cursor.execute(
                    """INSERT INTO question_images
                       (question_id, section, option_key, image_path, block_order)
                       VALUES (?, 'explanation', NULL, ?, ?)""",
                    (question_id, img_path, idx),
                )

        conn.commit()
        logger.info(
            f"Bulk-inserted {len(questions)} questions for exam_id={exam_id}"
        )

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_question(question_id: int, db_path: str = None) -> Optional[dict]:
    """Fetch a single question with options and images."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM questions WHERE id = ?", (question_id,)
        ).fetchone()
        if not row:
            return None
        return _hydrate_question(conn, dict(row))


def get_question_by_number(
    exam_id: int, question_number: int, db_path: str = None
) -> Optional[dict]:
    """Fetch a question by exam_id + question_number."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            """SELECT * FROM questions
               WHERE exam_id = ? AND question_number = ?""",
            (exam_id, question_number),
        ).fetchone()
        if not row:
            return None
        return _hydrate_question(conn, dict(row))


def get_exam_questions(exam_id: int, db_path: str = None) -> list[dict]:
    """Fetch all questions for an exam, ordered by question_number."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM questions
               WHERE exam_id = ?
               ORDER BY question_number""",
            (exam_id,),
        ).fetchall()
        return [_hydrate_question(conn, dict(r)) for r in rows]


def update_question(question_id: int, db_path: str = None, **fields) -> bool:
    """Update question text fields. Returns True if found."""
    allowed = {
        "question_number", "question_type", "question_text",
        "answer_text", "explanation_text", "page_start", "page_end",
        "raw_text",
    }
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return False

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [question_id]

    with get_connection(db_path) as conn:
        cursor = conn.execute(
            f"UPDATE questions SET {set_clause} WHERE id = ?", values
        )
        return cursor.rowcount > 0


def delete_question(question_id: int, db_path: str = None) -> list[str]:
    """
    Delete a question and return list of image paths that were associated.
    Caller should delete the files from filesystem.
    """
    with get_connection(db_path) as conn:
        # Collect image paths first
        rows = conn.execute(
            "SELECT image_path FROM question_images WHERE question_id = ?",
            (question_id,),
        ).fetchall()
        image_paths = [r["image_path"] for r in rows]

        # CASCADE will handle options + images
        conn.execute("DELETE FROM questions WHERE id = ?", (question_id,))
        return image_paths


# ─── Option CRUD ──────────────────────────────────────────────────────────────


def insert_option(
    question_id: int,
    option_key: str,
    option_text: str = "",
    is_correct: bool = False,
    db_path: str = None,
) -> int:
    """Insert a new option. Returns option_id."""
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            """INSERT INTO options (question_id, option_key, option_text, is_correct)
               VALUES (?, ?, ?, ?)""",
            (question_id, option_key, option_text, 1 if is_correct else 0),
        )
        return cursor.lastrowid


def update_option(option_id: int, db_path: str = None, **fields) -> bool:
    """Update an option."""
    allowed = {"option_key", "option_text", "is_correct"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return False

    # Convert is_correct to int
    if "is_correct" in fields:
        fields["is_correct"] = 1 if fields["is_correct"] else 0

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [option_id]

    with get_connection(db_path) as conn:
        cursor = conn.execute(
            f"UPDATE options SET {set_clause} WHERE id = ?", values
        )
        return cursor.rowcount > 0


def delete_option(option_id: int, db_path: str = None) -> bool:
    """Delete an option."""
    with get_connection(db_path) as conn:
        cursor = conn.execute("DELETE FROM options WHERE id = ?", (option_id,))
        return cursor.rowcount > 0


# ─── Image CRUD ──────────────────────────────────────────────────────────────


def insert_image(
    question_id: int,
    section: str,
    image_path: str,
    block_order: int = 0,
    option_key: str = None,
    db_path: str = None,
) -> int:
    """Insert a new image record. Returns image_id."""
    with get_connection(db_path) as conn:
        cursor = conn.execute(
            """INSERT INTO question_images
               (question_id, section, option_key, image_path, block_order)
               VALUES (?, ?, ?, ?, ?)""",
            (question_id, section, option_key, image_path, block_order),
        )
        return cursor.lastrowid


def get_question_images(question_id: int, db_path: str = None) -> list[dict]:
    """Get all images for a question."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """SELECT * FROM question_images
               WHERE question_id = ?
               ORDER BY section, block_order""",
            (question_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def update_image(image_id: int, db_path: str = None, **fields) -> Optional[str]:
    """
    Update image record. Returns old image_path if path changed, else None.
    """
    allowed = {"section", "option_key", "image_path", "block_order"}
    fields = {k: v for k, v in fields.items() if k in allowed}
    if not fields:
        return None

    with get_connection(db_path) as conn:
        old_path = None
        if "image_path" in fields:
            row = conn.execute(
                "SELECT image_path FROM question_images WHERE id = ?",
                (image_id,),
            ).fetchone()
            if row:
                old_path = row["image_path"]

        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [image_id]
        conn.execute(
            f"UPDATE question_images SET {set_clause} WHERE id = ?", values
        )
        return old_path


def delete_image(image_id: int, db_path: str = None) -> Optional[str]:
    """Delete an image record. Returns the image_path for filesystem cleanup."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT image_path FROM question_images WHERE id = ?",
            (image_id,),
        ).fetchone()
        if not row:
            return None

        conn.execute("DELETE FROM question_images WHERE id = ?", (image_id,))
        return row["image_path"]


def get_exam_image_paths(exam_id: int, db_path: str = None) -> list[str]:
    """Get all image paths for an exam (for cleanup)."""
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """SELECT qi.image_path
               FROM question_images qi
               JOIN questions q ON qi.question_id = q.id
               WHERE q.exam_id = ?""",
            (exam_id,),
        ).fetchall()
        return [r["image_path"] for r in rows]


# ─── Helper ──────────────────────────────────────────────────────────────────


def _hydrate_question(conn: sqlite3.Connection, question: dict) -> dict:
    """
    Enrich a question dict with its options and images.
    Produces the exact structure the UI expects:
        question_text, question_images, options[], answer_text,
        explanation_text, explanation_images
    """
    qid = question["id"]

    # Fetch options
    opt_rows = conn.execute(
        """SELECT * FROM options
           WHERE question_id = ? ORDER BY option_key""",
        (qid,),
    ).fetchall()

    # Fetch images grouped by section
    img_rows = conn.execute(
        """SELECT * FROM question_images
           WHERE question_id = ? ORDER BY section, block_order""",
        (qid,),
    ).fetchall()

    # Sort images into buckets
    question_images = []
    answer_images = []
    explanation_images = []
    option_images: dict[str, list[str]] = {}

    for img in img_rows:
        path = img["image_path"]
        sec = img["section"]
        if sec == "question":
            question_images.append(path)
        elif sec == "answer":
            answer_images.append(path)
        elif sec == "explanation":
            explanation_images.append(path)
        elif sec == "option":
            key = img["option_key"] or ""
            option_images.setdefault(key, []).append(path)

    # Build options list
    options = []
    for opt in opt_rows:
        key = opt["option_key"]
        options.append({
            "key": key,
            "text": opt["option_text"],
            "is_correct": bool(opt["is_correct"]),
            "images": option_images.get(key, []),
        })

    question["question_images"] = question_images
    question["answer_images"] = answer_images
    question["explanation_images"] = explanation_images
    question["options"] = options

    return question
