"""
Filesystem Storage Manager
===========================
Manages persistent file storage for PDFs and extracted images.
All paths are relative to the project root for portability.

Directory Layout:
    uploads/
    ├── raw_pdfs/          # Original uploaded PDFs
    └── images/
        └── {exam_name}/   # Extracted images per exam
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Project root: one level up from /parser/ package
_PROJECT_ROOT = Path(__file__).parent.parent.absolute()

UPLOADS_DIR = _PROJECT_ROOT / "uploads"
RAW_PDFS_DIR = UPLOADS_DIR / "raw_pdfs"
IMAGES_DIR = UPLOADS_DIR / "images"


def init_storage():
    """Ensure all required directories exist."""
    RAW_PDFS_DIR.mkdir(parents=True, exist_ok=True)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Storage initialized: {UPLOADS_DIR}")


def get_project_root() -> Path:
    return _PROJECT_ROOT


# ─── PDF Storage ──────────────────────────────────────────────────────────────


def save_pdf(source_path: str, filename: str) -> str:
    """
    Copy/move a PDF to uploads/raw_pdfs/.
    Returns the relative path from project root.
    """
    dest = RAW_PDFS_DIR / filename
    if str(Path(source_path).resolve()) != str(dest.resolve()):
        shutil.copy2(source_path, dest)
    rel = dest.relative_to(_PROJECT_ROOT)
    logger.info(f"PDF saved: {rel}")
    return str(rel)


def save_uploaded_file(file_obj, filename: str) -> str:
    """
    Save a Flask/FastAPI file upload object to raw_pdfs/.
    Returns the absolute path to the saved file.
    """
    dest = RAW_PDFS_DIR / filename
    file_obj.save(str(dest))
    logger.info(f"Uploaded PDF saved: {dest}")
    return str(dest)


def get_pdf_path(filename: str) -> Optional[str]:
    """Get absolute path to a stored PDF."""
    path = RAW_PDFS_DIR / filename
    return str(path) if path.exists() else None


def delete_pdf(filename: str) -> bool:
    """Delete a PDF from raw_pdfs/."""
    path = RAW_PDFS_DIR / filename
    if path.exists():
        path.unlink()
        logger.info(f"Deleted PDF: {filename}")
        return True
    return False


# ─── Image Storage ────────────────────────────────────────────────────────────


def get_exam_image_dir(exam_name: str) -> Path:
    """
    Get the image directory for an exam.
    Creates it if it doesn't exist.
    Returns absolute path.
    """
    safe_name = _sanitize_name(exam_name)
    image_dir = IMAGES_DIR / safe_name
    image_dir.mkdir(parents=True, exist_ok=True)
    return image_dir


def get_exam_image_dir_relative(exam_name: str) -> str:
    """
    Get the relative path (from project root) for exam images.
    E.g., 'uploads/images/my_exam'
    """
    safe_name = _sanitize_name(exam_name)
    return str(Path("uploads") / "images" / safe_name)


def save_image(source_path: str, exam_name: str, filename: str = None) -> str:
    """
    Copy an image into the exam's image folder.
    Returns relative path from project root.
    """
    image_dir = get_exam_image_dir(exam_name)
    fname = filename or Path(source_path).name
    dest = image_dir / fname

    if str(Path(source_path).resolve()) != str(dest.resolve()):
        shutil.copy2(source_path, dest)

    rel = dest.relative_to(_PROJECT_ROOT)
    return str(rel)


def delete_image_file(image_path: str) -> bool:
    """
    Delete an image file. Accepts relative (from project root) or absolute path.
    """
    path = _resolve_path(image_path)
    if path and path.exists():
        path.unlink()
        logger.info(f"Deleted image: {image_path}")
        return True
    logger.warning(f"Image not found for deletion: {image_path}")
    return False


def delete_exam_images(exam_name: str) -> int:
    """
    Delete the entire image folder for an exam.
    Returns the number of files deleted.
    """
    safe_name = _sanitize_name(exam_name)
    image_dir = IMAGES_DIR / safe_name
    count = 0
    if image_dir.exists():
        for f in image_dir.iterdir():
            if f.is_file():
                f.unlink()
                count += 1
        # Remove the directory if empty
        try:
            image_dir.rmdir()
        except OSError:
            pass  # Not empty (shouldn't happen, but be safe)
        logger.info(f"Deleted {count} images for exam: {exam_name}")
    return count


def delete_image_files(image_paths: list[str]) -> int:
    """Delete multiple image files. Returns count of successfully deleted."""
    count = 0
    for path in image_paths:
        if delete_image_file(path):
            count += 1
    return count


def cleanup_empty_exam_dir(exam_name: str):
    """Remove the exam image directory if it's empty."""
    safe_name = _sanitize_name(exam_name)
    image_dir = IMAGES_DIR / safe_name
    if image_dir.exists():
        try:
            # Only remove if empty
            if not any(image_dir.iterdir()):
                image_dir.rmdir()
                logger.info(f"Removed empty directory: {image_dir}")
        except OSError:
            pass


def resolve_image_path(relative_path: str) -> Optional[str]:
    """
    Resolve a relative image path to an absolute path.
    Tries multiple base directories for compatibility.
    """
    path = _resolve_path(relative_path)
    return str(path) if path and path.exists() else None


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _sanitize_name(name: str) -> str:
    """Sanitize a name for filesystem use."""
    return "".join(
        c if c.isalnum() or c in "-_ " else "_"
        for c in name
    ).strip().replace(" ", "_")[:100]


def _resolve_path(image_path: str) -> Optional[Path]:
    """
    Resolve a possibly-relative image path to an absolute Path.
    Tries:
      1. As-is (if absolute)
      2. Relative to project root
      3. Inside uploads/images/
      4. Inside output/questions/ (legacy)
      5. Inside storage/questions/ (legacy)
    """
    p = Path(image_path)

    # Already absolute
    if p.is_absolute():
        return p if p.exists() else None

    # Relative to project root
    candidate = _PROJECT_ROOT / p
    if candidate.exists():
        return candidate

    # Inside uploads/images (strip leading parts)
    candidate = IMAGES_DIR / p.name
    if candidate.exists():
        return candidate

    # Legacy: output/questions/
    candidate = _PROJECT_ROOT / "output" / "questions" / p
    if candidate.exists():
        return candidate
    candidate = _PROJECT_ROOT / "output" / "questions" / p.name
    if candidate.exists():
        return candidate

    # Legacy: storage/questions/
    candidate = _PROJECT_ROOT / "storage" / "questions" / p
    if candidate.exists():
        return candidate

    return None
