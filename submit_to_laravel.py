"""
Laravel API Submitter
=====================
Parses a PDF and submits the structured questions directly to the
Laravel Exams QA API endpoint.

Pipeline:
    1. Parse PDF using the local parser engine
    2. Transform output to match Laravel API schema
    3. Embed extracted images as base64 <img> tags in rich HTML fields
    4. Authenticate with Laravel Sanctum (get Bearer token)
    5. POST to /api/v1/import/json

Usage:
    python submit_to_laravel.py <pdf_path> [options]

Examples:
    # Basic usage — parse and submit
    python submit_to_laravel.py "C:/exams/AWS-SAA-C03.pdf" --provider "AWS" --exam-code "SAA-C03"

    # With exam title and page range
    python submit_to_laravel.py exam.pdf --provider "AWS" --exam-code "SAA-C03" \
        --exam-title "Solutions Architect Associate" --page-start 5 --page-end 100

    # Dry run — parse and show what would be sent, without submitting
    python submit_to_laravel.py exam.pdf --provider "AWS" --exam-code "SAA-C03" --dry-run

    # Submit a previously parsed JSON file (skip parsing)
    python submit_to_laravel.py --from-json output/exam_parsed.json --provider "AWS" --exam-code "SAA-C03"
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys
from pathlib import Path

import requests

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser.engine import ParserConfig, ParserEngine

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("submit_to_laravel")


# ─── Configuration ────────────────────────────────────────────────────────────

LARAVEL_BASE_URL = "http://localhost:8000"
LARAVEL_EMAIL = "info@coreminds.in"
LARAVEL_PASSWORD = "aq1sw2de3"


# ─── Authentication ──────────────────────────────────────────────────────────

def get_auth_token(base_url: str, email: str, password: str) -> str:
    """
    Authenticate with Laravel Sanctum and return a Bearer token.
    """
    login_url = f"{base_url}/api/login"
    logger.info(f"Authenticating with {login_url}...")

    try:
        resp = requests.post(
            login_url,
            json={"email": email, "password": password},
            headers={"Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            raise RuntimeError(f"Login failed: {data}")

        token = data["data"]["token"]
        logger.info("Authentication successful ✓")
        return token

    except requests.exceptions.ConnectionError:
        logger.error(
            f"Cannot connect to {base_url}. "
            "Is the Laravel server running? (php artisan serve)"
        )
        sys.exit(1)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        sys.exit(1)


# ─── Image Embedding ────────────────────────────────────────────────────────

def image_to_base64_tag(image_path: str, image_base_dir: str) -> str:
    """
    Read an image file and return an HTML <img> tag with base64 data URI.

    Args:
        image_path: Relative path from parser output (e.g. "questions/exam/img.jpeg")
        image_base_dir: Base directory where images are stored
    """
    # Try multiple possible locations for the image
    candidates = [
        Path(image_base_dir) / image_path,
        Path(image_path),
        Path(image_base_dir) / Path(image_path).name,
    ]

    abs_path = None
    for candidate in candidates:
        if candidate.exists():
            abs_path = candidate
            break

    if abs_path is None:
        logger.warning(f"Image not found: {image_path} (searched in {image_base_dir})")
        return f'<p><em>[Image not found: {image_path}]</em></p>'

    # Determine MIME type
    suffix = abs_path.suffix.lower()
    mime_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }
    mime_type = mime_map.get(suffix, "image/png")

    # Read and encode
    with open(abs_path, "rb") as f:
        image_data = base64.b64encode(f.read()).decode("ascii")

    return (
        f'<img src="data:{mime_type};base64,{image_data}" '
        f'alt="Question image" style="max-width: 100%; height: auto;" />'
    )


def embed_images_in_text(
    text: str,
    images: list[str],
    image_base_dir: str,
) -> str:
    """
    Combine text content with image references into rich HTML.

    Images are embedded as base64 <img> tags appended after the text.
    """
    html_parts = []

    # Add text content (wrap in <p> if it's plain text)
    if text.strip():
        if not text.strip().startswith("<"):
            html_parts.append(f"<p>{text}</p>")
        else:
            html_parts.append(text)

    # Embed each image
    for img_path in images:
        img_tag = image_to_base64_tag(img_path, image_base_dir)
        html_parts.append(img_tag)

    return "\n".join(html_parts)


# ─── Data Transformation ────────────────────────────────────────────────────

def transform_parsed_to_laravel(
    parse_result: dict,
    image_base_dir: str,
    access_level: str = "premium",
) -> list[dict]:
    """
    Transform parser output questions into the Laravel API format.

    Parser output:
        question_text, question_images[], options[].text, options[].is_correct,
        options[].images[], explanation_text, explanation_images[]

    Laravel API expects:
        question (HTML), explanation (HTML),
        options[].text (HTML), options[].is_correct (bool)
    """
    questions = parse_result.get("questions", [])
    transformed = []

    for q in questions:
        # Skip questions with anomalies (missing answer, etc.)
        if q.get("anomaly_score", 0) >= 50:
            logger.warning(
                f"Skipping question #{q['question_number']} "
                f"(anomaly_score={q['anomaly_score']})"
            )
            continue

        # Skip questions with no options
        if not q.get("options"):
            logger.warning(
                f"Skipping question #{q['question_number']} (no options)"
            )
            continue

        # Build rich HTML for question text + images
        question_html = embed_images_in_text(
            q.get("question_text", ""),
            q.get("question_images", []),
            image_base_dir,
        )

        # Build rich HTML for explanation + images
        explanation_html = embed_images_in_text(
            q.get("explanation_text", ""),
            q.get("explanation_images", []),
            image_base_dir,
        )

        # Transform options
        laravel_options = []
        for opt in q.get("options", []):
            # Embed option images into option text
            option_html = embed_images_in_text(
                opt.get("text", ""),
                opt.get("images", []),
                image_base_dir,
            )
            laravel_options.append({
                "text": option_html,
                "is_correct": bool(opt.get("is_correct", False)),
            })

        transformed.append({
            "question": question_html,
            "explanation": explanation_html,
            "access_level": access_level,
            "options": laravel_options,
        })

    return transformed


# ─── Submit to Laravel ───────────────────────────────────────────────────────

def submit_to_laravel(
    questions: list[dict],
    token: str,
    base_url: str,
    provider_name: str | None = None,
    exam_code: str | None = None,
    exam_title: str | None = None,
    batch_size: int = 50,
) -> dict:
    """
    Submit transformed questions to the Laravel API.

    Handles large sets by batching to avoid request size limits.
    """
    api_url = f"{base_url}/api/v1/import/json"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    total = len(questions)
    all_results = []

    # Batch the questions if there are many
    for i in range(0, total, batch_size):
        batch = questions[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total + batch_size - 1) // batch_size

        logger.info(
            f"Submitting batch {batch_num}/{total_batches} "
            f"({len(batch)} questions)..."
        )

        payload = {
            "questions": batch,
        }
        if provider_name:
            payload["provider_name"] = provider_name
        if exam_code:
            payload["exam_code"] = exam_code
        if exam_title:
            payload["exam_title"] = exam_title

        try:
            resp = requests.post(
                api_url,
                json=payload,
                headers=headers,
                timeout=600,
            )

            if resp.status_code == 201:
                result = resp.json()
                all_results.append(result)
                imported = result.get("data", {}).get("questions_imported", 0)
                logger.info(f"  ✓ Batch {batch_num}: {imported} questions imported")
            else:
                logger.error(
                    f"  ✗ Batch {batch_num} failed (HTTP {resp.status_code}): "
                    f"{resp.text[:500]}"
                )
                all_results.append({
                    "success": False,
                    "status_code": resp.status_code,
                    "error": resp.text[:500],
                })

        except requests.exceptions.RequestException as e:
            logger.error(f"  ✗ Batch {batch_num} network error: {e}")
            all_results.append({"success": False, "error": str(e)})

    # Summary
    total_imported = sum(
        r.get("data", {}).get("questions_imported", 0)
        for r in all_results
        if r.get("success")
    )
    total_failed = total - total_imported

    return {
        "total_questions": total,
        "total_imported": total_imported,
        "total_failed": total_failed,
        "batch_results": all_results,
    }


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Parse a PDF and submit questions to Laravel Exams QA API"
    )

    # Input source (one of these required)
    parser.add_argument(
        "pdf_path",
        nargs="?",
        help="Path to the PDF file to parse",
    )
    parser.add_argument(
        "--from-json",
        help="Skip parsing — load a previously parsed JSON file instead",
    )

    # Exam metadata
    parser.add_argument("--provider", help="Exam provider name (e.g. 'AWS', 'Microsoft')")
    parser.add_argument("--exam-code", help="Exam code (e.g. 'SAA-C03', 'AZ-900')")
    parser.add_argument("--exam-title", help="Exam title (e.g. 'Solutions Architect')")
    parser.add_argument(
        "--access-level",
        default="premium",
        choices=["free", "premium"],
        help="Default access level for imported questions (default: premium)",
    )

    # Page range
    parser.add_argument("--page-start", type=int, help="Start page (1-indexed)")
    parser.add_argument("--page-end", type=int, help="End page (1-indexed)")

    # Laravel connection
    parser.add_argument(
        "--laravel-url",
        default=LARAVEL_BASE_URL,
        help=f"Laravel app URL (default: {LARAVEL_BASE_URL})",
    )
    parser.add_argument("--email", default=LARAVEL_EMAIL, help="Admin email")
    parser.add_argument("--password", default=LARAVEL_PASSWORD, help="Admin password")

    # Options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and show the transformed data, but don't submit to Laravel",
    )
    parser.add_argument(
        "--save-transformed",
        help="Save the transformed JSON to a file before submitting",
    )
    parser.add_argument("--batch-size", type=int, default=10, help="Questions per API batch (default: 10, keep small for image-heavy PDFs)")
    parser.add_argument("--log-level", default="INFO", help="Log level")

    args = parser.parse_args()

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level.upper(), logging.INFO))

    # Validate input
    if not args.pdf_path and not args.from_json:
        parser.error("Provide either a PDF file path or --from-json <file>")

    # ── Step 1: Get parsed data ───────────────────────────────────────

    image_base_dir = "."  # Default; will be updated if we parse

    if args.from_json:
        # Load pre-parsed JSON
        logger.info(f"Loading parsed data from: {args.from_json}")
        with open(args.from_json, "r", encoding="utf-8") as f:
            parse_result = json.load(f)
        # Derive image base dir from the JSON file location
        image_base_dir = str(Path(args.from_json).parent.parent)
        logger.info(f"Loaded {len(parse_result.get('questions', []))} questions")
    else:
        # Parse the PDF
        pdf_path = os.path.abspath(args.pdf_path)
        if not os.path.exists(pdf_path):
            logger.error(f"PDF file not found: {pdf_path}")
            sys.exit(1)

        logger.info(f"Parsing PDF: {pdf_path}")

        config = ParserConfig(
            exam_name=args.exam_code or "",
            exam_provider=args.provider or "",
            image_base_dir="storage/questions",
            output_dir="output",
            log_level=args.log_level,
        )

        if args.page_start or args.page_end:
            config.page_range = (args.page_start or 1, args.page_end or 99999)

        engine = ParserEngine(config)
        result = engine.parse(pdf_path)
        parse_result = result.model_dump()

        # Image base dir is relative to where the parser saved them
        image_base_dir = "."

        logger.info(
            f"Parsed {len(parse_result['questions'])} questions "
            f"(success rate: {parse_result['validation']['success_rate']}%)"
        )

    # ── Step 2: Transform to Laravel format ───────────────────────────

    logger.info("Transforming data to Laravel API format...")
    laravel_questions = transform_parsed_to_laravel(
        parse_result,
        image_base_dir=image_base_dir,
        access_level=args.access_level,
    )

    if not laravel_questions:
        logger.error("No valid questions after transformation. Nothing to submit.")
        sys.exit(1)

    logger.info(f"Transformed {len(laravel_questions)} questions for submission")

    # ── Step 3: Save transformed data (optional) ──────────────────────

    if args.save_transformed:
        save_path = Path(args.save_transformed)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "provider_name": args.provider,
                    "exam_code": args.exam_code,
                    "exam_title": args.exam_title,
                    "questions": laravel_questions,
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
        logger.info(f"Saved transformed JSON to: {save_path}")

    # ── Step 4: Dry run check ─────────────────────────────────────────

    if args.dry_run:
        logger.info("=" * 60)
        logger.info("DRY RUN — Not submitting to Laravel")
        logger.info("=" * 60)
        logger.info(f"Provider:     {args.provider}")
        logger.info(f"Exam Code:    {args.exam_code}")
        logger.info(f"Exam Title:   {args.exam_title}")
        logger.info(f"Access Level: {args.access_level}")
        logger.info(f"Questions:    {len(laravel_questions)}")
        logger.info("")

        for i, q in enumerate(laravel_questions[:5], 1):
            text_preview = q["question"][:120].replace("\n", " ")
            num_opts = len(q["options"])
            correct = [
                chr(65 + j) for j, o in enumerate(q["options"]) if o["is_correct"]
            ]
            has_images = "data:image" in q["question"]
            logger.info(
                f"  Q{i}: {text_preview}..."
                f"\n       Options: {num_opts} | Correct: {','.join(correct)} | "
                f"Images: {'Yes' if has_images else 'No'}"
            )

        if len(laravel_questions) > 5:
            logger.info(f"  ... and {len(laravel_questions) - 5} more questions")

        logger.info("")
        logger.info("To submit, run again without --dry-run")
        return

    # ── Step 5: Authenticate and submit ───────────────────────────────

    token = get_auth_token(args.laravel_url, args.email, args.password)

    result = submit_to_laravel(
        questions=laravel_questions,
        token=token,
        base_url=args.laravel_url,
        provider_name=args.provider,
        exam_code=args.exam_code,
        exam_title=args.exam_title,
        batch_size=args.batch_size,
    )

    # ── Final summary ─────────────────────────────────────────────────

    logger.info("=" * 60)
    logger.info("IMPORT COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total questions parsed:   {result['total_questions']}")
    logger.info(f"Successfully imported:    {result['total_imported']}")
    logger.info(f"Failed:                   {result['total_failed']}")

    if result["total_failed"] > 0:
        logger.warning("Some questions failed to import. Check errors above.")
        sys.exit(1)
    else:
        logger.info("All questions imported successfully! ✓")


if __name__ == "__main__":
    main()
