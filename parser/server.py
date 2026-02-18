"""
HTTP Microservice
=================
Flask-based HTTP API for the PDF parser engine.

This allows Laravel to invoke the parser as a microservice
instead of a subprocess, enabling:
    - Health checks
    - Async job management
    - Status polling
    - Multi-PDF queue processing

Endpoints:
    POST   /api/parse         → Start parsing a PDF
    GET    /api/status/<id>   → Get parse job status
    GET    /api/result/<id>   → Get parse result
    POST   /api/batch         → Batch parse multiple PDFs
    GET    /api/health        → Health check
    GET    /api/info          → Parser version info
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from pathlib import Path

from flask import Flask, jsonify, request, render_template, send_from_directory
from flask_cors import CORS

from .engine import ParserConfig, ParserEngine
from . import crud
from . import database as db
from . import storage as fs_storage
from . import background_worker

logger = logging.getLogger(__name__)

# Configure template and static dirs relative to this file
_pkg_dir = Path(__file__).parent
app = Flask(
    __name__,
    template_folder=str(_pkg_dir / "templates"),
    static_folder=str(_pkg_dir / "static"),
)
CORS(app)

# ─── In-memory job store (use Redis/DB for production) ────────────────────────

jobs: dict[str, dict] = {}
jobs_lock = threading.Lock()


def create_app(config: dict = None) -> Flask:
    """Create and configure the Flask app."""
    if config:
        app.config.update(config)

    # Project root is one level up from the /parser/ package dir
    project_root = _pkg_dir.parent.absolute()

    # Default config using absolute paths
    app.config.setdefault("UPLOAD_DIR", str(project_root / "uploads"))
    app.config.setdefault("OUTPUT_DIR", str(project_root / "output"))
    app.config.setdefault("IMAGE_BASE_DIR", str(
        project_root / "output" / "questions"))
    app.config.setdefault("MAX_CONTENT_LENGTH", 500 * 1024 * 1024)  # 500MB

    # Ensure directories exist
    Path(app.config["UPLOAD_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["OUTPUT_DIR"]).mkdir(parents=True, exist_ok=True)

    # Initialize persistence layer
    fs_storage.init_storage()
    db.init_db()

    # Static routes for binary artifacts
    @app.route("/output/<path:filename>")
    def serve_output(filename):
        return send_from_directory(app.config["OUTPUT_DIR"], filename)

    @app.route("/storage/<path:filename>")
    def serve_storage(filename):
        return send_from_directory(str(project_root / "storage"), filename)

    @app.route("/uploads/<path:filename>")
    def serve_uploads(filename):
        """Serve files from uploads/ (images, pdfs)."""
        return send_from_directory(str(project_root / "uploads"), filename)

    @app.route("/questions/<path:filename>")
    def serve_questions(filename):
        """
        Serve question images from various possible locations.
        Uses fs_storage.resolve_image_path for robust lookup.
        """
        # Try resolving the path as is (might include redundant prefixes)
        abs_path = fs_storage.resolve_image_path(filename)
        
        # If not found, try stripping common prefixes and resolving again
        if not abs_path:
            for prefix in ["questions/", "storage/", "output/", "uploads/", "uploads/images/"]:
                if filename.startswith(prefix):
                    stripped = filename[len(prefix):]
                    abs_path = fs_storage.resolve_image_path(stripped)
                    if abs_path:
                        break
        
        if not abs_path:
            logger.warning(f"Image NOT FOUND: {filename}")
            return jsonify({"error": "Image not found", "path": filename}), 404

        target = Path(abs_path)
        return send_from_directory(str(target.parent), target.name)

    return app


# ─── Dashboard ────────────────────────────────────────────────────────────────


@app.route("/")
def dashboard():
    """Serve the web dashboard."""
    import time
    return render_template("index.html", cache_bust=int(time.time()))


# ─── Health Check ─────────────────────────────────────────────────────────────


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    with jobs_lock:
        active = sum(1 for j in jobs.values()
                     if j["status"] in ("queued", "processing"))
        total = len(jobs)
    return jsonify({
        "status": "healthy",
        "service": "pdf-parser",
        "version": "1.0.0",
        "active_jobs": active,
        "total_jobs": total,
    })


@app.route("/api/info", methods=["GET"])
def info():
    """Parser version and capability info."""
    return jsonify({
        "version": "1.0.0",
        "engine": "PyMuPDF",
        "fallback": "pdfplumber",
        "capabilities": [
            "text_extraction",
            "image_extraction",
            "structure_detection",
            "anomaly_detection",
            "batch_processing",
        ],
        "supported_formats": ["pdf"],
    })


# ─── Parse Endpoint ──────────────────────────────────────────────────────────


@app.route("/api/parse", methods=["POST"])
def parse_pdf():
    """
    Start parsing a PDF file.

    Accepts either:
        - A file upload (multipart/form-data)
        - A JSON body with file_path pointing to an existing file

    Returns a job ID for status polling.
    """
    job_id = str(uuid.uuid4())

    # Determine input source
    pdf_path = None

    if "file" in request.files:
        # File upload
        file = request.files["file"]
        if not file.filename:
            return jsonify({"error": "No file selected"}), 400

        upload_dir = Path(app.config["UPLOAD_DIR"])
        pdf_path = str(upload_dir / f"{job_id}_{file.filename}")
        file.save(pdf_path)

    elif request.is_json:
        # JSON body with file path
        data = request.get_json()
        pdf_path = data.get("file_path")
        if not pdf_path or not os.path.exists(pdf_path):
            return jsonify({
                "error": f"File not found: {pdf_path}"
            }), 404
    else:
        return jsonify({
            "error": "Provide a file upload or JSON with file_path"
        }), 400

    # Parse config from request
    params = request.form if request.content_type and "multipart" in request.content_type else (
        request.get_json() or {}
    )

    config = ParserConfig(
        output_dir=app.config.get("OUTPUT_DIR", "output"),
        image_base_dir=app.config.get("IMAGE_BASE_DIR", "storage/questions"),
        exam_name=params.get("exam_name", ""),
        exam_provider=params.get("exam_provider", ""),
        exam_version=params.get("exam_version", ""),
        exam_id=params.get("exam_id", job_id),
        log_level=params.get("log_level", "INFO"),
    )

    # Determine filename
    filename = os.path.basename(pdf_path) if pdf_path else "unknown.pdf"

    # Create job entry
    with jobs_lock:
        jobs[job_id] = {
            "id": job_id,
            "status": "queued",
            "pdf_path": pdf_path,
            "filename": filename,
            "created_at": time.time(),
            "started_at": None,
            "completed_at": None,
            "result": None,
            "error": None,
            "progress": 0,
        }

    # Start parsing in background thread
    thread = threading.Thread(
        target=_run_parse_job,
        args=(job_id, pdf_path, config),
        daemon=True,
    )
    thread.start()

    return jsonify({
        "job_id": job_id,
        "status": "queued",
        "message": "Parse job started",
    }), 202


@app.route("/api/parse/sync", methods=["POST"])
def parse_pdf_sync():
    """
    Parse a PDF synchronously and return the result immediately.

    For small PDFs or when the caller wants to wait.
    """
    pdf_path = None

    if "file" in request.files:
        file = request.files["file"]
        if not file.filename:
            return jsonify({"error": "No file selected"}), 400

        upload_dir = Path(app.config["UPLOAD_DIR"])
        job_id = str(uuid.uuid4())
        pdf_path = str(upload_dir / f"{job_id}_{file.filename}")
        file.save(pdf_path)
    elif request.is_json:
        data = request.get_json()
        pdf_path = data.get("file_path")
        if not pdf_path or not os.path.exists(pdf_path):
            return jsonify({"error": f"File not found: {pdf_path}"}), 404
    else:
        return jsonify({
            "error": "Provide a file upload or JSON with file_path"
        }), 400

    params = request.form if request.content_type and "multipart" in request.content_type else (
        request.get_json() or {}
    )

    config = ParserConfig(
        output_dir=app.config.get("OUTPUT_DIR", "output"),
        image_base_dir=app.config.get("IMAGE_BASE_DIR", "storage/questions"),
        exam_name=params.get("exam_name", ""),
        exam_provider=params.get("exam_provider", ""),
        exam_version=params.get("exam_version", ""),
        exam_id=params.get("exam_id"),
        log_level=params.get("log_level", "INFO"),
    )

    try:
        engine = ParserEngine(config)
        result = engine.parse(pdf_path)
        return jsonify(result.model_dump()), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Job Status ───────────────────────────────────────────────────────────────


@app.route("/api/status/<job_id>", methods=["GET"])
def get_status(job_id: str):
    """Get the status of a parse job."""
    with jobs_lock:
        job = jobs.get(job_id)

    if job:
        # Compute duration
        duration = None
        if job["started_at"] and job["completed_at"]:
            duration = round(job["completed_at"] - job["started_at"], 2)

        # Get question count from result
        questions_count = None
        if job["result"]:
            questions_count = len(job["result"].get("questions", []))

        return jsonify({
            "id": job["id"],
            "status": job["status"],
            "progress": job["progress"],
            "filename": job.get("filename", ""),
            "created_at": job["created_at"],
            "started_at": job["started_at"],
            "completed_at": job["completed_at"],
            "error": job["error"],
            "duration": duration,
            "questions_count": questions_count,
        })

    # ── Fallback: check SQLite for completed exams ───────────────
    exam = db.get_exam_by_job_id(job_id)
    if exam:
        return jsonify({
            "id": job_id,
            "status": "completed",
            "progress": 100,
            "filename": exam.get("original_filename", exam.get("name", "")),
            "created_at": exam.get("created_at", ""),
            "started_at": None,
            "completed_at": exam.get("created_at", ""),
            "error": None,
            "duration": None,
            "questions_count": exam.get("total_questions", 0),
        })

    return jsonify({"error": "Job not found"}), 404


@app.route("/api/result/<job_id>", methods=["GET"])
def get_result(job_id: str):
    """Get the full result of a completed parse job."""
    with jobs_lock:
        job = jobs.get(job_id)

    if job:
        if job["status"] != "completed":
            return jsonify({
                "error": f"Job not complete, status: {job['status']}",
                "status": job["status"],
            }), 400
        # Ensure blocks are present (handles jobs stored before enrichment)
        result_data = job["result"]
        _enrich_result_with_blocks(result_data)
        return jsonify(result_data)

    # ── Fallback: load from SQLite ────────────────────────────────
    exam = db.get_exam_by_job_id(job_id)
    if not exam:
        return jsonify({"error": "Job not found"}), 404

    # Try the stored result_json blob first (fastest, preserves blocks)
    result_json_str = exam.get("result_json", "")
    if result_json_str:
        try:
            return app.response_class(
                response=result_json_str,
                status=200,
                mimetype="application/json",
            )
        except Exception:
            logger.warning(
                f"Failed to serve stored result_json for job {job_id}")

    # Reconstruct from relational data as last resort
    exam_questions = db.get_exam_questions(exam["id"])
    # Convert each hydrated question to blocks format
    for q in exam_questions:
        _question_to_blocks(q)

    # Build validation from stored validation_json if available
    validation_obj = {
        "total_questions_detected": exam.get("total_questions", 0),
        "structured_successfully": len(exam_questions),
        "missing_question_numbers": [],
        "duplicate_question_numbers": [],
        "questions_missing_answer": [],
        "questions_missing_explanation": [],
        "failed_to_structure": [],
        "orphan_images": 0,
        "anomaly_breakdown": {},
    }
    validation_json_str = exam.get("validation_json", "")
    if validation_json_str:
        try:
            stored_val = json.loads(validation_json_str)
            summary = stored_val.get("summary", {})
            validation_obj["total_questions_detected"] = summary.get(
                "raw_detected_count",
                validation_obj["total_questions_detected"],
            )
            validation_obj["structured_successfully"] = summary.get(
                "parsed_count", len(exam_questions)
            )
            validation_obj["missing_question_numbers"] = [
                mq["question_number"]
                for mq in stored_val.get("missing_questions", [])
            ]
            validation_obj["duplicate_question_numbers"] = stored_val.get(
                "duplicate_question_numbers", []
            )
            validation_obj["questions_missing_answer"] = stored_val.get(
                "questions_missing_answer", []
            )
            validation_obj["questions_missing_explanation"] = stored_val.get(
                "questions_missing_explanation", []
            )
            validation_obj["anomaly_breakdown"] = stored_val.get(
                "anomaly_breakdown", {}
            )
            # Include full missing questions with reasons
            validation_obj["missing_questions"] = stored_val.get(
                "missing_questions", []
            )
            validation_obj["partially_structured"] = stored_val.get(
                "partially_structured", []
            )
            validation_obj["sequence_gaps"] = stored_val.get(
                "sequence_gaps", []
            )
            validation_obj["per_question_anomalies"] = stored_val.get(
                "per_question_anomalies", {}
            )
            validation_obj["fully_structured_count"] = summary.get(
                "fully_structured_count", 0
            )
            validation_obj["success_rate"] = summary.get(
                "success_rate", 0
            )
        except json.JSONDecodeError:
            logger.warning(
                f"Invalid validation_json for exam {exam.get('id')}"
            )

    reconstructed = {
        "exam": {
            "name": exam.get("name", ""),
            "provider": exam.get("provider", ""),
            "version": exam.get("version", ""),
            "source_pdf": exam.get("source_pdf", ""),
            "total_pages": exam.get("total_pages", 0),
            "file_hash": exam.get("file_hash", ""),
            "file_size_bytes": exam.get("file_size_bytes", 0),
        },
        "parse_version": {
            "parser_version": exam.get("parser_version", "1.0.0"),
            "parse_timestamp": exam.get("created_at", ""),
            "raw_block_count": 0,
            "structured_question_count": len(exam_questions),
        },
        "questions": exam_questions,
        "validation": validation_obj,
        "exam_db_id": exam.get("id"),
    }
    return jsonify(reconstructed)


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    """List all jobs with basic status info (in-memory + SQLite)."""
    result = []
    seen_job_ids: set[str] = set()

    # 1. In-memory jobs (active / recently completed)
    with jobs_lock:
        for job_id, job in jobs.items():
            seen_job_ids.add(job_id)
            duration = None
            if job["started_at"] and job["completed_at"]:
                duration = round(job["completed_at"] - job["started_at"], 2)
            elif job["started_at"]:
                duration = round(time.time() - job["started_at"], 2)

            questions_count = None
            if job["result"]:
                questions_count = len(job["result"].get("questions", []))

            result.append({
                "id": job["id"],
                "status": job["status"],
                "progress": job["progress"],
                "filename": job.get("filename", ""),
                "pdf_path": job.get("pdf_path", ""),
                "created_at": job["created_at"],
                "error": job["error"],
                "duration": duration,
                "questions_count": questions_count,
            })

    # 2. SQLite exams not already in memory
    try:
        exams = db.list_exams()
        for exam in exams:
            jid = exam.get("job_id", "")
            if not jid or jid in seen_job_ids:
                continue
            result.append({
                "id": jid,
                "exam_db_id": exam.get("id"),
                "status": "completed",
                "progress": 100,
                "filename": exam.get("original_filename") or exam.get("name", ""),
                "pdf_path": exam.get("file_path", ""),
                "created_at": exam.get("created_at", ""),
                "error": None,
                "duration": None,
                "questions_count": exam.get("total_questions", 0),
            })
    except Exception as e:
        logger.warning(f"Failed to list SQLite exams in /api/jobs: {e}")

    return jsonify(result)


# ─── Batch Parse ──────────────────────────────────────────────────────────────


@app.route("/api/batch", methods=["POST"])
def batch_parse():
    """
    Batch parse multiple PDFs.

    Accepts JSON body with list of file paths:
    {
        "files": ["/path/to/file1.pdf", "/path/to/file2.pdf"],
        "exam_provider": "AWS",
        ...
    }
    """
    data = request.get_json()
    if not data or "files" not in data:
        return jsonify({
            "error": "Provide a JSON body with 'files' array"
        }), 400

    files = data["files"]
    batch_id = str(uuid.uuid4())
    job_ids = []

    for file_path in files:
        if not os.path.exists(file_path):
            continue

        job_id = str(uuid.uuid4())

        config = ParserConfig(
            output_dir=app.config.get("OUTPUT_DIR", "output"),
            image_base_dir=app.config.get(
                "IMAGE_BASE_DIR", "storage/questions"
            ),
            exam_name=Path(file_path).stem,
            exam_provider=data.get("exam_provider", ""),
            exam_id=job_id,
            log_level=data.get("log_level", "INFO"),
        )

        with jobs_lock:
            jobs[job_id] = {
                "id": job_id,
                "batch_id": batch_id,
                "status": "queued",
                "pdf_path": file_path,
                "created_at": time.time(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
                "progress": 0,
            }

        thread = threading.Thread(
            target=_run_parse_job,
            args=(job_id, file_path, config),
            daemon=True,
        )
        thread.start()
        job_ids.append(job_id)

    return jsonify({
        "batch_id": batch_id,
        "job_ids": job_ids,
        "total_files": len(job_ids),
    }), 202


@app.route("/api/batch/<batch_id>", methods=["GET"])
def batch_status(batch_id: str):
    """Get status of all jobs in a batch."""
    with jobs_lock:
        batch_jobs = [
            {
                "id": j["id"],
                "status": j["status"],
                "progress": j["progress"],
                "pdf_path": j.get("pdf_path", ""),
            }
            for j in jobs.values()
            if j.get("batch_id") == batch_id
        ]

    if not batch_jobs:
        return jsonify({"error": "Batch not found"}), 404

    return jsonify({
        "batch_id": batch_id,
        "jobs": batch_jobs,
        "total": len(batch_jobs),
        "completed": sum(1 for j in batch_jobs if j["status"] == "completed"),
        "failed": sum(1 for j in batch_jobs if j["status"] == "failed"),
    })


# ─── Background Worker ───────────────────────────────────────────────────────





def _run_parse_job(job_id: str, pdf_path: str, config: ParserConfig):
    """Run a parse job in background thread. Persists results to SQLite."""
    import traceback

    with jobs_lock:
        jobs[job_id]["status"] = "processing"
        jobs[job_id]["started_at"] = time.time()
        jobs[job_id]["progress"] = 10

    try:
        logger.info(f"Job {job_id}: creating engine...")
        engine = ParserEngine(config)

        with jobs_lock:
            jobs[job_id]["progress"] = 30

        logger.info(f"Job {job_id}: starting parse of {pdf_path}")

        def progress_cb(current, total):
            # Scale 0-100% of extraction to 30-90% of total job progress
            pct = 30 + (current / total) * 60
            with jobs_lock:
                jobs[job_id]["progress"] = round(pct, 1)

        result = engine.parse(pdf_path, progress_callback=progress_cb)

        # ── Build UI-compatible result dict ───────────────────────────
        result_dict = result.model_dump()
        crud.enrich_result_with_blocks(result_dict)

        # ── Persist to SQLite ─────────────────────────────────────────
        persist_exam_id = None
        try:
            exam_name = config.exam_name or Path(pdf_path).stem
            stored_pdf = fs_storage.save_pdf(
                pdf_path, os.path.basename(pdf_path))

            # Serialize result for persistent storage
            result_json_str = json.dumps(
                result_dict, ensure_ascii=False, default=str)

            persist_exam_id = db.insert_exam(
                name=exam_name,
                file_path=stored_pdf,
                source_pdf=result.exam.source_pdf,
                file_hash=result.exam.file_hash,
                file_size_bytes=result.exam.file_size_bytes,
                total_pages=result.exam.total_pages,
                total_questions=len(result.questions),
                provider=config.exam_provider,
                version=config.exam_version,
                parser_version=result.parse_version.parser_version,
                job_id=job_id,
                result_json=result_json_str,
                original_filename=jobs.get(job_id, {}).get("filename", ""),
            )

            questions_data = [q.model_dump() for q in result.questions]
            db.bulk_insert_questions(persist_exam_id, questions_data)

            stored_count = db.count_exam_questions(persist_exam_id)
            logger.info(
                f"Job {job_id}: persisted to SQLite — exam_id={persist_exam_id}, "
                f"parsed={len(result.questions)}, stored={stored_count}, "
                f"db_path={db.get_db_path()}"
            )
        except Exception as persist_err:
            logger.error(
                f"Job {job_id}: SQLite persistence FAILED: {persist_err}",
                exc_info=True,
            )
            # Rollback partial DB data
            if persist_exam_id is not None:
                try:
                    db.delete_exam(persist_exam_id)
                except Exception:
                    pass

        with jobs_lock:
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["completed_at"] = time.time()
            jobs[job_id]["progress"] = 100
            jobs[job_id]["result"] = result_dict

        logger.info(
            f"Job {job_id} completed: "
            f"{len(result.questions)} questions parsed"
        )

    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Job {job_id} failed: {e}\n{tb}")

        with jobs_lock:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["completed_at"] = time.time()
            jobs[job_id]["error"] = f"{e}\n{tb}"


# ─── Persistent API Endpoints (SQLite-backed) ────────────────────────────────
# These endpoints read/write from SQLite + filesystem only.
# They do NOT rely on in-memory state and survive server restarts.


@app.route("/upload", methods=["POST"])
def upload_pdf():
    """
    Upload a PDF and start background parsing.

    Saves the file, creates an exam row (status='pending'),
    spawns a background worker, and returns immediately.

    Returns:
        {"exam_id": int, "status": "pending", "total_pages": int}
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "No file selected"}), 400

    if not file.filename.lower().endswith(".pdf"):
        return jsonify({"error": "Only PDF files are accepted"}), 400

    # Save uploaded file temporarily
    upload_dir = Path(app.config["UPLOAD_DIR"])
    upload_dir.mkdir(parents=True, exist_ok=True)
    temp_path = str(upload_dir / file.filename)
    file.save(temp_path)

    # Extract optional metadata from form
    exam_name = request.form.get("exam_name", "") or Path(file.filename).stem
    exam_provider = request.form.get("exam_provider", "")
    exam_version = request.form.get("exam_version", "")

    try:
        import hashlib as _hashlib

        # Store PDF persistently
        stored_pdf_path = fs_storage.save_pdf(temp_path, file.filename)

        # Quick metadata computation
        file_size = os.path.getsize(temp_path)
        sha256 = _hashlib.sha256()
        with open(temp_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        file_hash = sha256.hexdigest()

        # Get page count (fast — just opens PDF header)
        import fitz as _fitz
        with _fitz.open(temp_path) as _doc:
            total_pages = _doc.page_count

        # Create exam row (status = 'pending')
        exam_id = db.insert_exam(
            name=exam_name,
            file_path=stored_pdf_path,
            source_pdf=file.filename,
            file_hash=file_hash,
            file_size_bytes=file_size,
            total_pages=total_pages,
            total_questions=0,
            provider=exam_provider,
            version=exam_version,
            original_filename=file.filename,
        )
        # Set initial status + current_page via update
        db.update_exam(exam_id, status="pending", current_page=0)

        logger.info(
            f"Upload: exam_id={exam_id}, name={exam_name!r}, "
            f"pages={total_pages}, spawning worker"
        )

        # Build parser config
        config = ParserConfig(
            output_dir=app.config.get("OUTPUT_DIR", "output"),
            image_base_dir=str(fs_storage.IMAGES_DIR),
            exam_name=exam_name,
            exam_provider=exam_provider,
            exam_version=exam_version,
            exam_id=fs_storage._sanitize_name(exam_name),
        )

        # Spawn background worker
        background_worker.spawn_worker(
            exam_id=exam_id,
            pdf_path=temp_path,
            config=config,
            start_from_page=0,
        )

        return jsonify({
            "exam_id": exam_id,
            "status": "pending",
            "total_pages": total_pages,
            "message": "Parsing started in background",
        }), 202

    except Exception as e:
        logger.error(f"Upload failed: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/exam/<int:exam_id>", methods=["GET"])
def get_exam(exam_id: int):
    """
    Get full exam data from SQLite.
    Never re-parses the PDF.
    """
    exam = crud.get_exam(exam_id)
    if not exam:
        return jsonify({"error": "Exam not found"}), 404
    return jsonify(exam)


@app.route("/exam/<int:exam_id>/question/<int:number>", methods=["GET"])
def get_exam_question(exam_id: int, number: int):
    """
    Get a single question by exam_id + question_number.
    All data fetched from SQLite.
    """
    question = crud.get_exam_question(exam_id, number)
    if not question:
        return jsonify({"error": "Question not found"}), 404
    return jsonify(question)


@app.route("/exams", methods=["GET"])
def list_exams():
    """List all exams (summary)."""
    exams = crud.list_exams()
    return jsonify(exams)


# ─── Pause / Resume / Progress ───────────────────────────────────────────────


@app.route("/exam/<int:exam_id>/pause", methods=["POST"])
def pause_exam(exam_id: int):
    """
    Pause a running parse job.

    Sets status = 'paused'. The background worker checks status before
    each page and will stop gracefully.
    """
    exam = db.get_exam(exam_id)
    if not exam:
        return jsonify({"error": "Exam not found"}), 404

    current_status = exam.get("status", "")
    if current_status != "processing":
        return jsonify({
            "error": f"Cannot pause exam with status '{current_status}'. "
            f"Only 'processing' exams can be paused.",
            "status": current_status,
        }), 409

    # Set status to paused (worker will see this on next page check)
    db.update_exam(exam_id, status="paused")

    # Also signal the in-memory worker for faster response
    worker = background_worker.get_worker(exam_id)
    if worker:
        worker.request_stop()

    logger.info(
        f"Exam {exam_id}: Pause requested at page {exam.get('current_page', 0)}")

    return jsonify({
        "success": True,
        "exam_id": exam_id,
        "status": "paused",
        "current_page": exam.get("current_page", 0),
        "message": "Pause signal sent. Worker will stop after current page.",
    })


@app.route("/exam/<int:exam_id>/resume", methods=["POST"])
def resume_exam(exam_id: int):
    """
    Resume a paused or failed parse job.

    Restarts the background worker from current_page + 1.
    Does NOT restart from page 1.
    """
    exam = db.get_exam(exam_id)
    if not exam:
        return jsonify({"error": "Exam not found"}), 404

    current_status = exam.get("status", "")
    if current_status not in ("paused", "failed"):
        return jsonify({
            "error": f"Cannot resume exam with status '{current_status}'. "
            f"Only 'paused' or 'failed' exams can be resumed.",
            "status": current_status,
        }), 409

    # Check if a worker is already active
    existing_worker = background_worker.get_worker(exam_id)
    if existing_worker:
        return jsonify({
            "error": "A worker is already active for this exam.",
            "status": current_status,
        }), 409

    # Resolve PDF path
    file_path = exam.get("file_path", "")
    pdf_path = None
    if file_path:
        # file_path is relative to project root
        project_root = fs_storage.get_project_root()
        candidate = project_root / file_path
        if candidate.exists():
            pdf_path = str(candidate)

    if not pdf_path:
        return jsonify({
            "error": "PDF file not found. Cannot resume parsing.",
        }), 404

    current_page = exam.get("current_page", 0) or 0
    resume_from = current_page + 1
    total_pages = exam.get("total_pages", 0) or 0

    if resume_from > total_pages and total_pages > 0:
        return jsonify({
            "error": "All pages already processed.",
            "current_page": current_page,
            "total_pages": total_pages,
        }), 409

    # Build parser config
    exam_name = exam.get("name", "")
    config = ParserConfig(
        output_dir=app.config.get("OUTPUT_DIR", "output"),
        image_base_dir=str(fs_storage.IMAGES_DIR),
        exam_name=exam_name,
        exam_provider=exam.get("provider", ""),
        exam_version=exam.get("version", ""),
        exam_id=fs_storage._sanitize_name(exam_name),
    )

    # Spawn background worker from checkpoint
    background_worker.spawn_worker(
        exam_id=exam_id,
        pdf_path=pdf_path,
        config=config,
        start_from_page=resume_from,
    )

    logger.info(
        f"Exam {exam_id}: Resuming from page {resume_from}/{total_pages}"
    )

    return jsonify({
        "success": True,
        "exam_id": exam_id,
        "status": "processing",
        "resume_from_page": resume_from,
        "total_pages": total_pages,
        "message": f"Parsing resumed from page {resume_from}",
    })


@app.route("/exam/<int:exam_id>/progress", methods=["GET"])
def exam_progress(exam_id: int):
    """
    Get parsing progress for an exam.

    Returns:
        {status, current_page, total_pages, percentage, last_error,
         file_size_bytes, original_filename, created_at, total_questions}
    """
    exam = db.get_exam(exam_id)
    if not exam:
        return jsonify({"error": "Exam not found"}), 404

    current_page = exam.get("current_page", 0) or 0
    total_pages = exam.get("total_pages", 0) or 0
    status = exam.get("status", "pending")

    percentage = 0.0
    if total_pages > 0:
        percentage = round((current_page / total_pages) * 100, 2)

    # Real-time question count from DB (not the exam row which updates at end)
    question_count = db.count_exam_questions(exam_id)

    return jsonify({
        "exam_id": exam_id,
        "status": status,
        "current_page": current_page,
        "total_pages": total_pages,
        "percentage": percentage,
        "total_questions": question_count,
        "last_error": exam.get("last_error"),
        "file_size_bytes": exam.get("file_size_bytes", 0),
        "original_filename": exam.get("original_filename", ""),
        "created_at": exam.get("created_at", ""),
        "name": exam.get("name", ""),
    })


@app.route("/exam/<int:exam_id>/validation", methods=["GET"])
def exam_validation(exam_id: int):
    """
    Get detailed validation report for a completed exam.

    Returns comprehensive breakdown of:
    - Raw detected count (question anchors found in text scan)
    - Successfully parsed count (fully structured questions)
    - Missing/lost questions with per-question reason why
    - Partially structured questions with anomaly details
    - Sequence gaps, duplicates, anomaly breakdown

    If parsing is still in progress, returns a partial summary from
    what's been committed so far.
    """
    exam = db.get_exam(exam_id)
    if not exam:
        return jsonify({"error": "Exam not found"}), 404

    status = exam.get("status", "pending")

    # Try to return stored validation report (full data)
    validation_json_str = exam.get("validation_json", "")
    if validation_json_str:
        try:
            validation_data = json.loads(validation_json_str)
            validation_data["exam_id"] = exam_id
            validation_data["status"] = status
            return jsonify(validation_data)
        except json.JSONDecodeError:
            logger.warning(
                f"Invalid validation_json for exam {exam_id}"
            )

    # Fallback: compute a basic summary from what's in the DB
    question_count = db.count_exam_questions(exam_id)
    questions = db.get_exam_questions(exam_id)

    # Classify by completeness
    fully_structured = []
    partially_structured = []
    missing_answer = []
    missing_explanation = []

    for q in questions:
        has_text = bool((q.get("question_text") or "").strip())
        has_answer = bool((q.get("answer_text") or "").strip())
        has_explanation = bool((q.get("explanation_text") or "").strip())

        if has_text and has_answer:
            fully_structured.append(q.get("question_number", 0))
        else:
            reasons = []
            if not has_text:
                reasons.append("missing_question_text")
            if not has_answer:
                reasons.append("missing_answer")
            if not has_explanation:
                reasons.append("missing_explanation")
            partially_structured.append({
                "question_number": q.get("question_number", 0),
                "page_start": q.get("page_start", 0),
                "page_end": q.get("page_end", 0),
                "has_question_text": has_text,
                "has_answer": has_answer,
                "has_explanation": has_explanation,
                "reasons": reasons,
            })

        if not has_answer:
            missing_answer.append(q.get("question_number", 0))
        if not has_explanation:
            missing_explanation.append(q.get("question_number", 0))

    return jsonify({
        "exam_id": exam_id,
        "status": status,
        "summary": {
            "raw_detected_count": question_count,
            "parsed_count": question_count,
            "fully_structured_count": len(fully_structured),
            "partially_structured_count": len(partially_structured),
            "missing_lost_count": 0,
            "total_pages": exam.get("total_pages", 0),
        },
        "fully_structured": sorted(fully_structured),
        "partially_structured": partially_structured,
        "missing_questions": [],
        "questions_missing_answer": sorted(missing_answer),
        "questions_missing_explanation": sorted(missing_explanation),
        "message": (
            "Full validation report not yet available. "
            "This is a live summary from committed data."
            if status in ("pending", "processing", "paused")
            else "Validation report was not stored. Showing DB-based summary."
        ),
    })


# ─── CRUD: Update Endpoints ──────────────────────────────────────────────────


@app.route("/exam/<int:exam_id>", methods=["PUT"])
def update_exam(exam_id: int):
    """Update exam metadata."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    updated = db.update_exam(exam_id, **data)
    if not updated:
        return jsonify({"error": "Exam not found or no valid fields"}), 404
    return jsonify({"success": True})


@app.route("/question/<int:question_id>", methods=["PUT"])
def update_question(question_id: int):
    """Update question text fields."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    updated = crud.update_question(question_id, **data)
    if not updated:
        return jsonify({"error": "Question not found or no valid fields"}), 404
    return jsonify({"success": True})


@app.route("/option/<int:option_id>", methods=["PUT"])
def update_option(option_id: int):
    """Update an option."""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    updated = crud.update_option(option_id, **data)
    if not updated:
        return jsonify({"error": "Option not found or no valid fields"}), 404
    return jsonify({"success": True})


@app.route("/image/<int:image_id>", methods=["PUT"])
def update_image(image_id: int):
    """
    Update image section or replace image file.
    JSON body: {"section": "explanation", "option_key": null}
    Or multipart with new file + exam_name.
    """
    if request.content_type and "multipart" in request.content_type:
        # Replace image file
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        file = request.files["file"]
        exam_name = request.form.get("exam_name", "")
        if not exam_name:
            return jsonify({"error": "exam_name is required"}), 400

        temp_path = str(Path(app.config["UPLOAD_DIR"]) / file.filename)
        file.save(temp_path)

        try:
            crud.replace_image(image_id, temp_path, exam_name)
            # Clean up temp
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    else:
        # Update section/metadata
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        section = data.get("section")
        option_key = data.get("option_key")

        if section:
            crud.update_image_section(image_id, section, option_key)

        return jsonify({"success": True})


# ─── CRUD: Delete Endpoints ──────────────────────────────────────────────────


@app.route("/exam/<int:exam_id>", methods=["DELETE"])
def delete_exam(exam_id: int):
    """Delete an exam and all associated data + files."""
    # Stop any background parsing worker first
    worker = background_worker.get_worker(exam_id)
    if worker:
        db.update_exam(exam_id, status="paused")
        worker.request_stop()

    # Find associated in-memory job if any
    exam = db.get_exam(exam_id)
    job_id_to_purge = exam.get("job_id") if exam else None

    deleted = crud.delete_exam(exam_id)
    if not deleted:
        return jsonify({"error": "Exam not found"}), 404
    
    # Purge from memory
    if job_id_to_purge:
        with jobs_lock:
            if job_id_to_purge in jobs:
                del jobs[job_id_to_purge]
    
    return jsonify({"success": True, "message": f"Exam {exam_id} deleted"})


@app.route("/api/jobs/<job_id>", methods=["DELETE"])
def delete_job_api(job_id: str):
    """Delete a job from in-memory store."""
    with jobs_lock:
        if job_id in jobs:
            del jobs[job_id]
            return jsonify({"success": True, "message": f"Job {job_id} removed from memory"})
    return jsonify({"error": "Job not found in memory"}), 404


@app.route("/exams", methods=["DELETE"])
def delete_all_exams():
    """Delete ALL exams and their associated data + files. Full reset."""
    exams = crud.list_exams()
    deleted_count = 0
    for exam in exams:
        eid = exam.get("id")
        # Stop any active worker
        worker = background_worker.get_worker(eid)
        if worker:
            db.update_exam(eid, status="paused")
            worker.request_stop()
        if crud.delete_exam(eid):
            deleted_count += 1
    return jsonify({
        "success": True,
        "message": f"Deleted {deleted_count} exam(s)",
        "deleted_count": deleted_count,
    })


@app.route("/question/<int:question_id>", methods=["DELETE"])
def delete_question(question_id: int):
    """Delete a question and its images."""
    deleted = crud.delete_question(question_id)
    if not deleted:
        return jsonify({"error": "Question not found"}), 404
    return jsonify({"success": True})


@app.route("/option/<int:option_id>", methods=["DELETE"])
def delete_option(option_id: int):
    """Delete an option."""
    deleted = db.delete_option(option_id)
    if not deleted:
        return jsonify({"error": "Option not found"}), 404
    return jsonify({"success": True})


@app.route("/image/<int:image_id>", methods=["DELETE"])
def delete_image(image_id: int):
    """Delete an image from DB and filesystem."""
    deleted = crud.delete_image(image_id)
    if not deleted:
        return jsonify({"error": "Image not found"}), 404
    return jsonify({"success": True})


# ─── CRUD: Create Endpoints ──────────────────────────────────────────────────


@app.route("/exam/<int:exam_id>/question", methods=["POST"])
def add_question(exam_id: int):
    """
    Add a new question to an exam.
    JSON body: {question_number, question_text, answer_text, ...}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    question_id = crud.add_question(
        exam_id=exam_id,
        question_number=data.get("question_number", 0),
        question_text=data.get("question_text", ""),
        question_type=data.get("question_type", "mcq"),
        answer_text=data.get("answer_text", ""),
        explanation_text=data.get("explanation_text", ""),
        page_start=data.get("page_start", 0),
        page_end=data.get("page_end", 0),
    )
    return jsonify({"success": True, "question_id": question_id}), 201


@app.route("/question/<int:question_id>/option", methods=["POST"])
def add_option(question_id: int):
    """
    Add a new option to a question.
    JSON body: {option_key, option_text, is_correct}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    option_id = crud.add_option(
        question_id=question_id,
        option_key=data.get("option_key", ""),
        option_text=data.get("option_text", ""),
        is_correct=data.get("is_correct", False),
    )
    return jsonify({"success": True, "option_id": option_id}), 201


@app.route("/question/<int:question_id>/image", methods=["POST"])
def add_image(question_id: int):
    """
    Add a new image to a question.
    Multipart upload with: file, section, exam_name, option_key (optional)
    """
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    section = request.form.get("section", "question")
    exam_name = request.form.get("exam_name", "")
    option_key = request.form.get("option_key")
    block_order = int(request.form.get("block_order", 0))

    if not exam_name:
        return jsonify({"error": "exam_name is required"}), 400

    # Save temp
    temp_path = str(Path(app.config["UPLOAD_DIR"]) / file.filename)
    file.save(temp_path)

    try:
        image_id = crud.add_image(
            question_id=question_id,
            section=section,
            file_path=temp_path,
            exam_name=exam_name,
            option_key=option_key,
            block_order=block_order,
        )
        # Clean up temp if it's not in the final location
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        return jsonify({"success": True, "image_id": image_id}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Run Server ──────────────────────────────────────────────────────────────


def run_server(
    host: str = "0.0.0.0",
    port: int = 5000,
    debug: bool = False,
):
    """Start the microservice server."""
    create_app()
    app.run(host=host, port=port, debug=debug)


if __name__ == "__main__":
    run_server(debug=True)
