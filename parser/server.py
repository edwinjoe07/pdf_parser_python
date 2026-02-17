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
    app.config.setdefault("IMAGE_BASE_DIR", str(project_root / "output" / "questions"))
    app.config.setdefault("MAX_CONTENT_LENGTH", 500 * 1024 * 1024)  # 500MB

    # Ensure directories exist
    Path(app.config["UPLOAD_DIR"]).mkdir(parents=True, exist_ok=True)
    Path(app.config["OUTPUT_DIR"]).mkdir(parents=True, exist_ok=True)

    # Static routes for binary artifacts
    @app.route("/output/<path:filename>")
    def serve_output(filename):
        return send_from_directory(app.config["OUTPUT_DIR"], filename)

    @app.route("/storage/<path:filename>")
    def serve_storage(filename):
        return send_from_directory(str(project_root / "storage"), filename)

    @app.route("/questions/<path:filename>")
    def serve_questions(filename):
        # Normalize: strip redundant prefixes if they leaked in
        for prefix in ["questions/", "storage/", "output/"]:
            if filename.startswith(prefix):
                filename = filename[len(prefix):]
        
        logger.debug(f"Serving question image: {filename}")
        
        # 1. Try absolute path in output/questions
        output_q = Path(app.config["OUTPUT_DIR"]) / "questions"
        target = output_q / filename
        if target.exists():
            return send_from_directory(str(output_q), filename)
        
        # 2. Try legacy storage/questions
        storage_q = project_root / "storage" / "questions"
        target = storage_q / filename
        if target.exists():
            return send_from_directory(str(storage_q), filename)

        # 3. Last ditch: Recursive search for filename if it's just a file (no uuid)
        if "/" not in filename and "\\" not in filename:
            for p in output_q.rglob(filename):
                return send_from_directory(str(p.parent), p.name)
            for p in storage_q.rglob(filename):
                return send_from_directory(str(p.parent), p.name)

        logger.warning(f"Image NOT FOUND: {filename}")
        return jsonify({"error": "Image not found", "path": filename}), 404

    return app


# ─── Dashboard ────────────────────────────────────────────────────────────────


@app.route("/")
def dashboard():
    """Serve the web dashboard."""
    return render_template("index.html")


# ─── Health Check ─────────────────────────────────────────────────────────────


@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    with jobs_lock:
        active = sum(1 for j in jobs.values() if j["status"] in ("queued", "processing"))
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

    if not job:
        return jsonify({"error": "Job not found"}), 404

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


@app.route("/api/result/<job_id>", methods=["GET"])
def get_result(job_id: str):
    """Get the full result of a completed parse job."""
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        return jsonify({"error": "Job not found"}), 404

    if job["status"] != "completed":
        return jsonify({
            "error": f"Job not complete, status: {job['status']}",
            "status": job["status"],
        }), 400

    return jsonify(job["result"])


@app.route("/api/jobs", methods=["GET"])
def list_jobs():
    """List all jobs with basic status info."""
    with jobs_lock:
        result = []
        for job_id, job in jobs.items():
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
                "created_at": job["created_at"],
                "error": job["error"],
                "duration": duration,
                "questions_count": questions_count,
            })
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
    """Run a parse job in background thread."""
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

        with jobs_lock:
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["completed_at"] = time.time()
            jobs[job_id]["progress"] = 100
            jobs[job_id]["result"] = result.model_dump()

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
