"""
CLI Interface
=============
Command-line interface for the PDF parser engine.

Usage:
    python -m parser.cli parse <pdf_path> [options]
    python -m parser.cli batch <directory> [options]
    python -m parser.cli validate <json_path>
    python -m parser.cli info <pdf_path>
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

from .engine import ParserConfig, ParserEngine

console = Console()


@click.group()
@click.version_option(version="1.0.0", prog_name="pdf-parser")
def cli():
    """PDF Parser Engine — Scalable certification exam question extractor."""
    pass


@cli.command()
@click.argument("pdf_path", type=click.Path(exists=True))
@click.option(
    "--output", "-o",
    default="output",
    help="Output directory for parsed data",
)
@click.option(
    "--exam-name", "-n",
    default="",
    help="Exam name (defaults to filename)",
)
@click.option(
    "--exam-provider", "-p",
    default="",
    help="Exam provider/vendor name",
)
@click.option(
    "--exam-version", "-v",
    default="",
    help="Exam version identifier",
)
@click.option(
    "--exam-id",
    default=None,
    help="Custom exam ID for file organization",
)
@click.option(
    "--image-dir",
    default="storage/questions",
    help="Base directory for extracted images",
)
@click.option(
    "--image-format",
    default="png",
    type=click.Choice(["png", "jpg", "webp"]),
    help="Image output format",
)
@click.option(
    "--min-image-size",
    default=50,
    type=int,
    help="Minimum image dimension to extract (pixels)",
)
@click.option(
    "--page-start",
    default=None,
    type=int,
    help="Start page (1-indexed)",
)
@click.option(
    "--page-end",
    default=None,
    type=int,
    help="End page (1-indexed, inclusive)",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Logging level",
)
@click.option(
    "--log-file",
    default=None,
    help="Path to log file",
)
@click.option(
    "--no-raw-blocks",
    is_flag=True,
    default=False,
    help="Skip saving raw blocks snapshot",
)
@click.option(
    "--json-output",
    is_flag=True,
    default=False,
    help="Output only JSON result to stdout (for programmatic use)",
)
def parse(
    pdf_path: str,
    output: str,
    exam_name: str,
    exam_provider: str,
    exam_version: str,
    exam_id: str,
    image_dir: str,
    image_format: str,
    min_image_size: int,
    page_start: int,
    page_end: int,
    log_level: str,
    log_file: str,
    no_raw_blocks: bool,
    json_output: bool,
):
    """Parse a single PDF file into structured question entities."""

    if json_output:
        # Suppress console output for JSON mode
        log_level = "ERROR"

    page_range = None
    if page_start is not None or page_end is not None:
        page_range = (page_start or 1, page_end or 99999)

    config = ParserConfig(
        output_dir=output,
        image_base_dir=image_dir,
        image_format=image_format,
        min_image_size=min_image_size,
        exam_name=exam_name,
        exam_provider=exam_provider,
        exam_version=exam_version,
        exam_id=exam_id,
        page_range=page_range,
        log_level=log_level,
        log_file=log_file,
        save_raw_blocks=not no_raw_blocks,
    )

    if not json_output:
        console.print()
        console.print(
            Panel.fit(
                f"[bold cyan]PDF Parser Engine v1.0.0[/]\n"
                f"[dim]Parsing: {os.path.basename(pdf_path)}[/]",
                border_style="cyan",
            )
        )
        console.print()

    try:
        engine = ParserEngine(config)

        if not json_output:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Parsing PDF...", total=3)

                progress.update(task, description="Extracting blocks...")
                result = engine.parse(pdf_path)
                progress.update(task, completed=3)

            try:
                _display_results(result)
            except UnicodeEncodeError:
                # Windows console may not support special chars
                print(f"Parse complete: {len(result.questions)} questions")
                print(f"Success rate: {result.validation.success_rate}%")
        else:
            result = engine.parse(pdf_path)
            # Output clean JSON to stdout
            print(json.dumps(
                result.model_dump(),
                indent=2,
                ensure_ascii=False,
                default=str,
            ))

    except FileNotFoundError as e:
        console.print(f"[red]Error:[/] {e}")
        sys.exit(1)
    except RuntimeError as e:
        console.print(f"[red]Error:[/] {e}")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error:[/] {e}")
        if log_level == "DEBUG":
            console.print_exception()
        sys.exit(1)


@cli.command()
@click.argument("directory", type=click.Path(exists=True))
@click.option("--output", "-o", default="output", help="Output directory")
@click.option("--exam-provider", "-p", default="", help="Provider name")
@click.option("--log-level", default="INFO", help="Logging level")
@click.option("--image-dir", default="storage/questions", help="Image dir")
@click.option(
    "--parallel", "-j",
    default=1,
    type=int,
    help="Number of parallel parse workers (1 = sequential)",
)
def batch(
    directory: str,
    output: str,
    exam_provider: str,
    log_level: str,
    image_dir: str,
    parallel: int,
):
    """Batch parse all PDFs in a directory."""

    pdf_files = sorted(Path(directory).glob("*.pdf"))

    if not pdf_files:
        console.print(f"[yellow]No PDF files found in: {directory}[/]")
        return

    console.print()
    console.print(
        Panel.fit(
            f"[bold cyan]Batch PDF Parser[/]\n"
            f"[dim]Found {len(pdf_files)} PDFs in: {directory}[/]",
            border_style="cyan",
        )
    )
    console.print()

    results = []
    errors = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            "Processing PDFs...", total=len(pdf_files)
        )

        for pdf_file in pdf_files:
            progress.update(
                task,
                description=f"Parsing: {pdf_file.name}",
            )

            try:
                config = ParserConfig(
                    output_dir=output,
                    image_base_dir=image_dir,
                    exam_provider=exam_provider,
                    log_level=log_level,
                )
                engine = ParserEngine(config)
                result = engine.parse(str(pdf_file))
                results.append((pdf_file.name, result))
            except Exception as e:
                errors.append((pdf_file.name, str(e)))

            progress.advance(task)

    # Display batch summary
    _display_batch_summary(results, errors)


@cli.command()
@click.argument("json_path", type=click.Path(exists=True))
def validate(json_path: str):
    """Validate a previously generated parse result JSON."""

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    console.print()
    console.print(
        Panel.fit(
            f"[bold cyan]Validation Report[/]\n"
            f"[dim]File: {json_path}[/]",
            border_style="cyan",
        )
    )

    validation = data.get("validation", {})
    _display_validation_table(validation)


@cli.command()
@click.option("--host", default="0.0.0.0", help="Server host")
@click.option("--port", default=5000, type=int, help="Server port")
@click.option("--debug", is_flag=True, default=False, help="Debug mode")
def serve(host: str, port: int, debug: bool):
    """Start the HTTP microservice server for Laravel integration."""
    from .server import run_server

    console.print()
    console.print(
        Panel.fit(
            f"[bold cyan]PDF Parser Microservice[/]\n"
            f"[dim]Starting on {host}:{port}[/]",
            border_style="cyan",
        )
    )
    console.print()

    run_server(host=host, port=port, debug=debug)


@cli.command()
@click.argument("pdf_path", type=click.Path(exists=True))
def info(pdf_path: str):
    """Display PDF file information."""

    import fitz

    doc = fitz.open(pdf_path)

    console.print()
    table = Table(title="PDF Information", border_style="cyan")
    table.add_column("Property", style="bold")
    table.add_column("Value")

    table.add_row("File", os.path.basename(pdf_path))
    table.add_row("Pages", str(doc.page_count))
    table.add_row(
        "File Size",
        f"{os.path.getsize(pdf_path) / 1024 / 1024:.2f} MB",
    )

    metadata = doc.metadata or {}
    for key in ["title", "author", "subject", "creator", "producer"]:
        val = metadata.get(key, "")
        if val:
            table.add_row(key.title(), val)

    # Count images
    total_images = 0
    for page in doc:
        total_images += len(page.get_images(full=True))

    table.add_row("Total Images", str(total_images))

    doc.close()
    console.print(table)
    console.print()


# ─── Display Helpers ──────────────────────────────────────────────────────────


def _display_results(result):
    """Display parse results in a formatted table."""
    console.print()

    # Exam info
    exam = result.exam
    table = Table(title="Exam Information", border_style="cyan")
    table.add_column("Property", style="bold")
    table.add_column("Value")
    table.add_row("Name", exam.name or "(auto)")
    table.add_row("Provider", exam.provider or "(not set)")
    table.add_row("Source PDF", exam.source_pdf)
    table.add_row("Total Pages", str(exam.total_pages))
    table.add_row("File Hash", exam.file_hash[:16] + "...")
    console.print(table)
    console.print()

    # Validation summary
    _display_validation_table(result.validation.model_dump())

    # Parse version
    pv = result.parse_version
    console.print(
        f"[dim]Parser v{pv.parser_version} | "
        f"Blocks: {pv.raw_block_count} | "
        f"Questions: {pv.structured_question_count} | "
        f"Timestamp: {pv.parse_timestamp}[/]"
    )
    console.print()


def _display_validation_table(validation: dict):
    """Display validation report as a rich table."""
    table = Table(title="Validation Report", border_style="green")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")
    table.add_column("Status", justify="center")

    total = validation.get("total_questions_detected", 0)
    success = validation.get("structured_successfully", 0)
    rate = validation.get("success_rate", 0)

    # Status icons
    def status_icon(count, threshold=0):
        if count <= threshold:
            return "[green]✓[/]"
        return "[red]✗[/]"

    table.add_row(
        "Total Questions Detected",
        str(total),
        "[green]✓[/]" if total > 0 else "[red]✗[/]",
    )
    table.add_row(
        "Structured Successfully",
        f"{success} ({rate}%)",
        "[green]✓[/]" if rate >= 90 else "[yellow]⚠[/]",
    )

    missing = validation.get("missing_question_numbers", [])
    table.add_row(
        "Missing Question Numbers",
        str(len(missing)),
        status_icon(len(missing)),
    )

    dupes = validation.get("duplicate_question_numbers", [])
    table.add_row(
        "Duplicate Question Numbers",
        str(len(dupes)),
        status_icon(len(dupes)),
    )

    missing_ans = validation.get("questions_missing_answer", [])
    table.add_row(
        "Questions Missing Answer",
        str(len(missing_ans)),
        status_icon(len(missing_ans)),
    )

    missing_exp = validation.get("questions_missing_explanation", [])
    table.add_row(
        "Questions Missing Explanation",
        str(len(missing_exp)),
        status_icon(len(missing_exp)),
    )

    orphans = validation.get("orphan_images", 0)
    table.add_row(
        "Orphan Images",
        str(orphans),
        status_icon(orphans),
    )

    console.print(table)
    console.print()

    # Anomaly breakdown
    breakdown = validation.get("anomaly_breakdown", {})
    if breakdown:
        anomaly_table = Table(
            title="Anomaly Breakdown",
            border_style="yellow",
        )
        anomaly_table.add_column("Type", style="bold")
        anomaly_table.add_column("Count", justify="right")

        for atype, count in sorted(breakdown.items()):
            anomaly_table.add_row(atype, str(count))

        console.print(anomaly_table)
        console.print()


def _display_batch_summary(results, errors):
    """Display batch processing summary."""
    console.print()

    table = Table(title="Batch Processing Summary", border_style="cyan")
    table.add_column("PDF", style="bold")
    table.add_column("Questions", justify="right")
    table.add_column("Success Rate", justify="right")
    table.add_column("Anomalies", justify="right")
    table.add_column("Status", justify="center")

    total_questions = 0
    total_anomalies = 0

    for name, result in results:
        q_count = len(result.questions)
        rate = result.validation.success_rate
        anomalies = sum(
            result.validation.anomaly_breakdown.values()
        )

        total_questions += q_count
        total_anomalies += anomalies

        status = "[green]✓[/]" if rate >= 90 else "[yellow]⚠[/]"
        table.add_row(
            name,
            str(q_count),
            f"{rate}%",
            str(anomalies),
            status,
        )

    for name, error in errors:
        table.add_row(
            name,
            "-",
            "-",
            "-",
            "[red]✗ FAILED[/]",
        )

    console.print(table)
    console.print()
    console.print(
        f"[bold]Total:[/] {total_questions} questions from "
        f"{len(results)} PDFs, {total_anomalies} anomalies, "
        f"{len(errors)} failures"
    )
    console.print()


# ─── Entry point (for python -m parser.cli) ───────────────────────────────────


if __name__ == "__main__":
    cli()
