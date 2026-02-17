"""
Laravel Bridge
===============
Subprocess bridge script for Laravel queue job integration.

Laravel invokes this script via Process/exec with arguments:
    python laravel_bridge.py <pdf_path> <output_dir> [options_json]

The bridge:
    1. Reads the PDF path and config from CLI args
    2. Runs the parser engine
    3. Outputs the JSON result to stdout
    4. Returns exit code 0 on success, 1 on failure
    5. Writes error details to stderr

This provides a clean contract between Laravel and Python:
    - Input:  CLI arguments
    - Output: JSON to stdout
    - Errors: stderr + exit code 1
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parser.engine import ParserConfig, ParserEngine


def main():
    """Main entry point for Laravel bridge."""

    if len(sys.argv) < 3:
        print(
            json.dumps({
                "error": "Usage: python laravel_bridge.py <pdf_path> <output_dir> [options_json]"
            }),
            file=sys.stderr,
        )
        sys.exit(1)

    pdf_path = sys.argv[1]
    output_dir = sys.argv[2]

    # Optional JSON config string
    options = {}
    if len(sys.argv) > 3:
        try:
            options = json.loads(sys.argv[3])
        except json.JSONDecodeError as e:
            print(
                json.dumps({"error": f"Invalid options JSON: {e}"}),
                file=sys.stderr,
            )
            sys.exit(1)

    # Validate input
    if not os.path.exists(pdf_path):
        print(
            json.dumps({
                "error": f"PDF file not found: {pdf_path}",
                "pdf_path": pdf_path,
            }),
            file=sys.stderr,
        )
        sys.exit(1)

    # Build config
    config = ParserConfig(
        output_dir=output_dir,
        image_base_dir=options.get(
            "image_base_dir", "storage/questions"
        ),
        image_format=options.get("image_format", "png"),
        min_image_size=options.get("min_image_size", 50),
        exam_name=options.get("exam_name", ""),
        exam_provider=options.get("exam_provider", ""),
        exam_version=options.get("exam_version", ""),
        exam_id=options.get("exam_id"),
        log_level=options.get("log_level", "WARNING"),
        log_file=options.get("log_file"),
        save_raw_blocks=options.get("save_raw_blocks", True),
    )

    # Page range
    page_start = options.get("page_start")
    page_end = options.get("page_end")
    if page_start or page_end:
        config.page_range = (page_start or 1, page_end or 99999)

    # Run parser
    try:
        # Suppress console logging for subprocess mode
        logging.getLogger("parser").setLevel(logging.WARNING)

        engine = ParserEngine(config)
        result = engine.parse(pdf_path)

        # Output clean JSON to stdout
        output = result.model_dump()
        print(json.dumps(output, ensure_ascii=False, default=str))
        sys.exit(0)

    except Exception as e:
        error_info = {
            "error": str(e),
            "type": type(e).__name__,
            "traceback": traceback.format_exc(),
            "pdf_path": pdf_path,
        }
        print(
            json.dumps(error_info, ensure_ascii=False),
            file=sys.stderr,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
