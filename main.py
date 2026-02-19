"""
PDF Parser Service — Main Entry Point
======================================
Starts the persistent Flask-based parsing microservice.

Usage:
    python main.py                    # Default: 0.0.0.0:5000
    python main.py --port 8000        # Custom port
    python main.py --debug            # Debug mode
"""

import argparse
import logging
import sys

from parser.server import create_app, app

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="PDF Parser Service")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=5000, help="Bind port")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    args = parser.parse_args()

    # create_app() handles init_storage() + init_db() internally
    logger.info("Creating Flask app (initializes DB + storage)...")
    create_app()

    from parser.database import get_db_path
    logger.info(f"Database path: {get_db_path()}")
    logger.info(f"Starting server on {args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
