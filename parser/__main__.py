"""
Module entry point for: python -m parser

Allows running the parser directly as a module:
    python -m parser parse <pdf_path> [options]
    python -m parser batch <directory> [options]
    python -m parser serve [options]
"""

import sys

from .cli import cli


def main():
    cli()


if __name__ == "__main__":
    main()
