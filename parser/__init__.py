"""
PDF Parser Engine
=================
Scalable PDF ingestion and parsing system for certification exam questions.

Architecture:
    - Block Extractor: Extracts ordered blocks (text + images) from PDF pages
    - State Machine: Detects question structure via text anchors
    - Image Mapper: Maps images to question sections contextually
    - Anomaly Detector: Flags structural issues for review
    - Output Formatter: Produces structured JSON for Laravel persistence

Version: 1.0.0
"""

__version__ = "1.0.0"
