# PDF Parser Engine

Scalable PDF ingestion and parsing system for certification exam questions. Converts heterogeneous certification PDFs into structured, reviewable, publishable question entities.

## Architecture

```
PDF File
    ↓
Block Extractor (PyMuPDF)
    ↓
Ordered Content Blocks (text + images)
    ↓
State Machine Parser (text-anchor detection)
    ↓
Structured Questions (ParsedQuestion entities)
    ↓
Validation Engine
    ↓
JSON Output → Laravel Persistence Layer
```

## Quick Start

### Installation

```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
# source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt
```

### Parse a Single PDF

```bash
# Interactive CLI with rich output
python -m parser parse exam.pdf --exam-name "AWS SAA-C03" --exam-provider "AWS"

# JSON output for programmatic use (Laravel integration)
python -m parser parse exam.pdf --json-output

# With page range
python -m parser parse exam.pdf --page-start 5 --page-end 100

# Debug mode
python -m parser parse exam.pdf --log-level DEBUG
```

### Batch Parse

```bash
python -m parser batch ./pdfs/ --exam-provider "AWS" --output ./output
```

### HTTP Microservice

```bash
# Start the microservice
python -m parser serve --port 5000

# Parse via HTTP (async)
curl -X POST http://localhost:5000/api/parse \
  -H "Content-Type: application/json" \
  -d '{"file_path": "/path/to/exam.pdf", "exam_name": "AWS SAA"}'

# Parse via HTTP (sync)
curl -X POST http://localhost:5000/api/parse/sync \
  -H "Content-Type: application/json" \
  -d '{"file_path": "/path/to/exam.pdf"}'

# Check health
curl http://localhost:5000/api/health
```

### Laravel Bridge (Subprocess)

```bash
# Called by Laravel queue job:
python laravel_bridge.py /path/to/exam.pdf /output/dir '{"exam_name":"AWS","exam_id":"123"}'
```

## JSON Output Structure

```json
{
  "exam": {
    "name": "AWS SAA-C03",
    "provider": "AWS",
    "version": "2024",
    "source_pdf": "saa-c03.pdf",
    "total_pages": 455,
    "file_hash": "abc123...",
    "file_size_bytes": 2905549
  },
  "parse_version": {
    "parser_version": "1.0.0",
    "parse_timestamp": "2026-02-17T10:30:00Z",
    "raw_block_count": 5000,
    "structured_question_count": 600
  },
  "questions": [
    {
      "question_number": 1,
      "page_start": 4,
      "page_end": 5,
      "blocks": {
        "question": [
          {
            "type": "text",
            "content": "A company needs to...",
            "page_number": 4,
            "bbox": [72.0, 100.0, 540.0, 140.0],
            "order_index": 42
          },
          {
            "type": "image",
            "content": "images/page4_img1.png",
            "page_number": 4,
            "bbox": [72.0, 150.0, 540.0, 350.0],
            "order_index": 43
          }
        ],
        "answer": [...],
        "explanation": [...]
      },
      "anomalies": [],
      "anomaly_score": 0,
      "has_question_text": true,
      "has_answer": true,
      "has_explanation": true,
      "image_count": 1
    }
  ],
  "validation": {
    "total_questions_detected": 600,
    "structured_successfully": 598,
    "missing_question_numbers": [42],
    "duplicate_question_numbers": [],
    "questions_missing_answer": [42],
    "questions_missing_explanation": [100, 200],
    "orphan_images": 0,
    "anomaly_breakdown": {
      "missing_answer": 1,
      "missing_explanation": 2
    },
    "success_rate": 99.67
  }
}
```

## State Machine

```
┌──────────────────┐
│ SEEKING_QUESTION │
└────────┬─────────┘
         │ "Question: N" detected
         ▼
┌──────────────────┐
│ READING_QUESTION │──── text/images → question_blocks
└────────┬─────────┘
         │ "Answer:" detected
         ▼
┌──────────────────┐
│ READING_ANSWER   │──── text/images → answer_blocks
└────────┬─────────┘
         │ "Explanation:" detected
         ▼
┌──────────────────────┐
│ READING_EXPLANATION  │──── text/images → explanation_blocks
└──────────┬───────────┘
           │ "Question: N+1" detected
           ▼
       Finalize Q → back to SEEKING_QUESTION
```

## Anchor Patterns

| Anchor | Pattern | Example |
|--------|---------|---------|
| Question | `^\s*Question\s*:?\s*(\d+)\s*$` | `Question: 1`, `Question 42` |
| Answer | `^\s*Answer\s*:?\s*$` or inline | `Answer:`, `Answer: B` |
| Explanation | `^\s*Explanation\s*:?\s*$` or inline | `Explanation:`, `Explanation: ...` |

All patterns are **case-insensitive** with **optional colon**.

## Image Mapping Rules

- After Question anchor, before Answer → `question` section
- After Answer, before Explanation → `answer` section
- After Explanation, before next Question → `explanation` section
- **Images never define structure**
- **Never assign images across question boundaries**

## Anomaly Detection

| Type | Severity | Description |
|------|----------|-------------|
| `missing_answer` | 60 | Question has no answer section |
| `missing_question_text` | 80 | Question has no text content |
| `explanation_without_answer` | 50 | Explanation exists but answer missing |
| `orphan_image` | 30 | Section contains only images |
| `duplicate_question_number` | 40 | Same question number appears twice |
| `multi_page_fragmentation` | 10 | Question spans multiple pages |
| `missing_explanation` | 20 | Question has no explanation |

## Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test class
python -m pytest tests/test_parser.py::TestStateMachineParser -v
```

## Project Structure

```
pdf-parser-python/
├── parser/
│   ├── __init__.py          # Package init + version
│   ├── __main__.py          # python -m parser entry point
│   ├── models.py            # Pydantic data models
│   ├── block_extractor.py   # PDF block extraction (PyMuPDF)
│   ├── state_machine.py     # Text-anchor state machine parser
│   ├── validator.py         # Post-parse validation engine
│   ├── engine.py            # Main orchestrator
│   ├── cli.py               # Click CLI interface
│   └── server.py            # Flask HTTP microservice
├── tests/
│   └── test_parser.py       # Comprehensive test suite
├── laravel_bridge.py        # Laravel subprocess bridge
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Design Principles

- **Structure is text-anchor driven** — never image-driven
- **Parsing is deterministic** — same input always produces same output
- **Preserve original formatting exactly** — never auto-normalize
- **Allow duplicates** — report but don't suppress
- **Handle layout chaos gracefully** — heterogeneous PDFs welcome
- **No OCR, no ML** — pure text-anchor detection
- **Images are attachments, never structure drivers**
