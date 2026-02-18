# PDF Parser Engine: Tech Stack & Architectural Logic

## Project Overview
The **PDF Parser Engine** is a specialized tool designed to transform heterogeneous certification exam PDFs (often disorganized or inconsistently formatted) into highly structured JSON data. Its primary goal is to extract questions, multi-choice options, correct answers, and detailed explanations while maintaining the physical association of images with their respective sections.

---

## Core Technology Stack

| Component | Technology | Rationale |
| :--- | :--- | :--- |
| **Language** | Python 3.10+ | Ecosystem support for PDF manipulation and data processing. |
| **Engine** | PyMuPDF (fitz) | High-performance PDF rendering and block-level text extraction. |
| **Parsing Logic** | Custom State Machine | Provides deterministic, rule-based parsing without the overhead/unpredictability of LLMs or OCR. |
| **Data Models** | Pydantic | Ensures strict type safety and schema validation for the output JSON. |
| **Web Layer** | Flask | Lightweight microservice for asynchronous parsing requests. |
| **CLI** | Click & Rich | Robust command-line interface with formatted logging for debug-heavy workflows. |
| **Image Handling** | Pillow (PIL) | Used for validating, cropping, and processing extracted raster images. |

---

## Architectural Logic

The engine operates in three distinct phases to ensure robustness against varying PDF layouts.

### 1. Block Extraction (Linearization)
PDFs are non-linear by nature; text in the file doesn't always follow the reading order.
- **Logic**: We use PyMuPDF to extract "blocks." Each block includes text, coordinates (bbox), and page numbers.
- **Sorting**: Blocks are sorted primarily by **Page Number** and secondarily by **Y-coordinate** (top-to-bottom). This mirrors how a human reads the document.
- **Media Mapping**: Images are extracted as separate blocks but kept in the same sequence as the surrounding text.

### 2. The State Machine Parser
The heart of the project is a **Deterministic Finite State Machine (FSM)** found in `parser/state_machine.py`.

- **Why a State Machine?** Regex alone isn't enough for multi-line questions that span pages. The FSM maintains a "context" (e.g., "I am currently reading Question 12's explanation") until a new anchor is found.
- **States**:
  - `SEEKING_QUESTION`: Looking for the next "Question: N" marker.
  - `QUESTION_BODY`: Accumulating text and images for the question prompt.
  - `OPTION`: Detecting specific markers like `A.`, `B.`, or `(C)` to build the choice list.
  - `ANSWER`: Hunting for the Correct Answer anchor.
  - `EXPLANATION`: Gathering the rationale/reference material.
- **Anchors**: We use "Visual Anchors"—consistent text patterns that signal a transition. If the parser sees "Answer:", it switches state immediately, regardless of where it was.

### 3. Image Assignment Logic
A key challenge is assigning images (diagrams, code snippets) to the right part of a question.
- **Logic**: Images are "attachments" to the current state.
- If the FSM is in `QUESTION_BODY`, the image belongs to the prompt.
- If it just saw `Option C.`, the image belongs to that specific choice.
- **Boundaries**: Images are never allowed to "leak" across question boundaries. If a new "Question: N+1" is detected, any pending images from the previous page are either finalized or flagged.

---

## Key Design Decisions

### 1. Text-Anchor Driven (Not Layout Driven)
While we use coordinates for sorting, we **never** rely on "whitespace" or font sizes to determine structure. Different PDF generators use different fonts. We rely on the *content* (Anchors like "Question:", "Answer:") because they are the most stable part of a certification document.

### 2. Zero OCR (Optical Character Recognition)
We assume the PDF contains text metadata. By avoiding OCR, we maintain 100% accuracy on character extraction (including complex technical terms and code) and significantly reduce processing time.

### 3. Anomaly-First Validation
Instead of failing a whole file when one question is weird, the engine uses an **Anomaly Detection** system.
- It flags missing answers, orphan images (images with no text), or skipped question numbers.
- Each question gets an `anomaly_score`. This allows the downstream UI (Laravel) to highlight specific questions that need manual human review.

---

## Integration: The Laravel Bridge
The engine is designed to be a "worker" for a larger PHP/Laravel application.
- **Bridge**: `laravel_bridge.py` acts as a wrapper that Laravel can call via a subprocess.
- **Communication**: Parameters are passed via CLI; the engine writes structured JSON and images to a specific storage directory; Laravel then ingests the JSON into the SQL database.

## Workflow Modes
1. **Interactive CLI**: For developers to test new PDF formats with `DEBUG` logs.
2. **Batch Mode**: To process hundreds of PDFs in a single run.
3. **HTTP API**: For real-time parsing requests from other internal services.
