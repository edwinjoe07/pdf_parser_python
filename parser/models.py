"""
Data Models
===========
Pydantic models for structured PDF parsing output.
All models are serializable to JSON for Laravel consumption.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, computed_field


# ─── Enums ────────────────────────────────────────────────────────────────────


class BlockType(str, Enum):
    """Type of content block extracted from PDF."""
    TEXT = "text"
    IMAGE = "image"


class Section(str, Enum):
    """Section within a question entity."""
    QUESTION = "question"
    ANSWER = "answer"
    EXPLANATION = "explanation"


class AnomalyType(str, Enum):
    """Types of structural anomalies detected during parsing."""
    MISSING_ANSWER = "missing_answer"
    MISSING_QUESTION_TEXT = "missing_question_text"
    EXPLANATION_WITHOUT_ANSWER = "explanation_without_answer"
    ORPHAN_IMAGE = "orphan_image"
    DUPLICATE_QUESTION_NUMBER = "duplicate_question_number"
    MULTI_PAGE_FRAGMENTATION = "multi_page_fragmentation"
    UNRECOGNIZED_ANCHOR = "unrecognized_anchor"
    EMPTY_SECTION = "empty_section"
    MISSING_EXPLANATION = "missing_explanation"


class QuestionStatus(str, Enum):
    """Lifecycle status of a parsed question."""
    DRAFT = "draft"
    APPROVED = "approved"
    PUBLISHED = "published"


class QuestionType(str, Enum):
    """Supported question formats."""
    MCQ = "mcq"
    HOTSPOT = "hotspot"
    DRAG_DROP = "drag_drop"
    CASE_STUDY = "case_study"
    MATCHING = "matching"
    TEXT_ONLY = "text_only"


# ─── Block Models ─────────────────────────────────────────────────────────────


class ContentBlock(BaseModel):
    """
    A single content block extracted from the PDF.
    Preserves exact position, page, and ordering.
    """
    type: BlockType
    content: str = Field(
        description="Text content or relative image file path"
    )
    page_number: int = Field(ge=1)
    bbox: tuple[float, float, float, float] = Field(
        description="Bounding box as (x0, y0, x1, y1)"
    )
    order_index: int = Field(
        ge=0,
        description="Global ordering index across all pages"
    )
    font_info: Optional[FontInfo] = None

    def model_dump(self, **kwargs):
        data = super().model_dump(**kwargs)
        if "bbox" in data and isinstance(data["bbox"], tuple):
            data["bbox"] = list(data["bbox"])
        return data


class FontInfo(BaseModel):
    """Font metadata for a text block."""
    name: Optional[str] = None
    size: Optional[float] = None
    flags: Optional[int] = None
    color: Optional[int] = None
    is_bold: bool = False
    is_italic: bool = False


# Fix forward reference
ContentBlock.model_rebuild()


# ─── Anomaly Model ────────────────────────────────────────────────────────────


class Anomaly(BaseModel):
    """A structural anomaly detected in a question."""
    type: AnomalyType
    severity: int = Field(
        ge=0, le=100,
        description="Severity score 0-100"
    )
    message: str
    context: Optional[dict] = None


# ─── Question Model ──────────────────────────────────────────────────────────


class ParsedQuestion(BaseModel):
    """
    A fully parsed question entity with all sections
    and associated anomalies.
    """
    question_number: int
    question_type: QuestionType = QuestionType.MCQ
    page_start: int
    page_end: int
    blocks: dict[str, list[ContentBlock]] = Field(
        default_factory=lambda: {
            "question": [],
            "answer": [],
            "explanation": []
        }
    )
    anomalies: list[Anomaly] = Field(default_factory=list)
    raw_text: str = Field(
        default="",
        description="Full raw text of the question for search/debug"
    )

    @computed_field
    @property
    def anomaly_score(self) -> int:
        """Aggregate anomaly score (0-100)."""
        if not self.anomalies:
            return 0
        return min(100, sum(a.severity for a in self.anomalies))

    @computed_field
    @property
    def has_question_text(self) -> bool:
        return any(
            b.type == BlockType.TEXT and b.content.strip()
            for b in self.blocks.get("question", [])
        )

    @computed_field
    @property
    def has_answer(self) -> bool:
        return any(
            b.type == BlockType.TEXT and b.content.strip()
            for b in self.blocks.get("answer", [])
        )

    @computed_field
    @property
    def has_explanation(self) -> bool:
        return any(
            b.type == BlockType.TEXT and b.content.strip()
            for b in self.blocks.get("explanation", [])
        )

    @computed_field
    @property
    def image_count(self) -> int:
        return sum(
            1
            for section_blocks in self.blocks.values()
            for b in section_blocks
            if b.type == BlockType.IMAGE
        )


# ─── Exam / Parse Result Models ──────────────────────────────────────────────


class ExamMetadata(BaseModel):
    """Metadata about the source PDF / exam."""
    name: str = ""
    provider: str = ""
    version: str = ""
    source_pdf: str = ""
    total_pages: int = 0
    file_hash: str = ""
    file_size_bytes: int = 0


class ParseVersion(BaseModel):
    """Version tracking for a parse run."""
    parser_version: str = "1.0.0"
    parse_timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    raw_block_count: int = 0
    structured_question_count: int = 0


class ValidationReport(BaseModel):
    """Post-parse validation report."""
    total_questions_detected: int = 0
    structured_successfully: int = 0
    missing_question_numbers: list[int] = Field(default_factory=list)
    duplicate_question_numbers: list[int] = Field(default_factory=list)
    questions_missing_answer: list[int] = Field(default_factory=list)
    questions_missing_explanation: list[int] = Field(default_factory=list)
    orphan_images: int = 0
    anomaly_breakdown: dict[str, int] = Field(default_factory=dict)

    @computed_field
    @property
    def success_rate(self) -> float:
        if self.total_questions_detected == 0:
            return 0.0
        return round(
            self.structured_successfully / self.total_questions_detected * 100,
            2
        )


class ParseResult(BaseModel):
    """
    Complete output of a parse run.
    This is the top-level JSON structure returned to Laravel.
    """
    exam: ExamMetadata
    parse_version: ParseVersion
    questions: list[ParsedQuestion] = Field(default_factory=list)
    validation: ValidationReport = Field(
        default_factory=ValidationReport
    )

    def compute_file_hash(self, filepath: str) -> str:
        """Compute SHA-256 hash of source file."""
        sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
