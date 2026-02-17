"""
State Machine Parser
====================
Deterministic state machine for detecting certification exam question structure
based on text-anchors (Question, Answer, Explanation).
"""

from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Optional

from .models import (
    Anomaly,
    AnomalyType,
    BlockType,
    ContentBlock,
    ParsedQuestion,
    Section,
)

logger = logging.getLogger(__name__)

# ─── Anchor Patterns ──────────────────────────────────────────────────────────

# Matches "Question: 1", "Question 42" at start of line
QUESTION_PATTERN = re.compile(
    r"^\s*Question\s*:?\s*(\d+)", re.IGNORECASE
)

# Matches "A.", "B.", "A)", "(A)" style options
OPTION_PATTERN = re.compile(
    r"^\s*([A-Z])[\.\)]\s*", re.IGNORECASE
)

# Matches "Answer:", "Answer", "ANSWER:", "Correct Answer:", "Ans:", "Ans."
ANSWER_PATTERN = re.compile(
    r"^\s*(?:Correct\s+)?(?:Answer|Ans|Key)[\s.:]*", re.IGNORECASE
)

# Matches "Explanation:", "Reference:", "Rationale:"
EXPLANATION_PATTERN = re.compile(
    r"^\s*(Explanation|Reference|Rationale)\s*:?\s*", re.IGNORECASE
)

# Patterns to ignore (headers, footers, page counters)
IGNORE_PATTERNS = [
    re.compile(r"^\s*Questions and Answers PDF.*$", re.IGNORECASE), # Heuristic for common dumps
    re.compile(r"^\s*(Page\s*)?\d+\s*(/|of)\s*\d+\s*$", re.IGNORECASE), # "8/528", "Page 8 of 528"
    re.compile(r"^\s*Question\s*\d+\s*$"), # Solo "Question 5" (not the anchor)
    re.compile(r"^https?://[^\s]+$"), # Lone URLs
]


class ParserState(Enum):
    """Internal states of the parsing machine."""
    SEEKING_QUESTION = "SEEKING_QUESTION"
    READING_QUESTION = "READING_QUESTION"
    READING_OPTIONS = "READING_OPTIONS"
    READING_ANSWER = "READING_ANSWER"
    READING_EXPLANATION = "READING_EXPLANATION"


class StateMachineParser:
    """
    Finite State Machine that transforms an ordered sequence of ContentBlocks
    into structured ParsedQuestion entities.
    """

    def __init__(self):
        self.state = ParserState.SEEKING_QUESTION
        self.current_question: Optional[ParsedQuestion] = None
        self.questions: list[ParsedQuestion] = []
        self.question_numbers: set[int] = set()

    def parse(self, blocks: list[ContentBlock]) -> list[ParsedQuestion]:
        """
        Parse a list of blocks into questions.
        
        Args:
            blocks: List of ContentBlock objects in reading order.
            
        Returns:
            List of ParsedQuestion objects.
        """
        self.state = ParserState.SEEKING_QUESTION
        self.current_question = None
        self.questions = []
        self.question_numbers = set()

        for block in blocks:
            self._process_block(block)

        # Finalize the last question if any
        if self.current_question:
            self._finalize_question()

        return self.questions

    def _process_block(self, block: ContentBlock):
        """Process a single content block based on current state."""

        # 1. Images are always added to the current section (if any)
        if block.type == BlockType.IMAGE:
            if self.current_question:
                section = self._get_current_section_key()
                self.current_question.blocks[section].append(block)
                self.current_question.page_end = max(
                    self.current_question.page_end, block.page_number
                )
            return

        # 2. Text blocks: split into lines for high-resolution anchor detection
        lines = block.content.split("\n")
        for line in lines:
            line_str = line.strip()
            if not line_str:
                continue

            # ─── Noise Reduction ───
            if any(p.match(line_str) for p in IGNORE_PATTERNS):
                continue

            # ─── Anchor Detection ───

            # Question Start (Highest Priority)
            q_match = QUESTION_PATTERN.match(line_str)
            if q_match:
                q_num = int(q_match.group(1))
                self._start_new_question(q_num, block)
                remainder = line_str[q_match.end() :].strip()
                if remainder:
                    self._add_to_section(remainder, block)
                continue

            # Don't process before first question
            if not self.current_question:
                continue

            # Answer Anchor
            ans_match = ANSWER_PATTERN.match(line_str)
            if ans_match:
                self.state = ParserState.READING_ANSWER
                remainder = line_str[ans_match.end() :].strip()
                if remainder:
                    self._add_to_section(remainder, block)
                continue

            # Explanation/Reference Anchor
            exp_match = EXPLANATION_PATTERN.match(line_str)
            if exp_match:
                self.state = ParserState.READING_EXPLANATION
                remainder = line_str[exp_match.end() :].strip()
                if remainder:
                    self._add_to_section(remainder, block)
                continue

            # Option Anchor (Only valid in Question/Options phase)
            opt_match = OPTION_PATTERN.match(line_str)
            if opt_match and self.state in [
                ParserState.READING_QUESTION,
                ParserState.READING_OPTIONS,
            ]:
                self.state = ParserState.READING_OPTIONS
                self._add_to_section(line_str, block)
                continue

            # ─── Content Accumulation ───
            self._add_to_section(line_str, block)

    def _start_new_question(self, q_num: int, anchor_block: ContentBlock):
        """Finalize current question and start a new one."""
        if self.current_question:
            self._finalize_question()

        logger.info(f"Detected Question {q_num} on page {anchor_block.page_number}")
        self.current_question = ParsedQuestion(
            question_number=q_num,
            page_start=anchor_block.page_number,
            page_end=anchor_block.page_number,
        )

        # Basic duplicate detection
        if q_num in self.question_numbers:
            self.current_question.anomalies.append(
                Anomaly(
                    type=AnomalyType.DUPLICATE_QUESTION_NUMBER,
                    severity=40,
                    message=f"Duplicate question number detected: {q_num}",
                    context={"number": q_num},
                )
            )

        self.question_numbers.add(q_num)
        self.state = ParserState.READING_QUESTION

    def _add_to_section(self, content: str, source: ContentBlock):
        """Add text content to current active section."""
        key = self._get_current_section_key()
        self.current_question.blocks[key].append(self._clone_block(source, content))
        self.current_question.page_end = max(
            self.current_question.page_end, source.page_number
        )

    def _get_current_section_key(self) -> str:
        """Map enum state to dictionary block key."""
        if self.state == ParserState.READING_QUESTION:
            return "question"
        if self.state == ParserState.READING_OPTIONS:
            return "options"
        if self.state == ParserState.READING_ANSWER:
            return "answer"
        if self.state == ParserState.READING_EXPLANATION:
            return "explanation"
        return "question"

    def _finalize_question(self):
        """Apply final logic to a finished question."""
        q = self.current_question
        
        # Basic integrity checks
        if not q.has_question_text:
            q.anomalies.append(Anomaly(
                type=AnomalyType.MISSING_QUESTION_TEXT,
                severity=80,
                message="Question has no text content"
            ))

        if not q.has_answer:
            q.anomalies.append(Anomaly(
                type=AnomalyType.MISSING_ANSWER,
                severity=60,
                message="Question has no answer section"
            ))

        if q.has_explanation and not q.has_answer:
            q.anomalies.append(Anomaly(
                type=AnomalyType.EXPLANATION_WITHOUT_ANSWER,
                severity=50,
                message="Explanation exists but answer is missing"
            ))

        if not q.has_explanation:
            q.anomalies.append(Anomaly(
                type=AnomalyType.MISSING_EXPLANATION,
                severity=20,
                message="Question has no explanation"
            ))

        # Check for orphan images (sections with only images)
        for section, blocks in q.blocks.items():
            if blocks and all(b.type == BlockType.IMAGE for b in blocks):
                q.anomalies.append(Anomaly(
                    type=AnomalyType.ORPHAN_IMAGE,
                    severity=30,
                    message=f"Section '{section}' contains only images",
                    context={"section": section}
                ))

        self.questions.append(q)

    def _clone_block(self, block: ContentBlock, new_content: str) -> ContentBlock:
        """Create a copy of a block with modified content (for inline anchors)."""
        return ContentBlock(
            type=block.type,
            content=new_content,
            page_number=block.page_number,
            bbox=block.bbox,
            order_index=block.order_index,
            font_info=block.font_info,
        )
