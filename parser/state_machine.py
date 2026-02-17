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

# Matches "Question: 1", "Question 42", "QUESTION: 100" (case-insensitive)
QUESTION_PATTERN = re.compile(
    r"^\s*Question\s*:?\s*(\d+)\s*$", re.IGNORECASE
)

# Matches "Answer:", "Answer", "ANSWER:" (standalone)
ANSWER_PATTERN = re.compile(
    r"^\s*Answer\s*:?\s*$", re.IGNORECASE
)

# Matches "Answer: B", "Answer: The correct answer is..."
ANSWER_INLINE_PATTERN = re.compile(
    r"^\s*Answer\s*:?\s*(.+)$", re.IGNORECASE
)

# Matches "Explanation:", "Explanation" (standalone)
EXPLANATION_PATTERN = re.compile(
    r"^\s*Explanation\s*:?\s*$", re.IGNORECASE
)

# Matches "Explanation: S3 is..."
EXPLANATION_INLINE_PATTERN = re.compile(
    r"^\s*Explanation\s*:?\s*(.+)$", re.IGNORECASE
)


class ParserState(Enum):
    """Internal states of the parsing machine."""
    SEEKING_QUESTION = "SEEKING_QUESTION"
    READING_QUESTION = "READING_QUESTION"
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
        
        # 1. Look for Question Anchor (always triggers a new question)
        if block.type == BlockType.TEXT:
            match = QUESTION_PATTERN.match(block.content)
            if match:
                q_num = int(match.group(1))
                self._start_new_question(q_num, block)
                return

        # 2. State-specific processing
        if self.state == ParserState.SEEKING_QUESTION:
            # Ignore anything before the first question
            return

        elif self.state == ParserState.READING_QUESTION:
            self._handle_reading_question(block)

        elif self.state == ParserState.READING_ANSWER:
            self._handle_reading_answer(block)

        elif self.state == ParserState.READING_EXPLANATION:
            self._handle_reading_explanation(block)

    def _start_new_question(self, q_num: int, anchor_block: ContentBlock):
        """Finalize current question and start a new one."""
        if self.current_question:
            self._finalize_question()

        logger.debug(f"Starting question {q_num} at page {anchor_block.page_number}")
        
        self.current_question = ParsedQuestion(
            question_number=q_num,
            page_start=anchor_block.page_number,
            page_end=anchor_block.page_number,
        )
        
        # Check for duplicate numbers
        if q_num in self.question_numbers:
            self.current_question.anomalies.append(Anomaly(
                type=AnomalyType.DUPLICATE_QUESTION_NUMBER,
                severity=40,
                message=f"Question number {q_num} appeared previously",
                context={"number": q_num}
            ))
        
        self.question_numbers.add(q_num)
        self.state = ParserState.READING_QUESTION

    def _handle_reading_question(self, block: ContentBlock):
        """Process blocks while in READING_QUESTION state."""
        if block.type == BlockType.TEXT:
            # Check for Answer Anchor
            ans_inline = ANSWER_INLINE_PATTERN.match(block.content)
            ans_standalone = ANSWER_PATTERN.match(block.content)

            if ans_standalone:
                self.state = ParserState.READING_ANSWER
                return
            elif ans_inline:
                # Add inline content to answer section
                content = ans_inline.group(1).strip()
                if content:
                    self.current_question.blocks["answer"].append(
                        self._clone_block(block, content)
                    )
                self.state = ParserState.READING_ANSWER
                return

        # Not an anchor, add to question section
        self.current_question.blocks["question"].append(block)
        self.current_question.page_end = max(
            self.current_question.page_end, block.page_number
        )

    def _handle_reading_answer(self, block: ContentBlock):
        """Process blocks while in READING_ANSWER state."""
        if block.type == BlockType.TEXT:
            # Check for Explanation Anchor
            exp_inline = EXPLANATION_INLINE_PATTERN.match(block.content)
            exp_standalone = EXPLANATION_PATTERN.match(block.content)

            if exp_standalone:
                self.state = ParserState.READING_EXPLANATION
                return
            elif exp_inline:
                # Add inline content to explanation section
                content = exp_inline.group(1).strip()
                if content:
                    self.current_question.blocks["explanation"].append(
                        self._clone_block(block, content)
                    )
                self.state = ParserState.READING_EXPLANATION
                return

        # Not an anchor, add to answer section
        self.current_question.blocks["answer"].append(block)
        self.current_question.page_end = max(
            self.current_question.page_end, block.page_number
        )

    def _handle_reading_explanation(self, block: ContentBlock):
        """Process blocks while in READING_EXPLANATION state."""
        # Explanation section continues until next Question anchor
        self.current_question.blocks["explanation"].append(block)
        self.current_question.page_end = max(
            self.current_question.page_end, block.page_number
        )

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
