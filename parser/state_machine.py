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
    FontInfo,
    ParsedQuestion,
    QuestionOption,
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

# Patterns to ignore (headers, footers, page counters, exam dump boilerplate)
IGNORE_PATTERNS = [
    re.compile(r"^\s*Questions and Answers PDF.*$",
               re.IGNORECASE),  # Heuristic for common dumps
    re.compile(r"^\s*(Page\s*)?\d+\s*(/|of)\s*\d+\s*$",
               re.IGNORECASE),  # "8/528", "Page 8 of 528"
    # Solo "Question 5" (not the anchor)
    re.compile(r"^\s*Question\s*\d+\s*$"),
    re.compile(r"^https?://[^\s]+$"),  # Lone URLs
    # "Box 1:", "Box 2:" noise
    re.compile(r"^\s*Box\s*\d+\s*:", re.IGNORECASE),
    # "Select and Place:" noise
    re.compile(r"^\s*Select and Place:", re.IGNORECASE),
    # Exam dump boilerplate
    re.compile(r"^\s*Thank\s+you\s+for\s+your\s+visit\.?\s*$", re.IGNORECASE),
    re.compile(r"^\s*Visit\s+us\s+at\b", re.IGNORECASE),
    re.compile(r"^\s*For\s+more\s+questions\b", re.IGNORECASE),
    re.compile(r"^\s*Get\s+certified\b", re.IGNORECASE),
    re.compile(r"^\s*Download\s+free\b", re.IGNORECASE),
    re.compile(r"examtopics?\.(com|org|net)", re.IGNORECASE),
    re.compile(r"certification.s*prep", re.IGNORECASE),
]


class ParserState(Enum):
    """Internal states based on visual section detection."""
    SEEKING_QUESTION = "SEEKING_QUESTION"
    QUESTION_BODY = "QUESTION_BODY"
    OPTION = "OPTION"
    ANSWER = "ANSWER"
    EXPLANATION = "EXPLANATION"


class StateMachineParser:
    """
    Finite State Machine that transforms an ordered sequence of ContentBlocks
    into structured ParsedQuestion entities with strict media ownership.
    """

    def __init__(self):
        self.state = ParserState.SEEKING_QUESTION
        self.current_question: Optional[ParsedQuestion] = None
        self.current_option: Optional[QuestionOption] = None
        self.questions: list[ParsedQuestion] = []
        self.question_numbers: set[int] = set()

    def reset(self):
        """Reset the state machine for a fresh parsing run."""
        self.state = ParserState.SEEKING_QUESTION
        self.current_question = None
        self.current_option = None
        self.questions = []
        self.question_numbers = set()

    def finalize(self):
        """Finalize any pending (in-progress) question at end of parsing."""
        if self.current_question:
            self._finalize_question()

    def parse(self, blocks: list[ContentBlock]) -> list[ParsedQuestion]:
        """Parse blocks into structured questions."""
        self.state = ParserState.SEEKING_QUESTION
        self.current_question = None
        self.current_option = None
        self.questions = []
        self.question_numbers = set()

        for block in blocks:
            self._process_block(block)

        # Finalize the last question
        if self.current_question:
            self._finalize_question()

        return self.questions

    def _process_block(self, block: ContentBlock):
        """Process a block based on strict visual context."""

        # ─── 1. Image Block: Strict Assignment ───
        if block.type == BlockType.IMAGE:
            if not self.current_question:
                logger.debug(
                    f"Skipping orphan image (pre-amble) at page {block.page_number}")
                return

            self._assign_image(block)
            return

        # ─── 2. Text Block: State Transitions & Content ───
        lines = block.content.split("\n")

        for line in lines:
            line_str = line.strip()
            if not line_str:
                continue

            # Check noise patterns (Headers/Footers)
            if any(p.match(line_str) for p in IGNORE_PATTERNS):
                continue

            # Anchor Detection (Case-Insensitive)

            # Question Anchor (e.g. "Question: 13")
            q_match = QUESTION_PATTERN.match(line_str)
            if q_match:
                q_num = int(q_match.group(1))
                self._start_new_question(q_num, block)
                remainder = line_str[q_match.end():].strip()
                if remainder:
                    self._append_text(remainder)
                continue

            if not self.current_question:
                continue

            # Option Anchor (e.g. "A.")
            opt_match = OPTION_PATTERN.match(line_str)
            if opt_match and self.state in [ParserState.QUESTION_BODY, ParserState.OPTION]:
                key = opt_match.group(1).upper()
                self._start_new_option(key)
                remainder = line_str[opt_match.end():].strip()
                if remainder:
                    self._append_text(remainder)
                continue

            # Answer Anchor (e.g. "Answer: B")
            ans_match = ANSWER_PATTERN.match(line_str)
            if ans_match:
                self.state = ParserState.ANSWER
                self.current_option = None
                remainder = line_str[ans_match.end():].strip()
                if remainder:
                    self._append_text(remainder)
                continue

            # Explanation Anchor (e.g. "Explanation:")
            exp_match = EXPLANATION_PATTERN.match(line_str)
            if exp_match:
                self.state = ParserState.EXPLANATION
                self.current_option = None
                remainder = line_str[exp_match.end():].strip()
                if remainder:
                    self._append_text(remainder)
                continue

            # Accumulate content in current state
            self._append_text(line_str)

    def _start_new_question(self, q_num: int, block: ContentBlock):
        """Finalize previous and start fresh state."""
        if self.current_question:
            self._finalize_question()

        logger.info(f"Detected Question {q_num} on page {block.page_number}")

        self.current_question = ParsedQuestion(
            question_number=q_num,
            page_start=block.page_number,
            page_end=block.page_number,
        )
        self.current_option = None
        self.state = ParserState.QUESTION_BODY
        self.question_numbers.add(q_num)

    def _start_new_option(self, key: str):
        """Switch to OPTION state and create structure."""
        self.state = ParserState.OPTION
        self.current_option = QuestionOption(key=key)
        self.current_question.options.append(self.current_option)

    def _append_text(self, text: str):
        """Append text to the active part of the current question."""
        if not self.current_question:
            return

        if self.state == ParserState.QUESTION_BODY:
            if self.current_question.question_text:
                self.current_question.question_text += " " + text
            else:
                self.current_question.question_text = text

        elif self.state == ParserState.OPTION:
            if self.current_option:
                if self.current_option.text:
                    self.current_option.text += " " + text
                else:
                    self.current_option.text = text

        elif self.state == ParserState.ANSWER:
            if self.current_question.answer_text:
                self.current_question.answer_text += " " + text
            else:
                self.current_question.answer_text = text

        elif self.state == ParserState.EXPLANATION:
            if self.current_question.explanation_text:
                self.current_question.explanation_text += " " + text
            else:
                self.current_question.explanation_text = text

    def _assign_image(self, block: ContentBlock):
        """Strict assignment of images based on state."""
        q = self.current_question
        path = block.content

        # Debug logging as requested
        print(f"[Q{q.question_number}] Assigning image to {self.state}")

        if self.state == ParserState.QUESTION_BODY:
            q.question_images.append(path)

        elif self.state == ParserState.OPTION:
            if self.current_option:
                self.current_option.images.append(path)
            else:
                # Fallback to question body if option object missing
                q.question_images.append(path)

        elif self.state == ParserState.ANSWER:
            q.answer_images.append(path)

        elif self.state == ParserState.EXPLANATION:
            q.explanation_images.append(path)

        else:
            logger.warning(
                f"Orphan image at page {block.page_number} in state {self.state}")

        q.page_end = max(q.page_end, block.page_number)

    def _finalize_question(self):
        """Basic validation and storage."""
        q = self.current_question

        # ── Remove ghost/empty options (no text AND no images) ──
        q.options = [
            opt for opt in q.options
            if opt.text.strip() or opt.images
        ]

        # ── Clean up boilerplate explanation text ──
        if q.explanation_text:
            cleaned = q.explanation_text.strip()
            # If explanation is just boilerplate noise, clear it
            if any(p.match(cleaned) for p in IGNORE_PATTERNS):
                q.explanation_text = ""

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
        else:
            # Automatically mark correct options based on answer_text
            # Regex to find single uppercase letters usually representing keys (A, B, C...)
            # We look for A, B or A, B, C or Answer: A etc.
            ans_keys = re.findall(r"\b([A-Z])\b", q.answer_text.upper())
            if ans_keys:
                for opt in q.options:
                    if opt.key.upper() in ans_keys:
                        opt.is_correct = True

        # Check for orphan sections (images only)
        if not q.question_text and q.question_images:
            q.anomalies.append(Anomaly(
                type=AnomalyType.ORPHAN_IMAGE,
                severity=30,
                message="Question body contains only images",
                context={"section": "question"}
            ))

        self.questions.append(q)
