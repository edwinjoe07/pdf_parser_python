"""
State Machine Parser
====================
Deterministic state machine for detecting certification exam question structure
based on text-anchors (Question, Answer, Explanation).

Optimized for the Dumpsgate / exam-dump PDF format:
    - Cover page (page 1): title, boilerplate, exam code, question count
    - Header on every page: "Questions and Answers PDF\n{page}/{total}"
    - Footer URLs: e.g. https://dumpsgate.com/...
    - Question structure:
        Question: N
        <question text>
        A. <option>
        B. <option>
        ...
        Answer: <letter(s)>
        Explanation: / Reference: / Solution: / Rationale:
        <explanation text>
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
    QuestionType,
    Section,
)

logger = logging.getLogger(__name__)

# ─── Anchor Patterns ──────────────────────────────────────────────────────────

# Matches "Question: 1", "Question 42", "Q: 1", "item 1"
QUESTION_PATTERN = re.compile(
    r"^\s*(?:Question|Q|item)\b\s*:?\s*(\d+)", re.IGNORECASE
)

# Matches "A.", "B.", "A)", "(A)", "A:", "A -" style options
OPTION_PATTERN = re.compile(
    r"^\s*\(?([A-Z])\s*[\.\):\-\u2013\u2014]\s*", re.IGNORECASE
)

# Matches "Answer: A", "Ans: A", "Key: A", "Correct Answer: A"
# Must have a colon OR be followed directly by a single letter A-E (using lookahead)
ANSWER_PATTERN = re.compile(
    r"^\s*(?:Correct\s+)?(?:Answer|Ans|Key|Correct\s+Key)\s*(?::\s*|(?=\s+[A-E]\b))", re.IGNORECASE
)

# Matches "Explanation:", "Reference:", "Rationale:", "Solution:", "References:"
# MUST have a colon to avoid false positives in body text.
EXPLANATION_PATTERN = re.compile(
    r"^\s*(?:Explanation|Rationale|Solution|Reference|References)\s*:\s*", re.IGNORECASE
)

# Matches standalone "HOTSPOT" line (case-insensitive)
HOTSPOT_PATTERN = re.compile(r"^\s*HOTSPOT\s*$", re.IGNORECASE)

# ─── Noise / Boilerplate Patterns ─────────────────────────────────────────────
# These lines are ALWAYS ignored, no matter what parser state we are in.
IGNORE_PATTERNS = [
    # ── Dumpsgate PDF header/footer ──
    re.compile(r"^\s*Questions and Answers PDF.*$",
               re.IGNORECASE),
    # Page counters: "8/528", "Page 8 of 528", "2/10", "110/218"
    re.compile(r"^\s*(Page\s*)?\d+\s*(/|of)\s*\d+\s*$",
               re.IGNORECASE),

    # ── Cover page boilerplate ──
    # "Thank you for choosing us for your <EXAM> preparation!"
    re.compile(r"^\s*Thank\s+you\s+for\s+(choosing|your)\b", re.IGNORECASE),
    # "We're confident these materials will help you succeed."
    re.compile(r"^\s*We.re\s+confident\s+these\s+materials\b", re.IGNORECASE),
    # "Best of luck with your studies!"
    re.compile(r"^\s*Best\s+of\s+luck\s+with\s+your\s+studies", re.IGNORECASE),
    # Lines that are just the exam code or question count (standalone short alphanumeric)
    # e.g. "RHIA", "1828", "SAFe-RTE", "286", "CTP"
    # Only match these on the cover page — handled separately in _is_cover_page_noise

    # ── Section headers / topic markers ──
    re.compile(r"^\s*Topic\s+\d+[\s,]", re.IGNORECASE),
    re.compile(r"^\s*Product\s+Questions\s*:\s*\d+\s*$", re.IGNORECASE),

    # ── Separator lines ──
    re.compile(r"^\s*[=\-]{4,}\s*$"),  # "============" or "------------"

    # ── Lone URLs ──
    re.compile(r"^\s*https?://[^\s]+\s*$"),

    # ── Dumpsgate boilerplate text ──
    re.compile(r"^\s*Thank\s+you\s+for\s+your\s+visit\.?\s*$", re.IGNORECASE),
    re.compile(r"^\s*Visit\s+us\s+at\b", re.IGNORECASE),
    re.compile(r"^\s*For\s+more\s+questions\b", re.IGNORECASE),
    re.compile(r"^\s*Get\s+certified\b", re.IGNORECASE),
    re.compile(r"^\s*Download\s+free\b", re.IGNORECASE),
    re.compile(r"examtopics?\.(com|org|net)", re.IGNORECASE),
    re.compile(r"certification.s*prep", re.IGNORECASE),
    re.compile(r"dumpsgate\.com", re.IGNORECASE),

    # ── Common Exam Dump Headers ──
    # e.g. "CY0-001: Actual Exam Q&A |", "MS-900: Actual Exam Q&A", "Latest Exam Questions"
    re.compile(r"^\s*[A-Z0-9\-]+\s*:\s*Actual\s+Exam\s+Q&A\s*\|?.*$", re.IGNORECASE),
    re.compile(r"^\s*Actual\s+Exam\s+Questions\s*&\s*Answers\s*$", re.IGNORECASE),
    re.compile(r"^\s*Practice\s+Questions\s*&\s*Answers\s*$", re.IGNORECASE),
    re.compile(r"^\s*Guaranteed\s+Success\b", re.IGNORECASE),
    re.compile(r"^\s*Pass\s+Exam\s+with\s+High\s+Score\s*$", re.IGNORECASE),
    # ── Exam Instructions ──
    re.compile(r"^\s*\(?Choose\s+(the\s+)?(best|all\s+that\s+apply)\s+answer\.?\)?\s*$", re.IGNORECASE),
]

# Cover page noise: standalone lines that are just a number or short exam code
# These are only checked on pages where no question has been detected yet
COVER_PAGE_NOISE = re.compile(
    r"^\s*(?:\d{1,5}|[A-Z][A-Za-z0-9\-_\.]{0,30})\s*$"
)

# Pattern to detect standalone "Question N" without content (just a page-end artifact)
SOLO_QUESTION_NUM = re.compile(r"^\s*Question\s*\d+\s*$", re.IGNORECASE)


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
        self._cover_page_done = False  # Track if we've moved past page 1

    def reset(self):
        """Reset the state machine for a fresh parsing run."""
        self.state = ParserState.SEEKING_QUESTION
        self.current_question = None
        self.current_option = None
        self.questions = []
        self.question_numbers = set()
        self._cover_page_done = False

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
        self._cover_page_done = False

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

            # Check noise patterns (Headers/Footers/Boilerplate)
            if self._is_noise(line_str, block.page_number):
                continue

            # Anchor Detection (Case-Insensitive)

            # Question Anchor (e.g. "Question: 13")
            q_match = QUESTION_PATTERN.match(line_str)
            if q_match:
                # Check if this is just a standalone "Question N" at page end
                # (these appear as artifacts and should be ignored)
                if SOLO_QUESTION_NUM.match(line_str):
                    # Only ignore if it's a solo "Question N" with no colon
                    # "Question: N" with colon IS a real anchor
                    if ":" not in line_str:
                        continue

                q_num = int(q_match.group(1))
                self._start_new_question(q_num, block)
                remainder = line_str[q_match.end():].strip()
                if remainder:
                    self._append_text(remainder)
                continue

            # HOTSPOT marker — standalone line right after question anchor
            if self.current_question and self.state == ParserState.QUESTION_BODY:
                if HOTSPOT_PATTERN.match(line_str):
                    self.current_question.question_type = QuestionType.HOTSPOT
                    logger.info(f"Question {self.current_question.question_number} marked as HOTSPOT")
                    continue

            if not self.current_question:
                # Before any question is detected, skip everything
                # (covers page 1 boilerplate, topic headers, etc.)
                continue

            # Option Anchor (e.g. "A.")
            # Special handling: if multiple options are in one line, we need to split them.
            # e.g. "A. Option A B. Option B"
            # Option Anchor (e.g. "A.", "1.", "(A)")
            # Support letters A-Z or numbers 1-9
            # Allow dots, parens, colons, dashes, tabs, or at least 2 consecutive spaces as a separator
            # Negative lookahead (?!\d) protects IP addresses like "10.0.1.1" by ensuring a dot after a number isn't followed by another number.
            opt_matches = list(re.finditer(r"(?:^|(?<=\s))\(?([A-Z]|\d{1,2})\b\s*(?:(?:[\.\-\u2013\u2014](?!\d))|[\):\t]|\s{2,})\s*", line_str))
            
            # If we find options, we process them.
            if opt_matches and self.state in [ParserState.QUESTION_BODY, ParserState.OPTION, ParserState.ANSWER]:
                # If the first match doesn't start at 0, the text before it belongs to the previous state
                if opt_matches[0].start() > 0:
                    prefix = line_str[:opt_matches[0].start()].strip()
                    if prefix:
                        self._append_text(prefix)
                
                # Process each segment
                for i, match in enumerate(opt_matches):
                    key = match.group(1).upper()
                    self._start_new_option(key, block.bbox)
                    
                    # End of current option is the start of next match or end of line
                    end_pos = opt_matches[i+1].start() if i + 1 < len(opt_matches) else len(line_str)
                    remainder = line_str[match.end():end_pos].strip()
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

            # Explanation Anchor (e.g. "Explanation:", "Reference:", "Solution:")
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

    def _is_noise(self, line: str, page_number: int) -> bool:
        """Check if a line is noise (headers, footers, boilerplate)."""
        # Standard noise patterns
        if any(p.match(line) for p in IGNORE_PATTERNS):
            return True

        # On the cover page (page 1, before any question detected),
        # also filter out standalone exam codes and numbers
        if not self._cover_page_done and not self.current_question:
            if COVER_PAGE_NOISE.match(line):
                return True

        return False

    def _start_new_question(self, q_num: int, block: ContentBlock):
        """Finalize previous and start fresh state."""
        if self.current_question:
            self._finalize_question()

        # We've definitely moved past the cover page
        self._cover_page_done = True

        logger.info(f"Detected Question {q_num} on page {block.page_number}")

        self.current_question = ParsedQuestion(
            question_number=q_num,
            page_start=block.page_number,
            page_end=block.page_number,
        )
        self.current_option = None
        self.state = ParserState.QUESTION_BODY
        self.question_numbers.add(q_num)

    def _start_new_option(self, key: str, bbox: Optional[tuple[float, float, float, float]] = None):
        """Switch to OPTION state and create structure."""
        self.state = ParserState.OPTION
        self.current_option = QuestionOption(key=key, bbox=bbox)
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
        """Strict assignment of images based on state and spatial proximity."""
        q = self.current_question
        path = block.content
        img_bbox = block.bbox

        if self.state == ParserState.QUESTION_BODY:
            q.question_images.append(path)

        elif self.state == ParserState.OPTION:
            # Spatial Assignment Logic:
            # If multiple options exist on the current page, find the one with the best X-overlap
            # or the one directly above this image.
            
            target_option = self.current_option
            
            # If there are multiple options, check if another option on the same row is a better fit
            if len(q.options) > 1:
                # Find options on the same page as the image
                # (We assume options are already added to q.options in order)
                
                # Check for X-overlap if the image is below some options
                best_opt = None
                max_overlap = -1.0
                
                for opt in q.options:
                    if not opt.bbox:
                        continue
                    
                    # Check horizontal overlap (X-axis)
                    overlap = min(img_bbox[2], opt.bbox[2]) - max(img_bbox[0], opt.bbox[0])
                    if overlap > max_overlap:
                        max_overlap = overlap
                        best_opt = opt
                
                if best_opt and max_overlap > 0:
                    target_option = best_opt
                    logger.debug(f"[Q{q.question_number}] Spatially assigned image to Option {target_option.key}")

            if target_option:
                target_option.images.append(path)
            else:
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
        """Basic validation, answer marking, and storage."""
        q = self.current_question
        is_hotspot = q.question_type == QuestionType.HOTSPOT

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

        # HOTSPOT questions are expected to have no selectable options
        # and their "answer" is typically an image — so skip answer
        # anomaly flagging for HOTSPOT questions
        if not is_hotspot:
            if not q.has_answer:
                q.anomalies.append(Anomaly(
                    type=AnomalyType.MISSING_ANSWER,
                    severity=60,
                    message="Question has no answer section"
                ))
            else:
                # ── Mark correct options from answer text ──
                self._mark_correct_options(q)

        # Check for orphan sections (images only)
        if not q.question_text and q.question_images:
            q.anomalies.append(Anomaly(
                type=AnomalyType.ORPHAN_IMAGE,
                severity=30,
                message="Question body contains only images",
                context={"section": "question"}
            ))

        self.questions.append(q)

    def _mark_correct_options(self, q: ParsedQuestion):
        """
        Parse the answer_text to find correct option keys and mark them.

        Handles multiple formats:
            - "B"           → single answer
            - "C, D"        → comma-separated
            - "AB"          → concatenated (no space)
            - "A,B"         → comma-separated (no space)
            - "A, C"        → comma-separated with space
            - "AD"          → concatenated
        """
        answer = q.answer_text.strip().upper()
        if not answer:
            return

        # Get the set of valid option keys for this question
        valid_keys = {opt.key.upper() for opt in q.options}

        # Strategy 1: Find standalone uppercase letters
        # e.g., "B" → ["B"], "C, D" → ["C", "D"], "A,B" → ["A", "B"]
        # We use word boundaries \b to avoid picking up letters from inside words 
        # like "THE" or "CORRECT"
        ans_keys = set(re.findall(r"\b([A-Z])\b", answer))
        
        # Strategy 2: If no standalone letters found, try finding any uppercase letters
        # but only if the string is very short (likely just the keys)
        if not ans_keys and len(answer) <= 5:
            ans_keys.update(re.findall(r"([A-Z])", answer))

        # Only mark keys that actually exist as options
        final_keys = ans_keys & valid_keys if valid_keys else ans_keys

        for opt in q.options:
            if opt.key.upper() in final_keys:
                opt.is_correct = True
