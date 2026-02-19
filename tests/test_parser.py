"""
Test Suite for PDF Parser Engine
=================================
Unit and integration tests for all parser components.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from parser.models import (
    Anomaly,
    AnomalyType,
    BlockType,
    ContentBlock,
    ExamMetadata,
    FontInfo,
    ParsedQuestion,
    ParseResult,
    ParseVersion,
    Section,
    ValidationReport,
)
from parser.state_machine import (
    ANSWER_INLINE_PATTERN,
    ANSWER_PATTERN,
    EXPLANATION_INLINE_PATTERN,
    EXPLANATION_PATTERN,
    QUESTION_PATTERN,
    ParserState,
    StateMachineParser,
)
from parser.validator import ValidationEngine


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestContentBlock:
    """Test ContentBlock model."""

    def test_text_block_creation(self):
        block = ContentBlock(
            type=BlockType.TEXT,
            content="What is AWS Lambda?",
            page_number=1,
            bbox=(10.0, 20.0, 300.0, 40.0),
            order_index=0,
        )
        assert block.type == BlockType.TEXT
        assert block.content == "What is AWS Lambda?"
        assert block.page_number == 1
        assert block.bbox == (10.0, 20.0, 300.0, 40.0)

    def test_image_block_creation(self):
        block = ContentBlock(
            type=BlockType.IMAGE,
            content="images/page1_img1.png",
            page_number=1,
            bbox=(10.0, 100.0, 400.0, 300.0),
            order_index=1,
        )
        assert block.type == BlockType.IMAGE
        assert "png" in block.content

    def test_block_serialization(self):
        block = ContentBlock(
            type=BlockType.TEXT,
            content="Test",
            page_number=1,
            bbox=(0.0, 0.0, 100.0, 50.0),
            order_index=0,
        )
        data = block.model_dump()
        assert data["type"] == "text"
        assert data["page_number"] == 1

    def test_font_info(self):
        font = FontInfo(
            name="Arial",
            size=12.0,
            flags=20,
            is_bold=True,
            is_italic=False,
        )
        block = ContentBlock(
            type=BlockType.TEXT,
            content="Bold text",
            page_number=1,
            bbox=(0.0, 0.0, 100.0, 20.0),
            order_index=0,
            font_info=font,
        )
        assert block.font_info.is_bold is True
        assert block.font_info.name == "Arial"


class TestParsedQuestion:
    """Test ParsedQuestion model."""

    def test_empty_question(self):
        q = ParsedQuestion(
            question_number=1,
            page_start=1,
            page_end=1,
        )
        assert q.question_number == 1
        assert q.anomaly_score == 0
        assert q.has_question_text is False
        assert q.has_answer is False

    def test_complete_question(self):
        q = ParsedQuestion(
            question_number=1,
            page_start=1,
            page_end=2,
            blocks={
                "question": [
                    ContentBlock(
                        type=BlockType.TEXT,
                        content="What is EC2?",
                        page_number=1,
                        bbox=(0, 0, 100, 20),
                        order_index=0,
                    ),
                ],
                "answer": [
                    ContentBlock(
                        type=BlockType.TEXT,
                        content="B",
                        page_number=1,
                        bbox=(0, 40, 100, 60),
                        order_index=2,
                    ),
                ],
                "explanation": [
                    ContentBlock(
                        type=BlockType.TEXT,
                        content="EC2 is a compute service",
                        page_number=2,
                        bbox=(0, 0, 100, 20),
                        order_index=3,
                    ),
                ],
            },
        )
        assert q.has_question_text is True
        assert q.has_answer is True
        assert q.has_explanation is True

    def test_anomaly_score(self):
        q = ParsedQuestion(
            question_number=1,
            page_start=1,
            page_end=1,
            anomalies=[
                Anomaly(
                    type=AnomalyType.MISSING_ANSWER,
                    severity=60,
                    message="No answer",
                ),
                Anomaly(
                    type=AnomalyType.MISSING_EXPLANATION,
                    severity=20,
                    message="No explanation",
                ),
            ],
        )
        assert q.anomaly_score == 80

    def test_anomaly_score_caps_at_100(self):
        q = ParsedQuestion(
            question_number=1,
            page_start=1,
            page_end=1,
            anomalies=[
                Anomaly(
                    type=AnomalyType.MISSING_ANSWER,
                    severity=60,
                    message="",
                ),
                Anomaly(
                    type=AnomalyType.MISSING_QUESTION_TEXT,
                    severity=80,
                    message="",
                ),
            ],
        )
        assert q.anomaly_score == 100

    def test_image_count(self):
        q = ParsedQuestion(
            question_number=1,
            page_start=1,
            page_end=1,
            blocks={
                "question": [
                    ContentBlock(
                        type=BlockType.IMAGE,
                        content="img1.png",
                        page_number=1,
                        bbox=(0, 0, 100, 100),
                        order_index=0,
                    ),
                ],
                "answer": [
                    ContentBlock(
                        type=BlockType.IMAGE,
                        content="img2.png",
                        page_number=1,
                        bbox=(0, 100, 100, 200),
                        order_index=1,
                    ),
                ],
                "explanation": [],
            },
        )
        assert q.image_count == 2


class TestValidationReport:
    """Test ValidationReport model."""

    def test_success_rate(self):
        report = ValidationReport(
            total_questions_detected=100,
            structured_successfully=95,
        )
        assert report.success_rate == 95.0

    def test_empty_report(self):
        report = ValidationReport()
        assert report.success_rate == 0.0
        assert report.total_questions_detected == 0


# ═══════════════════════════════════════════════════════════════════════════════
# REGEX PATTERN TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestAnchorPatterns:
    """Test regex patterns for structural anchors."""

    def test_question_patterns(self):
        # Should match
        assert QUESTION_PATTERN.match("Question: 1")
        assert QUESTION_PATTERN.match("Question:1")
        assert QUESTION_PATTERN.match("Question 1")
        assert QUESTION_PATTERN.match("question: 42")
        assert QUESTION_PATTERN.match("QUESTION: 100")
        assert QUESTION_PATTERN.match("  Question: 5  ")

        # Should NOT match
        assert not QUESTION_PATTERN.match("Question: What is AWS?")
        assert not QUESTION_PATTERN.match("The question is about AWS")
        assert not QUESTION_PATTERN.match("Question:")

    def test_answer_patterns(self):
        # Standalone anchor
        assert ANSWER_PATTERN.match("Answer:")
        assert ANSWER_PATTERN.match("Answer")
        assert ANSWER_PATTERN.match("answer:")
        assert ANSWER_PATTERN.match("ANSWER:")
        assert ANSWER_PATTERN.match("  Answer:  ")

        # With inline content
        assert ANSWER_INLINE_PATTERN.match("Answer: B")
        assert ANSWER_INLINE_PATTERN.match("Answer: The correct answer is B")
        assert ANSWER_INLINE_PATTERN.match("answer: A, C")

    def test_explanation_patterns(self):
        # Standalone anchor
        assert EXPLANATION_PATTERN.match("Explanation:")
        assert EXPLANATION_PATTERN.match("Explanation")
        assert EXPLANATION_PATTERN.match("explanation:")
        assert EXPLANATION_PATTERN.match("  Explanation:  ")

        # With inline content
        assert EXPLANATION_INLINE_PATTERN.match(
            "Explanation: S3 is an object storage service"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# STATE MACHINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestStateMachineParser:
    """Test the state machine parser."""

    def _make_text_block(
        self, content: str, page: int = 1, order: int = 0
    ) -> ContentBlock:
        return ContentBlock(
            type=BlockType.TEXT,
            content=content,
            page_number=page,
            bbox=(0, 0, 100, 20),
            order_index=order,
        )

    def _make_image_block(
        self, page: int = 1, order: int = 0
    ) -> ContentBlock:
        return ContentBlock(
            type=BlockType.IMAGE,
            content="test_image.png",
            page_number=page,
            bbox=(0, 0, 100, 100),
            order_index=order,
        )

    def test_single_complete_question(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("What is AWS Lambda?", order=1),
            self._make_text_block("Answer: B", order=2),
            self._make_text_block("Explanation: Lambda is serverless", order=3),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        q = questions[0]
        assert q.question_number == 1
        assert q.has_question_text
        assert q.has_answer
        assert q.has_explanation

    def test_multiple_questions(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("What is EC2?", order=1),
            self._make_text_block("Answer: A", order=2),
            self._make_text_block("Explanation: EC2 is compute", order=3),
            self._make_text_block("Question: 2", order=4),
            self._make_text_block("What is S3?", order=5),
            self._make_text_block("Answer: C", order=6),
            self._make_text_block("Explanation: S3 is storage", order=7),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 2
        assert questions[0].question_number == 1
        assert questions[1].question_number == 2

    def test_question_without_answer(self):
        blocks = [
            self._make_text_block("Question: 1"),
            self._make_text_block("What is VPC?"),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        assert not questions[0].has_answer
        assert any(
            a.type == AnomalyType.MISSING_ANSWER
            for a in questions[0].anomalies
        )

    def test_multi_page_question(self):
        blocks = [
            self._make_text_block("Question: 1", page=3, order=0),
            self._make_text_block("Long question text...", page=3, order=1),
            self._make_text_block("...continued", page=4, order=2),
            self._make_text_block("Answer: A", page=4, order=3),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        assert questions[0].page_start == 3
        assert questions[0].page_end == 4

    def test_image_attachment_to_question(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("Look at this diagram:", order=1),
            self._make_image_block(order=2),
            self._make_text_block("Answer: B", order=3),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        q = questions[0]
        # Image should be in question section
        images = [
            b for b in q.blocks["question"]
            if b.type == BlockType.IMAGE
        ]
        assert len(images) == 1

    def test_image_attachment_to_answer(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("What is this?", order=1),
            self._make_text_block("Answer:", order=2),
            self._make_text_block("The answer is B", order=3),
            self._make_image_block(order=4),
            self._make_text_block("Explanation:", order=5),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        images = [
            b for b in questions[0].blocks["answer"]
            if b.type == BlockType.IMAGE
        ]
        assert len(images) == 1

    def test_image_attachment_to_explanation(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("What?", order=1),
            self._make_text_block("Answer: A", order=2),
            self._make_text_block("Explanation:", order=3),
            self._make_text_block("See diagram below:", order=4),
            self._make_image_block(order=5),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        images = [
            b for b in questions[0].blocks["explanation"]
            if b.type == BlockType.IMAGE
        ]
        assert len(images) == 1

    def test_images_never_cross_question_boundaries(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("Q1 text", order=1),
            self._make_text_block("Answer: A", order=2),
            self._make_text_block("Explanation: E1", order=3),
            self._make_image_block(order=4),  # Should stay in Q1
            self._make_text_block("Question: 2", order=5),
            self._make_text_block("Q2 text", order=6),
            self._make_text_block("Answer: B", order=7),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 2
        # Image should be in Q1's explanation, not Q2
        q1_images = sum(
            1
            for section in questions[0].blocks.values()
            for b in section
            if b.type == BlockType.IMAGE
        )
        q2_images = sum(
            1
            for section in questions[1].blocks.values()
            for b in section
            if b.type == BlockType.IMAGE
        )
        assert q1_images == 1
        assert q2_images == 0

    def test_duplicate_question_numbers(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("First Q1", order=1),
            self._make_text_block("Answer: A", order=2),
            self._make_text_block("Question: 1", order=3),
            self._make_text_block("Second Q1", order=4),
            self._make_text_block("Answer: B", order=5),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 2
        # Second Q1 should have duplicate anomaly
        assert any(
            a.type == AnomalyType.DUPLICATE_QUESTION_NUMBER
            for a in questions[1].anomalies
        )

    def test_case_insensitive_anchors(self):
        blocks = [
            self._make_text_block("QUESTION: 1", order=0),
            self._make_text_block("Test question", order=1),
            self._make_text_block("ANSWER: A", order=2),
            self._make_text_block("EXPLANATION: Test", order=3),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        assert questions[0].has_question_text
        assert questions[0].has_answer
        assert questions[0].has_explanation

    def test_optional_colon_in_anchors(self):
        blocks = [
            self._make_text_block("Question 1", order=0),
            self._make_text_block("Test", order=1),
            self._make_text_block("Answer A", order=2),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        # "Answer A" won't match because it has content after "Answer"
        # but no colon. The inline pattern should match "Answer A" where "A" is inline.
        assert len(questions) == 1

    def test_inline_answer_content(self):
        blocks = [
            self._make_text_block("Question: 1", order=0),
            self._make_text_block("What is it?", order=1),
            self._make_text_block("Answer: B, C", order=2),
            self._make_text_block("Explanation: Because reasons", order=3),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        q = questions[0]
        # Answer section should contain "B, C"
        answer_texts = [
            b.content for b in q.blocks["answer"]
            if b.type == BlockType.TEXT
        ]
        assert any("B, C" in t for t in answer_texts)

    def test_content_before_first_question_ignored(self):
        blocks = [
            self._make_text_block("Some header text", order=0),
            self._make_text_block("Table of contents", order=1),
            self._make_text_block("Question: 1", order=2),
            self._make_text_block("Actual question", order=3),
            self._make_text_block("Answer: A", order=4),
        ]

        parser = StateMachineParser()
        questions = parser.parse(blocks)

        assert len(questions) == 1
        # Question text should not include header/TOC
        q_texts = [
            b.content for b in questions[0].blocks["question"]
            if b.type == BlockType.TEXT
        ]
        assert "Some header text" not in q_texts
        assert "Table of contents" not in q_texts


# ═══════════════════════════════════════════════════════════════════════════════
# VALIDATION ENGINE TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestValidationEngine:
    """Test the validation engine."""

    def test_empty_questions(self):
        validator = ValidationEngine()
        report = validator.validate([])
        assert report.total_questions_detected == 0
        assert report.success_rate == 0.0

    def test_perfect_parse(self):
        questions = [
            ParsedQuestion(
                question_number=i,
                page_start=i,
                page_end=i,
                blocks={
                    "question": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content=f"Q{i}",
                            page_number=i,
                            bbox=(0, 0, 100, 20),
                            order_index=0,
                        ),
                    ],
                    "answer": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content="A",
                            page_number=i,
                            bbox=(0, 20, 100, 40),
                            order_index=1,
                        ),
                    ],
                    "explanation": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content="E",
                            page_number=i,
                            bbox=(0, 40, 100, 60),
                            order_index=2,
                        ),
                    ],
                },
            )
            for i in range(1, 11)
        ]

        validator = ValidationEngine()
        report = validator.validate(questions)

        assert report.total_questions_detected == 10
        assert report.structured_successfully == 10
        assert report.success_rate == 100.0
        assert len(report.missing_question_numbers) == 0
        assert len(report.duplicate_question_numbers) == 0

    def test_gap_detection(self):
        questions = [
            ParsedQuestion(
                question_number=n,
                page_start=1,
                page_end=1,
                blocks={
                    "question": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content="Q",
                            page_number=1,
                            bbox=(0, 0, 100, 20),
                            order_index=0,
                        ),
                    ],
                    "answer": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content="A",
                            page_number=1,
                            bbox=(0, 20, 100, 40),
                            order_index=1,
                        ),
                    ],
                    "explanation": [],
                },
            )
            for n in [1, 2, 5, 6, 10]
        ]

        validator = ValidationEngine()
        report = validator.validate(questions)

        # Missing: 3, 4, 7, 8, 9
        assert set(report.missing_question_numbers) == {3, 4, 7, 8, 9}

    def test_duplicate_detection(self):
        questions = [
            ParsedQuestion(
                question_number=n,
                page_start=1,
                page_end=1,
                blocks={
                    "question": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content="Q",
                            page_number=1,
                            bbox=(0, 0, 100, 20),
                            order_index=0,
                        ),
                    ],
                    "answer": [
                        ContentBlock(
                            type=BlockType.TEXT,
                            content="A",
                            page_number=1,
                            bbox=(0, 20, 100, 40),
                            order_index=1,
                        ),
                    ],
                    "explanation": [],
                },
            )
            for n in [1, 2, 2, 3, 3, 3]
        ]

        validator = ValidationEngine()
        report = validator.validate(questions)

        assert 2 in report.duplicate_question_numbers
        assert 3 in report.duplicate_question_numbers


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════════════


class TestParseResultSerialization:
    """Test that ParseResult serializes correctly for Laravel."""

    def test_full_result_json(self):
        result = ParseResult(
            exam=ExamMetadata(
                name="AWS SAA-C03",
                provider="AWS",
                version="2024",
                source_pdf="saa-c03.pdf",
                total_pages=100,
            ),
            parse_version=ParseVersion(
                parser_version="1.0.0",
                raw_block_count=500,
                structured_question_count=50,
            ),
            questions=[
                ParsedQuestion(
                    question_number=1,
                    page_start=1,
                    page_end=1,
                    blocks={
                        "question": [
                            ContentBlock(
                                type=BlockType.TEXT,
                                content="What is EC2?",
                                page_number=1,
                                bbox=(0, 0, 100, 20),
                                order_index=0,
                            ),
                        ],
                        "answer": [
                            ContentBlock(
                                type=BlockType.TEXT,
                                content="B",
                                page_number=1,
                                bbox=(0, 20, 100, 40),
                                order_index=1,
                            ),
                        ],
                        "explanation": [
                            ContentBlock(
                                type=BlockType.TEXT,
                                content="Compute service",
                                page_number=1,
                                bbox=(0, 40, 100, 60),
                                order_index=2,
                            ),
                        ],
                    },
                ),
            ],
            validation=ValidationReport(
                total_questions_detected=1,
                structured_successfully=1,
            ),
        )

        # Serialize to JSON
        data = result.model_dump()
        json_str = json.dumps(data, default=str, ensure_ascii=False)

        # Deserialize back
        parsed = json.loads(json_str)

        assert parsed["exam"]["name"] == "AWS SAA-C03"
        assert parsed["exam"]["provider"] == "AWS"
        assert len(parsed["questions"]) == 1
        assert parsed["questions"][0]["question_number"] == 1
        assert parsed["questions"][0]["blocks"]["question"][0]["content"] == "What is EC2?"
        assert parsed["validation"]["total_questions_detected"] == 1
        assert parsed["validation"]["success_rate"] == 100.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
