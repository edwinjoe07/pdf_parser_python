"""
Validation Engine
=================
Post-parse validation and reporting.

After parsing each PDF, generates a comprehensive report:
    - Total Questions Detected
    - Structured Successfully
    - Missing Question Numbers (gaps in sequence)
    - Duplicate Question Numbers
    - Questions Missing Answer
    - Questions Missing Explanation
    - Orphan Images
    - Anomaly breakdown by type

Never silently ignores failures.
"""

from __future__ import annotations

import logging
from collections import Counter

from .models import (
    AnomalyType,
    BlockType,
    ParsedQuestion,
    ValidationReport,
)

logger = logging.getLogger(__name__)


class ValidationEngine:
    """
    Validates parsed questions and produces a comprehensive report.
    """

    def validate(
        self,
        questions: list[ParsedQuestion],
    ) -> ValidationReport:
        """
        Run full validation on parsed questions.

        Args:
            questions: List of parsed questions to validate.

        Returns:
            ValidationReport with all detected issues.
        """
        report = ValidationReport()

        if not questions:
            logger.warning("No questions to validate")
            return report

        report.total_questions_detected = len(questions)

        # Collect all question numbers
        question_numbers = [q.question_number for q in questions]
        number_counts = Counter(question_numbers)

        # Find duplicates
        report.duplicate_question_numbers = sorted([
            num for num, count in number_counts.items() if count > 1
        ])

        # Find missing numbers (gaps in sequence)
        if question_numbers:
            min_num = min(question_numbers)
            max_num = max(question_numbers)
            expected = set(range(min_num, max_num + 1))
            actual = set(question_numbers)
            report.missing_question_numbers = sorted(expected - actual)

        # Analyze each question
        structured_count = 0
        orphan_image_count = 0
        anomaly_counts: dict[str, int] = {}

        for q in questions:
            # Check if question is fully structured
            is_structured = (
                q.has_question_text
                and q.has_answer
            )

            if is_structured:
                structured_count += 1

            # Track missing answers
            if not q.has_answer:
                report.questions_missing_answer.append(q.question_number)

            # Track missing explanations
            if not q.has_explanation:
                report.questions_missing_explanation.append(q.question_number)

            # Count anomalies by type
            for anomaly in q.anomalies:
                key = anomaly.type.value
                anomaly_counts[key] = anomaly_counts.get(key, 0) + 1

                if anomaly.type == AnomalyType.ORPHAN_IMAGE:
                    orphan_image_count += 1

        report.structured_successfully = structured_count
        report.orphan_images = orphan_image_count
        report.anomaly_breakdown = anomaly_counts

        # Log summary
        logger.info("=" * 60)
        logger.info("VALIDATION REPORT")
        logger.info("=" * 60)
        logger.info(
            f"Total Questions Detected: {report.total_questions_detected}"
        )
        logger.info(
            f"Structured Successfully: {report.structured_successfully} "
            f"({report.success_rate}%)"
        )
        logger.info(
            f"Missing Question Numbers: "
            f"{len(report.missing_question_numbers)}"
        )
        logger.info(
            f"Duplicate Question Numbers: "
            f"{len(report.duplicate_question_numbers)}"
        )
        logger.info(
            f"Questions Missing Answer: "
            f"{len(report.questions_missing_answer)}"
        )
        logger.info(
            f"Questions Missing Explanation: "
            f"{len(report.questions_missing_explanation)}"
        )
        logger.info(f"Orphan Images: {report.orphan_images}")

        if report.anomaly_breakdown:
            logger.info("Anomaly Breakdown:")
            for anomaly_type, count in sorted(
                report.anomaly_breakdown.items()
            ):
                logger.info(f"  • {anomaly_type}: {count}")

        logger.info("=" * 60)

        return report
