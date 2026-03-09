<?php

// Ensure any quizzes whose exams are soft-deleted are marked inactive
$count = 0;
// We need to fetch all quizzes
$quizzes = \App\Models\Quiz::all();

foreach ($quizzes as $quiz) {
    // If the exam doesn't exist (hard deleted) or is soft-deleted
    $exam = \App\Models\Exam::withTrashed()->find($quiz->exam_id);
    if (!$exam || $exam->trashed()) {
        if ($quiz->is_active) {
            $quiz->update(['is_active' => false]);
            $count++;
            echo "Deactivated Quiz ID: {$quiz->id} for deleted Exam ID: {$quiz->exam_id}\n";
        }
    }
}

echo "Successfully deactivated {$count} orphaned quizzes.\n";
