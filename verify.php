<?php

// Find all exams
$exams = \App\Models\Exam::withTrashed()->get();
echo "Total Exams (including trashed): " . $exams->count() . "\n";

foreach ($exams as $exam) {
    $code = $exam->exam_code ?? '';
    $title = $exam->title ?? '';
    if (stripos($code, '70-301') !== false || stripos($title, '70-301') !== false) {
        echo "Found Exam ID: {$exam->id}, Code: {$code}, Title: {$title}, Trashed: " . ($exam->trashed() ? 'Yes' : 'No') . "\n";
    }
}

// Find all quizzes
$quizzes = \App\Models\Quiz::all();
foreach ($quizzes as $quiz) {
    if (stripos($quiz->title ?? '', '70-301') !== false) {
        $exam = $quiz->exam()->withTrashed()->first();
        echo "Found Quiz ID: {$quiz->id}, Title: {$quiz->title}, Exam ID: {$quiz->exam_id}, Exam Exists: " . ($exam ? ($exam->trashed() ? 'Soft Deleted' : 'Yes') : 'No') . "\n";
    }
}

// Check newly parsed PDFs free tier
$latestQuiz = \App\Models\Quiz::orderBy('id', 'desc')->first();
if ($latestQuiz) {
    echo "Latest Quiz ID: {$latestQuiz->id}, Title: {$latestQuiz->title}\n";
    $freeCount = $latestQuiz->questions()->where('access_level', 'free')->count();
    $premiumCount = $latestQuiz->questions()->where('access_level', 'premium')->count();
    echo "Free questions: {$freeCount}, Premium questions: {$premiumCount}\n";
} else {
    echo "No quizzes found.\n";
}
