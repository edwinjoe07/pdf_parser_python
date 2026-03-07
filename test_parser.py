"""
Test the improved parser against actual PDFs from D:\CoreMinds\PDF\.
Parses a few samples and reports question counts, integrity, and any issues.
"""
import sys, os, random, json

# Add project root to path
sys.path.insert(0, r"C:\Users\jeeva\Downloads\pdf_parser_python-main")

from parser.block_extractor import BlockExtractor
from parser.state_machine import StateMachineParser
from parser.validator import ValidationEngine
import fitz

base_dir = r"D:\CoreMinds\PDF"
out_file = r"C:\Users\jeeva\Downloads\pdf_parser_python-main\test_results.txt"

# Pick 6 diverse samples
test_cases = [
    ("AHIMA", "RHIA-qnzvxc.pdf"),         # Large (464 pages, 1828 q), no explanations
    ("CIW", "1D0-61A-9a4z5a.pdf"),         # Small (21 pages), empty explanations
    ("Fortinet", "NSE5_FAZ-6.0-21rjtd.pdf"),  # Tiny (10 pages), no explanations
    ("Scrum", "SAFE-RTE-sl8wb0.pdf"),       # Medium (120 pages), rich explanations
    ("ISC2", "Certified-in-Cybersecurity-lx8zil.pdf"),  # Footer URLs, no explanations
    ("Alibaba", "220-1201-1-g14lvs.pdf"),   # Sections, separator lines, explanations
]

def test_pdf(folder, filename):
    pdf_path = os.path.join(base_dir, folder, filename)
    if not os.path.exists(pdf_path):
        return f"FILE NOT FOUND: {pdf_path}"
    
    doc = fitz.open(pdf_path)
    total_pages = doc.page_count
    
    # Setup extractor with temp dir
    import tempfile
    img_dir = tempfile.mkdtemp()
    extractor = BlockExtractor(image_output_dir=img_dir)
    
    # Extract all blocks
    blocks = extractor.extract(pdf_path)
    
    # Parse with state machine
    parser = StateMachineParser()
    questions = parser.parse(blocks)
    
    # Validate
    validator = ValidationEngine()
    report = validator.validate(questions)
    
    doc.close()
    
    # Analyze results
    result_lines = []
    result_lines.append(f"PDF: {folder}/{filename}")
    result_lines.append(f"  Pages: {total_pages}")
    result_lines.append(f"  Blocks extracted: {len(blocks)}")
    result_lines.append(f"  Questions parsed: {len(questions)}")
    result_lines.append(f"  Success rate: {report.success_rate}%")
    result_lines.append(f"  Missing answer: {len(report.questions_missing_answer)}")
    result_lines.append(f"  Missing explanation: {len(report.questions_missing_explanation)}")
    result_lines.append(f"  Duplicates: {len(report.duplicate_question_numbers)}")
    result_lines.append(f"  Missing numbers: {len(report.missing_question_numbers)}")
    
    # Check first 3 questions for quality
    for i, q in enumerate(questions[:3]):
        result_lines.append(f"  --- Q{q.question_number} (pages {q.page_start}-{q.page_end}) ---")
        result_lines.append(f"    Text: {q.question_text[:120]}...")
        result_lines.append(f"    Options: {len(q.options)}")
        for opt in q.options:
            correct = " ✓" if opt.is_correct else ""
            result_lines.append(f"      {opt.key}. {opt.text[:80]}{correct}")
        result_lines.append(f"    Answer: {q.answer_text}")
        result_lines.append(f"    Explanation: {q.explanation_text[:100] if q.explanation_text else '(none)'}")
        result_lines.append(f"    Anomaly Score: {q.anomaly_score}")
    
    # Check any boilerplate contamination
    boilerplate_issues = []
    for q in questions:
        if "Questions and Answers PDF" in q.question_text:
            boilerplate_issues.append(f"Q{q.question_number}: Header in question text")
        if "Thank you for choosing" in q.question_text:
            boilerplate_issues.append(f"Q{q.question_number}: Cover page in question text")
        if q.explanation_text and "Questions and Answers PDF" in q.explanation_text:
            boilerplate_issues.append(f"Q{q.question_number}: Header in explanation")
        if "dumpsgate.com" in (q.question_text + q.explanation_text).lower():
            boilerplate_issues.append(f"Q{q.question_number}: dumpsgate URL in content")
        # Check options for contamination
        for opt in q.options:
            if "Questions and Answers PDF" in opt.text:
                boilerplate_issues.append(f"Q{q.question_number}, Option {opt.key}: Header in option")
    
    if boilerplate_issues:
        result_lines.append(f"  ⚠ BOILERPLATE CONTAMINATION ({len(boilerplate_issues)}):")
        for issue in boilerplate_issues[:10]:
            result_lines.append(f"    - {issue}")
    else:
        result_lines.append(f"  ✓ No boilerplate contamination detected")
    
    return "\n".join(result_lines)


if __name__ == "__main__":
    print("Testing improved parser against sample PDFs...")
    print("=" * 70)
    
    results = []
    for folder, filename in test_cases:
        print(f"\nTesting {folder}/{filename}...")
        try:
            result = test_pdf(folder, filename)
            results.append(result)
            print(result)
        except Exception as e:
            err = f"ERROR testing {folder}/{filename}: {e}"
            results.append(err)
            print(err)
            import traceback
            traceback.print_exc()
        print("-" * 70)
    
    # Write results to file
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n\n".join(results))
    
    print(f"\nResults written to {out_file}")
