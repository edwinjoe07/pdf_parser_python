import os
import sys
import json
from parser.engine import ParserEngine, ParserConfig

def scan_pdf_for_issues(pdf_path):
    config = ParserConfig(
        exam_name="Scan",
        output_dir="debug_output",
        save_raw_blocks=False,
        save_snapshots=False
    )
    engine = ParserEngine(config)
    try:
        result = engine.parse(pdf_path)
        print(f"\nResults for {os.path.basename(pdf_path)}:")
        print(f"Total Questions: {result.validation.total_questions_detected}")
        print(f"Success Rate: {result.validation.success_rate}%")
        
        issues_found = False
        for q in result.questions:
            has_issue = False
            issue_desc = []
            
            if not q.options:
                has_issue = True
                issue_desc.append("NO OPTIONS")
            elif not any(opt.is_correct for opt in q.options):
                has_issue = True
                issue_desc.append("NO OPTION SELECTED")
            
            if not q.has_explanation:
                has_issue = True
                issue_desc.append("NO EXPLANATION")
            
            if has_issue:
                issues_found = True
                print(f"  Q{q.question_number} (Page {q.page_start}): {', '.join(issue_desc)}")
                # Show first bit of text to help identify
                print(f"    Text: {q.question_text[:100]}...")
        
        if not issues_found:
            print("  No issues found in this PDF.")
            
    except Exception as e:
        print(f"Error parsing {pdf_path}: {e}")

if __name__ == "__main__":
    # Test on a few PDFs
    pdfs = [
        r"D:\CoreMinds\PDF\AWS\SAA-C03-f1hsqc.pdf",
        r"D:\CoreMinds\PDF\AWS\SOA-C02-mrhln7.pdf",
        r"D:\CoreMinds\PDF\AWS\SAA-C03-g0ldvk.pdf"
    ]
    for pdf in pdfs:
        scan_pdf_for_issues(pdf)
