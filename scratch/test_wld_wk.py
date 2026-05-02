
import logging
from parser.engine import ParserEngine
from parser.models import ParseResult

# Configure logging
logging.basicConfig(level=logging.INFO)

def debug_wld_wk_pdf():
    engine = ParserEngine()
    pdf = r"D:\CoreMinds\PDF\Wld wk\GR1-scprnz.pdf"
    
    print(f"--- Running Parse on {pdf} ---")
    result: ParseResult = engine.parse(pdf)
    
    print(f"\n--- Results for First 3 Questions ---")
    for q in result.questions[:3]:
        print(f"\nQ{q.question_number}:")
        print(f"Options: {[o.key for o in q.options]}")
        print(f"Correct Option(s): {[o.key for o in q.options if o.is_correct]}")
        print(f"Raw Answer Text Extracted: {repr(q.answer_text)}")
        print(f"Raw Explanation Text Extracted: {repr(q.explanation_text)}")

if __name__ == "__main__":
    debug_wld_wk_pdf()
