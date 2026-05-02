
import logging
from parser.engine import ParsingEngine
from parser.models import ParseResult

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_full_parse():
    engine = ParserEngine()
    pdf = r"D:\CoreMinds\PDF\Symantec\250-556-7x9nzu.pdf"
    
    print(f"--- Running Full Parse on {pdf} ---")
    result: ParseResult = engine.parse(pdf)
    
    print(f"\n--- Results ---")
    print(f"Total Questions Detected: {result.validation.total_questions_detected}")
    print(f"Structured Successfully: {result.validation.structured_successfully}")
    
    question_nums = [q.question_number for q in result.questions]
    print(f"Question Numbers: {question_nums}")
    
    # Check Q10 specifically
    q10 = next((q for q in result.questions if q.question_number == 10), None)
    if q10:
        print(f"\nQ10 Details:")
        print(f"Text: {repr(q10.question_text[:100])}...")
        print(f"Options Count: {len(q10.options)}")
        print(f"Anomalies: {[a.type.value for a in q10.anomalies]}")
    else:
        print("\nQ10 NOT FOUND!")

if __name__ == "__main__":
    test_full_parse()
