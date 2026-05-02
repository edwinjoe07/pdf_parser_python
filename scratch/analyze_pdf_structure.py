import fitz
import json
import os
from parser.block_extractor import BlockExtractor
from parser.state_machine import StateMachineParser, ParserState

def analyze_pdf_structure(pdf_path, pages=None):
    extractor = BlockExtractor(image_output_dir="debug_images")
    parser = StateMachineParser()
    
    with fitz.open(pdf_path) as doc:
        total_pages = doc.page_count
        if pages is None:
            pages = range(1, min(total_pages + 1, 20)) # First 20 pages
        
        for page_num in pages:
            print(f"\n--- Page {page_num} ---")
            blocks = extractor.extract_from_page(doc, page_num)
            
            # Show blocks with bbox and type
            for b in blocks:
                content_preview = b.content.replace('\n', '\\n')[:50]
                print(f"[{b.type}] bbox={b.bbox} order={b.order_index} content: {content_preview}")
            
            # Run state machine on these blocks
            questions = parser.parse(blocks)
            for q in questions:
                print(f"\nParsed Question {q.question_number}:")
                print(f"  Options: {[opt.key for opt in q.options]}")
                print(f"  Correct: {[opt.key for opt in q.options if opt.is_correct]}")
                print(f"  Images in Question: {len(q.question_images)}")
                for i, opt in enumerate(q.options):
                    print(f"  Option {opt.key} images: {len(opt.images)}")
                print(f"  Explanation Text length: {len(q.explanation_text)}")
                print(f"  Explanation Images: {len(q.explanation_images)}")

if __name__ == "__main__":
    pdf = r"D:\CoreMinds\PDF\AWS\SAA-C03-f1hsqc.pdf"
    # Scan a wider range of pages to find problematic ones
    analyze_pdf_structure(pdf, pages=range(1, 100))
