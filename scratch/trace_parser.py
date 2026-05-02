
import logging
import fitz
from parser.state_machine import StateMachineParser
from parser.block_extractor import BlockExtractor
from parser.models import BlockType

# Trace processing
def trace_parsing(pdf_path, page_num):
    doc = fitz.open(pdf_path)
    extractor = BlockExtractor(image_output_dir="temp_imgs")
    blocks = extractor.extract_from_page(doc, page_num)
    
    parser = StateMachineParser()
    
    print(f"--- Tracing Parser for Page {page_num} ---")
    for b in blocks:
        if b.type == BlockType.TEXT:
            print(f"\n[BLOCK] Bbox: {[round(x,1) for x in b.bbox]} | Content: {repr(b.content)}")
            prev_state = parser.state
            parser._process_block(b)
            print(f"  State Change: {prev_state} -> {parser.state}")
            
            if parser.current_question:
                print(f"  Current Q: {parser.current_question.question_number}")
                print(f"  Current Text: {repr(parser.current_question.question_text[:100])}...")
                print(f"  Current Options: {[o.key for o in parser.current_question.options]}")

if __name__ == "__main__":
    pdf = r"D:\CoreMinds\PDF\Symantec\250-556-7x9nzu.pdf"
    trace_parsing(pdf, 3)
