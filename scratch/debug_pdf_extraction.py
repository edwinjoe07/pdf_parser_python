
import fitz
import sys

def debug_pdf(pdf_path, page_num):
    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    
    print(f"--- Raw Text Blocks for Page {page_num} ---")
    blocks = page.get_text("blocks")
    for b in blocks:
        print(f"Bbox: {b[:4]} | Content: {repr(b[4])}")
    
    print("\n--- Dict Format (with Spans) ---")
    page_dict = page.get_text("dict")
    for block in page_dict["blocks"]:
        if block["type"] == 0:
            for line in block["lines"]:
                for span in line["spans"]:
                    print(f"Font: {span['font']} | Size: {span['size']:.1f} | Text: {repr(span['text'])}")

if __name__ == "__main__":
    # Target PDF from screenshot
    pdf = r"D:\CoreMinds\PDF\Symantec\250-556-7x9nzu.pdf"
    debug_pdf(pdf, 3)
