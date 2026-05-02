
import fitz

def debug_full_page(pdf_path, page_num):
    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    
    print(f"--- Full Block Sequence for Page {page_num} ---")
    blocks = page.get_text("blocks")
    # Sort blocks by vertical then horizontal position
    blocks.sort(key=lambda b: (b[1], b[0]))
    
    for i, b in enumerate(blocks):
        print(f"Block {i} | Bbox: {[round(x,1) for x in b[:4]]} | Content: {repr(b[4])}")

if __name__ == "__main__":
    pdf = r"D:\CoreMinds\PDF\Symantec\250-556-7x9nzu.pdf"
    debug_full_page(pdf, 3)
