"""Analyze HOTSPOT question structure across PDFs."""
import fitz
import os
import sys

PDF_DIR = r"D:\CoreMinds\PDF"

# Pick a few Microsoft PDFs (known to have HOTSPOT questions)
test_pdfs = [
    os.path.join(PDF_DIR, "Microsoft", "AZ-104-ltqthi.pdf"),
    os.path.join(PDF_DIR, "Microsoft", "AZ-500-5pxwgn.pdf"),
    os.path.join(PDF_DIR, "Microsoft", "SC-300-visxx7.pdf"),
    os.path.join(PDF_DIR, "Microsoft", "MS-102-siwakg.pdf"),
    os.path.join(PDF_DIR, "Microsoft", "PL-300-khhiih.pdf"),
]

for pdf_path in test_pdfs:
    if not os.path.exists(pdf_path):
        print(f"SKIP: {pdf_path} not found")
        continue
    
    print(f"\n{'='*80}")
    print(f"PDF: {os.path.basename(pdf_path)}")
    print(f"{'='*80}")
    
    doc = fitz.open(pdf_path)
    print(f"Total pages: {len(doc)}")
    
    # Search for HOTSPOT/Hot Area mentions
    hotspot_pages = []
    for page_num in range(len(doc)):
        text = doc[page_num].get_text()
        text_upper = text.upper()
        if "HOTSPOT" in text_upper or "HOT AREA" in text_upper:
            hotspot_pages.append(page_num)
    
    print(f"Pages with HOTSPOT/Hot Area: {hotspot_pages[:20]}{'...' if len(hotspot_pages) > 20 else ''}")
    print(f"Total HOTSPOT pages: {len(hotspot_pages)}")
    
    # Show first 3 hotspot pages in detail
    for pg in hotspot_pages[:3]:
        print(f"\n--- Page {pg+1} ---")
        text = doc[pg].get_text()
        print(text[:3000])
        if len(text) > 3000:
            print("... [truncated]")
    
    # Also look at the page AFTER each hotspot page (answer/explanation may be there)
    if hotspot_pages:
        next_pg = hotspot_pages[0] + 1
        if next_pg < len(doc):
            print(f"\n--- Page {next_pg+1} (page after first HOTSPOT) ---")
            text = doc[next_pg].get_text()
            print(text[:3000])
    
    doc.close()
    print()
