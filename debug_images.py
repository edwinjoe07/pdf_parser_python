import fitz
import sys
import os

pdf_path = r"d:\coreminds\parsing\pdf-parser-python\uploads\c1f6f5ec-a39d-4a08-a68b-94274cba938c_PL-300-khhiih.pdf"

with open("debug_results.txt", "w", encoding="utf-8") as f:
    if not os.path.exists(pdf_path):
        f.write(f"Error: PDF not found at {pdf_path}\n")
        sys.exit(1)

    doc = fitz.open(pdf_path)

    f.write(f"Opened PDF: {pdf_path}\n")
    f.write(f"Pages: {len(doc)}\n")

    # Inspect pages 4 to 12 (0-indexed: 3 to 11)
    start_page = 3
    end_page = 11

    for page_num in range(start_page, end_page + 1):
        page = doc[page_num]
        f.write(f"\n--- Page {page_num + 1} ---\n")
        
        images = page.get_images(full=True)
        f.write(f"  get_images(full=True) count: {len(images)}\n")
        
        for i, img in enumerate(images):
            xref = img[0]
            try:
                base_image = doc.extract_image(xref)
                width = base_image["width"]
                height = base_image["height"]
                ext = base_image["ext"]
                size = len(base_image["image"])
                f.write(f"    Image {i+1}: xref={xref}, w={width}, h={height}, ext={ext}, size={size} bytes\n")
            except Exception as e:
                f.write(f"    Image {i+1}: xref={xref} FAILED to extract: {e}\n")

        # Also check if there are drawings (vector graphics)
        drawings = page.get_drawings()
        f.write(f"  get_drawings() count: {len(drawings)}\n")
