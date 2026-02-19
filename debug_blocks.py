import fitz
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

pdf_path = r"D:\coreminds\AWS (2)\AWS\SOA-C02-mrhln7.pdf"
doc = fitz.open(pdf_path)

for page_num in [13, 14]:
    page = doc[page_num - 1]
    print(f"\n--- Page {page_num} ---")
    
    print("Images (get_images):")
    for img in page.get_images(full=True):
        xref = img[0]
        rects = page.get_image_rects(xref)
        area = 0
        if rects:
            r = rects[0]
            area = (r.x1 - r.x0) * (r.y1 - r.y0)
        print(f"  xref={xref}, area={area:.1f}, bbox={rects}")
        
    print("\nBlocks (get_text):")
    d = page.get_text("dict")
    for b in d["blocks"]:
        bbox = b["bbox"]
        if b['type'] == 0:
            text = "".join(l["spans"][0]["text"] for l in b["lines"] if l["spans"])
            print(f"  TEXT area={(bbox[2]-bbox[0])*(bbox[3]-bbox[1]):.1f} bbox={[int(x) for x in bbox]} snippet: {text[:30]}...")
        else:
            print(f"  IMAGE area={(bbox[2]-bbox[0])*(bbox[3]-bbox[1]):.1f} bbox={[int(x) for x in bbox]}")
doc.close()
