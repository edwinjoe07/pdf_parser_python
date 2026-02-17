import fitz
import sys

pdf_path = r"D:\coreminds\AWS (2)\AWS\SOA-C02-mrhln7.pdf"
doc = fitz.open(pdf_path)

with open("debug_results_v2.txt", "w", encoding="utf-8") as out:
    for page_num in [13, 14]:
        page = doc[page_num - 1]
        out.write(f"\n--- Page {page_num} ---\n")
        
        out.write("Images (get_images):\n")
        for img in page.get_images(full=True):
            xref = img[0]
            rects = page.get_image_rects(xref)
            area = 0
            if rects:
                r = rects[0]
                area = (r.x1 - r.x0) * (r.y1 - r.y0)
            out.write(f"  xref={xref}, area={area:.1f}, bbox={rects}\n")
            
        out.write("\nBlocks (get_text):\n")
        d = page.get_text("dict")
        for b in d["blocks"]:
            bbox = b["bbox"]
            if b['type'] == 0:
                text = "".join(span["text"] for line in b.get("lines", []) for span in line.get("spans", []))
                out.write(f"  TEXT area={(bbox[2]-bbox[0])*(bbox[3]-bbox[1]):.1f} bbox={[int(x) for x in bbox]} snippet: {text[:50]}\n")
            else:
                out.write(f"  IMAGE area={(bbox[2]-bbox[0])*(bbox[3]-bbox[1]):.1f} bbox={[int(x) for x in bbox]}\n")
doc.close()
