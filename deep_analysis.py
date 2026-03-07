"""
Deep structural analysis of PDFs from D:\CoreMinds\PDF\
Samples 10 folders, 1 PDF each, extracts pages 1-3 + a middle page fully.
Focus: understand exact line-by-line structure, headers, footers, question anchors,
option formats, answer formats, explanation formats.
"""
import os, random, re, fitz

base_dir = r"D:\CoreMinds\PDF"
out = r"C:\Users\jeeva\Downloads\pdf_parser_python-main\deep_analysis.txt"

random.seed(123)
folders = [f for f in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, f))]
sample_folders = random.sample(folders, min(12, len(folders)))

with open(out, "w", encoding="utf-8") as f:
    for folder in sample_folders:
        folder_path = os.path.join(base_dir, folder)
        pdfs = [p for p in os.listdir(folder_path) if p.lower().endswith(".pdf")]
        if not pdfs:
            continue
        pdf = random.choice(pdfs)
        pdf_path = os.path.join(folder_path, pdf)
        
        f.write(f"\n{'='*80}\n")
        f.write(f"FOLDER: {folder} | FILE: {pdf}\n")
        f.write(f"{'='*80}\n")
        
        try:
            doc = fitz.open(pdf_path)
            total = len(doc)
            f.write(f"Total Pages: {total}\n\n")
            
            # Sample pages: 1, 2, 3, middle, last
            pages = [0, 1, 2]
            if total > 6:
                pages.append(total // 2)
            if total > 3:
                pages.append(3)
            
            for pi in pages:
                if pi >= total:
                    continue
                page = doc[pi]
                text = page.get_text("text")
                f.write(f"--- PAGE {pi+1} (full text) ---\n")
                f.write(text)
                f.write(f"\n--- END PAGE {pi+1} ---\n\n")
                
                # Also dump the dict blocks for page 2 to see structure
                if pi == 1:
                    f.write(f"--- PAGE 2 BLOCK STRUCTURE ---\n")
                    page_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
                    for bi, block in enumerate(page_dict.get("blocks", [])):
                        if block["type"] == 0:
                            lines_text = []
                            for line in block.get("lines", []):
                                lt = "".join(s["text"] for s in line.get("spans", []))
                                lines_text.append(lt)
                            combined = "\n".join(lines_text)
                            f.write(f"  BLOCK[{bi}] type=TEXT bbox={block['bbox']}\n")
                            f.write(f"    content: {repr(combined[:200])}\n")
                        elif block["type"] == 1:
                            f.write(f"  BLOCK[{bi}] type=IMAGE bbox={block.get('bbox','?')}\n")
                    f.write(f"--- END PAGE 2 BLOCK STRUCTURE ---\n\n")
            
            doc.close()
        except Exception as e:
            f.write(f"ERROR: {e}\n")

print(f"Deep analysis written to {out}")
