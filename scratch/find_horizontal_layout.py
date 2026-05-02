import fitz
import os
from parser.block_extractor import BlockExtractor

def find_horizontal_options(pdf_path):
    extractor = BlockExtractor(image_output_dir="debug_images")
    with fitz.open(pdf_path) as doc:
        for page_num in range(1, min(50, doc.page_count + 1)):
            blocks = extractor.extract_from_page(doc, page_num)
            
            # Group blocks by Y-coordinate (with some tolerance)
            lines = {}
            for b in blocks:
                y_mid = (b.bbox[1] + b.bbox[3]) / 2
                found_line = False
                for y_line in lines:
                    if abs(y_mid - y_line) < 5: # 5 points tolerance
                        lines[y_line].append(b)
                        found_line = True
                        break
                if not found_line:
                    lines[y_mid] = [b]
            
            # Find lines with multiple blocks
            for y_line in sorted(lines.keys()):
                line_blocks = sorted(lines[y_line], key=lambda x: x.bbox[0])
                if len(line_blocks) > 1:
                    has_option = any("A." in b.content or "B." in b.content or "C." in b.content or "D." in b.content for b in line_blocks if b.type == "text")
                    if has_option:
                        print(f"Page {page_num}: Horizontal blocks detected at Y={y_line:.1f}")
                        for b in line_blocks:
                            content_preview = b.content.replace('\n', '\\n')[:30]
                            print(f"  [{b.type}] x0={b.bbox[0]:.1f} content: {content_preview}")

if __name__ == "__main__":
    pdf = r"D:\CoreMinds\PDF\AWS\SOA-C02-mrhln7.pdf"
    find_horizontal_options(pdf)
