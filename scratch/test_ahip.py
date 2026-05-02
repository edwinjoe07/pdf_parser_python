
import fitz

def dump_ahip_blocks():
    pdf = r"D:\CoreMinds\PDF\AHIP\AHM-250-00vf5y.pdf"
    doc = fitz.open(pdf)
    page = doc[0]
    
    print("--- RAW TEXT BLOCKS FOR AHIP PAGE 1 ---")
    blocks = page.get_text("blocks")
    blocks.sort(key=lambda b: (b[1], b[0]))
    
    for i, b in enumerate(blocks[:8]):
        if b[6] == 0:  # Text block
            print(f"Block {i} | Content: {repr(b[4])}")

if __name__ == "__main__":
    dump_ahip_blocks()
