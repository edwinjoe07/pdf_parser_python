
import fitz

def dump_wld_wk_blocks():
    pdf = r"D:\CoreMinds\PDF\Wld wk\GR1-scprnz.pdf"
    doc = fitz.open(pdf)
    page = doc[0]
    
    print("--- RAW TEXT BLOCKS FOR PAGE 1 ---")
    blocks = page.get_text("blocks")
    blocks.sort(key=lambda b: (b[1], b[0]))
    
    for i, b in enumerate(blocks):
        if b[6] == 0:  # Text block
            print(f"Block {i} | Content: {repr(b[4])}")

if __name__ == "__main__":
    dump_wld_wk_blocks()
