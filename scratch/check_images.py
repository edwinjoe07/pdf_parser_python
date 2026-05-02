
import fitz

def check_images(pdf_path, page_num):
    doc = fitz.open(pdf_path)
    page = doc[page_num - 1]
    
    print(f"--- Images on Page {page_num} ---")
    img_list = page.get_images()
    print(f"Total images: {len(img_list)}")
    
    for i, img in enumerate(img_list):
        xref = img[0]
        rects = page.get_image_rects(xref)
        if rects:
            print(f"Image {i} (xref {xref}) | Bbox: {[round(x,1) for x in rects[0]]}")
        else:
            print(f"Image {i} (xref {xref}) | No rect found")

if __name__ == "__main__":
    pdf = r"D:\CoreMinds\PDF\Symantec\250-556-7x9nzu.pdf"
    check_images(pdf, 3)
