import fitz
import os

def find_complex_pdfs(root_dir):
    complex_files = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(".pdf"):
                path = os.path.join(root, file)
                try:
                    with fitz.open(path) as doc:
                        # Check first 50 pages for HOTSPOT or DRAG
                        found = False
                        for i in range(min(50, doc.page_count)):
                            text = doc[i].get_text().upper()
                            if "HOTSPOT" in text or "DRAG AND DROP" in text or "SELECT AND PLACE" in text:
                                complex_files.append(path)
                                found = True
                                break
                        if found:
                            print(f"Found complex PDF: {path}")
                except Exception as e:
                    pass
    return complex_files

if __name__ == "__main__":
    find_complex_pdfs(r"D:\CoreMinds\PDF")
