
import fitz

def find_page(pdf_path, text):
    doc = fitz.open(pdf_path)
    for i in range(doc.page_count):
        page = doc[i]
        if page.search_for(text):
            print(f"Found '{text}' on page {i+1}")
            return i + 1
    print(f"'{text}' not found")
    return None

if __name__ == "__main__":
    pdf = r"D:\CoreMinds\PDF\Symantec\250-556-7x9nzu.pdf"
    find_page(pdf, "Question: 10")
