import os
import random
import fitz  # PyMuPDF

base_dir = r"D:\CoreMinds\PDF"
output_file = r"C:\Users\jeeva\Downloads\pdf_parser_python-main\sample_analysis.txt"

def analyze_pdfs():
    folders = [f for f in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, f))]
    random.seed(42)  # For reproducibility
    sample_folders = random.sample(folders, 5)
    
    with open(output_file, "w", encoding="utf-8") as out:
        for folder in sample_folders:
            folder_path = os.path.join(base_dir, folder)
            pdfs = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]
            if not pdfs:
                continue
            
            sample_pdf = random.choice(pdfs)
            pdf_path = os.path.join(folder_path, sample_pdf)
            
            out.write(f"=== Analyzer Target: {folder}/{sample_pdf} ===\n")
            
            try:
                doc = fitz.open(pdf_path)
                out.write(f"Total Pages: {len(doc)}\n")
                
                # Sample the first 2 pages and a random middle page
                pages_to_sample = [0, 1]
                if len(doc) > 5:
                    pages_to_sample.append(len(doc) // 2)
                    
                for page_num in pages_to_sample:
                    if page_num < len(doc):
                        page = doc[page_num]
                        text = page.get_text("text")
                        out.write(f"--- Page {page_num + 1} ---\n")
                        # Write the first 1000 characters of the page
                        out.write(text[:1500] + "\n...\n")
            except Exception as e:
                out.write(f"Error reading PDF: {e}\n")
            
            out.write("\n" + "="*50 + "\n\n")

if __name__ == "__main__":
    analyze_pdfs()
    print(f"Analysis written to {output_file}")
