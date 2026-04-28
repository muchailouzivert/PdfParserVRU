from pathlib import Path
import fitz
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import os


def extract_text_from_pdf(pdf_path: Path):
    try:
        doc = fitz.open(pdf_path)
        text = "\n".join(page.get_text() for page in doc)
        doc.close()

        if not text.strip():
            return None

        return {
            "doc_id": pdf_path.stem,
            "path": str(pdf_path),
            "text": text,
            "length": len(text.split()),
        }

    except Exception as e:
        print(f"Error {pdf_path.name}: {e}")
        return None


def process_batch(batch_files: list, batch_num: int, total_batches: int) -> list:
    documents = []
    print(f"\nBatch {batch_num}/{total_batches} — {len(batch_files)} files")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(extract_text_from_pdf, p): p for p in batch_files}

        done = 0
        for future in as_completed(futures):
            result = future.result()
            done += 1
            if done % 50 == 0:
                print(f"  {done}/{len(batch_files)} done")
            if result:
                documents.append(result)

    return documents


# ── Constants (defined before main so process_batch can see MAX_WORKERS) ──────
BATCH_SIZE = 200
MAX_WORKERS = max(1, os.cpu_count() - 1)

pdf_dir = Path("../dataset_comparative_2020_2026")
output_file = Path("../dataset_comparative_2020_2026/comparative_files/parsed_pdfs.parquet")


if __name__ == "__main__":

    pdf_files = list(pdf_dir.rglob("*.pdf"))
    print(f"Total PDFs found: {len(pdf_files)}")

    if output_file.exists():
        existing_df = pd.read_parquet(output_file)
        processed_paths = set(existing_df["path"])
        print(f"Already processed: {len(processed_paths)}")
    else:
        existing_df = pd.DataFrame()
        processed_paths = set()
        print("No existing dataset — starting fresh.")

    remaining_files = [p for p in pdf_files if str(p) not in processed_paths]
    print(f"Remaining: {len(remaining_files)} PDFs")

    total_batches = (len(remaining_files) + BATCH_SIZE - 1) // BATCH_SIZE
    new_documents = []

    for batch_num in range(total_batches):
        batch_files = remaining_files[batch_num * BATCH_SIZE:(batch_num + 1) * BATCH_SIZE]
        batch_docs = process_batch(batch_files, batch_num + 1, total_batches)
        new_documents.extend(batch_docs)

        batch_df = pd.DataFrame(new_documents)
        combined_df = pd.concat([existing_df, batch_df], ignore_index=True)
        combined_df.to_parquet(output_file, index=False)
        print(f"Saved {len(combined_df)} total documents")

    print("\nDone!")