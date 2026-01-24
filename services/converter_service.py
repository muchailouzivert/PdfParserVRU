import threading
from config import MAX_CONVERT_WORKERS, PDF_DIR
from services.docx_pdf_converter import convert_docx_to_pdf

_convert_sem = threading.Semaphore(MAX_CONVERT_WORKERS)

def convert_docx_to_pdf_limited(docx_path: str) -> tuple[bool, str, str]:
    with _convert_sem:
        return convert_docx_to_pdf(docx_path, PDF_DIR)
