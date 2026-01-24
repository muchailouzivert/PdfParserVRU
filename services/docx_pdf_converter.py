# docx_pdf_converter_word.py
import os
import time
from typing import Tuple
import pythoncom
import win32com.client


def convert_docx_to_pdf_word(docx_path: str, pdf_dir: str) -> Tuple[bool, str, str]:
    """
    Конвертація DOCX -> PDF через Microsoft Word (COM).
    Повертає: (ok, pdf_path, error)

    Важливо: для потоків треба pythoncom.CoInitialize() / CoUninitialize().
    """
    docx_path = os.path.abspath(docx_path)
    os.makedirs(pdf_dir, exist_ok=True)

    base = os.path.splitext(os.path.basename(docx_path))[0]
    pdf_path = os.path.join(pdf_dir, base + ".pdf")
    pdf_path_abs = os.path.abspath(pdf_path)

    # якщо PDF вже є і не пустий — можна пропускати
    if os.path.exists(pdf_path_abs) and os.path.getsize(pdf_path_abs) > 0:
        return True, pdf_path_abs, "SKIP_EXISTS"

    word = None
    doc = None

    try:
        pythoncom.CoInitialize()

        word = win32com.client.DispatchEx("Word.Application")
        word.Visible = False
        word.DisplayAlerts = 0  # без діалогів

        doc = word.Documents.Open(docx_path, ReadOnly=True)
        wdFormatPDF = 17

        doc.SaveAs(pdf_path_abs, FileFormat=wdFormatPDF)

        for _ in range(50):
            if os.path.exists(pdf_path_abs) and os.path.getsize(pdf_path_abs) > 0:
                break
            time.sleep(0.1)

        if not os.path.exists(pdf_path_abs) or os.path.getsize(pdf_path_abs) == 0:
            return False, "", "PDF не створився або 0 байт"

        return True, pdf_path_abs, ""

    except Exception as e:
        return False, "", f"Word COM error: {e}"

    finally:
        try:
            if doc is not None:
                doc.Close(False)
        except Exception:
            pass

        try:
            if word is not None:
                word.Quit()
        except Exception:
            pass

        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass
