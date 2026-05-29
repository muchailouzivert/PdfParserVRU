from pathlib import Path
from unittest.mock import Mock

import fitz
import pytest

from parsers.parse_pdfs import extract_text_from_pdf, process_batch


def create_test_pdf(path: Path, text: str):
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), text)
    doc.save(path)
    doc.close()


def test_extract_text_from_pdf_returns_document_data(tmp_path):
    pdf_path = tmp_path / "test_document.pdf"
    create_test_pdf(pdf_path, "This is a test PDF document for parser validation.")

    result = extract_text_from_pdf(pdf_path)

    assert result is not None
    assert result["doc_id"] == "test_document"
    assert result["path"] == str(pdf_path)
    assert "test PDF document" in result["text"]
    assert result["length"] > 0


def test_extract_text_from_pdf_returns_none_for_empty_pdf(tmp_path):
    pdf_path = tmp_path / "empty.pdf"

    doc = fitz.open()
    doc.new_page()
    doc.save(pdf_path)
    doc.close()

    result = extract_text_from_pdf(pdf_path)

    assert result is None


def test_extract_text_from_pdf_returns_none_for_invalid_file(tmp_path):
    invalid_pdf = tmp_path / "broken.pdf"
    invalid_pdf.write_text("not a real pdf", encoding="utf-8")

    result = extract_text_from_pdf(invalid_pdf)

    assert result is None


def test_process_batch_collects_only_valid_results(monkeypatch):
    fake_files = [
        Path("valid_1.pdf"),
        Path("empty.pdf"),
        Path("valid_2.pdf"),
    ]

    def fake_extract(path):
        if path.name == "empty.pdf":
            return None

        return {
            "doc_id": path.stem,
            "path": str(path),
            "text": "тестовий текст",
            "length": 2,
        }

    class FakeFuture:
        def __init__(self, result):
            self._result = result

        def result(self):
            return self._result

    class FakeExecutor:
        def __init__(self, max_workers=None):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, func, path):
            return FakeFuture(fake_extract(path))

    def fake_as_completed(futures):
        return list(futures.keys())

    monkeypatch.setattr("parsers.parse_pdfs.ProcessPoolExecutor", FakeExecutor)
    monkeypatch.setattr("parsers.parse_pdfs.as_completed", fake_as_completed)

    result = process_batch(fake_files, batch_num=1, total_batches=1)

    assert len(result) == 2
    assert result[0]["doc_id"] == "valid_1"
    assert result[1]["doc_id"] == "valid_2"