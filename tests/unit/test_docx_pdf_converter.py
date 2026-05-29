from pathlib import Path
from unittest.mock import Mock

import services.docx_pdf_converter as converter


def test_convert_docx_to_pdf_word_skips_when_pdf_already_exists(tmp_path):
    docx_path = tmp_path / "document.docx"
    pdf_dir = tmp_path / "pdf"
    pdf_dir.mkdir()

    docx_path.write_bytes(b"fake docx")
    existing_pdf = pdf_dir / "document.pdf"
    existing_pdf.write_bytes(b"existing pdf")

    ok, pdf_path, err = converter.convert_docx_to_pdf_word(str(docx_path), str(pdf_dir))

    assert ok is True
    assert Path(pdf_path).name == "document.pdf"
    assert err == "SKIP_EXISTS"


def test_convert_docx_to_pdf_word_success(monkeypatch, tmp_path):
    docx_path = tmp_path / "document.docx"
    pdf_dir = tmp_path / "pdf"
    docx_path.write_bytes(b"fake docx")

    created_pdf_holder = {}

    class FakeDoc:
        def SaveAs(self, pdf_path, FileFormat):
            created_pdf_holder["path"] = pdf_path
            Path(pdf_path).write_bytes(b"pdf content")

        def Close(self, save_changes):
            created_pdf_holder["closed"] = True

    class FakeDocuments:
        def Open(self, path, ReadOnly=True):
            assert path == str(docx_path.resolve())
            return FakeDoc()

    class FakeWord:
        def __init__(self):
            self.Visible = True
            self.DisplayAlerts = 1
            self.Documents = FakeDocuments()

        def Quit(self):
            created_pdf_holder["quit"] = True

    fake_pythoncom = Mock()
    fake_win32com = Mock()
    fake_win32com.client.DispatchEx.return_value = FakeWord()

    monkeypatch.setattr(converter, "pythoncom", fake_pythoncom)
    monkeypatch.setattr(converter, "win32com", fake_win32com)

    ok, pdf_path, err = converter.convert_docx_to_pdf_word(str(docx_path), str(pdf_dir))

    assert ok is True
    assert Path(pdf_path).exists()
    assert Path(pdf_path).read_bytes() == b"pdf content"
    assert err == ""
    fake_pythoncom.CoInitialize.assert_called_once()
    fake_pythoncom.CoUninitialize.assert_called_once()
    fake_win32com.client.DispatchEx.assert_called_once_with("Word.Application")
    assert created_pdf_holder["closed"] is True
    assert created_pdf_holder["quit"] is True


def test_convert_docx_to_pdf_word_returns_error_on_com_exception(monkeypatch, tmp_path):
    docx_path = tmp_path / "document.docx"
    pdf_dir = tmp_path / "pdf"
    docx_path.write_bytes(b"fake docx")

    fake_pythoncom = Mock()
    fake_win32com = Mock()
    fake_win32com.client.DispatchEx.side_effect = Exception("Word is not available")

    monkeypatch.setattr(converter, "pythoncom", fake_pythoncom)
    monkeypatch.setattr(converter, "win32com", fake_win32com)

    ok, pdf_path, err = converter.convert_docx_to_pdf_word(str(docx_path), str(pdf_dir))

    assert ok is False
    assert pdf_path == ""
    assert "Word COM error" in err
    fake_pythoncom.CoInitialize.assert_called_once()
    fake_pythoncom.CoUninitialize.assert_called_once()


def test_convert_docx_to_pdf_word_returns_error_when_pdf_not_created(monkeypatch, tmp_path):
    docx_path = tmp_path / "document.docx"
    pdf_dir = tmp_path / "pdf"
    docx_path.write_bytes(b"fake docx")

    class FakeDoc:
        def SaveAs(self, pdf_path, FileFormat):
            pass

        def Close(self, save_changes):
            pass

    class FakeDocuments:
        def Open(self, path, ReadOnly=True):
            return FakeDoc()

    class FakeWord:
        def __init__(self):
            self.Visible = True
            self.DisplayAlerts = 1
            self.Documents = FakeDocuments()

        def Quit(self):
            pass

    fake_pythoncom = Mock()
    fake_win32com = Mock()
    fake_win32com.client.DispatchEx.return_value = FakeWord()

    monkeypatch.setattr(converter, "pythoncom", fake_pythoncom)
    monkeypatch.setattr(converter, "win32com", fake_win32com)
    monkeypatch.setattr(converter.time, "sleep", lambda _: None)

    ok, pdf_path, err = converter.convert_docx_to_pdf_word(str(docx_path), str(pdf_dir))

    assert ok is False
    assert pdf_path == ""
    assert err == "PDF не створився або 0 байт"