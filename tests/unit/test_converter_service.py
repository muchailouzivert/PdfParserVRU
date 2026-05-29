def test_convert_docx_to_pdf_limited_calls_converter(monkeypatch):
    import services.converter_service as converter_service

    called = {}

    def fake_convert(docx_path, pdf_dir):
        called["docx_path"] = docx_path
        called["pdf_dir"] = pdf_dir
        return True, "/tmp/out.pdf", ""

    monkeypatch.setattr(converter_service, "PDF_DIR", "/tmp/pdf")
    monkeypatch.setattr(converter_service, "convert_docx_to_pdf_word", fake_convert)

    result = converter_service.convert_docx_to_pdf_limited("/tmp/input.docx")

    assert result == (True, "/tmp/out.pdf", "")
    assert called["docx_path"] == "/tmp/input.docx"
    assert called["pdf_dir"] == "/tmp/pdf"