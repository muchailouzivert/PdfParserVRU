from types import SimpleNamespace
from unittest.mock import Mock

import services.chunk_downloader as chunk_downloader


def make_response(status_code=200, content=b"", headers=None):
    return SimpleNamespace(
        status_code=status_code,
        content=content,
        headers=headers or {},
    )


def test_get_header_ci_finds_header_case_insensitive():
    headers = {
        "Content-Type": "application/pdf",
        "ChunkSize": "10",
        "size": "20",
    }

    assert chunk_downloader.get_header_ci(headers, "content-type") == "application/pdf"
    assert chunk_downloader.get_header_ci(headers, "chunksize") == "10"
    assert chunk_downloader.get_header_ci(headers, "SIZE") == "20"


def test_get_header_ci_returns_none_when_missing():
    assert chunk_downloader.get_header_ci({"A": "1"}, "Missing") is None


def test_download_file_by_chunks_downloads_single_chunk(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.return_value = make_response(
        status_code=200,
        content=b"abc",
        headers={
            "ChunkSize": "10",
            "Size": "3",
            "Type": "application/pdf",
        },
    )

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is True
    assert mime == "application/pdf"
    assert err == ""
    assert out_path.read_bytes() == b"abc"
    assert not (tmp_path / "file.pdf.part").exists()

    session.get.assert_called_once()


def test_download_file_by_chunks_downloads_multiple_chunks(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.side_effect = [
        make_response(
            status_code=200,
            content=b"abc",
            headers={
                "ChunkSize": "3",
                "Size": "8",
                "Type": "application/pdf",
            },
        ),
        make_response(
            status_code=200,
            content=b"def",
            headers={"Content-Type": "application/octet-stream"},
        ),
        make_response(
            status_code=200,
            content=b"gh",
            headers={"Content-Type": "application/octet-stream"},
        ),
    ]

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is True
    assert mime == "application/pdf"
    assert err == ""
    assert out_path.read_bytes() == b"abcdefgh"
    assert session.get.call_count == 3


def test_download_file_by_chunks_fails_on_initial_http_error(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.return_value = make_response(status_code=500)

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is False
    assert mime == ""
    assert err == "HTTP 500"
    assert not out_path.exists()


def test_download_file_by_chunks_fails_on_html_initial_response(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.return_value = make_response(
        status_code=200,
        content=b"<html></html>",
        headers={
            "Content-Type": "text/html",
            "ChunkSize": "10",
            "Size": "10",
        },
    )

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is False
    assert mime == ""
    assert "HTML" in err
    assert not out_path.exists()


def test_download_file_by_chunks_fails_when_required_headers_missing(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.return_value = make_response(
        status_code=200,
        content=b"abc",
        headers={"Content-Type": "application/pdf"},
    )

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is False
    assert mime == ""
    assert "Нема ChunkSize/Size" in err


def test_download_file_by_chunks_fails_on_later_chunk_http_error(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.side_effect = [
        make_response(
            status_code=200,
            content=b"abc",
            headers={
                "ChunkSize": "3",
                "Size": "6",
                "Type": "application/pdf",
            },
        ),
        make_response(status_code=404),
    ]

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is False
    assert mime == "application/pdf"
    assert err == "HTTP 404 на чанку 1"
    assert not out_path.exists()
    assert (tmp_path / "file.pdf.part").exists()


def test_download_file_by_chunks_fails_on_later_chunk_html_response(tmp_path, monkeypatch):
    out_path = tmp_path / "file.pdf"

    session = Mock()
    session.get.side_effect = [
        make_response(
            status_code=200,
            content=b"abc",
            headers={
                "ChunkSize": "3",
                "Size": "6",
                "Type": "application/pdf",
            },
        ),
        make_response(
            status_code=200,
            content=b"<html></html>",
            headers={"Content-Type": "text/html"},
        ),
    ]

    monkeypatch.setattr(chunk_downloader, "CHUNK_API_URL", "https://example.test/chunk")

    ok, mime, err = chunk_downloader.download_file_by_chunks(session, 123, str(out_path))

    assert ok is False
    assert mime == "application/pdf"
    assert err == "HTML на чанку 1"
    assert not out_path.exists()
    assert (tmp_path / "file.pdf.part").exists()