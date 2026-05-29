import requests

import services.http_client as http_client


def test_parse_cookie_header_parses_valid_cookies():
    result = http_client.parse_cookie_header("sid=abc123; token=qwerty; theme=dark")

    assert result == {
        "sid": "abc123",
        "token": "qwerty",
        "theme": "dark",
    }


def test_parse_cookie_header_ignores_empty_and_invalid_parts():
    result = http_client.parse_cookie_header("sid=abc123; invalid; ; token=qwerty")

    assert result == {
        "sid": "abc123",
        "token": "qwerty",
    }


def test_parse_cookie_header_keeps_value_with_equals():
    result = http_client.parse_cookie_header("token=a=b=c; sid=123")

    assert result["token"] == "a=b=c"
    assert result["sid"] == "123"


def test_get_session_creates_session_with_headers_and_cookies(monkeypatch):
    if hasattr(http_client.thread_local, "session"):
        delattr(http_client.thread_local, "session")

    monkeypatch.setattr(http_client, "HEADERS_BASE", {"User-Agent": "TestAgent"})
    monkeypatch.setattr(http_client, "COOKIE_HEADER", "sid=abc123; token=qwerty")

    session = http_client.get_session()

    assert isinstance(session, requests.Session)
    assert session.headers["User-Agent"] == "TestAgent"
    assert session.cookies.get("sid") == "abc123"
    assert session.cookies.get("token") == "qwerty"


def test_get_session_reuses_thread_local_session(monkeypatch):
    if hasattr(http_client.thread_local, "session"):
        delattr(http_client.thread_local, "session")

    monkeypatch.setattr(http_client, "HEADERS_BASE", {"User-Agent": "TestAgent"})
    monkeypatch.setattr(http_client, "COOKIE_HEADER", "sid=abc123")

    first = http_client.get_session()
    second = http_client.get_session()

    assert first is second