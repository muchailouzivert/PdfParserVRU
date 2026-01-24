import threading
import requests

from config import COOKIE_HEADER, HEADERS_BASE

thread_local = threading.local()

def parse_cookie_header(cookie_header: str) -> dict:
    cookies = {}
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()
    return cookies

def get_session() -> requests.Session:
    sess = getattr(thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.headers.update(HEADERS_BASE)
        sess.cookies.update(parse_cookie_header(COOKIE_HEADER))
        thread_local.session = sess
    return sess
