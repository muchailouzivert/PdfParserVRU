import re

DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4})")

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def safe_filename(name: str, max_len: int = 180) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name[:max_len] if name else "file"
