import math
import os
import requests
from config import CHUNK_API_URL

def get_header_ci(headers: dict, name: str) -> str | None:
    name_l = name.lower()
    for k, v in headers.items():
        if k.lower() == name_l:
            return v
    return None

def download_file_by_chunks(session: requests.Session, file_id: int, out_path: str) -> tuple[bool, str, str]:
    r0 = session.get(
        CHUNK_API_URL,
        headers={"x-file-id": str(file_id), "x-current-chunk": "0"},
        timeout=45
    )
    if r0.status_code != 200:
        return False, "", f"HTTP {r0.status_code}"

    ct = (r0.headers.get("Content-Type") or "").lower()
    if "text/html" in ct:
        return False, "", "HTML (cookies протухли або немає доступу)"

    chunk_size = get_header_ci(r0.headers, "ChunkSize")
    total_size = get_header_ci(r0.headers, "Size")
    mime_type = get_header_ci(r0.headers, "Type") or (r0.headers.get("Content-Type") or "")

    if not chunk_size or not total_size:
        return False, "", f"Нема ChunkSize/Size (headers: {dict(r0.headers)})"

    chunk_size = int(chunk_size)
    total_size = int(total_size)
    total_chunks = math.ceil(total_size / chunk_size)

    tmp_path = out_path + ".part"
    with open(tmp_path, "wb") as f:
        f.write(r0.content)
        for chunk_idx in range(1, total_chunks):
            r = session.get(
                CHUNK_API_URL,
                headers={"x-file-id": str(file_id), "x-current-chunk": str(chunk_idx)},
                timeout=45
            )
            if r.status_code != 200:
                return False, mime_type, f"HTTP {r.status_code} на чанку {chunk_idx}"
            ctt = (r.headers.get("Content-Type") or "").lower()
            if "text/html" in ctt:
                return False, mime_type, f"HTML на чанку {chunk_idx}"
            f.write(r.content)

    os.replace(tmp_path, out_path)
    return True, mime_type, ""
