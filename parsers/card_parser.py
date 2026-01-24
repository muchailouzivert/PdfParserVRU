import re
from datetime import datetime, date
from bs4 import BeautifulSoup
from utils.text import norm, DATE_RE

def parse_date_ua(s: str) -> date | None:
    try:
        return datetime.strptime(s, "%d.%m.%Y").date()
    except Exception:
        return None

def find_row_value(soup: BeautifulSoup, label_contains: str) -> str | None:
    label_contains = label_contains.lower()
    for row in soup.select("div.info div.row"):
        cols = row.find_all("div", class_=re.compile(r"\bcol\b"))
        if len(cols) < 2:
            continue
        left = norm(cols[0].get_text(" ", strip=True)).lower()
        if label_contains in left:
            return norm(cols[1].get_text(" ", strip=True)) or None
    return None

def parse_card(card_html: str) -> dict:
    soup = BeautifulSoup(card_html, "html.parser")

    reg_block = find_row_value(soup, "Номер, дата реєстрації")
    reg_num = None
    reg_date = None
    if reg_block:
        parts = reg_block.split()
        reg_num = parts[0] if parts else None
        m = DATE_RE.search(reg_block)
        reg_date = parse_date_ua(m.group(1)) if m else None

    rubric = find_row_value(soup, "Рубрика законопроекту")
    initiators = find_row_value(soup, "Ініціатор(и) законопроекту")
    main_committee = find_row_value(soup, "Головний комітет")

    comparative = []
    for a in soup.select("a.downloadFile[data-id]"):
        title = norm(a.get_text(" ", strip=True))
        if "порівняльна таблиця" in title.lower():
            file_id = a.get("data-id")
            ext = (a.get("data-ext") or "").strip()
            file_name = (a.get("data-file-name") or "").strip()
            comparative.append({
                "file_id": int(file_id) if file_id and file_id.isdigit() else file_id,
                "ext": ext,
                "file_name": file_name,
                "title": title,
            })

    passage = []
    tbody = soup.select_one("#nav-tab1 table tbody")
    if tbody:
        for tr in tbody.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) >= 2:
                d = norm(tds[0].get_text(" ", strip=True))
                st = norm(tds[1].get_text(" ", strip=True))
                if d and st:
                    passage.append({"date": d, "status": st})

    return {
        "reg_num": reg_num,
        "reg_date": reg_date,
        "rubric": rubric,
        "initiators": initiators,
        "main_committee": main_committee,
        "comparative_tables": comparative,
        "passage": passage,
    }
