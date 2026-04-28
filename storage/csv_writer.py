import csv
import os
import threading
from config import BILLS_CSV, COMP_CSV, PASSAGE_CSV

csv_lock = threading.Lock()

def ensure_csv_headers():
    if not os.path.exists(BILLS_CSV):
        with open(BILLS_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "card_id", "reg_num", "reg_date",
                "rubric", "initiators", "main_committee",
                "has_comparative_table"
            ])

    if not os.path.exists(COMP_CSV):
        with open(COMP_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "card_id", "reg_num", "reg_date",
                "file_id", "ext", "file_name", "title",
                "tmp_path", "download_status", "mime_type",
                "pdf_path", "convert_status"
            ])

    if not os.path.exists(PASSAGE_CSV):
        with open(PASSAGE_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow([
                "card_id", "reg_num", "reg_date",
                "status_date", "status_text"
            ])

def append_rows(path: str, rows: list[list]):
    if not rows:
        return
    with csv_lock:
        with open(path, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)
