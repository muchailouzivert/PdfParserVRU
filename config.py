import os
from datetime import date
import threading

START_DATE = date(2020, 1, 1)
END_DATE = date(2026, 1, 21)

CARD_ID_START = 1
CARD_ID_END = 67500

MAX_WORKERS = 8
BATCH_SIZE = 500

MAX_CONVERT_WORKERS = 2

BASE = "https://itd.rada.gov.ua"
CARD_URL = BASE + "/billinfo/Bills/Card/{card_id}"
CHUNK_API_URL = BASE + "/billinfo/api/file/download/"

OUT_DIR = "dataset_comparative_2020_2026"
FILES_DIR = os.path.join(OUT_DIR, "comparative_files")
PDF_DIR = os.path.join(OUT_DIR, "comparative_pdfs")
STATE_FILE = os.path.join(OUT_DIR, "state.txt")

BILLS_CSV = os.path.join(OUT_DIR, "bills.csv")
COMP_CSV = os.path.join(OUT_DIR, "comparative_tables.csv")
PASSAGE_CSV = os.path.join(OUT_DIR, "passage.csv")

DELAY_MIN = 0.05
DELAY_MAX = 0.20

COOKIE_HEADER = r"""_ga_R9WGJW46EG=GS2.1.s1761503289$o1$g1$t1761503322$j27$l0$h0; _ga=GA1.1.527722634.1761503289; blok1=empty; _ga_G9VY19PRSD=GS2.1.s1762972281$o2$g0$t1762972284$j57$l0$h0; db=4; serv=10; sid=aa13fc38b-5b76-4413-85a5-927ef4789e2b; blok4=68372; _ga_0X0P16DWWE=GS2.1.s1768907357$o3$g1$t1768907532$j60$l0$h0"""

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}

PDF_DIR = os.path.join(OUT_DIR, "pdf")

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(FILES_DIR, exist_ok=True)
    os.makedirs(PDF_DIR, exist_ok=True)

convert_semaphore = threading.Semaphore(MAX_CONVERT_WORKERS)
