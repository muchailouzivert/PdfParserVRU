import os
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import config
from services.http_client import get_session
from parsers.card_parser import parse_card
from services.chunk_downloader import download_file_by_chunks
from services.docx_pdf_converter import convert_docx_to_pdf_word
from storage.csv_writer import ensure_csv_headers, append_rows
from storage.state_store import load_state, save_state
from utils.text import safe_filename

def batched(start: int, end: int, batch_size: int):
    cur = start
    while cur <= end:
        yield cur, min(end, cur + batch_size - 1)
        cur += batch_size

def process_card_id(card_id: int) -> dict:
    sess = get_session()
    time.sleep(random.uniform(config.DELAY_MIN, config.DELAY_MAX))

    try:
        r = sess.get(config.CARD_URL.format(card_id=card_id), timeout=30)
    except Exception:
        return {"ok": False, "skip": True}

    if r.status_code != 200:
        return {"ok": False, "skip": True}

    parsed = parse_card(r.text)

    reg_date = parsed["reg_date"]
    if not reg_date:
        return {"ok": True, "skip": True}

    if reg_date < config.START_DATE or reg_date > config.END_DATE:
        return {"ok": True, "skip": True}

    reg_num = parsed["reg_num"]
    has_comp = 1 if parsed["comparative_tables"] else 0

    bills_rows = [[
        card_id,
        reg_num,
        reg_date.isoformat(),
        parsed["rubric"],
        parsed["initiators"],
        parsed["main_committee"],
        has_comp
    ]]

    passage_rows = []
    for row in parsed["passage"]:
        passage_rows.append([card_id, reg_num, reg_date.isoformat(), row["date"], row["status"]])

    comp_rows = []
    downloaded = 0

    for comp in parsed["comparative_tables"]:
        file_id = comp["file_id"]
        ext = comp["ext"] or ".bin"
        title = comp["title"]
        file_name = comp["file_name"] or f"card_{card_id}_comparative_{file_id}"

        base_name = safe_filename(file_name)
        out_path = os.path.join(config.FILES_DIR, base_name)
        if not out_path.lower().endswith(ext.lower()):
            out_path += ext

        download_status = "SKIP_EXISTS"
        mime = ""
        pdf_path = ""
        convert_status = ""

        # 1) Якщо файл  існує
        if os.path.exists(out_path):
            if ext.lower() == ".pdf":
                pdf_path = out_path
                convert_status = "SKIP_ALREADY_PDF"

            elif ext.lower() == ".docx":
                # Word COM: конвертація
                with config.convert_semaphore: 
                    conv_ok, pdf_path, conv_err = convert_docx_to_pdf_word(out_path, config.PDF_DIR)
                convert_status = "OK" if conv_ok else f"FAIL: {conv_err}"

            else:
                convert_status = "SKIP_NOT_DOCX_OR_PDF"

            comp_rows.append([
                card_id, reg_num, reg_date.isoformat(),
                file_id, ext, file_name, title,
                out_path, download_status, mime,
                pdf_path, convert_status
            ])
            continue

        # 2) Якщо не існує
        ok, mime, err = download_file_by_chunks(sess, file_id, out_path)
        download_status = "OK" if ok else f"FAIL: {err}"

        if ok:
            downloaded += 1

            if ext.lower() == ".pdf":
                pdf_path = out_path
                convert_status = "SKIP_ALREADY_PDF"

            elif ext.lower() == ".docx":
                with config.convert_semaphore:
                    conv_ok, pdf_path, conv_err = convert_docx_to_pdf_word(out_path, config.PDF_DIR)
                convert_status = "OK" if conv_ok else f"FAIL: {conv_err}"

            else:
                convert_status = "SKIP_NOT_DOCX_OR_PDF"

        comp_rows.append([
            card_id, reg_num, reg_date.isoformat(),
            file_id, ext, file_name, title,
            out_path, download_status, mime,
            pdf_path, convert_status
        ])

    # запис у CSV (потокобезпечно всередині append_rows)
    append_rows(config.BILLS_CSV, bills_rows)
    append_rows(config.PASSAGE_CSV, passage_rows)
    append_rows(config.COMP_CSV, comp_rows)

    return {"ok": True, "skip": False, "downloaded": downloaded}

def main():
    config.ensure_dirs()
    ensure_csv_headers()

    start_from = load_state(config.CARD_ID_START)
    print(f"▶ Старт card_id={start_from} .. {config.CARD_ID_END}")
    print(f"▶ Дати: {config.START_DATE.isoformat()} .. {config.END_DATE.isoformat()}")
    print(f"▶ Потоки: {config.MAX_WORKERS}, батч: {config.BATCH_SIZE}, convert_limit: {config.MAX_CONVERT_WORKERS}")
    print(f"▶ Вихід: {config.OUT_DIR}")
    print("ℹКонвертація DOCX->PDF: Microsoft Word (COM/pywin32)")

    total_downloaded = 0
    total_matched = 0

    for b_start, b_end in batched(start_from, config.CARD_ID_END, config.BATCH_SIZE):
        ids = list(range(b_start, b_end + 1))

        with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as ex:
            futures = [ex.submit(process_card_id, cid) for cid in ids]

            for fut in tqdm(as_completed(futures), total=len(futures), desc=f"Batch {b_start}-{b_end}"):
                res = fut.result()
                if res.get("ok") and not res.get("skip"):
                    total_matched += 1
                    total_downloaded += int(res.get("downloaded", 0))

        save_state(b_end + 1)
        print(f"Батч {b_start}-{b_end} завершено. state -> {b_end + 1}. matched={total_matched}, downloaded={total_downloaded}")

    print("\nГОТОВО")
    print(f"Біллів у діапазоні дат: {total_matched}")
    print(f"Завантажено порівняльних таблиць: {total_downloaded}")
    print(f"bills.csv: {config.BILLS_CSV}")
    print(f"comparative_tables.csv: {config.COMP_CSV}")
    print(f"passage.csv: {config.PASSAGE_CSV}")
    print(f"state.txt: {config.STATE_FILE}")


if __name__ == "__main__":
    main()
