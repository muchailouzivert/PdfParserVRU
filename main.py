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
        ext = (comp["ext"] or ".bin").lower()
        title = comp["title"]
        file_name = comp["file_name"] or f"card_{card_id}_comparative_{file_id}"

        # --- кінцевий PDF шлях (ВСІ PDF В ОДНУ ПАПКУ)
        pdf_base = safe_filename(file_name)
        pdf_path = os.path.join(config.PDF_DIR, pdf_base + ".pdf")

        # Якщо PDF вже існує - пропускаємо
        if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0:
            comp_rows.append([
                card_id, reg_num, reg_date.isoformat(),
                file_id, ext, file_name, title,
                "",                  # tmp_path
                "SKIP_EXISTS",       # download_status
                "",                  # mime_type
                pdf_path,            # pdf_path
                "SKIP_ALREADY_PDF"   # convert_status
            ])
            continue

        # --- тимчасовий шлях для завантаження (DOCX/PDF з сервера)
        tmp_name = safe_filename(file_name)
        tmp_path = os.path.join(config.FILES_DIR, tmp_name)
        if not tmp_path.lower().endswith(ext):
            tmp_path += ext

        download_status = ""
        mime = ""
        convert_status = ""
        out_pdf_path = ""

        # качаємо тільки якщо tmp ще нема (або якщо 0 байт)
        need_download = not os.path.exists(tmp_path) or os.path.getsize(tmp_path) == 0

        if need_download:
            ok, mime, err = download_file_by_chunks(sess, file_id, tmp_path)
            download_status = "OK" if ok else f"FAIL: {err}"
            if not ok:
                comp_rows.append([
                    card_id, reg_num, reg_date.isoformat(),
                    file_id, ext, file_name, title,
                    tmp_path, download_status, mime,
                    "", "SKIP_NO_FILE"
                ])
                continue
            downloaded += 1
        else:
            download_status = "SKIP_TMP_EXISTS"
            
        if ext == ".pdf":
            try:
                os.replace(tmp_path, pdf_path)
                out_pdf_path = pdf_path
                convert_status = "SKIP_ALREADY_PDF"
            except Exception as e:
                convert_status = f"FAIL_MOVE_PDF: {e}"

        elif ext == ".docx":
            with config.convert_semaphore:
                conv_ok, out_pdf_path, conv_err = convert_docx_to_pdf_word(tmp_path, config.PDF_DIR)

            convert_status = "OK" if conv_ok else f"FAIL: {conv_err}"

            if conv_ok:
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception as e:
                    convert_status = f"OK_BUT_DOCX_NOT_DELETED: {e}"

        else:
            convert_status = "SKIP_UNSUPPORTED_EXT"

        comp_rows.append([
            card_id, reg_num, reg_date.isoformat(),
            file_id, ext, file_name, title,
            tmp_path, download_status, mime,
            out_pdf_path if out_pdf_path and os.path.exists(out_pdf_path) else "",
            convert_status
        ])

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
    print(f"▶ PDF папка: {config.PDF_DIR}")
    print("ℹ️ Конвертація DOCX->PDF: Microsoft Word (COM/pywin32)")

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
        print(f"✅ Батч {b_start}-{b_end} завершено. state -> {b_end + 1}. matched={total_matched}, downloaded={total_downloaded}")

    print("\nГОТОВО")
    print(f"Біллів у діапазоні дат: {total_matched}")
    print(f"Завантажено порівняльних таблиць: {total_downloaded}")
    print(f"bills.csv: {config.BILLS_CSV}")
    print(f"comparative_tables.csv: {config.COMP_CSV}")
    print(f"passage.csv: {config.PASSAGE_CSV}")
    print(f"state.txt: {config.STATE_FILE}")


if __name__ == "__main__":
    main()
