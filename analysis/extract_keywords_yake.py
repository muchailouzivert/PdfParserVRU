from pathlib import Path
import pandas as pd
import json
import os
import time
import yake
from concurrent.futures import ProcessPoolExecutor, as_completed

BASE_DIR    = Path("../dataset_comparative_2020_2026")
input_path  = BASE_DIR / "comparative_files" / "enriched_docs.parquet"
output_path = BASE_DIR / "comparative_files" / "keywords_yake.parquet"
output_csv  = BASE_DIR / "comparative_files" / "keywords_yake_meta.csv"

BATCH_SIZE  = 200
MAX_WORKERS = max(1, os.cpu_count() - 1)


def num_keywords(word_count: int) -> int:
    return max(5, min(50, word_count // 100))


def extract_yake(args: tuple) -> dict:
    doc_id, text, word_count = args
    k = num_keywords(word_count)

    extractor = yake.KeywordExtractor(
        lan="uk",
        n=2,
        dedupLim=0.7,
        top=k,
    )
    try:
        keywords = extractor.extract_keywords(text)
        kw_list = [kw for kw, score in keywords]
    except Exception as e:
        print(f"Помилка YAKE ({doc_id}): {e}")
        kw_list = []

    return {
        "doc_id":        doc_id,
        "keywords_yake": json.dumps(kw_list, ensure_ascii=False),
        "num_keywords":  len(kw_list),
    }


def process_batch(batch: list, batch_num: int, total_batches: int) -> list:
    results = []
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(extract_yake, args): args[0]
            for args in batch
        }
        for future in as_completed(futures):
            results.append(future.result())
    return results


def main():
    print("Завантажуємо дані...")
    df = pd.read_parquet(input_path)
    print(f"  Документів: {len(df):,}")

    args_list = [
        (row["doc_id"], row["text"], row["length"])
        for _, row in df.iterrows()
    ]

    total_batches = (len(args_list) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"\nВитягуємо ключові слова (YAKE, workers={MAX_WORKERS}, batch_size={BATCH_SIZE})...")

    start_time = time.time()
    results    = []

    for batch_num in range(total_batches):
        batch        = args_list[batch_num * BATCH_SIZE:(batch_num + 1) * BATCH_SIZE]
        batch_results = process_batch(batch, batch_num + 1, total_batches)
        results.extend(batch_results)

        elapsed   = time.time() - start_time
        per_batch = elapsed / (batch_num + 1)
        remaining = per_batch * (total_batches - batch_num - 1)
        print(f"  Батч {batch_num+1:,}/{total_batches:,} — "
              f"{elapsed:.0f}с, ~{remaining/60:.1f}хв залишилось")

    total_time = time.time() - start_time

    print(f"\nЧас виконання ")
    print(f"  Всього: {total_time:.1f}с ({total_time/60:.1f} хв)")
    print(f"  На документ: {total_time/len(args_list):.3f}с")

    kw_df = pd.DataFrame(results)

    meta_cols = ["doc_id", "length", "rubric", "outcome", "num_stages",
                 "main_committee", "card_id", "reg_num", "reg_date"]
    final_df = df[meta_cols].merge(kw_df, on="doc_id", how="left")

    # Зберігаємо час для порівняння з KeyBERT
    timing = {
        "method":    "YAKE",
        "total_s":   round(total_time, 2),
        "total_min": round(total_time / 60, 2),
        "per_doc_s": round(total_time / len(args_list), 4),
        "num_docs":  len(args_list),
    }
    with open(BASE_DIR / "comparative_files" / "yake_timing.json", "w") as f:
        json.dump(timing, f, indent=2)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(output_path, index=False)
    final_df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"\nСередня к-сть KW: {final_df['num_keywords'].mean():.1f}")
    print(f"Мін / Макс KW:    {final_df['num_keywords'].min()} / {final_df['num_keywords'].max()}")
    print(f"\nЗбережено → {output_path}")
    print(f"Збережено → {output_csv}")
    print(f"Час → yake_timing.json")

    sample = final_df.iloc[0]
    print(f"\nПриклад — {sample['doc_id']}:")
    print(f"  KW: {json.loads(sample['keywords_yake'])}")


if __name__ == "__main__":
    main()